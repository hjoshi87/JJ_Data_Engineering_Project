"""
Manufacturing Maintenance Analytics ETL Pipeline

This module transforms raw manufacturing and maintenance data into 
analytics-ready fact tables. It implements a modular, testable design 
with comprehensive data quality checks.

Production Considerations:
- All timestamps are converted to UTC (source timezone: Europe/Paris)
- Null handling is explicit - no silent drops
- All transformations are idempotent (safe to re-run)
- Pipeline logs all quality checks and volumes for monitoring
"""

import logging
from typing import Dict, Tuple
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    to_timestamp, to_utc_timestamp, coalesce, split, trim, upper,
    dense_rank, row_number, datediff, date_format, lit, concat, size, regexp_extract, length,
    to_json
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType, ArrayType, MapType
)


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configure logging for the ETL pipeline.
    
    Args:
        log_level: Logging level (INFO, DEBUG, WARNING, ERROR)
    
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('manufacturing_etl.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# ============================================================================
# SPARK SESSION INITIALIZATION
# ============================================================================

def create_spark_session(app_name: str = "ManufacturingETL") -> SparkSession:
    """
    Initialize Spark session with Windows-friendly configuration.
    
    Configurations:
    - Memory: 2GB driver, 1GB per executor (adjust for cluster size)
    - Partitions: Auto-determined by Spark  
    - Shuffle: Balanced for moderate data volumes
    
    Args:
        app_name: Name for Spark application
    
    Returns:
        Initialized SparkSession
    """
    import os
    
    # Ensure JAVA_HOME is set
    if 'JAVA_HOME' not in os.environ:
        logger.warning("JAVA_HOME not set. Spark may fail.")
    
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.repl.local.exec.bind", "true") \
            .master("local[*]") \
            .getOrCreate()
        
        logger.info(f"Spark session created: {app_name}")
        logger.info(f"Spark version: {spark.version}")
        
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def read_csv_with_validation(
    spark: SparkSession,
    file_path: str,
    expected_row_count_range: Tuple[int, int] = None,
    schema: StructType = None
) -> DataFrame:
    """
    Read CSV file with schema validation and quality checks.
    
    Args:
        spark: SparkSession instance
        file_path: Path to CSV file
        expected_row_count_range: Tuple of (min, max) expected rows for validation
        schema: Optional explicit schema (inferred if not provided)
    
    Returns:
        Spark DataFrame with validated data
    
    Raises:
        ValueError: If row count validation fails or file not found
    """
    try:
        # Build reader with proper schema handling
        reader = spark.read.option("header", "true")
        
        if schema is not None:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")
        
        df = reader.csv(file_path)
        
        row_count = df.count()
        logger.info(f"Read {file_path}: {row_count} rows, {len(df.columns)} columns")
        
        # Validate row count if provided
        if expected_row_count_range:
            min_count, max_count = expected_row_count_range
            if not (min_count <= row_count <= max_count):
                raise ValueError(
                    f"Row count {row_count} outside expected range "
                    f"[{min_count}, {max_count}]"
                )
            logger.info(f"[OK] Row count validation passed")
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to read CSV {file_path}: {str(e)}")
        raise


# ============================================================================
# DATA VALIDATION FUNCTIONS
# ============================================================================

def validate_maintenance_data(df: DataFrame) -> Dict[str, any]:
    """
    Comprehensive validation of maintenance events data.
    
    Checks:
    - No duplicate event IDs
    - Timestamp consistency (end >= start)
    - Cost and downtime are non-negative
    - Critical columns are populated
    
    Returns:
        Dictionary with validation results and metrics
    """
    validation_report = {
        'table': 'maintenance_events',
        'total_rows': df.count(),
        'passed': True,
        'issues': []
    }
    
    # Check 1: Duplicate event IDs
    duplicate_count = df.groupBy('event_id').count().filter(
        col('count') > 1
    ).count()
    if duplicate_count > 0:
        validation_report['issues'].append(
            f"Found {duplicate_count} duplicate event_ids"
        )
        validation_report['passed'] = False
    else:
        logger.info("[OK] No duplicate event IDs detected")
    
    # Check 2: Timestamp consistency
    timestamp_invalid = df.filter(
        col('end_time') < col('start_time')
    ).count()
    if timestamp_invalid > 0:
        validation_report['issues'].append(
            f"Found {timestamp_invalid} events with end_time < start_time"
        )
        validation_report['passed'] = False
    else:
        logger.info("[OK] Timestamp consistency validated")
    
    # Check 3: Negative values
    negative_cost = df.filter(col('cost_eur') < 0).count()
    negative_downtime = df.filter(col('downtime_min') < 0).count()
    
    if negative_cost > 0:
        validation_report['issues'].append(
            f"Found {negative_cost} events with negative cost"
        )
    if negative_downtime > 0:
        validation_report['issues'].append(
            f"Found {negative_downtime} events with negative downtime"
        )
    
    if negative_cost == 0 and negative_downtime == 0:
        logger.info("[OK] No negative values detected")
    
    # Check 4: Critical column nulls
    null_counts = {
        col_name: df.filter(col(col_name).isNull()).count()
        for col_name in ['event_id', 'start_time', 'end_time', 'downtime_min']
    }
    
    critical_nulls = {k: v for k, v in null_counts.items() if v > 0}
    if critical_nulls:
        validation_report['issues'].append(
            f"Found nulls in critical columns: {critical_nulls}"
        )
        validation_report['passed'] = False
    else:
        logger.info("[OK] No nulls in critical columns")
    
    # Log summary
    logger.info(f"Maintenance validation: {validation_report['passed']}")
    if not validation_report['passed']:
        logger.warning(f"Issues found: {validation_report['issues']}")
    
    return validation_report


def validate_factory_data(df: DataFrame) -> Dict[str, any]:
    """Validate manufacturing factory dataset."""
    validation_report = {
        'table': 'manufacturing_factory',
        'total_rows': df.count(),
        'passed': True,
        'issues': []
    }
    
    # Check OEE components (should be 0-1)
    for oee_col in ['availability', 'performance', 'quality']:
        invalid_oee = df.filter(
            (col(oee_col) < 0) | (col(oee_col) > 1)
        ).count()
        
        if invalid_oee > 0:
            validation_report['issues'].append(
                f"Found {invalid_oee} rows with {oee_col} outside [0, 1]"
            )
            validation_report['passed'] = False
    
    if validation_report['passed']:
        logger.info("[OK] OEE components within valid range")
    
    # Check produced <= planned
    qty_invalid = df.filter(col('produced_qty') > col('planned_qty')).count()
    if qty_invalid > 0:
        validation_report['issues'].append(
            f"Found {qty_invalid} rows where produced_qty > planned_qty"
        )
    
    logger.info(f"Factory data validation: {validation_report['passed']}")
    return validation_report


def validate_operators_data(df: DataFrame) -> Dict[str, any]:
    """Validate operators roster data."""
    validation_report = {
        'table': 'operators',
        'total_rows': df.count(),
        'passed': True,
        'issues': []
    }
    
    # Check unique operator IDs
    duplicate_ops = df.groupBy('operator_id').count().filter(
        col('count') > 1
    ).count()
    
    if duplicate_ops > 0:
        validation_report['issues'].append(
            f"Found {duplicate_ops} duplicate operator IDs"
        )
        validation_report['passed'] = False
    
    # Check reliability score range (should be 0-100)
    invalid_score = df.filter(
        (col('reliability_score') < 0) | (col('reliability_score') > 100)
    ).count()
    
    if invalid_score > 0:
        validation_report['issues'].append(
            f"Found {invalid_score} invalid reliability scores"
        )
    
    logger.info(f"Operators validation: {validation_report['passed']}")
    return validation_report


# ============================================================================
# DATA TRANSFORMATION FUNCTIONS
# ============================================================================

def clean_maintenance_events(df: DataFrame) -> DataFrame:
    """
    Clean and standardize maintenance events data.
    
    Transformations:
    1. Parse timestamps and convert to UTC (source: Europe/Paris)
    2. Handle parts_used field (comma-separated string → array)
    3. Classify events as planned vs unplanned
    4. Flag data quality issues
    5. Trim whitespace from string columns
    
    Returns:
        Cleaned DataFrame
    """
    df_clean = df
    
    # Transformation 1: Timestamp parsing and UTC conversion
    df_clean = df_clean.withColumn(
        'start_time_utc',
        to_utc_timestamp(
            to_timestamp(col('start_time'), 'yyyy-MM-dd HH:mm:ss'),
            'Europe/Paris'
        )
    ).withColumn(
        'end_time_utc',
        to_utc_timestamp(
            to_timestamp(col('end_time'), 'yyyy-MM-dd HH:mm:ss'),
            'Europe/Paris'
        )
    )
    
    logger.info("[OK] Timestamps converted to UTC")
    
    # Transformation 2: Parse parts_used (comma-separated to array)
    df_clean = df_clean.withColumn(
        'parts_list',
        when(
            (col('parts_used').isNull()) | (col('parts_used') == ''),
            None
        ).otherwise(
            split(trim(col('parts_used')), ',')
        )
    ).withColumn(
        'parts_count',
        when(col('parts_list').isNull(), 0).otherwise(
            size(col('parts_list'))
        )
    )
    
    logger.info("[OK] Parts parsed and counted")
    
    # Transformation 3: Classify maintenance events
    df_clean = df_clean.withColumn(
        'is_unplanned',
        col('reason') == 'Unplanned Breakdown'
    ).withColumn(
        'downtime_category',
        when(col('downtime_min') == 0, lit('monitoring')).otherwise(lit('downtime'))
    )
    
    logger.info("[OK] Maintenance events classified")
    
    # Transformation 4: Flag data quality issues
    df_clean = df_clean.withColumn(
        'data_quality_flags',
        when(col('cost_eur').isNull(), lit('missing_cost')).otherwise(lit(''))
    ).withColumn(
        'data_quality_flags',
        when(
            col('parts_used').isNull() | (col('parts_used') == ''),
            concat(col('data_quality_flags'), lit('missing_parts'))
        ).otherwise(col('data_quality_flags'))
    )
    
    logger.info("[OK] Data quality flags added")
    
    # Transformation 5: Trim whitespace from string columns
    string_cols = [field.name for field in df_clean.schema.fields if str(field.dataType) == 'StringType']
    for col_name in string_cols:
        df_clean = df_clean.withColumn(col_name, trim(col(col_name)))
    
    logger.info("[OK] Whitespace trimmed")
    
    return df_clean


def enrich_maintenance_with_operators(
    df_maintenance: DataFrame,
    df_operators: DataFrame
) -> DataFrame:
    """
    Enrich maintenance events with operator details.
    
    Uses LEFT JOIN to preserve all maintenance events.
    Drops duplicate columns from operators table.
    
    Returns:
        Enriched DataFrame
    """
    # Drop columns from operators that don't add value
    df_ops_clean = df_operators.select(
        col('operator_id'),
        col('skill_level'),
        col('reliability_score')
    )
    
    # Join on technician_id = operator_id
    df_enriched = df_maintenance.join(
        df_ops_clean,
        df_maintenance.technician_id == df_ops_clean.operator_id,
        'left'
    )
    
    # Add flag for operator not found
    df_enriched = df_enriched.withColumn(
        'technician_found',
        when(col('operator_id').isNull(), False).otherwise(True)
    )
    
    # For external contractors, assign default values
    df_enriched = df_enriched.withColumn(
        'operator_skill_level',
        coalesce(col('skill_level'), lit('Unknown'))
    ).withColumn(
        'operator_reliability',
        coalesce(col('reliability_score'), lit(50.0))
    )
    
    # Drop the temporary columns (skill_level, reliability_score, operator_id)
    df_enriched = df_enriched.drop('skill_level', 'reliability_score', 'operator_id')
    
    logger.info("[OK] Maintenance events enriched with operator data")
    
    return df_enriched


def create_fact_maintenance(df: DataFrame) -> DataFrame:
    """
    Create analytics-ready fact table for maintenance events.
    Dimensions:
    - When: start_time_utc, end_time_utc, date_key
    - Where: factory_id, line_id
    - What: maintenance_type, reason, outcome
    - Who: technician_id, operator details
    
    Facts:
    - downtime_min, cost_eur
    - cost_per_downtime_min (for efficiency metrics)
    - parts_count
    - severity_level
    - quality_flags
    
    Grain: One row per maintenance event
    """
    fact_table = df.select(
        # Keys
        col('event_id').alias('maint_event_id'),
        col('factory_id'),
        col('line_id'),
        col('start_time_utc').alias('start_timestamp'),
        col('end_time_utc').alias('end_timestamp'),
        date_format(col('start_time_utc'), 'yyyy-MM-dd').alias('event_date'),
        date_format(col('start_time_utc'), 'yyyy-MM').alias('event_year_month'),
        
        # Maintenance Details
        col('maintenance_type'),
        col('reason'),
        col('outcome'),
        col('is_unplanned').alias('is_unplanned_breakdown'),
        col('downtime_category'),
        
        # Facts
        col('downtime_min'),
        col('cost_eur'),
        when(
            col('downtime_min') > 0,
            (col('cost_eur') / col('downtime_min')).cast('decimal(10,2)')
        ).otherwise(lit(0)).alias('cost_per_downtime_min'),
        
        # Severity Classification
        when(
            (col('cost_eur') > 2500) | (col('downtime_min') > 120),
            lit('High')
        ).when(
            (col('cost_eur') > 1500) | (col('downtime_min') > 60),
            lit('Medium')
        ).otherwise(lit('Low')).alias('severity_level'),
        
        # Resources
        col('technician_id'),
        col('operator_skill_level'),
        col('operator_reliability'),
        col('parts_count'),
        col('parts_list'),
        
        # Scheduling
        col('next_due_date'),
        datediff(
            col('next_due_date'),
            date_format(col('start_time_utc'), 'yyyy-MM-dd')
        ).alias('days_to_next_due'),
        
        # Quality Flags
        col('data_quality_flags'),
        col('technician_found').alias('technician_in_roster'),
        
        # Metadata
        lit(datetime.now()).alias('load_timestamp')
    )
    
    logger.info("[OK] Fact table created with all dimensions and facts")
    
    return fact_table


# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def create_maintenance_summary(df: DataFrame) -> DataFrame:
    """
    Create summary metrics for monitoring and dashboards.
    
    Aggregations:
    - By factory, line, maintenance type
    - Cost and downtime metrics
    - Event counts and rates
    """
    summary = df.groupBy(
        col('factory_id'),
        col('line_id'),
        col('maintenance_type'),
        col('event_date')
    ).agg(
        count(lit(1)).alias('event_count'),
        spark_sum('downtime_min').alias('total_downtime_min'),
        avg('downtime_min').alias('avg_downtime_min'),
        spark_sum('cost_eur').alias('total_cost_eur'),
        avg('cost_eur').alias('avg_cost_eur'),
        spark_sum(
            when(col('is_unplanned_breakdown') == True, 1).otherwise(0)
        ).alias('unplanned_count')
    ).withColumn(
        'unplanned_pct',
        (col('unplanned_count') / col('event_count') * 100).cast('decimal(5,2)')
    )
    
    logger.info("[OK] Maintenance summary aggregations created")
    
    return summary


# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def export_dataframe(
    df: DataFrame,
    output_path: str,
    format_type: str = 'parquet',
    mode: str = 'overwrite'
) -> None:
    """
    Export DataFrame to storage with proper complex type serialization for CSV.
    
    Supported formats:
    - 'parquet': Columnar, compressed, schema-preserving (recommended)
    - 'csv': Text format, universal compatibility (with JSON serialization)
    
    Critical: Complex columns (Array, Map, Struct, Timestamp) are converted to
    JSON/string format for CSV to prevent data corruption.
    
    Args:
        df: DataFrame to export
        output_path: Output file path
        format_type: 'parquet' or 'csv'
        mode: 'overwrite', 'append', 'ignore', 'error'
    """
    from pyspark.sql.types import ArrayType, MapType, StructType, TimestampType
    from pyspark.sql.functions import to_json, col, date_format
    
    # ========================================================================
    # CRITICAL: Prepare DataFrame for CSV export
    # ========================================================================
    def prepare_for_csv(df_to_prep: DataFrame) -> DataFrame:
        """
        Convert complex data types to CSV-safe string representations.
        
        Conversions:
        1. ArrayType (e.g., parts_list) → JSON array string
           Example: ["PART-001", "PART-002"] → '["PART-001","PART-002"]'
           
        2. MapType → JSON object string
           Example: {key1: val1} → '{"key1":"val1"}'
           
        3. StructType → JSON object string
           Example: {a: 1, b: 2} → '{"a":1,"b":2}'
           
        4. TimestampType (load_timestamp) → ISO format string
           Example: 2025-01-18T15:30:45.123 → '2025-01-18 15:30:45'
        """
        
        # Step 1: Convert Array, Map, and Struct columns to JSON strings
        # These columns will be scrambled in CSV unless converted
        complex_cols = [
            f.name for f in df_to_prep.schema.fields 
            if isinstance(f.dataType, (ArrayType, MapType, StructType))
        ]
        
        logger.info(f"[INFO] Converting {len(complex_cols)} complex columns to JSON for CSV")
        logger.info(f"       Complex columns: {complex_cols}")
        
        for col_name in complex_cols:
            # to_json() converts arrays/maps/structs to JSON string representation
            # This preserves the data structure in a readable format
            df_to_prep = df_to_prep.withColumn(col_name, to_json(col(col_name)))
            logger.info(f"       ✓ {col_name}: converted to JSON string")
        
        # Step 2: Convert Timestamp columns to ISO format strings
        # Timestamp objects don't serialize well to CSV
        timestamp_cols = [
            f.name for f in df_to_prep.schema.fields 
            if isinstance(f.dataType, TimestampType)
        ]
        
        logger.info(f"[INFO] Converting {len(timestamp_cols)} timestamp columns to strings")
        
        for col_name in timestamp_cols:
            # date_format() converts timestamp to human-readable string
            # Format: 'yyyy-MM-dd HH:mm:ss' is readable and sortable
            df_to_prep = df_to_prep.withColumn(
                col_name, 
                date_format(col(col_name), 'yyyy-MM-dd HH:mm:ss')
            )
            logger.info(f"       ✓ {col_name}: converted to ISO format string")
        
        logger.info("[OK] DataFrame prepared for CSV export")
        return df_to_prep
    
    # ========================================================================
    # EXPORT LOGIC
    # ========================================================================
    try:
        if format_type.lower() == 'parquet':
            try:
                # Try Parquet first (preferred format)
                logger.info(f"[INFO] Attempting Parquet export: {output_path}")
                
                df.coalesce(1).write \
                    .mode(mode) \
                    .format('parquet') \
                    .save(output_path)
                
                logger.info(f"[OK] Exported to Parquet: {output_path}")
                return
            
            except Exception as parquet_error:
                # Parquet failed (likely Hadoop issue on Windows)
                parquet_error_msg = str(parquet_error)
                
                if 'HADOOP_HOME' in parquet_error_msg or 'winutils' in parquet_error_msg:
                    logger.warning(f"[WARNING] Parquet export failed (Hadoop not available)")
                    logger.warning(f"[INFO] Falling back to CSV format")
                    
                    # Convert to CSV-safe format
                    fallback_path = output_path.replace('.parquet', '.csv')
                    csv_ready_df = prepare_for_csv(df)
                    
                    # Write CSV with header
                    csv_ready_df.coalesce(1).write \
                        .mode(mode) \
                        .format('csv') \
                        .option('header', 'true') \
                        .option('quote', '"') \
                        .option('escape', '"') \
                        .option('quoteAll', 'true') \
                        .save(fallback_path)
                    
                    logger.info(f"[OK] Exported to CSV (fallback): {fallback_path}")
                    return
                else:
                    # Some other error - re-raise
                    raise
        
        elif format_type.lower() == 'csv':
            logger.info(f"[INFO] Exporting to CSV: {output_path}")
            
            # Prepare the DataFrame for CSV (convert complex types)
            csv_ready_df = prepare_for_csv(df)
            
            # Write CSV with header
            csv_ready_df.coalesce(1).write \
                .mode(mode) \
                .format('csv') \
                .option('header', 'true') \
                .option('quote', '"') \
                .option('escape', '"') \
                .option('quoteAll', 'true') \
                .save(output_path)
            
            logger.info(f"[OK] Exported to CSV: {output_path}")
        
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    except Exception as e:
        logger.error(f"[ERROR] Failed to export {output_path}: {str(e)}")
        raise


# ============================================================================
# MAIN PIPELINE ORCHESTRATION
# ============================================================================

def run_etl_pipeline(
    input_dir: str,
    output_dir: str,
    config: Dict = None
) -> Dict[str, any]:
    """
    Execute complete ETL pipeline.
    
    Args:
        input_dir: Directory containing input CSV files
        output_dir: Directory for output files
        config: Optional configuration overrides
    
    Returns:
        Pipeline execution report with metrics and status
    """
    config = config or {}
    start_time = datetime.now()
    report = {
        'pipeline_status': 'Running',
        'start_time': start_time.isoformat(),
        'validations': {},
        'row_counts': {},
        'errors': []
    }
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info("="*70)
        logger.info("STARTING MANUFACTURING ETL PIPELINE")
        logger.info("="*70)
        
        # ========== EXTRACTION ==========
        logger.info("\n[1/4] EXTRACTION PHASE")
        logger.info("-" * 70)
        
        df_maintenance = read_csv_with_validation(
            spark,
            f"{input_dir}/maintenance_events.csv",
            expected_row_count_range=(90, 100)
        )
        report['row_counts']['maintenance_raw'] = df_maintenance.count()
        
        df_factory = read_csv_with_validation(
            spark,
            f"{input_dir}/manufacturing_factory_dataset.csv",
            expected_row_count_range=(2000, 2100)
        )
        report['row_counts']['factory_raw'] = df_factory.count()
        
        df_operators = read_csv_with_validation(
            spark,
            f"{input_dir}/operators_roster.csv",
            expected_row_count_range=(18, 22)
        )
        report['row_counts']['operators_raw'] = df_operators.count()
        
        # ========== VALIDATION ==========
        logger.info("\n[2/4] VALIDATION PHASE")
        logger.info("-" * 70)
        
        report['validations']['maintenance'] = validate_maintenance_data(df_maintenance)
        report['validations']['factory'] = validate_factory_data(df_factory)
        report['validations']['operators'] = validate_operators_data(df_operators)
        
        # Check if validations passed
        all_valid = all(
            v.get('passed', True)
            for v in report['validations'].values()
        )
        
        if not all_valid:
            logger.warning("Some validations failed - review carefully")
        
        # ========== TRANSFORMATION ==========
        logger.info("\n[3/4] TRANSFORMATION PHASE")
        logger.info("-" * 70)
        
        # Clean maintenance data
        df_maintenance_clean = clean_maintenance_events(df_maintenance)
        
        # Enrich with operator data
        df_maintenance_enriched = enrich_maintenance_with_operators(
            df_maintenance_clean,
            df_operators
        )
        
        # Create fact table
        fact_maintenance = create_fact_maintenance(df_maintenance_enriched)
        report['row_counts']['fact_maintenance'] = fact_maintenance.count()
        
        # Create summary metrics
        summary_maintenance = create_maintenance_summary(fact_maintenance)
        
        logger.info(f"[OK] Transformed {report['row_counts']['fact_maintenance']} "
                   f"maintenance events")
        
        # ========== EXPORT ==========
        logger.info("\n[4/4] EXPORT PHASE")
        logger.info("-" * 70)
        
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Export fact table in both formats
        export_dataframe(
            fact_maintenance,
            f"{output_dir}/fact_maintenance_events.parquet",
            format_type='parquet'
        )
        
        export_dataframe(
            fact_maintenance,
            f"{output_dir}/fact_maintenance_events.csv",
            format_type='csv'
        )
        
        # Export summary
        export_dataframe(
            summary_maintenance,
            f"{output_dir}/summary_maintenance_metrics.parquet",
            format_type='parquet'
        )
        
        export_dataframe(
            summary_maintenance,
            f"{output_dir}/summary_maintenance_metrics.csv",
            format_type='csv'
        )
        
        logger.info(f"[OK] All outputs exported to {output_dir}")
        
        # ========== COMPLETION ==========
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        
        report['pipeline_status'] = 'Success'
        report['end_time'] = end_time.isoformat()
        report['duration_seconds'] = duration_seconds
        
        logger.info("\n" + "="*70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"Duration: {duration_seconds:.2f} seconds")
        logger.info(f"Output directory: {output_dir}")
        
        spark.stop()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        report['pipeline_status'] = 'Failed'
        report['errors'].append(str(e))
    
    return report


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Configuration
    INPUT_DIRECTORY = "./data/raw"
    OUTPUT_DIRECTORY = "./data/processed"
    
    # Run pipeline
    execution_report = run_etl_pipeline(
        input_dir=INPUT_DIRECTORY,
        output_dir=OUTPUT_DIRECTORY
    )
    
    # Print summary
    print("\n" + "="*70)
    print("PIPELINE EXECUTION REPORT")
    print("="*70)
    print(f"Status: {execution_report['pipeline_status']}")
    print(f"Duration: {execution_report.get('duration_seconds', 'N/A')} seconds")
    print(f"\nRow Counts:")
    for key, count in execution_report['row_counts'].items():
        print(f"  {key}: {count}")
    
    if execution_report['errors']:
        print(f"\nErrors: {execution_report['errors']}")