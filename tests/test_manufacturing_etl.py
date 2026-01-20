"""
Unit tests for Manufacturing ETL Pipeline

Test Coverage:
- Data extraction and validation
- Transformation logic
- Edge case handling
- Output quality

Run with: pytest test_manufacturing_etl.py -v
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from pyspark.sql.functions import col, lit

# Import functions from pipeline
from manufacturing_etl_pipeline import (
    validate_maintenance_data,
    validate_factory_data,
    clean_maintenance_events,
    enrich_maintenance_with_operators,
    create_fact_maintenance
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create a test Spark session."""
    return SparkSession.builder \
        .appName("test_etl") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


@pytest.fixture
def sample_maintenance_data(spark):
    """Create sample maintenance events for testing."""
    data = [
        {
            "event_id": "MEV-001",
            "factory_id": "FRA-PLANT-01",
            "line_id": "Line-A",
            "maintenance_type": "Preventive",
            "reason": "Planned Maintenance",
            "start_time": "2025-11-03 10:00:00",
            "end_time": "2025-11-03 10:30:00",
            "downtime_min": 30,
            "technician_id": "TECH-001",
            "parts_used": "PART-001,PART-002",
            "cost_eur": 1500.00,
            "outcome": "Restored",
            "next_due_date": "2025-11-10"
        },
        {
            "event_id": "MEV-002",
            "factory_id": "FRA-PLANT-01",
            "line_id": "Line-B",
            "maintenance_type": "Corrective",
            "reason": "Unplanned Breakdown",
            "start_time": "2025-11-04 14:00:00",
            "end_time": "2025-11-04 15:15:00",
            "downtime_min": 75,
            "technician_id": "TECH-002",
            "parts_used": None,
            "cost_eur": 2300.00,
            "outcome": "Restored",
            "next_due_date": "2025-11-15"
        },
        {
            "event_id": "MEV-003",
            "factory_id": "FRA-PLANT-01",
            "line_id": "Line-C",
            "maintenance_type": "Inspection",
            "reason": "Planned Maintenance",
            "start_time": "2025-11-05 09:00:00",
            "end_time": "2025-11-05 09:00:00",
            "downtime_min": 0,
            "technician_id": "TECH-001",
            "parts_used": "",
            "cost_eur": 500.00,
            "outcome": "Restored",
            "next_due_date": "2025-11-12"
        }
    ]
    
    return spark.createDataFrame(data)


@pytest.fixture
def sample_factory_data(spark):
    """Create sample factory production data."""
    data = [
        {
            "timestamp": "2025-11-03 06:00:00",
            "factory_id": "FRA-PLANT-01",
            "line_id": "Line-A",
            "shift": "Shift-1",
            "product_id": "P-Widget",
            "planned_qty": 50,
            "produced_qty": 48,
            "scrap_qty": 1,
            "defects_count": 1,
            "machine_state": "Running",
            "availability": 1.0,
            "performance": 1.0,
            "quality": 0.98,
            "oee": 0.98,
            "operator_id": "OP-001"
        },
        {
            "timestamp": "2025-11-03 06:15:00",
            "factory_id": "FRA-PLANT-01",
            "line_id": "Line-B",
            "shift": "Shift-1",
            "product_id": "P-Widget",
            "planned_qty": 60,
            "produced_qty": 55,
            "scrap_qty": 2,
            "defects_count": 3,
            "machine_state": "Running",
            "availability": 0.95,
            "performance": 0.92,
            "quality": 0.94,
            "oee": 0.82,
            "operator_id": "OP-002"
        }
    ]
    
    return spark.createDataFrame(data)


@pytest.fixture
def sample_operators_data(spark):
    """Create sample operators roster."""
    data = [
        {
            "operator_id": "OP-001",
            "name": "John Doe",
            "factory_id": "FRA-PLANT-01",
            "primary_line": "Line-A",
            "skill_level": "Senior",
            "reliability_score": 92.5
        },
        {
            "operator_id": "OP-002",
            "name": "Jane Smith",
            "factory_id": "FRA-PLANT-01",
            "primary_line": "Line-B",
            "skill_level": "Intermediate",
            "reliability_score": 85.0
        }
    ]
    
    return spark.createDataFrame(data)


# ============================================================================
# TEST SUITE 1: DATA VALIDATION
# ============================================================================

class TestMaintenanceValidation:
    """Test maintenance data validation logic."""
    
    def test_valid_data_passes(self, sample_maintenance_data):
        """Test that valid data passes all validations."""
        result = validate_maintenance_data(sample_maintenance_data)
        assert result['passed'] == True
        assert len(result['issues']) == 0
        assert result['total_rows'] == 3
    
    def test_duplicate_event_ids_detected(self, spark):
        """Test detection of duplicate event IDs."""
        data = [
            {
                "event_id": "MEV-001", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            },
            {
                "event_id": "MEV-001",  # Duplicate!
                "factory_id": "FRA-PLANT-01", "line_id": "Line-B",
                "maintenance_type": "Preventive", "reason": "Planned Maintenance",
                "start_time": "2025-11-03 11:00:00",
                "end_time": "2025-11-03 11:30:00",
                "downtime_min": 30, "technician_id": "TECH-002",
                "parts_used": "PART-002", "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_maintenance_data(df)
        
        assert result['passed'] == False
        assert any('duplicate' in issue.lower() for issue in result['issues'])
    
    def test_invalid_timestamps_detected(self, spark):
        """Test detection of end_time before start_time."""
        data = [
            {
                "event_id": "MEV-001", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:30:00",
                "end_time": "2025-11-03 10:00:00",  # Invalid: end before start
                "downtime_min": -30, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_maintenance_data(df)
        
        assert result['passed'] == False
        assert any('timestamp' in issue.lower() for issue in result['issues'])
    
    def test_null_critical_columns_detected(self, spark):
        """Test detection of nulls in critical columns."""
        data = [
            {
                "event_id": None,  # Null critical column
                "factory_id": "FRA-PLANT-01", "line_id": "Line-A",
                "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_maintenance_data(df)
        
        assert result['passed'] == False


class TestFactoryValidation:
    """Test factory data validation logic."""
    
    def test_valid_factory_data_passes(self, sample_factory_data):
        """Test valid factory data passes validation."""
        result = validate_factory_data(sample_factory_data)
        assert result['passed'] == True
    
    def test_oee_component_validation(self, spark):
        """Test OEE components stay within 0-1 range."""
        data = [
            {
                "timestamp": "2025-11-03 06:00:00",
                "factory_id": "FRA-PLANT-01", "line_id": "Line-A",
                "shift": "Shift-1", "product_id": "P-Widget",
                "planned_qty": 50, "produced_qty": 48,
                "scrap_qty": 1, "defects_count": 1,
                "machine_state": "Running",
                "availability": 1.2,  # Invalid: > 1
                "performance": 1.0, "quality": 0.98,
                "oee": 0.98, "operator_id": "OP-001"
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_factory_data(df)
        
        assert result['passed'] == False
    
    def test_produced_qty_validation(self, spark):
        """Test that produced_qty doesn't exceed planned_qty."""
        data = [
            {
                "timestamp": "2025-11-03 06:00:00",
                "factory_id": "FRA-PLANT-01", "line_id": "Line-A",
                "shift": "Shift-1", "product_id": "P-Widget",
                "planned_qty": 50,
                "produced_qty": 55,  # Invalid: exceeds planned
                "scrap_qty": 1, "defects_count": 1,
                "machine_state": "Running",
                "availability": 1.0, "performance": 1.0,
                "quality": 0.98, "oee": 0.98, "operator_id": "OP-001"
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_factory_data(df)
        
        assert result['passed'] == False


# ============================================================================
# TEST SUITE 2: DATA TRANSFORMATION
# ============================================================================

class TestCleanMaintenanceEvents:
    """Test maintenance event cleaning transformations."""
    
    def test_timestamp_parsing(self, spark, sample_maintenance_data):
        """Test timestamps are parsed correctly."""
        df_clean = clean_maintenance_events(sample_maintenance_data)
        
        # Check that UTC timestamps were created
        assert 'start_time_utc' in df_clean.columns
        assert 'end_time_utc' in df_clean.columns
        
        # Verify timestamp values are valid
        result = df_clean.select('start_time_utc', 'end_time_utc').collect()
        assert result[0]['start_time_utc'] is not None
    
    def test_parts_parsing(self, spark, sample_maintenance_data):
        """Test parts_used field is properly parsed."""
        df_clean = clean_maintenance_events(sample_maintenance_data)
        
        # First event should have 2 parts
        result = df_clean.filter(col('event_id') == 'MEV-001').collect()[0]
        assert result['parts_count'] > 0
        
        # Third event (empty string) should have 0 parts
        result_empty = df_clean.filter(col('event_id') == 'MEV-003').collect()[0]
        assert result_empty['parts_count'] == 0
    
    def test_null_parts_handling(self, spark):
        """Test that null parts_used is handled correctly."""
        data = [
            {
                "event_id": "MEV-001", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30, "technician_id": "TECH-001",
                "parts_used": None,  # Explicitly null
                "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        result = df_clean.collect()[0]
        assert result['parts_count'] == 0
    
    def test_classification_flags(self, spark, sample_maintenance_data):
        """Test that classification flags are added correctly."""
        df_clean = clean_maintenance_events(sample_maintenance_data)
        
        # Event 2 should be marked as unplanned
        unplanned = df_clean.filter(col('event_id') == 'MEV-002').collect()[0]
        assert unplanned['is_unplanned'] == True
        
        # Event 1 should be marked as planned
        planned = df_clean.filter(col('event_id') == 'MEV-001').collect()[0]
        assert planned['is_unplanned'] == False
        
        # Event 3 should be marked as monitoring (0 downtime)
        monitoring = df_clean.filter(col('event_id') == 'MEV-003').collect()[0]
        assert monitoring['downtime_category'] == 'monitoring'


class TestEnrichmentTransformations:
    """Test enrichment with operator data."""
    
    def test_left_join_preserves_all_events(
        self, spark, sample_maintenance_data, sample_operators_data
    ):
        """Test that left join preserves all maintenance events."""
        # Create mock technician_id that doesn't match any operator
        data = [
            {
                "event_id": "MEV-001", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30, "technician_id": "UNKNOWN-TECH",
                "parts_used": "PART-001", "cost_eur": 1500.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df_maint = spark.createDataFrame(data)
        df_enriched = enrich_maintenance_with_operators(df_maint, sample_operators_data)
        
        # Should still have 1 row (join didn't drop the event)
        assert df_enriched.count() == 1
        
        # Should have technician_found=False flag
        result = df_enriched.collect()[0]
        assert result['technician_found'] == False
    
    def test_operator_details_added(
        self, spark, sample_maintenance_data, sample_operators_data
    ):
        """Test that operator details are enriched correctly."""
        df_enriched = enrich_maintenance_with_operators(
            sample_maintenance_data,
            sample_operators_data
        )
        
        # Find enriched event for TECH-001 (maps to OP-001)
        result = df_enriched.filter(col('event_id') == 'MEV-001').collect()[0]
        
        # Skills should be enriched
        assert result['operator_skill_level'] == 'Senior'
        assert result['operator_reliability'] == 92.5


# ============================================================================
# TEST SUITE 3: FACT TABLE CREATION
# ============================================================================

class TestFactTableCreation:
    """Test fact table structure and calculations."""
    
    def test_fact_table_structure(self, spark, sample_maintenance_data):
        """Test that fact table has all required columns."""
        df_clean = clean_maintenance_events(sample_maintenance_data)
        fact_table = create_fact_maintenance(df_clean)
        
        required_columns = [
            'maint_event_id', 'factory_id', 'line_id',
            'maintenance_type', 'reason', 'outcome',
            'downtime_min', 'cost_eur', 'severity_level',
            'load_timestamp'
        ]
        
        for col_name in required_columns:
            assert col_name in fact_table.columns, f"Missing column: {col_name}"
    
    def test_severity_classification(self, spark):
        """Test severity level classification logic."""
        data = [
            {
                "event_id": "HIGH", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Corrective",
                "reason": "Unplanned Breakdown",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 13:00:00",  # 180 min (high)
                "downtime_min": 180, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 3000.00,  # High cost
                "outcome": "Restored", "next_due_date": "2025-11-10"
            },
            {
                "event_id": "MEDIUM", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-B", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-04 10:00:00",
                "end_time": "2025-11-04 11:15:00",  # 75 min (medium)
                "downtime_min": 75, "technician_id": "TECH-002",
                "parts_used": "PART-002", "cost_eur": 1700.00,  # Medium cost
                "outcome": "Restored", "next_due_date": "2025-11-15"
            },
            {
                "event_id": "LOW", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-C", "maintenance_type": "Inspection",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-05 10:00:00",
                "end_time": "2025-11-05 10:15:00",  # 15 min (low)
                "downtime_min": 15, "technician_id": "TECH-001",
                "parts_used": "", "cost_eur": 500.00,  # Low cost
                "outcome": "Restored", "next_due_date": "2025-11-12"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        fact_table = create_fact_maintenance(df_clean)
        
        # Check severity levels
        high = fact_table.filter(col('maint_event_id') == 'HIGH').collect()[0]
        assert high['severity_level'] == 'High'
        
        low = fact_table.filter(col('maint_event_id') == 'LOW').collect()[0]
        assert low['severity_level'] == 'Low'
    
    def test_cost_per_downtime_calculation(self, spark):
        """Test cost per downtime minute calculation."""
        data = [
            {
                "event_id": "CALC-TEST", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Corrective",
                "reason": "Unplanned Breakdown",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 11:00:00",  # 60 minutes
                "downtime_min": 60, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 600.00,  # 10 EUR/min
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        fact_table = create_fact_maintenance(df_clean)
        
        result = fact_table.collect()[0]
        expected_cost_per_min = Decimal('600.00') / Decimal('60')
        
        assert result['cost_per_downtime_min'] == expected_cost_per_min


# ============================================================================
# TEST SUITE 4: EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test handling of edge cases and unusual data."""
    
    def test_zero_downtime_events(self, spark):
        """Test handling of monitoring-only events (zero downtime)."""
        data = [
            {
                "event_id": "ZERO-DT", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Inspection",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:00:00",  # Same time = 0 downtime
                "downtime_min": 0, "technician_id": "TECH-001",
                "parts_used": "", "cost_eur": 0.00,
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        # Should not fail, should mark as monitoring
        result = df_clean.collect()[0]
        assert result['downtime_category'] == 'monitoring'
        assert result['downtime_min'] == 0
    
    def test_very_long_downtime_events(self, spark):
        """Test handling of extremely long downtime events."""
        data = [
            {
                "event_id": "LONG-DT", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Corrective",
                "reason": "Unplanned Breakdown",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-05 10:00:00",  # 2880 minutes = 48 hours
                "downtime_min": 2880, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": 5000.00,
                "outcome": "Root Cause Identified",
                "next_due_date": "2025-11-20"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        fact_table = create_fact_maintenance(df_clean)
        
        # Should be classified as High severity
        result = fact_table.collect()[0]
        assert result['severity_level'] == 'High'
        assert result['downtime_min'] == 2880
    
    def test_missing_cost_data(self, spark):
        """Test handling of events with missing cost information."""
        data = [
            {
                "event_id": "NO-COST", "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A", "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30, "technician_id": "TECH-001",
                "parts_used": "PART-001", "cost_eur": None,  # Missing cost
                "outcome": "Restored", "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        # Should handle gracefully without throwing exception
        result = df_clean.collect()[0]
        assert result['data_quality_flags'] is not None


# ============================================================================
# TEST EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Run with pytest
    # pytest test_manufacturing_etl.py -v
    pass