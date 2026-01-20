"""
ULTRA-SIMPLE TEST FILE - No fancy fixtures, just basic tests

Since Spark works fine (we proved it), the problem must be with:
- Session-scoped fixtures
- Complex fixture passing
- Test framework interference

This version uses the simplest possible approach.
"""

import sys
import os
from pathlib import Path

# Set env vars FIRST
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import pytest
from pyspark.sql import SparkSession

# Import pipeline functions
from manufacturing_etl_pipeline import (
    validate_maintenance_data,
    clean_maintenance_events,
    enrich_maintenance_with_operators,
    create_fact_maintenance
)


# ============================================================================
# SIMPLE HELPER - Create Spark session for each test
# ============================================================================

def get_spark():
    """Create a fresh Spark session for each test."""
    return SparkSession.builder \
        .appName("test_etl") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


# ============================================================================
# TEST 1: Validate - Valid Case
# ============================================================================

def test_1_validate_valid():
    """TEST 1: Validation works on valid data"""
    spark = get_spark()
    
    try:
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
            }
        ]
        
        df = spark.createDataFrame(data)
        result = validate_maintenance_data(df)
        
        assert isinstance(result, dict)
        assert 'passed' in result
        assert result['total_rows'] == 1
        
        print("✓ TEST 1 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 2: Validate - Null Detection
# ============================================================================

def test_2_validate_null():
    """TEST 2: Validation detects nulls"""
    spark = get_spark()
    
    try:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        # Schema needed because event_id is NULL
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("factory_id", StringType(), True),
            StructField("line_id", StringType(), True),
            StructField("maintenance_type", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("downtime_min", IntegerType(), True),
            StructField("technician_id", StringType(), True),
            StructField("parts_used", StringType(), True),
            StructField("cost_eur", DoubleType(), True),
            StructField("outcome", StringType(), True),
            StructField("next_due_date", StringType(), True)
        ])
        
        data = [
            {
                "event_id": None,  # NULL
                "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A",
                "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30,
                "technician_id": "TECH-001",
                "parts_used": "PART-001",
                "cost_eur": 1500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        result = validate_maintenance_data(df)
        
        assert isinstance(result, dict)
        assert not result['passed'] or len(result.get('issues', [])) > 0
        
        print("✓ TEST 2 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 3: Clean - Timestamp Parsing
# ============================================================================

def test_3_clean_timestamps():
    """TEST 3: Cleaning parses timestamps"""
    spark = get_spark()
    
    try:
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
                "parts_used": "PART-001",
                "cost_eur": 1500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        assert df_clean is not None
        assert df_clean.count() == 1
        assert 'start_time_utc' in df_clean.columns
        assert 'end_time_utc' in df_clean.columns
        
        print("✓ TEST 3 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 4: Clean - Parts Handling
# ============================================================================

def test_4_clean_parts():
    """TEST 4: Cleaning handles parts"""
    spark = get_spark()
    
    try:
        data = [
            {
                "event_id": "WITH-PARTS",
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
                "event_id": "NULL-PARTS",
                "factory_id": "FRA-PLANT-01",
                "line_id": "Line-B",
                "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 11:00:00",
                "end_time": "2025-11-03 11:30:00",
                "downtime_min": 30,
                "technician_id": "TECH-002",
                "parts_used": None,
                "cost_eur": 1500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        assert df_clean is not None
        assert df_clean.count() == 2
        assert 'parts_count' in df_clean.columns
        
        print("✓ TEST 4 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 5: Enrich - Preserves Rows
# ============================================================================

def test_5_enrich_preserves():
    """TEST 5: Enrichment preserves all events"""
    spark = get_spark()
    
    try:
        maint_data = [
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
                "parts_used": "PART-001",
                "cost_eur": 1500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        ops_data = [
            {
                "operator_id": "OP-001",
                "name": "John Doe",
                "factory_id": "FRA-PLANT-01",
                "primary_line": "Line-A",
                "skill_level": "Senior",
                "reliability_score": 92.5
            }
        ]
        
        df_maint = spark.createDataFrame(maint_data)
        df_clean = clean_maintenance_events(df_maint)
        
        df_ops = spark.createDataFrame(ops_data)
        df_enriched = enrich_maintenance_with_operators(df_clean, df_ops)
        
        assert df_enriched is not None
        assert df_enriched.count() >= df_clean.count()
        
        print("✓ TEST 5 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 6: Fact Table
# ============================================================================

def test_6_fact_table():
    """TEST 6: Fact table creation works"""
    spark = get_spark()
    
    try:
        maint_data = [
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
                "parts_used": "PART-001",
                "cost_eur": 1500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        ops_data = [
            {
                "operator_id": "OP-001",
                "name": "John Doe",
                "factory_id": "FRA-PLANT-01",
                "primary_line": "Line-A",
                "skill_level": "Senior",
                "reliability_score": 92.5
            }
        ]
        
        df_maint = spark.createDataFrame(maint_data)
        df_clean = clean_maintenance_events(df_maint)
        
        df_ops = spark.createDataFrame(ops_data)
        df_enriched = enrich_maintenance_with_operators(df_clean, df_ops)
        
        fact_table = create_fact_maintenance(df_enriched)
        
        assert fact_table is not None
        assert fact_table.count() > 0
        assert len(fact_table.columns) > 0
        
        print("✓ TEST 6 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 7: Edge Case - Zero Downtime
# ============================================================================

def test_7_zero_downtime():
    """TEST 7: Zero downtime handled"""
    spark = get_spark()
    
    try:
        data = [
            {
                "event_id": "MONITORING",
                "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A",
                "maintenance_type": "Inspection",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:00:00",
                "downtime_min": 0,
                "technician_id": "TECH-001",
                "parts_used": "",
                "cost_eur": 500.00,
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data)
        df_clean = clean_maintenance_events(df)
        
        assert df_clean.count() == 1
        
        print("✓ TEST 7 PASSED")
        
    finally:
        spark.stop()


# ============================================================================
# TEST 8: Edge Case - Missing Data
# ============================================================================

def test_8_missing_data():
    """TEST 8: Missing data handled"""
    spark = get_spark()
    
    try:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        # Schema needed because cost_eur is NULL
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("factory_id", StringType(), True),
            StructField("line_id", StringType(), True),
            StructField("maintenance_type", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("downtime_min", IntegerType(), True),
            StructField("technician_id", StringType(), True),
            StructField("parts_used", StringType(), True),
            StructField("cost_eur", DoubleType(), True),
            StructField("outcome", StringType(), True),
            StructField("next_due_date", StringType(), True)
        ])
        
        data = [
            {
                "event_id": "NO-COST",
                "factory_id": "FRA-PLANT-01",
                "line_id": "Line-A",
                "maintenance_type": "Preventive",
                "reason": "Planned Maintenance",
                "start_time": "2025-11-03 10:00:00",
                "end_time": "2025-11-03 10:30:00",
                "downtime_min": 30,
                "technician_id": "TECH-001",
                "parts_used": "PART-001",
                "cost_eur": None,  # NULL
                "outcome": "Restored",
                "next_due_date": "2025-11-10"
            }
        ]
        
        df = spark.createDataFrame(data, schema=schema)
        df_clean = clean_maintenance_events(df)
        
        assert df_clean.count() == 1
        
        print("✓ TEST 8 PASSED")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    pass