# Johnson & Johnson Manufacturing Analytics ETL Pipeline

## Project Overview
Manufacturing maintenance analytics platform transforming raw CSV data into actionable insights for BI teams and operations. Professional data pipeline for manufacturing maintenance events using PySpark and PostgreSQL.

## Quick Results (94 Maintenance Events)

| Question | Answer |
|----------|--------|
| Total maintenance cost? | **169,389.62 EUR** |
| Total downtime minutes? | **6,180 minutes (103 hours)** |
| Number of maintenance events? | **94 events** |
| Unplanned breakdowns? | **23 (24.47%)** |
| Average downtime per event? | **65.74 minutes** |

## Quick Start

### Prerequisites
- Python 3.11+
- Apache Spark 3.3+

### Installation
```bash
pip install -r requirements.txt
```

### Run Pipeline
```bash
python src/manufacturing_etl_pipeline.py
```

### Run Tests
```bash
pytest tests/ -v
```

## Configuration

The `config/` folder controls pipeline behavior:

| File | Purpose |
|------|---------|
| **database.yaml** | PostgreSQL connection (host, port, credentials) |
| **etl.yaml** | Pipeline config (input/output paths, Spark settings) |
| **analysis.yaml** | Analysis parameters (query timeouts, metrics) |
| **logging.yaml** | Logging configuration (level, format, file paths) |

### Setup

```bash
# 1. Set database password
export DB_PASSWORD=your_password

# 2. Update config files if needed
# - config/etl.yaml: Adjust paths if different
# - config/logging.yaml: Configure log level

# 3. Run pipeline
python src/manufacturing_etl_pipeline.py
```

### Example Configs

**database.yaml** - PostgreSQL settings
```yaml
database:
  host: localhost
  port: 5432
  name: manufacturing
  user: postgres
  password: ${DB_PASSWORD}
```

**etl.yaml** - Pipeline configuration
```yaml
pipeline:
  input_dir: ./data/raw
  output_dir: ./data/processed

spark:
  app_name: manufacturing-etl
  master: local[4]
  executor_memory: 1g
```
---

## Project Structure
```
Solution/
├── README.md                    ← You are here (Task 1)
├── requirements.txt
├── sql/
│   └── queries.sql             ← All 5 business questions queries (Task 2)
├── src/
│   └── manufacturing_etl_pipeline.py    ← Main ETL code (with PySpark) (Task 3)
├── tests/
│   └── test_manufacturing_etl.py        ← 8 unit tests, 100% passing (Task 4)
├── docs/
│   ├── PIPELINE_DESIGN.md                 # Architecture, phases, and design decisions
│   ├── DATA_QUALITY_STRATEGY.md           # Data cleaning approach and quality metrics
│   ├── QUERIES.md                         # SQL queries with results
│   ├── ASSUMPTIONS.md                     # Project assumptions
│   ├── TESTING_STRATEGY.md                # Test coverage and production extensions
│   └── EDGE_CASES.md                      # Edge cases currently handled by pipeline
├── data/
│   ├── raw/
│   │   ├── maintenance_events.csv         (94 events)
│   │   ├── manufacturing_factory_dataset.csv
│   │   └── operators_roster.csv
│   └── processed/
│       ├── fact_maintenance_events.parquet
│       ├── fact_maintenance_events.csv
│       ├── summary_maintenance_metrics.parquet
│       └── summary_maintenance_metrics.csv
├── config/                                ← Configuration files
│   ├── database.yaml                      # PostgreSQL settings
│   ├── etl.yaml                           # Pipeline config
│   ├── analysis.yaml                      # Analysis parameters
│   └── logging.yaml                       # Logging config

```

## Analysis: Business Questions Answered (Task 2)

| Question | Query | Answer |
|----------|-------|--------|
| Total maintenance cost? | Q1 | ~€169,389.62 |
| Total downtime minutes? | Q2 | 6180 minutes (103 hours) |
| Number of maintenance events? | Q3 | 94 events |
| Unplanned breakdowns? | Q4 | ~23 (24.47%) |
| Avg downtime/event? | Q5 | ~65.74 minutes |

See: `docs/QUERIES.md` for detailed SQL queries and thought process

## ETL Pipeline Architecture (Task 3)
Raw CSVs → Extract → Validate → Clean → Enrich → Transform → Export
                                                              ↓
                                                    CSV + Parquet files
                                                              ↓
                                                    PostgreSQL Database


**6 Phases:**

1. Extract - Load 3 CSV files
2. Validate - Check data quality (94/94 passed)
3. Clean - Convert timestamps, parse arrays
4. Enrich - Join with operator details
5. Transform - Create fact table (94 × 26 columns)
6. Export - Write CSV + Parquet

## ETL Pipeline Design Decisions
See: `docs/PIPELINE_DESIGN.md` for full architecture and rationale


## Data Quality & Testing (Task 4)
Quality Metrics:

- 94/94 events validated (100%)
- 0 data loss
- 71/94 technicians matched (75.5%)
- 8/8 tests passing (100%)

## Test Coverage:
✓ test_1_validate_valid              - Correct data passes

✓ test_2_validate_null               - NULL detection

✓ test_3_clean_timestamps            - Timestamp conversion

✓ test_4_clean_parts                 - Array parsing

✓ test_5_enrich_preserves            - Data preservation

✓ test_6_fact_table                  - Output creation

✓ test_7_zero_downtime               - Edge case handling

✓ test_8_missing_data                - Edge case handling


## Edge Cases Handled

1. Zero Downtime Events - Maintenance inspections with no downtime
2. Missing Technician Data - Technicians not in roster (preserved as NULL)
3. Missing Cost Data - Cost values not available (handled explicitly)
4. Timestamp Anomalies - Converted to UTC for consistency
5. Duplicate Events - Validated (none found in data)

See: `docs/EDGE_CASES.md` for details

## Key Features
✅ Data Validation - 100% of input data validated before processing

✅ Edge Case Handling - Zero downtime, missing data, unmatched technicians

✅ Multiple Outputs - Both Parquet (compressed) and CSV (universal)

✅ Comprehensive Tests - 8 unit tests with 100% pass rate

✅ Production-Ready - Logging, error handling, modular design

✅ SQL Analysis - 5 queries answering key business questions

## Technologies

- ETL Engine - Apache Spark / PySpark
- Language - Python 3.11+
- Testing - pytest
- Database - PostgreSQL
- Output Formats - CSV, Parquet

## Documentation

- PIPELINE_DESIGN.md - Architecture, phases, and design decisions
- DATA_QUALITY_STRATEGY.md - Data cleaning approach and quality metrics
- QUERIES.md - SQL analysis queries with results
- ASSUMPTIONS.md - Project assumptions and risks
- TESTING_STRATEGY.md - Test coverage and production extensions
- EDGE_CASES.md - Edge cases handled by pipeline

## Performance

1. Pipeline Execution: ~2 minutes end-to-end
2. Data Size: 94 events → ~1 MB memory
3. Test Suite: 84 seconds (includes Spark overhead)
4. Scalability: Design scales to 100x+ volume

## Author
Heramb Joshi

## Contact
jheramb8@gmail.com