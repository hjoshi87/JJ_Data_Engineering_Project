# Johnson & Johnson Manufacturing Analytics ETL Pipeline

## Project Overview
Manufacturing maintenance analytics platform transforming raw CSV data into actionable insights for BI teams and operations.

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

## Project Structure
```
Solution/
├── README.md                    ← You are here
├── requirements.txt
├── sql/
│   └── queries.sql             ← All 5 business questions queries
├── src/
│   └── manufacturing_etl_pipeline.py    ← Main ETL code (with PySpark)
├── tests/
│   └── test_manufacturing_etl.py        ← 19 unit tests
├── docs/
│   ├── ARCHITECTURE.md
│   └── EDGE_CASES.md
└── data/
    ├── raw/
    │   ├── maintenance_events.csv
    │   ├── manufacturing_factory_dataset.csv
    │   └── operators_roster.csv
    └── processed/
        ├── fact_maintenance_events.parquet
        ├── fact_maintenance_events.csv
        ├── summary_maintenance_metrics.parquet
        └── summary_maintenance_metrics.csv
```

## Business Questions Answered

| Question | Query | Answer |
|----------|-------|--------|
| Total maintenance cost? | Q1 | ~€190,000 |
| Total downtime minutes? | Q2 | ~4,200 min |
| Number of events? | Q3 | 96 events |
| Unplanned breakdowns? | Q4 | ~26 (27%) |
| Avg downtime/event? | Q5 | ~45 min |

## Key Features
✅ Validates 100% of input data  
✅ Handles edge cases (zeros, nulls, duplicates)  
✅ Produces Parquet + CSV outputs  
✅ 19 comprehensive unit tests  
✅ Production-ready logging & error handling  

## Edge Cases Handled
1. **Zero Downtime Events** - Separated as "monitoring" category
2. **Missing Technician Data** - Left join preserves all events
3. **Very High Downtime** - Flagged as High severity
4. **Timestamp Anomalies** - Validated and converted to UTC
5. **Missing Cost Data** - Flagged with data quality column

## Data Quality
- ✓ 96 maintenance events processed
- ✓ 2,018 factory production records
- ✓ 20 operators in roster
- ✓ All data validated before transformation

## Testing
```bash
pytest tests/ -v
```

Results: **19 tests, all passing**

## Architecture
See `docs/ARCHITECTURE.md` for system design and technology stack.

## Edge Cases & Trade-offs
See `docs/EDGE_CASES.md` for detailed analysis of edge case handling.

## Author
Heramb Joshi

## Contact
jheramb8@gmail.com