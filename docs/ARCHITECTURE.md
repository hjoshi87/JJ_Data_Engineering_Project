# System Architecture

## Data Flow Pipeline
```
CSV Files
    ↓
Extract (Read & validate file format)
    ↓
Validate (Check data quality, duplicates, ranges)
    ↓
Transform (Clean, parse, normalize, enrich)
    ↓
Quality Assurance (Verify outputs)
    ↓
Export (Parquet + CSV)
    ↓
Analytics Ready Data
```

## Component Design

### 1. Extraction Layer
- Read 3 CSV files into Spark DataFrames
- Validate row counts
- Check schema consistency
- **Functions:**
  - `read_csv_with_validation()`

### 2. Validation Layer
- Check for duplicate event IDs
- Validate timestamp consistency (end >= start)
- Ensure costs and downtime are non-negative
- Verify OEE components in 0-1 range
- **Functions:**
  - `validate_maintenance_data()`
  - `validate_factory_data()`
  - `validate_operators_data()`

### 3. Transformation Layer
- Parse timestamps and convert to UTC (Europe/Paris → UTC)
- Parse parts_used field (comma-separated string)
- Classify events (planned vs unplanned)
- Flag data quality issues
- Enrich with operator details (left join)
- Create fact table with business dimensions
- **Functions:**
  - `clean_maintenance_events()`
  - `enrich_maintenance_with_operators()`
  - `create_fact_maintenance()`

### 4. Aggregation Layer
- Create summary metrics by line, type, date
- Calculate rates and percentages
- **Functions:**
  - `create_maintenance_summary()`

### 5. Export Layer
- Write to Parquet (columnar, compressed)
- Write to CSV (human-readable)
- **Functions:**
  - `export_dataframe()`

## Key Design Decisions

### Decision 1: Modular Functions
**Why:** Each transformation is independent, enabling unit testing and reuse

### Decision 2: UTC Timestamp Standardization
**Why:** Eliminates timezone ambiguity at scale, prevents DST bugs

### Decision 3: Left Join with Operators
**Why:** Preserves all maintenance events (including external contractors)

### Decision 4: Data Quality Flags
**Why:** Flags issues rather than silently dropping data, maintains integrity

### Decision 5: Separated Monitoring Events
**Why:** Zero-downtime events marked separately, supports flexible reporting

## Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Processing | Apache Spark | Distributed, scalable, SQL-friendly |
| Output Format | Parquet | Columnar, compressed, schema-preserving |
| Output Format | CSV | Universal, human-readable |
| Testing | pytest | Fast, clean, pytest fixtures |
| Language | Python | Easy to learn, good ecosystem |

## Performance Characteristics

### Current (96 rows)
- Runtime: ~30 seconds
- Memory: ~2GB peak
- Output: ~1MB (combined Parquet + CSV)

### Scaled (100M rows)
- Runtime: ~3-5 minutes (with 4-node cluster)
- Memory: ~10GB per executor
- Output: ~1TB
- Partitioning: By date (monthly)

## Future Enhancements
- [ ] Streaming support (Kafka input)
- [ ] Automated scheduling (Airflow/Databricks)
- [ ] ML pipeline for anomaly detection
- [ ] Real-time dashboards (Looker/Tableau)