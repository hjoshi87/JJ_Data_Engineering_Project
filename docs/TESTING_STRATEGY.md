# Testing Strategy

## Current Test Coverage

### Unit Tests (8 tests - 100% passing)

```python
✓ test_1_validate_valid              - Validates correct data passes validation
✓ test_2_validate_null               - Detects NULL values in critical fields
✓ test_3_clean_timestamps            - Converts timestamps to UTC correctly
✓ test_4_clean_parts                 - Parses comma-separated parts to array
✓ test_5_enrich_preserves            - LEFT JOIN preserves all events
✓ test_6_fact_table                  - Creates output fact table correctly
✓ test_7_zero_downtime               - Handles zero downtime edge case
✓ test_8_missing_data                - Handles NULL cost edge case
```

**Execution:** 84.33 seconds (includes Spark startup)  
**Coverage:** All pipeline functions + edge cases  
**Status:** ✅ 100% passing

---

## Production Extensions - What to Add in Future for Production:

### 1. Integration Tests
**Purpose:** Full pipeline with real data

```python
def test_integration_full_pipeline():
    """Run complete pipeline, verify output"""
    report = run_etl_pipeline('./data/raw', './data/processed')
    assert report['pipeline_status'] == 'Success'
    assert report['row_counts']['fact_maintenance'] >= 50
```

**When:** After unit tests pass

---

### 2. Data Quality Tests
**Purpose:** Validate output meets business rules

```python
def test_output_data_quality():
    """Verify fact table is valid"""
    df = read_csv('./data/processed/fact_maintenance_events.csv')
    
    # No negative costs
    assert df[df['cost_eur'] < 0].count() == 0
    # All events have IDs
    assert df[df['maint_event_id'].isNull()].count() == 0
    # Row count within range
    assert 90 <= df.count() <= 100
```

**When:** Each pipeline run

---

### 3. Performance Tests
**Purpose:** Ensure acceptable execution time

```python
def test_pipeline_performance():
    """Pipeline should finish in < 5 minutes"""
    start = time.time()
    run_etl_pipeline(...)
    duration = time.time() - start
    assert duration < 300  # 5 minutes
```

**When:** Before deployment

---

### 4. Regression Tests
**Purpose:** Results don't change unexpectedly

```python
def test_results_consistency():
    """Key metrics should match baseline"""
    report = run_etl_pipeline(...)
    
    assert report['row_counts']['fact_maintenance'] == 94
    assert 169000 <= report['total_cost'] <= 170000
```

**When:** Monitor each run

---

### 5. Database Tests
**Purpose:** Can connect and import

```python
def test_postgresql_connection():
    """Verify database connection works"""
    conn = psycopg2.connect(config['database'])
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM maintenance_events')
    count = cursor.fetchone()[0]
    assert count > 0
```

**When:** Before analysis

---

## Test Pyramid

```
                 ▲
               / | \
            /    |    \
         /       |       \  E2E Tests (1)
      /          |          \
   /             |             \
 ┌─────────────────────────────────┐
 │   Integration Tests (3-4)       │
 │   - Full pipeline                │
 │   - Data quality                 │
 │   - Performance                  │
 ├─────────────────────────────────┤
 │                                 │
 │    Unit Tests (8)               │
 │    - Functions                  │
 │    - Edge cases                 │
 │    - Validation                 │
 └─────────────────────────────────┘
```

---

## Test Execution Strategy

### Development
```bash
# Run unit tests only (fast)
pytest tests/test_manufacturing_etl.py -v
# Time: ~84 seconds
```

### Pre-Deployment
```bash
# Run all tests
pytest tests/test_manufacturing_etl.py -v
pytest tests/test_production.py -v
# Time: ~2 minutes
```

### Production (CI/CD)
```yaml
# .github/workflows/tests.yml
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Unit Tests
        run: pytest tests/test_manufacturing_etl.py -v
      - name: Production Tests
        run: pytest tests/test_production.py -v
      - name: Data Quality Check
        run: pytest tests/test_data_quality.py -v
```

---

## Coverage Summary

| Test Type | Current | Target | Gap |
|-----------|---------|--------|-----|
| Unit Tests | 8 | 8 | ✅ Complete |
| Integration | 0 | 3-4 | TODO for Future |
| Data Quality | 0 | 1-2 | TODO for Future |
| Performance | 0 | 1 | TODO for Future |
| Database | 0 | 1 | TODO for Future |

---

## Success Criteria

### Unit Tests (100%: Currently in tests directory)
- ✅ All 8 tests pass
- ✅ No flaky tests
- ✅ <100 sec execution

### Integration Tests (For Future)
- ✅ Full pipeline succeeds
- ✅ Output files created
- ✅ Row counts correct

### Data Quality (For Future)
- ✅ No negative values
- ✅ No NULLs in required fields
- ✅ Data types correct

### Performance (For Future)
- ✅ Completes in <5 minutes
- ✅ Memory usage <2GB
- ✅ CPU utilization reasonable

