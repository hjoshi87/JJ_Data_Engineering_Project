# PostgreSQL Analysis Queries

## Query 1: Cost Analysis

```sql
SELECT 
    SUM(cost_eur) AS total_maintenance_cost,
    COUNT(*) AS event_count,
    ROUND(AVG(cost_eur)::numeric, 2) AS avg_cost_per_event,
    MIN(cost_eur) AS min_cost,
    MAX(cost_eur) AS max_cost
FROM maintenance_events
WHERE cost_eur IS NOT NULL AND cost_eur >= 0;
```

**Results:**
| Metric | Value |
|--------|-------|
| Total Cost | 169,389.62 EUR |
| Event Count | 94 |
| Average | 1,802.02 EUR |
| Min | 171.85 EUR |
| Max | 3,457.66 EUR |

**Thought Process:**
- SUM() for total, AVG/MIN/MAX for distribution
- No JOINs needed → simple, efficient
- WHERE filters NULLs and negatives

---

## Query 2: Downtime Analysis

```sql
SELECT 
    SUM(downtime_min) AS total_downtime_minutes,
    SUM(CASE WHEN downtime_min > 0 THEN downtime_min ELSE 0 END) 
        AS total_productive_downtime_min,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN downtime_min > 0 THEN 1 END) AS events_with_downtime,
    COUNT(CASE WHEN downtime_min = 0 THEN 1 END) AS monitoring_only_events,
    ROUND(SUM(downtime_min)::NUMERIC / 60, 2) AS total_hours
FROM maintenance_events
WHERE downtime_min IS NOT NULL;
```

**Results:**
| Metric | Value |
|--------|-------|
| Total Downtime | 6,180 minutes |
| Total Hours | 103.00 |
| Total Events | 94 |
| Events with Downtime | 94 |
| Monitoring Only | 0 |

**Thought Process:**
- Single pass through table
- CASE WHEN for categorization
- Conversion to hours for readability

---

## Query 3: Event Composition

```sql
SELECT 
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(DISTINCT factory_id) AS factory_count,
    COUNT(DISTINCT line_id) AS line_count,
    COUNT(DISTINCT maintenance_type) AS maintenance_type_count
FROM maintenance_events;
```

**Results:**
| Metric | Value |
|--------|-------|
| Total Events | 94 |
| Unique Events | 94 |
| Factory Count | 1 |
| Line Count | 4 |
| Maintenance Types | 2 |

**Thought Process:**
- COUNT(DISTINCT) shows composition
- Verifies no duplicates in data
- Shows scope: 1 factory, 4 lines

---

## Query 4: Unplanned Breakdowns

```sql
SELECT 
    COUNT(*) AS unplanned_breakdowns,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM maintenance_events), 2) 
        AS pct_of_total_events,
    SUM(downtime_min) AS total_unplanned_downtime_min,
    ROUND(AVG(downtime_min), 2) AS avg_unplanned_downtime_min,
    SUM(cost_eur) AS total_unplanned_cost_eur,
    ROUND(AVG(cost_eur)::numeric, 2) AS avg_unplanned_cost_eur
FROM maintenance_events
WHERE reason = 'Unplanned Breakdown';
```

**Results:**
| Metric | Value |
|--------|-------|
| Unplanned Count | 23 |
| % of Total | 24.47% |
| Total Downtime | 1,560 min |
| Avg Downtime | 67.83 min |
| Total Cost | 37,322.72 EUR |
| Avg Cost | 1,622.73 EUR |

**Thought Process:**
- WHERE filters to unplanned only
- Shows impact: 24% of events = 25% of downtime, 22% of cost
- Single pass, multiple aggregations

---

## Query 5: Downtime Distribution

```sql
SELECT 
    COUNT(*) AS total_events,
    COUNT(CASE WHEN downtime_min > 0 THEN 1 END) AS events_with_downtime,
    ROUND(AVG(downtime_min), 2) AS mean_downtime_min,
    ROUND(AVG(CASE WHEN downtime_min > 0 THEN downtime_min END), 2)  
        AS mean_downtime_excluding_zeros,
    MIN(downtime_min) AS min_downtime_min,
    MAX(downtime_min) AS max_downtime_min,
    ROUND(STDDEV_POP(downtime_min), 2) AS stddev_downtime_min
FROM maintenance_events
WHERE downtime_min IS NOT NULL;
```

**Results:**
| Metric | Value |
|--------|-------|
| Total Events | 94 |
| Events with Downtime | 94 |
| Mean | 65.74 min |
| Min | 15 min |
| Max | 180 min |
| Std Dev | 37.90 |

**Thought Process:**
- AVG for central tendency
- MIN/MAX for range
- STDDEV for variation (57.7% - high variation)
- All in one pass

---

## Query Design Principles

✅ **Single table scan** - All aggregations in one pass
✅ **WHERE filters early** - Eliminate NULLs before aggregation
✅ **CASE WHEN for subsets** - More efficient than separate queries
✅ **Explicit rounding** - 2 decimals for money
✅ **Clear aliases** - Self-documenting column names

---

## Business Insights

- **Cost:** 169K EUR across 94 events (1.8K average)
- **Downtime:** 103 hours total (66 min average)
- **Events:** Single factory, 4 lines, 2 types
- **Unplanned:** 24% of events, but 25% of downtime and 22% of cost
- **Variation:** High (std dev 37.9) - events unpredictable