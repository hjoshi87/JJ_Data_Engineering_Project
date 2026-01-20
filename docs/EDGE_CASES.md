# Edge Cases & Trade-off Analysis

## Edge Case 1: Zero Downtime Maintenance Events

### Scenario
Inspection runs that don't stop production (0 minutes downtime).

### Example
```
event_id: MEV-123
maintenance_type: Inspection
downtime_min: 0
cost_eur: 500
```

### Three Approaches Considered

#### ❌ Approach A: Include in All Metrics
```
Pros:
- Complete count of all maintenance activities
- Captures scheduling patterns

Cons:
- Skews average downtime (45 min becomes 35 min if removed)
- Confuses "maintenance" with "monitoring"
- Users see misleading metrics
```

#### ❌ Approach B: Exclude from Dataset
```
Pros:
- Clean downtime-causing metrics

Cons:
- Data loss (never acceptable!)
- Underreports maintenance workload
- Can't answer "how many checks did we do?"
```

#### ✅ Approach C: Separate Into Buckets (CHOSEN)
```
Pros:
- Flexibility: stakeholders choose their metric
- Preserves data integrity
- Enables different use cases:
  * BI: can show both metrics
  * Operations: sees only production impact
  * Maintenance: sees all activities

Cons:
- Slightly more complex
- Requires documentation
```

### Implementation
```python
df.withColumn('downtime_category',
    when(col('downtime_min') == 0, 'monitoring')
    .otherwise('productive_downtime'))
```

### Result
Users can filter: `WHERE downtime_category = 'productive_downtime'`

---

## Edge Case 2: Missing Technician Data

### Scenario
Technician TECH-UNKNOWN is not in operators roster (external contractor or data gap).

### Example
```
event_id: MEV-456
technician_id: TECH-UNKNOWN
(no matching row in operators table)
```

### Two Approaches Considered

#### ❌ Approach A: Inner Join (Drop Events)
```
Cons:
- Loses maintenance events
- Data loss is never acceptable!
- No audit trail
```

#### ✅ Approach B: Left Join + Flag (CHOSEN)
```
Pros:
- Preserves all events
- Flags unknown technicians
- Enables investigation
- BI can decide how to handle

Cons:
- Nulls in operator columns
```

### Implementation
```python
df_enriched = df_maintenance.join(
    df_operators,
    df_maintenance.technician_id == df_operators.operator_id,
    'left'
).withColumn(
    'technician_found',
    when(col('operator_id').isNull(), False).otherwise(True)
)
```

### Result
```
event_id | technician_id | operator_found | skill_level
----------|---------------|-----------------|-----------
MEV-001   | TECH-001      | True            | Senior
MEV-002   | TECH-UNKNOWN  | False           | Unknown
```

---

## Edge Case 3: Very High Downtime Events (48+ hours)

### Scenario
Major breakdown causes 2,880 minutes (48 hours) downtime.

### Example
```
event_id: MEV-789
maintenance_type: Corrective
reason: Unplanned Breakdown
downtime_min: 2880
cost_eur: 5000.00
```

### Approach: Flag for Investigation
```
Severity Level: High (cost > 2500 OR downtime > 120 min)
Action: Alert operations team
```

### Implementation
```python
.withColumn(
    'severity_level',
    when(
        (col('cost_eur') > 2500) | (col('downtime_min') > 120),
        'High'
    ).when(
        (col('cost_eur') > 1500) | (col('downtime_min') > 60),
        'Medium'
    ).otherwise('Low')
)
```

---

## Edge Case 4: Timestamp Anomalies

### Scenario 1: End Time Before Start Time
```
start_time: 2025-11-03 10:30:00
end_time: 2025-11-03 10:00:00
```

**Resolution:** Caught by validation, flagged, investigated

### Scenario 2: Same Start/End Time
```
start_time: 2025-11-03 10:00:00
end_time: 2025-11-03 10:00:00
downtime_min: 0
```

**Resolution:** Normal (monitoring event)

### Scenario 3: Timezone Confusion
**Assumption:** All times are Europe/Paris (source factory timezone)

**Resolution:** Convert all to UTC
```python
.withColumn(
    'start_time_utc',
    to_utc_timestamp(
        to_timestamp(col('start_time')),
        'Europe/Paris'
    )
)
```

---

## Edge Case 5: Null Cost Data

### Scenario
~5% of events have missing cost_eur

### Three Approaches Considered

#### ❌ Approach A: Fill with Zero
```
Cons:
- False "free maintenance" signals
- Misleading cost reports
- Hides data collection issues
```

#### ❌ Approach B: Drop Rows
```
Cons:
- Data loss
- Never acceptable!
```

#### ✅ Approach C: Flag with Quality Column (CHOSEN)
```
Pros:
- Preserves all data
- Flags for investigation
- Users can choose to filter
- Enables root cause analysis

Cons:
- Requires explanation
```

### Implementation
```python
.withColumn(
    'cost_data_quality',
    when(col('cost_eur').isNull(), 'pending_cost')
    .when(col('cost_eur') == 0, 'free_or_in_house')
    .otherwise('confirmed')
)
```

### Result
```
event_id | cost_eur | cost_data_quality
---------|----------|------------------
MEV-001  | 1500.00  | confirmed
MEV-002  | NULL     | pending_cost
MEV-003  | 0.00     | free_or_in_house
```

---

## Summary Table

| Edge Case | Approach | Why | Tradeoff |
|-----------|----------|-----|----------|
| Zero downtime | Separate buckets | Flexibility | Slightly more complex |
| Missing tech | Left join + flag | Data preservation | Nulls in output |
| High downtime | Flag as High severity | Alert for review | Requires investigation |
| Timestamps | Validate + convert to UTC | Data consistency | Assumes Europe/Paris |
| Null costs | Flag with quality column | Transparency | Requires filtering logic |


