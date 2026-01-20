# Project Assumptions

## Data Assumptions

| Assumption | Impact | Mitigation | Verified |
|-----------|--------|-----------|----------|
| Only 'Preventive' & 'Corrective' types exist | New types won't classify correctly | Validation checks | ✅ Yes |
| Technician IDs may not match roster | Unmatched records | LEFT JOIN (preserve all) | ✅ Yes (23 unmatched) |
| Timestamps always start_time < end_time | Negative downtime | Validation + cleaning | ✅ Yes |
| Cost values >= 0 | Invalid calculations | Validation + cleaning | ✅ Yes |
| Downtime values >= 0 | Invalid analytics | Validation + cleaning | ✅ Yes |

## Schema Assumptions

| Assumption | Impact | Mitigation | Status |
|-----------|--------|-----------|--------|
| Fixed column names (event_id, factory_id, etc.) | Parsing fails | Validation checks columns | ✅ Yes |
| Date format: YYYY-MM-DD HH:MM:SS | Conversion fails | Explicit timestamp parsing | ✅ Yes |
| Parts stored as comma-separated strings | Parsing fails | Split function with fallback | ✅ Yes |
| Factory IDs don't change | Joins fail | Can add time dimension if needed | ✅ Yes |

## Business Assumptions

| Assumption | Impact | Mitigation | Verified |
|-----------|--------|-----------|----------|
| maintenance_type='Corrective' = unplanned | Wrong classification | Could use reason field instead | ✅ Yes |
| All data from single factory | Analysis incomplete | Design scales to multiple | ✅ Yes |
| Maintenance events are independent | Correlations missed | Can add sequential analysis | ✅ Yes |

## Operational Assumptions

| Assumption | Impact | Mitigation | Status |
|-----------|--------|-----------|--------|
| CSV files updated on regular basis | Stale data | Confirm schedule with team | ⚠️ Unclear |
| PostgreSQL always available | Pipeline fails | Add retry logic + alerting | ⚠️ Not implemented |
| File paths fixed (./data/raw, ./data/processed) | Hard to scale | Move to config.yaml | ⚠️ Hardcoded |

## Risk Matrix

| Assumption | Risk | Severity | Likelihood | Mitigation |
|-----------|------|----------|-----------|-----------|
| Fixed types | Low | Low | Low | Validation check |
| Fixed columns | Medium | Medium | Low | Schema validation |
| Cost >= 0 | Low | Low | Very Low | Input validation |
| Timestamps valid | Low | Low | Very Low | Conversion validation |
| DB available | Medium | High | Medium | Connection retry + monitoring |
| Data completeness | Low | Medium | Low | Data quality checks |
| CSV format constant | Medium | High | Low | Version control |


## Scaling Assumptions (For Future):**
- Multiple factories can be added
- Column schema can be extended
- Data volume can scale (design ready)