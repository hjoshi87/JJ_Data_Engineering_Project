## Data Quality & Cleaning Strategy
## Functions Implemented

1. validate_maintenance_data()
Validation Rules:

Required columns present
No NULLs in: event_id, factory_id, maintenance_type
Cost values >= 0
Downtime values >= 0

Results: 94/94 events passed (100%)

Tests:

test_1_validate_valid ✅
test_2_validate_null ✅


2. clean_maintenance_events()
Transformations:
Issue | Fix | Example | 
Timestamp strings | Convert to UTC | "2025-01-18 10:00:00" → timestamp
Parts as string | Split into array | "PART-001,PART-002" → ["PART-001", "PART-002"]
NULL parts| Empty array | None → []
Negative downtime| Set to 0 | -5 → 0 
NULL cost | Keep NULL | NULL → NULL

Results: All 94 events cleaned successfully

Tests:

test_3_clean_timestamps ✅
test_4_clean_parts ✅
test_7_zero_downtime ✅
test_8_missing_data ✅


3. enrich_maintenance_with_operators()
Operation: LEFT JOIN maintenance with operators table on technician_id
Adds:

operator_skill_level
operator_reliability
technician_found (boolean)

Results:

71/94 matched (75.5%)
23/94 unmatched (24.5% - kept for data completeness)

Test: test_5_enrich_preserves ✅

Quality Assurance Summary
Phase | Result | Status|
Validation | 94/94 passed✅ | 100%
Cleaning | 94/94 cleaned✅ | 100%
Enrichment| 94/94 processed✅ | 100%
Data loss | 0/94✅ | None
Test pass rate| 8/8✅ | 100%

Key Decisions
Why explicit NULL handling?

- Clear what happens with each field
- Self-documenting code
- Easy to debug

Why LEFT JOIN (not INNER)?

- Preserves all events
- No data loss
- Unmatched technicians still analyzable

Why keep unmatched records?

- Lose 24% of data if filtered
- Can always filter later in SQL
- Shows data completeness issues