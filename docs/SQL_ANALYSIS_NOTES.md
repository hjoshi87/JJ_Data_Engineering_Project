# SQL Analysis Notes

## Data Observations from CSV Review

### maintenance_events.csv
- Total rows: 96 (excluding header)
- event_id: Unique identifier for each event
- cost_eur: Values range from ~184 to ~3,458
- downtime_min: Values range from 0 to 180
- reason: "Planned Maintenance" or "Unplanned Breakdown"
- maintenance_type: Preventive, Corrective, Inspection, Calibration

### Key Findings
1. **Q1 (Total Cost):** Sum of all cost_eur values
2. **Q2 (Total Downtime):** Sum of downtime_min values
3. **Q3 (Event Count):** Row count = 96
4. **Q4 (Unplanned):** Count where reason = "Unplanned Breakdown"
5. **Q5 (Average Downtime):** AVG(downtime_min)

### Data Quality Issues
- Some cost_eur values might be NULL (check for nulls)
- Some downtime_min values are 0 (monitoring-only events)
- Some parts_used are empty strings

### Next Step
Run PySpark pipeline to handle these quality issues programmatically.