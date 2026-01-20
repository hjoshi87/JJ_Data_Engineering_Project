-- ============================================================================
-- JOHNSON & JOHNSON MANUFACTURING ANALYTICS - SQL SOLUTIONS
-- ============================================================================

-- QUESTION 1: Total Maintenance Cost
-- Purpose: Budget tracking and cost forecasting
-- Expected: Single row with cost metrics

SELECT 
    SUM(cost_eur) AS total_maintenance_cost,
    COUNT(*) AS event_count,
    ROUND(AVG(cost_eur), 2) AS avg_cost_per_event,
    MIN(cost_eur) AS min_cost,
    MAX(cost_eur) AS max_cost
FROM maintenance_events
WHERE cost_eur IS NOT NULL 
  AND cost_eur >= 0;

-- Expected Result: ~€190,000 total across 96 events

-- ============================================================================

-- QUESTION 2: Total Downtime Minutes
-- Purpose: Production planning and SLA management

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

-- Expected Result: ~4,200 minutes (~70 hours)

-- ============================================================================

-- QUESTION 3: Total Maintenance Events
-- Purpose: Maintenance activity count and composition

SELECT 
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(DISTINCT factory_id) AS factory_count,
    COUNT(DISTINCT line_id) AS line_count,
    COUNT(DISTINCT maintenance_type) AS maintenance_type_count
FROM maintenance_events;

-- Expected Result: 96 total events, 3 lines, 5 maintenance types

-- ============================================================================

-- QUESTION 4: Unplanned Breakdowns
-- Purpose: Reliability metrics and reactive vs preventive split

SELECT 
    COUNT(*) AS unplanned_breakdowns,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM maintenance_events), 2) 
        AS pct_of_total_events,
    SUM(downtime_min) AS total_unplanned_downtime_min,
    ROUND(AVG(downtime_min), 2) AS avg_unplanned_downtime_min,
    SUM(cost_eur) AS total_unplanned_cost_eur,
    ROUND(AVG(cost_eur), 2) AS avg_unplanned_cost_eur
FROM maintenance_events
WHERE reason = 'Unplanned Breakdown';

-- Expected Result: ~26 breakdowns (27% of total)

-- ============================================================================

-- QUESTION 5: Average Downtime Per Event
-- Purpose: Process efficiency and response time analysis

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

-- Expected Result: ~45 minutes average downtime