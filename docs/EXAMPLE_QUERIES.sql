-- ============================================================================
-- Boston Reliability Engine - Example Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Route Recommendation for Specific Commute
-- Use Case: "Which mode should I take from Kendall to Financial District at 8am?"
-- ----------------------------------------------------------------------------

SELECT
    transportation_mode,
    reliability_score,
    reliability_category,
    median_time_min,
    p90_time_min,
    transit_volatility_cv,
    most_reliable_mode,
    fastest_mode,
    is_recommended,
    time_delta_from_fastest_min
FROM capstone.fct_mode_comparison
WHERE location_h3 = '8844c2a9bffffff'  -- Kendall Square area
  AND hour_bucket = 'AM_PEAK'
  AND day_type = 'weekday'
  AND weather_condition = 'clear'
ORDER BY reliability_score DESC;


-- ----------------------------------------------------------------------------
-- Query 2: Weather Impact Analysis
-- Use Case: "How much does rain affect my commute?"
-- ----------------------------------------------------------------------------

SELECT
    transportation_mode,
    weather_condition,
    reliability_score,
    median_time_min,
    reliability_delta_from_clear,
    time_delta_from_clear_min,
    weather_impact_category,
    weather_resilience_score
FROM capstone.fct_weather_impact
WHERE location_h3 = '8844c2a9bffffff'
  AND hour_bucket = 'AM_PEAK'
  AND day_type = 'weekday'
  AND transportation_mode IN ('MBTA', 'Driving')
ORDER BY
    transportation_mode,
    CASE weather_condition
        WHEN 'clear' THEN 1
        WHEN 'rain' THEN 2
        WHEN 'snow' THEN 3
    END;


-- ----------------------------------------------------------------------------
-- Query 3: MBTA Line Performance Rankings
-- Use Case: "Which MBTA routes are most reliable?"
-- ----------------------------------------------------------------------------

SELECT
    route_id,
    route_long_name,
    AVG(reliability_score) AS avg_reliability,
    AVG(p50_delay_sec) / 60.0 AS avg_median_delay_min,
    AVG(transit_volatility_cv) AS avg_volatility,
    SUM(trip_count) AS total_observations,
    COUNT(DISTINCT stop_h3_index) AS num_stops_analyzed
FROM capstone.fct_mbta_route_reliability
WHERE day_type = 'weekday'
  AND hour_bucket IN ('AM_PEAK', 'PM_PEAK')
GROUP BY route_id, route_long_name
ORDER BY avg_reliability DESC;


-- ----------------------------------------------------------------------------
-- Query 4: Peak vs Off-Peak Comparison
-- Use Case: "Should I leave earlier/later to avoid delays?"
-- ----------------------------------------------------------------------------

SELECT
    hour_bucket,
    transportation_mode,
    AVG(reliability_score) AS avg_reliability,
    AVG(median_time_min) AS avg_median_time_min,
    AVG(p90_time_min) AS avg_p90_time_min,
    COUNT(*) AS route_scenarios
FROM capstone.fct_mode_comparison
WHERE day_type = 'weekday'
  AND weather_condition = 'clear'
GROUP BY hour_bucket, transportation_mode
ORDER BY hour_bucket, avg_reliability DESC;


-- ----------------------------------------------------------------------------
-- Query 5: Worst Performing MBTA Segments
-- Use Case: "Which stations have the most delays?"
-- ----------------------------------------------------------------------------

SELECT
    stop_h3_index,
    route_id,
    route_long_name,
    hour_bucket,
    reliability_score,
    reliability_category,
    p50_delay_sec / 60.0 AS median_delay_min,
    p90_delay_sec / 60.0 AS p90_delay_min,
    transit_volatility_cv,
    trip_count
FROM capstone.fct_mbta_route_reliability
WHERE day_type = 'weekday'
  AND weather_condition = 'clear'
  AND hour_bucket IN ('AM_PEAK', 'PM_PEAK')
ORDER BY reliability_score ASC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- Query 6: Traffic Impact on Driving
-- Use Case: "How much does traffic add to my drive time?"
-- ----------------------------------------------------------------------------

SELECT
    route_name,
    hour_bucket,
    weather_condition,
    baseline_duration_min,
    median_duration_min,
    median_duration_min - baseline_duration_min AS traffic_added_time_min,
    avg_traffic_impact_pct,
    reliability_score,
    trip_count
FROM capstone.fct_driving_route_reliability
WHERE day_type = 'weekday'
ORDER BY avg_traffic_impact_pct DESC;


-- ----------------------------------------------------------------------------
-- Query 7: Mode Alpha - Best Mode by Weather
-- Use Case: "What's the best mode for each weather condition?"
-- ----------------------------------------------------------------------------

SELECT
    location_h3,
    hour_bucket,
    weather_condition,
    most_reliable_mode,
    fastest_mode,
    most_predictable_mode,
    COUNT(*) AS scenarios_analyzed
FROM capstone.fct_mode_comparison
WHERE day_type = 'weekday'
  AND rank_by_reliability = 1  -- Only show the winner
GROUP BY
    location_h3,
    hour_bucket,
    weather_condition,
    most_reliable_mode,
    fastest_mode,
    most_predictable_mode
ORDER BY location_h3, hour_bucket, weather_condition;


-- ----------------------------------------------------------------------------
-- Query 8: Reliability Over Time (requires date partitioning)
-- Use Case: "Is MBTA reliability improving?"
-- ----------------------------------------------------------------------------

-- Note: This requires adding date fields to fact tables
-- Placeholder query structure:

/*
SELECT
    DATE_TRUNC('week', schedule_date) AS week,
    route_id,
    AVG(reliability_score) AS avg_weekly_reliability,
    AVG(on_time_rate) AS avg_on_time_rate
FROM capstone.fct_mbta_route_reliability
WHERE schedule_date >= CURRENT_DATE - 90  -- Last 3 months
GROUP BY week, route_id
ORDER BY week DESC, route_id;
*/


-- ----------------------------------------------------------------------------
-- Query 9: Confidence Levels Check
-- Use Case: "Which routes have enough data for reliable analysis?"
-- ----------------------------------------------------------------------------

SELECT
    transportation_mode,
    confidence_level,
    COUNT(*) AS num_route_scenarios,
    AVG(trip_count) AS avg_observations,
    MIN(trip_count) AS min_observations,
    MAX(trip_count) AS max_observations
FROM capstone.fct_mode_comparison
GROUP BY transportation_mode, confidence_level
ORDER BY transportation_mode, confidence_level;


-- ----------------------------------------------------------------------------
-- Query 10: Daily Commute Planner
-- Use Case: "Give me recommendations for my commute today"
-- ----------------------------------------------------------------------------

-- Replace with your actual location H3 and time preferences
WITH my_commute AS (
    SELECT '8844c2a9bffffff' AS my_location_h3,  -- Replace with your origin
           'AM_PEAK' AS my_time,
           'weekday' AS my_day_type,
           'clear' AS current_weather  -- Update based on today's weather
),

recommendations AS (
    SELECT
        mc.transportation_mode,
        mc.reliability_score,
        mc.reliability_category,
        mc.median_time_min,
        mc.p90_time_min,
        mc.is_recommended,
        mc.time_delta_from_fastest_min,
        mc.reliability_delta_from_best,
        mc.most_reliable_mode,
        mc.fastest_mode
    FROM capstone.fct_mode_comparison mc
    CROSS JOIN my_commute c
    WHERE mc.location_h3 = c.my_location_h3
      AND mc.hour_bucket = c.my_time
      AND mc.day_type = c.my_day_type
      AND mc.weather_condition = c.current_weather
)

SELECT
    *,
    CASE
        WHEN is_recommended IS NOT NULL THEN '🌟 RECOMMENDED'
        WHEN reliability_score >= 85 THEN '✅ Good Option'
        WHEN reliability_score >= 70 THEN '⚠️ Moderate Option'
        ELSE '❌ Not Recommended'
    END AS recommendation_flag
FROM recommendations
ORDER BY reliability_score DESC;


-- ============================================================================
-- Data Quality Validation Queries
-- ============================================================================

-- Check 1: Verify all reliability scores are between 0-100
SELECT 'MBTA' AS source, COUNT(*) AS invalid_scores
FROM capstone.fct_mbta_route_reliability
WHERE reliability_score < 0 OR reliability_score > 100
UNION ALL
SELECT 'Driving', COUNT(*)
FROM capstone.fct_driving_route_reliability
WHERE reliability_score < 0 OR reliability_score > 100;


-- Check 2: Verify percentile ordering (P95 >= P90 >= P50)
SELECT COUNT(*) AS invalid_percentile_orders
FROM capstone.fct_mbta_route_reliability
WHERE p95_delay_sec < p90_delay_sec
   OR p90_delay_sec < p50_delay_sec;


-- Check 3: Verify minimum sample sizes
SELECT
    confidence_level,
    COUNT(*) AS num_routes,
    MIN(trip_count) AS min_sample,
    AVG(trip_count) AS avg_sample
FROM capstone.fct_mbta_route_reliability
GROUP BY confidence_level
ORDER BY confidence_level;


-- ============================================================================
-- Summary Statistics
-- ============================================================================

-- Overall platform statistics
SELECT
    'Total MBTA Routes Analyzed' AS metric,
    COUNT(DISTINCT CONCAT(stop_h3_index, '-', route_id))::VARCHAR AS value
FROM capstone.fct_mbta_route_reliability
UNION ALL
SELECT
    'Total Driving Routes Analyzed',
    COUNT(DISTINCT route_name)::VARCHAR
FROM capstone.fct_driving_route_reliability
UNION ALL
SELECT
    'Total Observations (MBTA)',
    SUM(trip_count)::VARCHAR
FROM capstone.fct_mbta_route_reliability
UNION ALL
SELECT
    'Total Observations (Driving)',
    SUM(trip_count)::VARCHAR
FROM capstone.fct_driving_route_reliability
UNION ALL
SELECT
    'Average MBTA Reliability Score',
    ROUND(AVG(reliability_score), 2)::VARCHAR
FROM capstone.fct_mbta_route_reliability
UNION ALL
SELECT
    'Average Driving Reliability Score',
    ROUND(AVG(reliability_score), 2)::VARCHAR
FROM capstone.fct_driving_route_reliability;