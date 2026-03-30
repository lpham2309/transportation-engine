-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Boston Reliability Engine - Gold Layer (SQL)
-- MAGIC
-- MAGIC Business metrics and analytics tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Table: MBTA Route Reliability
-- MAGIC
-- MAGIC Reliability metrics aggregated by route, time, and weather.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fct_mbta_route_reliability (
  CONSTRAINT sufficient_sample_size EXPECT (trip_count >= 30)
)
COMMENT 'Reliability metrics for MBTA routes by stratification'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  stop_h3_index,
  route_id,
  route_long_name,
  hour_bucket,
  day_type,
  weather_condition,

  -- Counts
  COUNT(*) AS trip_count,
  SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_count,

  -- Delay statistics
  PERCENTILE_APPROX(delay_seconds, 0.50) AS p50_delay_sec,
  PERCENTILE_APPROX(delay_seconds, 0.90) AS p90_delay_sec,
  PERCENTILE_APPROX(delay_seconds, 0.95) AS p95_delay_sec,
  AVG(delay_seconds) AS mean_delay_sec,
  STDDEV(delay_seconds) AS stddev_delay_sec,

  -- On-time metrics
  CAST(SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS on_time_rate,
  CAST(SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS on_time_score,

  -- Volatility metrics
  CASE
    WHEN ABS(AVG(delay_seconds)) > 0.1
    THEN STDDEV(delay_seconds) / ABS(AVG(delay_seconds)) * 100
    ELSE 0.0
  END AS transit_volatility_cv,

  -- Reliability category (placeholder until formula finalized)
  CAST(NULL AS DOUBLE) AS stability_score,
  CAST(NULL AS DOUBLE) AS reliability_score,

  -- Categories
  'insufficient_formula' AS reliability_category,
  CASE
    WHEN STDDEV(delay_seconds) / NULLIF(ABS(AVG(delay_seconds)), 0) * 100 < 15 THEN 'low_volatility'
    WHEN STDDEV(delay_seconds) / NULLIF(ABS(AVG(delay_seconds)), 0) * 100 < 30 THEN 'medium_volatility'
    ELSE 'high_volatility'
  END AS volatility_category,

  -- Confidence
  CASE
    WHEN COUNT(*) < 30 THEN 'insufficient_data'
    WHEN COUNT(*) < 100 THEN 'low_confidence'
    ELSE 'standard_confidence'
  END AS confidence_level,

  -- Planning metrics
  PERCENTILE_APPROX(delay_seconds, 0.90) / 60.0 AS recommended_buffer_min,
  'MBTA' AS transportation_mode

FROM LIVE.stg_mbta_performance
WHERE delay_seconds IS NOT NULL
GROUP BY stop_h3_index, route_id, route_long_name, hour_bucket, day_type, weather_condition
HAVING COUNT(*) >= 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Table: Driving Route Reliability
-- MAGIC
-- MAGIC Reliability metrics for driving routes.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fct_driving_route_reliability (
  CONSTRAINT sufficient_sample_size EXPECT (trip_count >= 20)
)
COMMENT 'Reliability metrics for driving routes'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  origin_h3_index,
  destination_h3_index,
  route_name,
  hour_bucket,
  day_type,
  weather_condition,

  -- Counts
  COUNT(*) AS trip_count,
  SUM(CASE WHEN duration_in_traffic_seconds <= duration_seconds + 300 THEN 1 ELSE 0 END) AS on_time_count,

  -- Duration statistics
  PERCENTILE_APPROX(duration_in_traffic_seconds, 0.50) AS p50_duration_sec,
  PERCENTILE_APPROX(duration_in_traffic_seconds, 0.90) AS p90_duration_sec,
  PERCENTILE_APPROX(duration_in_traffic_seconds, 0.95) AS p95_duration_sec,
  AVG(duration_in_traffic_seconds) AS mean_duration_sec,
  STDDEV(duration_in_traffic_seconds) AS stddev_duration_sec,

  -- Baseline metrics
  AVG(duration_seconds) AS avg_baseline_duration_sec,
  AVG(traffic_delay_seconds) AS avg_traffic_delay_sec,
  AVG(traffic_impact_pct) AS avg_traffic_impact_pct,

  -- On-time metrics
  CAST(SUM(CASE WHEN duration_in_traffic_seconds <= duration_seconds + 300 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS on_time_rate,
  CAST(SUM(CASE WHEN duration_in_traffic_seconds <= duration_seconds + 300 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS on_time_score,

  -- Volatility
  CASE
    WHEN AVG(duration_in_traffic_seconds) > 0.1
    THEN STDDEV(duration_in_traffic_seconds) / AVG(duration_in_traffic_seconds) * 100
    ELSE 0.0
  END AS transit_volatility_cv,

  -- Reliability (placeholder)
  CAST(NULL AS DOUBLE) AS stability_score,
  CAST(NULL AS DOUBLE) AS reliability_score,

  -- Categories
  'insufficient_formula' AS reliability_category,
  CASE
    WHEN STDDEV(duration_in_traffic_seconds) / NULLIF(AVG(duration_in_traffic_seconds), 0) * 100 < 15 THEN 'low_volatility'
    WHEN STDDEV(duration_in_traffic_seconds) / NULLIF(AVG(duration_in_traffic_seconds), 0) * 100 < 30 THEN 'medium_volatility'
    ELSE 'high_volatility'
  END AS volatility_category,

  CASE
    WHEN COUNT(*) < 20 THEN 'insufficient_data'
    WHEN COUNT(*) < 50 THEN 'low_confidence'
    ELSE 'standard_confidence'
  END AS confidence_level,

  -- Planning metrics
  PERCENTILE_APPROX(duration_in_traffic_seconds, 0.50) / 60.0 AS median_duration_min,
  PERCENTILE_APPROX(duration_in_traffic_seconds, 0.90) / 60.0 AS recommended_buffer_min,
  AVG(duration_seconds) / 60.0 AS baseline_duration_min,
  'Driving' AS transportation_mode

FROM LIVE.stg_driving_performance
WHERE duration_in_traffic_seconds IS NOT NULL
GROUP BY origin_h3_index, destination_h3_index, route_name, hour_bucket, day_type, weather_condition
HAVING COUNT(*) >= 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Table: Mode Comparison
-- MAGIC
-- MAGIC Side-by-side comparison of MBTA and Driving.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fct_mode_comparison
COMMENT 'Side-by-side comparison of MBTA and Driving transportation modes'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH all_modes AS (
  -- MBTA data
  SELECT
    stop_h3_index AS location_h3,
    route_id,
    route_long_name,
    hour_bucket,
    day_type,
    weather_condition,
    transportation_mode,
    reliability_score,
    reliability_category,
    p50_delay_sec / 60.0 AS median_time_min,
    p90_delay_sec / 60.0 AS p90_time_min,
    transit_volatility_cv,
    trip_count,
    confidence_level
  FROM LIVE.fct_mbta_route_reliability

  UNION ALL

  -- Driving data
  SELECT
    origin_h3_index AS location_h3,
    route_name AS route_id,
    route_name AS route_long_name,
    hour_bucket,
    day_type,
    weather_condition,
    transportation_mode,
    reliability_score,
    reliability_category,
    median_duration_min AS median_time_min,
    recommended_buffer_min AS p90_time_min,
    transit_volatility_cv,
    trip_count,
    confidence_level
  FROM LIVE.fct_driving_route_reliability
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition ORDER BY reliability_score DESC NULLS LAST) AS rank_by_reliability,
    ROW_NUMBER() OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition ORDER BY median_time_min ASC NULLS LAST) AS rank_by_speed,
    ROW_NUMBER() OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition ORDER BY transit_volatility_cv ASC NULLS LAST) AS rank_by_predictability,
    FIRST_VALUE(transportation_mode) OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition ORDER BY reliability_score DESC NULLS LAST) AS most_reliable_mode,
    FIRST_VALUE(transportation_mode) OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition ORDER BY median_time_min ASC NULLS LAST) AS fastest_mode,
    MIN(median_time_min) OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition) AS min_median_time,
    MAX(reliability_score) OVER (PARTITION BY location_h3, hour_bucket, day_type, weather_condition) AS max_reliability
  FROM all_modes
)

SELECT
  location_h3,
  route_id,
  route_long_name,
  hour_bucket,
  day_type,
  weather_condition,
  transportation_mode,
  reliability_score,
  reliability_category,
  median_time_min,
  p90_time_min,
  transit_volatility_cv,
  trip_count,
  confidence_level,
  rank_by_reliability,
  rank_by_speed,
  rank_by_predictability,
  most_reliable_mode,
  fastest_mode,
  -- Recommendation
  CASE
    WHEN rank_by_reliability = 1 AND rank_by_speed <= 2 THEN transportation_mode
    WHEN reliability_score >= 85 AND rank_by_speed = 1 THEN transportation_mode
    WHEN rank_by_reliability = 1 THEN transportation_mode
    ELSE NULL
  END AS is_recommended,
  -- Deltas
  median_time_min - min_median_time AS time_delta_from_fastest_min,
  reliability_score - max_reliability AS reliability_delta_from_best
FROM ranked;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Table: Weather Impact
-- MAGIC
-- MAGIC Analysis of weather impact on transportation.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fct_weather_impact
COMMENT 'Analysis of weather impact on transportation reliability'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH all_modes AS (
  -- MBTA
  SELECT
    'MBTA' AS transportation_mode,
    route_id,
    route_long_name,
    stop_h3_index AS location_h3,
    hour_bucket,
    day_type,
    weather_condition,
    reliability_score,
    p50_delay_sec / 60.0 AS median_time_min,
    p90_delay_sec / 60.0 AS p90_time_min,
    transit_volatility_cv,
    trip_count
  FROM LIVE.fct_mbta_route_reliability

  UNION ALL

  -- Driving
  SELECT
    'Driving' AS transportation_mode,
    route_name AS route_id,
    route_name AS route_long_name,
    origin_h3_index AS location_h3,
    hour_bucket,
    day_type,
    weather_condition,
    reliability_score,
    median_duration_min AS median_time_min,
    recommended_buffer_min AS p90_time_min,
    transit_volatility_cv,
    trip_count
  FROM LIVE.fct_driving_route_reliability
),

with_weather_order AS (
  SELECT
    *,
    CASE
      WHEN weather_condition = 'clear' THEN 1
      WHEN weather_condition = 'rain' THEN 2
      WHEN weather_condition = 'snow' THEN 3
      ELSE 4
    END AS weather_order
  FROM all_modes
),

with_lags AS (
  SELECT
    *,
    reliability_score - LAG(reliability_score) OVER (
      PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
      ORDER BY weather_order
    ) AS reliability_delta_from_clear,
    median_time_min - LAG(median_time_min) OVER (
      PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
      ORDER BY weather_order
    ) AS time_delta_from_clear_min,
    transit_volatility_cv - LAG(transit_volatility_cv) OVER (
      PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
      ORDER BY weather_order
    ) AS volatility_delta_from_clear
  FROM with_weather_order
)

SELECT
  transportation_mode,
  route_id,
  route_long_name,
  location_h3,
  hour_bucket,
  day_type,
  weather_condition,
  reliability_score,
  median_time_min,
  p90_time_min,
  transit_volatility_cv,
  trip_count,
  reliability_delta_from_clear,
  time_delta_from_clear_min,
  volatility_delta_from_clear,
  -- Weather impact category
  CASE
    WHEN ABS(reliability_delta_from_clear) >= 20 THEN 'major_impact'
    WHEN ABS(reliability_delta_from_clear) >= 10 THEN 'moderate_impact'
    WHEN ABS(reliability_delta_from_clear) >= 5 THEN 'minor_impact'
    ELSE 'minimal_impact'
  END AS weather_impact_category,
  -- Weather resilience score
  CASE
    WHEN weather_condition = 'clear' THEN NULL
    ELSE 100.0 - LEAST(100.0, ABS(COALESCE(reliability_delta_from_clear, 0)))
  END AS weather_resilience_score
FROM with_lags;
