-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Boston Reliability Engine - Silver Layer (SQL)
-- MAGIC
-- MAGIC Cleaned and enriched data with business logic transformations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Table: Weather Daily
-- MAGIC
-- MAGIC Enhanced weather data with severity scoring.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE stg_weather_daily (
  CONSTRAINT valid_date EXPECT (observation_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_condition EXPECT (weather_condition IN ('clear', 'rain', 'snow'))
)
COMMENT 'Enhanced weather data with severity scoring'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  observation_date,
  station_id,
  station_name,
  precipitation_inches,
  temp_max_f,
  temp_min_f,
  snow_inches,
  snow_depth_inches,
  wind_speed_mph,
  weather_condition,
  fetched_at,
  -- Detailed weather categorization
  CASE
    WHEN snow_inches > 2 THEN 'heavy_snow'
    WHEN snow_inches > 0 THEN 'light_snow'
    WHEN precipitation_inches > 0.5 THEN 'heavy_rain'
    WHEN precipitation_inches > 0.1 THEN 'light_rain'
    WHEN temp_max_f < 20 THEN 'extreme_cold'
    ELSE 'clear'
  END AS weather_category_detailed,
  -- Weather severity score (0-100)
  LEAST(100,
    COALESCE(precipitation_inches * 10, 0) +
    COALESCE(snow_inches * 20, 0) +
    CASE WHEN temp_max_f < 20 THEN 30 WHEN temp_max_f > 95 THEN 20 ELSE 0 END +
    CASE WHEN wind_speed_mph > 30 THEN 20 WHEN wind_speed_mph > 15 THEN 10 ELSE 0 END
  ) AS weather_severity_score
FROM LIVE.bronze_openmeteo_weather;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Table: MBTA Performance
-- MAGIC
-- MAGIC Predictions joined with schedules to calculate delays.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE stg_mbta_performance (
  CONSTRAINT valid_delay EXPECT (delay_seconds IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_schedule EXPECT (scheduled_arrival IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_hour_bucket EXPECT (hour_bucket IN ('AM_PEAK', 'PM_PEAK', 'MIDDAY', 'OFF_PEAK')),
  CONSTRAINT valid_day_type EXPECT (day_type IN ('weekday', 'weekend'))
)
COMMENT 'MBTA predictions joined with schedules, enriched with weather'
PARTITIONED BY (schedule_date)
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  -- Surrogate key
  MD5(CONCAT(COALESCE(p.prediction_id, '_null_'), '||', COALESCE(s.schedule_id, '_null_'))) AS performance_id,

  -- Time fields
  p.arrival_time AS predicted_arrival,
  s.arrival_time AS scheduled_arrival,

  -- Delay calculation
  CAST(UNIX_TIMESTAMP(p.arrival_time) - UNIX_TIMESTAMP(s.arrival_time) AS INT) AS delay_seconds,

  -- On-time flag (within 5 minutes = 300 seconds)
  CASE WHEN UNIX_TIMESTAMP(p.arrival_time) - UNIX_TIMESTAMP(s.arrival_time) <= 300 THEN TRUE ELSE FALSE END AS is_on_time,

  -- Location
  p.stop_id,
  p.stop_h3_index,

  -- Route info
  p.route_id,
  p.route_long_name,

  -- Date and time
  s.schedule_date,
  HOUR(s.arrival_time) AS hour,

  -- Hour bucket stratification
  CASE
    WHEN HOUR(s.arrival_time) >= 6 AND HOUR(s.arrival_time) < 9 THEN 'AM_PEAK'
    WHEN HOUR(s.arrival_time) >= 16 AND HOUR(s.arrival_time) < 19 THEN 'PM_PEAK'
    WHEN HOUR(s.arrival_time) >= 9 AND HOUR(s.arrival_time) < 16 THEN 'MIDDAY'
    ELSE 'OFF_PEAK'
  END AS hour_bucket,

  -- Day type
  CASE WHEN DAYOFWEEK(s.schedule_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,

  -- Trip info
  p.trip_id,
  p.fetched_at,

  -- Weather (joined)
  COALESCE(w.weather_condition, 'clear') AS weather_condition,
  w.precipitation_inches,
  w.temp_max_f,
  w.temp_min_f,
  w.snow_inches,
  w.wind_speed_mph,
  w.weather_category_detailed,
  w.weather_severity_score

FROM LIVE.bronze_mbta_predictions p
INNER JOIN LIVE.bronze_mbta_schedules s
  ON p.trip_id = s.trip_id AND p.stop_id = s.stop_id
LEFT JOIN LIVE.stg_weather_daily w
  ON DATE(s.arrival_time) = w.observation_date
WHERE s.arrival_time IS NOT NULL AND p.arrival_time IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Table: Driving Performance
-- MAGIC
-- MAGIC Driving data with traffic impact calculations.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE stg_driving_performance (
  CONSTRAINT valid_duration EXPECT (duration_in_traffic_seconds IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_hour_bucket EXPECT (hour_bucket IN ('AM_PEAK', 'PM_PEAK', 'MIDDAY', 'OFF_PEAK')),
  CONSTRAINT valid_day_type EXPECT (day_type IN ('weekday', 'weekend'))
)
COMMENT 'Driving data enriched with weather and traffic impact'
PARTITIONED BY (fetched_date)
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  -- Surrogate key
  MD5(CONCAT(COALESCE(d.route_name, '_null_'), '||', COALESCE(CAST(d.departure_time AS STRING), '_null_'), '||', COALESCE(CAST(d.fetched_at AS STRING), '_null_'))) AS driving_id,

  -- Route info
  d.route_name,
  d.origin_latitude,
  d.origin_longitude,
  d.origin_h3_index,
  d.destination_latitude,
  d.destination_longitude,
  d.destination_h3_index,

  -- Duration metrics
  d.distance_meters,
  d.duration_seconds,
  d.duration_in_traffic_seconds,
  d.departure_time,
  d.fetched_date,
  d.fetched_at,

  -- Time stratification
  HOUR(d.departure_time) AS hour,
  CASE
    WHEN HOUR(d.departure_time) >= 6 AND HOUR(d.departure_time) < 9 THEN 'AM_PEAK'
    WHEN HOUR(d.departure_time) >= 16 AND HOUR(d.departure_time) < 19 THEN 'PM_PEAK'
    WHEN HOUR(d.departure_time) >= 9 AND HOUR(d.departure_time) < 16 THEN 'MIDDAY'
    ELSE 'OFF_PEAK'
  END AS hour_bucket,
  CASE WHEN DAYOFWEEK(d.fetched_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,

  -- Traffic impact
  CAST(d.duration_in_traffic_seconds - d.duration_seconds AS INT) AS traffic_delay_seconds,
  CASE
    WHEN d.duration_seconds > 0
    THEN CAST((d.duration_in_traffic_seconds - d.duration_seconds) AS DOUBLE) / d.duration_seconds * 100
    ELSE 0.0
  END AS traffic_impact_pct,

  -- Weather (joined)
  COALESCE(w.weather_condition, 'clear') AS weather_condition,
  w.precipitation_inches,
  w.temp_max_f,
  w.weather_category_detailed,
  w.weather_severity_score

FROM LIVE.bronze_driving_routes d
LEFT JOIN LIVE.stg_weather_daily w
  ON d.fetched_date = w.observation_date;
