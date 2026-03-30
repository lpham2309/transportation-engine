-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Boston Reliability Engine - Bronze Layer (SQL)
-- MAGIC
-- MAGIC Raw data ingestion from Databricks Volume using Auto Loader.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Table: MBTA Predictions

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_mbta_predictions (
  CONSTRAINT valid_prediction_id EXPECT (prediction_id IS NOT NULL),
  CONSTRAINT valid_route_id EXPECT (route_id IS NOT NULL)
)
COMMENT 'Raw MBTA real-time predictions'
PARTITIONED BY (fetched_date)
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  prediction_id,
  CAST(arrival_time AS TIMESTAMP) AS arrival_time,
  CAST(departure_time AS TIMESTAMP) AS departure_time,
  CAST(direction_id AS INT) AS direction_id,
  schedule_relationship,
  status,
  stop_id,
  stop_name,
  CAST(stop_latitude AS DOUBLE) AS stop_latitude,
  CAST(stop_longitude AS DOUBLE) AS stop_longitude,
  stop_h3_index,
  route_id,
  route_long_name,
  CAST(route_type AS INT) AS route_type,
  trip_id,
  CAST(fetched_at AS TIMESTAMP) AS fetched_at,
  CAST(fetched_date AS DATE) AS fetched_date,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine/raw/mbta_predictions/',
  format => 'json',
  inferColumnTypes => 'false'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Table: MBTA Schedules

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_mbta_schedules (
  CONSTRAINT valid_schedule_id EXPECT (schedule_id IS NOT NULL),
  CONSTRAINT valid_route_id EXPECT (route_id IS NOT NULL)
)
COMMENT 'Raw MBTA scheduled arrivals'
PARTITIONED BY (schedule_date)
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  schedule_id,
  CAST(arrival_time AS TIMESTAMP) AS arrival_time,
  CAST(departure_time AS TIMESTAMP) AS departure_time,
  CAST(direction_id AS INT) AS direction_id,
  CAST(drop_off_type AS INT) AS drop_off_type,
  CAST(pickup_type AS INT) AS pickup_type,
  CAST(stop_sequence AS INT) AS stop_sequence,
  CAST(timepoint AS BOOLEAN) AS timepoint,
  stop_id,
  stop_name,
  CAST(stop_latitude AS DOUBLE) AS stop_latitude,
  CAST(stop_longitude AS DOUBLE) AS stop_longitude,
  stop_h3_index,
  route_id,
  route_long_name,
  CAST(route_type AS INT) AS route_type,
  trip_id,
  CAST(schedule_date AS DATE) AS schedule_date,
  CAST(fetched_at AS TIMESTAMP) AS fetched_at,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine/raw/mbta_schedules/',
  format => 'json',
  inferColumnTypes => 'false'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Table: Open-Meteo Weather

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_openmeteo_weather (
  CONSTRAINT valid_observation_date EXPECT (observation_date IS NOT NULL),
  CONSTRAINT valid_weather_condition EXPECT (weather_condition IN ('clear', 'rain', 'snow'))
)
COMMENT 'Raw daily weather observations from Open-Meteo API'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  CAST(observation_date AS DATE) AS observation_date,
  station_id,
  station_name,
  CAST(precipitation_inches AS DOUBLE) AS precipitation_inches,
  CAST(temp_max_f AS DOUBLE) AS temp_max_f,
  CAST(temp_min_f AS DOUBLE) AS temp_min_f,
  CAST(snow_inches AS DOUBLE) AS snow_inches,
  CAST(snow_depth_inches AS DOUBLE) AS snow_depth_inches,
  CAST(wind_speed_mph AS DOUBLE) AS wind_speed_mph,
  weather_condition,
  CAST(fetched_at AS TIMESTAMP) AS fetched_at,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine/raw/openmeteo_weather/',
  format => 'json',
  inferColumnTypes => 'false'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Table: Driving Routes

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_driving_routes (
  CONSTRAINT valid_route_name EXPECT (route_name IS NOT NULL),
  CONSTRAINT valid_duration EXPECT (duration_seconds IS NOT NULL AND duration_seconds > 0)
)
COMMENT 'Raw Google Maps driving times with traffic data'
PARTITIONED BY (fetched_date)
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  route_name,
  origin_address,
  CAST(origin_latitude AS DOUBLE) AS origin_latitude,
  CAST(origin_longitude AS DOUBLE) AS origin_longitude,
  origin_h3_index,
  destination_address,
  CAST(destination_latitude AS DOUBLE) AS destination_latitude,
  CAST(destination_longitude AS DOUBLE) AS destination_longitude,
  destination_h3_index,
  CAST(distance_meters AS INT) AS distance_meters,
  CAST(duration_seconds AS INT) AS duration_seconds,
  CAST(duration_in_traffic_seconds AS INT) AS duration_in_traffic_seconds,
  CAST(departure_time AS TIMESTAMP) AS departure_time,
  CAST(fetched_date AS DATE) AS fetched_date,
  CAST(fetched_at AS TIMESTAMP) AS fetched_at,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine/raw/driving_routes/',
  format => 'json',
  inferColumnTypes => 'false'
);
