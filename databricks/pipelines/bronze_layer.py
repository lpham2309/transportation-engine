# Databricks notebook source
# MAGIC %md
# MAGIC # Boston Reliability Engine - Bronze Layer
# MAGIC
# MAGIC This notebook defines Delta Live Tables for the Bronze (raw) layer.
# MAGIC Data is ingested from S3 via Auto Loader for streaming/incremental processing.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Volume paths for raw data landing zone
VOLUME_BASE_PATH = "/Volumes/bootcamp_students/zachy_lam_pham3110/boston-reliability-engine/raw"
CHECKPOINT_BASE = "/checkpoints/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: MBTA Predictions (Streaming)

# COMMAND ----------

@dlt.table(
    name="bronze_mbta_predictions",
    comment="Raw MBTA real-time predictions with H3 geospatial indexing",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "stop_h3_index,route_id"
    },
    partition_cols=["fetched_date"]
)
@dlt.expect("valid_prediction_id", "prediction_id IS NOT NULL")
@dlt.expect("valid_route_id", "route_id IS NOT NULL")
def bronze_mbta_predictions():
    """
    Ingests MBTA predictions from S3 using Auto Loader.
    Supports both streaming and batch processing.
    """
    schema = StructType([
        StructField("prediction_id", StringType(), False),
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("schedule_relationship", StringType(), True),
        StructField("status", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_latitude", DoubleType(), True),
        StructField("stop_longitude", DoubleType(), True),
        StructField("stop_h3_index", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_type", IntegerType(), True),
        StructField("trip_id", StringType(), True),
        StructField("fetched_at", StringType(), True),
        StructField("fetched_date", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/mbta_predictions/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(f"{VOLUME_BASE_PATH}/mbta_predictions/")
        .withColumn("arrival_time", to_timestamp("arrival_time"))
        .withColumn("departure_time", to_timestamp("departure_time"))
        .withColumn("fetched_at", to_timestamp("fetched_at"))
        .withColumn("fetched_date", to_date("fetched_date"))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: MBTA Schedules (Streaming)

# COMMAND ----------

@dlt.table(
    name="bronze_mbta_schedules",
    comment="Raw MBTA scheduled arrivals with H3 geospatial indexing",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "schedule_date,route_id"
    },
    partition_cols=["schedule_date"]
)
@dlt.expect("valid_schedule_id", "schedule_id IS NOT NULL")
@dlt.expect("valid_route_id", "route_id IS NOT NULL")
def bronze_mbta_schedules():
    """
    Ingests MBTA schedules from S3 using Auto Loader.
    """
    schema = StructType([
        StructField("schedule_id", StringType(), False),
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("drop_off_type", IntegerType(), True),
        StructField("pickup_type", IntegerType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("timepoint", BooleanType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_latitude", DoubleType(), True),
        StructField("stop_longitude", DoubleType(), True),
        StructField("stop_h3_index", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_type", IntegerType(), True),
        StructField("trip_id", StringType(), True),
        StructField("schedule_date", StringType(), True),
        StructField("fetched_at", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/mbta_schedules/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(f"{VOLUME_BASE_PATH}/mbta_schedules/")
        .withColumn("arrival_time", to_timestamp("arrival_time"))
        .withColumn("departure_time", to_timestamp("departure_time"))
        .withColumn("fetched_at", to_timestamp("fetched_at"))
        .withColumn("schedule_date", to_date("schedule_date"))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: NOAA Weather (Batch)

# COMMAND ----------

@dlt.table(
    name="bronze_noaa_weather",
    comment="Raw daily weather observations from Boston Logan Airport",
    table_properties={
        "quality": "bronze"
    }
)
@dlt.expect("valid_observation_date", "observation_date IS NOT NULL")
@dlt.expect("valid_weather_condition", "weather_condition IN ('clear', 'rain', 'snow')")
def bronze_noaa_weather():
    """
    Ingests NOAA weather data from S3.
    Daily batch processing (weather data is released daily).
    """
    schema = StructType([
        StructField("observation_date", StringType(), False),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("precipitation_inches", DoubleType(), True),
        StructField("temp_max_f", DoubleType(), True),
        StructField("temp_min_f", DoubleType(), True),
        StructField("snow_inches", DoubleType(), True),
        StructField("snow_depth_inches", DoubleType(), True),
        StructField("wind_speed_mph", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("fetched_at", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/noaa_weather/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(f"{VOLUME_BASE_PATH}/noaa_weather/")
        .withColumn("observation_date", to_date("observation_date"))
        .withColumn("fetched_at", to_timestamp("fetched_at"))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: Driving Routes (Batch)

# COMMAND ----------

@dlt.table(
    name="bronze_driving_routes",
    comment="Raw Google Maps driving times with traffic data",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["fetched_date"]
)
@dlt.expect("valid_route_name", "route_name IS NOT NULL")
@dlt.expect("valid_duration", "duration_seconds IS NOT NULL AND duration_seconds > 0")
def bronze_driving_routes():
    """
    Ingests Google Maps driving data from S3.
    """
    schema = StructType([
        StructField("route_name", StringType(), True),
        StructField("origin_address", StringType(), True),
        StructField("origin_latitude", DoubleType(), True),
        StructField("origin_longitude", DoubleType(), True),
        StructField("origin_h3_index", StringType(), True),
        StructField("destination_address", StringType(), True),
        StructField("destination_latitude", DoubleType(), True),
        StructField("destination_longitude", DoubleType(), True),
        StructField("destination_h3_index", StringType(), True),
        StructField("distance_meters", IntegerType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("duration_in_traffic_seconds", IntegerType(), True),
        StructField("departure_time", StringType(), True),
        StructField("fetched_date", StringType(), True),
        StructField("fetched_at", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/driving_routes/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(f"{VOLUME_BASE_PATH}/driving_routes/")
        .withColumn("departure_time", to_timestamp("departure_time"))
        .withColumn("fetched_at", to_timestamp("fetched_at"))
        .withColumn("fetched_date", to_date("fetched_date"))
        .withColumn("_ingested_at", current_timestamp())
    )
