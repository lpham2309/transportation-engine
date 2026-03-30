# Databricks notebook source
# MAGIC %md
# MAGIC # Boston Reliability Engine - Silver Layer
# MAGIC
# MAGIC This notebook defines Delta Live Tables for the Silver (cleaned/enriched) layer.
# MAGIC Transforms Bronze data with business logic, joins, and data quality expectations.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: stg_weather_daily
# MAGIC
# MAGIC Enhanced weather data with severity scoring and detailed categorization.
# MAGIC Must be processed first as other silver tables depend on it.

# COMMAND ----------

@dlt.table(
    name="stg_weather_daily",
    comment="Enhanced weather data with severity scoring",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_date", "observation_date IS NOT NULL")
@dlt.expect("valid_condition", "weather_condition IN ('clear', 'rain', 'snow')")
def stg_weather_daily():
    """
    Transforms raw weather data with categorization and severity scoring.
    Migrated from: dbt_project/models/capstone/staging/stg_weather_daily.sql
    """
    return (
        dlt.read("bronze_openmeteo_weather")
        # Detailed weather categorization
        .withColumn(
            "weather_category_detailed",
            when(col("snow_inches") > 2, "heavy_snow")
            .when(col("snow_inches") > 0, "light_snow")
            .when(col("precipitation_inches") > 0.5, "heavy_rain")
            .when(col("precipitation_inches") > 0.1, "light_rain")
            .when(col("temp_max_f") < 20, "extreme_cold")
            .otherwise("clear")
        )
        # Weather severity score (0-100)
        .withColumn(
            "weather_severity_score",
            least(
                lit(100),
                # Precipitation impact
                coalesce(col("precipitation_inches") * 10, lit(0)) +
                # Snow impact (higher weight)
                coalesce(col("snow_inches") * 20, lit(0)) +
                # Temperature impact
                when(col("temp_max_f") < 20, 30)
                .when(col("temp_max_f") > 95, 20)
                .otherwise(0) +
                # Wind impact
                when(col("wind_speed_mph") > 30, 20)
                .when(col("wind_speed_mph") > 15, 10)
                .otherwise(0)
            )
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: stg_mbta_performance
# MAGIC
# MAGIC Joins MBTA predictions with schedules to calculate delays.
# MAGIC Enriched with weather data and time-based stratification.

# COMMAND ----------

@dlt.table(
    name="stg_mbta_performance",
    comment="MBTA predictions joined with schedules, enriched with weather",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "stop_h3_index,route_id,schedule_date"
    },
    partition_cols=["schedule_date"]
)
@dlt.expect_or_drop("valid_delay", "delay_seconds IS NOT NULL")
@dlt.expect_or_drop("valid_schedule", "scheduled_arrival IS NOT NULL")
@dlt.expect("valid_hour_bucket", "hour_bucket IN ('AM_PEAK', 'PM_PEAK', 'MIDDAY', 'OFF_PEAK')")
@dlt.expect("valid_day_type", "day_type IN ('weekday', 'weekend')")
def stg_mbta_performance():
    """
    Joins predictions with schedules and enriches with weather data.
    Migrated from: dbt_project/models/capstone/staging/stg_mbta_performance.sql

    Key transformations:
    - Join predictions with schedules on trip_id + stop_id
    - Calculate delay_seconds (predicted - scheduled)
    - Derive is_on_time (delay <= 5 minutes)
    - Add hour_bucket and day_type stratification
    - Left join with weather data
    """
    # Read source tables
    predictions = dlt.read_stream("bronze_mbta_predictions").alias("p")
    schedules = dlt.read("bronze_mbta_schedules").alias("s")
    weather = dlt.read("stg_weather_daily").alias("w")

    # Join predictions with schedules
    joined = (
        predictions
        .join(
            schedules,
            (col("p.trip_id") == col("s.trip_id")) &
            (col("p.stop_id") == col("s.stop_id")),
            "inner"
        )
        .filter(
            col("s.arrival_time").isNotNull() &
            col("p.arrival_time").isNotNull()
        )
    )

    # Calculate metrics and derive columns
    with_calculations = (
        joined
        # Surrogate key (replaces dbt_utils.generate_surrogate_key)
        .withColumn(
            "performance_id",
            md5(concat_ws("||",
                coalesce(col("p.prediction_id").cast("string"), lit("_null_")),
                coalesce(col("s.schedule_id").cast("string"), lit("_null_"))
            ))
        )
        .withColumn("predicted_arrival", col("p.arrival_time"))
        .withColumn("scheduled_arrival", col("s.arrival_time"))
        # Delay calculation (Spark equivalent of TIMESTAMPDIFF)
        .withColumn(
            "delay_seconds",
            (unix_timestamp(col("p.arrival_time")) - unix_timestamp(col("s.arrival_time"))).cast("int")
        )
        # On-time flag (within 5 minutes = 300 seconds)
        .withColumn(
            "is_on_time",
            when(col("delay_seconds") <= 300, True).otherwise(False)
        )
        # Hour extraction
        .withColumn("hour", hour(col("s.arrival_time")))
        # Hour bucket stratification
        .withColumn(
            "hour_bucket",
            when((col("hour") >= 6) & (col("hour") < 9), "AM_PEAK")
            .when((col("hour") >= 16) & (col("hour") < 19), "PM_PEAK")
            .when((col("hour") >= 9) & (col("hour") < 16), "MIDDAY")
            .otherwise("OFF_PEAK")
        )
        # Day type (weekend vs weekday)
        .withColumn(
            "day_type",
            when(dayofweek(col("s.schedule_date")).isin(1, 7), "weekend")
            .otherwise("weekday")
        )
    )

    # Join with weather data
    result = (
        with_calculations
        .join(
            weather,
            to_date(col("scheduled_arrival")) == col("w.observation_date"),
            "left"
        )
        .select(
            "performance_id",
            "predicted_arrival",
            "scheduled_arrival",
            "delay_seconds",
            "is_on_time",
            col("p.stop_id").alias("stop_id"),
            col("p.stop_h3_index").alias("stop_h3_index"),
            col("p.route_id").alias("route_id"),
            col("p.route_long_name").alias("route_long_name"),
            col("s.schedule_date").alias("schedule_date"),
            "hour",
            "hour_bucket",
            "day_type",
            col("p.trip_id").alias("trip_id"),
            col("p.fetched_at").alias("fetched_at"),
            coalesce(col("w.weather_condition"), lit("clear")).alias("weather_condition"),
            col("w.precipitation_inches").alias("precipitation_inches"),
            col("w.temp_max_f").alias("temp_max_f"),
            col("w.temp_min_f").alias("temp_min_f"),
            col("w.snow_inches").alias("snow_inches"),
            col("w.wind_speed_mph").alias("wind_speed_mph"),
            col("w.weather_category_detailed").alias("weather_category_detailed"),
            col("w.weather_severity_score").alias("weather_severity_score")
        )
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: stg_driving_performance
# MAGIC
# MAGIC Enriches driving data with traffic impact calculations and weather.

# COMMAND ----------

@dlt.table(
    name="stg_driving_performance",
    comment="Google Maps driving data enriched with weather and traffic impact",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "origin_h3_index,fetched_date"
    },
    partition_cols=["fetched_date"]
)
@dlt.expect_or_drop("valid_duration", "duration_in_traffic_seconds IS NOT NULL")
@dlt.expect("valid_hour_bucket", "hour_bucket IN ('AM_PEAK', 'PM_PEAK', 'MIDDAY', 'OFF_PEAK')")
@dlt.expect("valid_day_type", "day_type IN ('weekday', 'weekend')")
def stg_driving_performance():
    """
    Transforms driving data with traffic impact calculations.
    Migrated from: dbt_project/models/capstone/staging/stg_driving_performance.sql

    Key transformations:
    - Generate surrogate key
    - Calculate traffic_delay_seconds and traffic_impact_pct
    - Add hour_bucket and day_type stratification
    - Left join with weather data
    """
    driving = dlt.read_stream("bronze_driving_routes").alias("d")
    weather = dlt.read("stg_weather_daily").alias("w")

    with_calculations = (
        driving
        # Surrogate key
        .withColumn(
            "driving_id",
            md5(concat_ws("||",
                coalesce(col("route_name"), lit("_null_")),
                coalesce(col("departure_time").cast("string"), lit("_null_")),
                coalesce(col("fetched_at").cast("string"), lit("_null_"))
            ))
        )
        # Hour extraction
        .withColumn("hour", hour(col("departure_time")))
        # Hour bucket stratification
        .withColumn(
            "hour_bucket",
            when((col("hour") >= 6) & (col("hour") < 9), "AM_PEAK")
            .when((col("hour") >= 16) & (col("hour") < 19), "PM_PEAK")
            .when((col("hour") >= 9) & (col("hour") < 16), "MIDDAY")
            .otherwise("OFF_PEAK")
        )
        # Day type
        .withColumn(
            "day_type",
            when(dayofweek(col("fetched_date")).isin(1, 7), "weekend")
            .otherwise("weekday")
        )
        # Traffic delay calculation
        .withColumn(
            "traffic_delay_seconds",
            (col("duration_in_traffic_seconds") - col("duration_seconds")).cast("int")
        )
        # Traffic impact percentage
        .withColumn(
            "traffic_impact_pct",
            when(
                col("duration_seconds") > 0,
                ((col("duration_in_traffic_seconds") - col("duration_seconds")).cast("double")
                 / col("duration_seconds")) * 100
            ).otherwise(0.0)
        )
    )

    # Join with weather
    result = (
        with_calculations
        .join(
            weather,
            col("d.fetched_date") == col("w.observation_date"),
            "left"
        )
        .select(
            "driving_id",
            col("d.route_name").alias("route_name"),
            col("d.origin_latitude").alias("origin_latitude"),
            col("d.origin_longitude").alias("origin_longitude"),
            col("d.origin_h3_index").alias("origin_h3_index"),
            col("d.destination_latitude").alias("destination_latitude"),
            col("d.destination_longitude").alias("destination_longitude"),
            col("d.destination_h3_index").alias("destination_h3_index"),
            col("d.distance_meters").alias("distance_meters"),
            col("d.duration_seconds").alias("duration_seconds"),
            col("d.duration_in_traffic_seconds").alias("duration_in_traffic_seconds"),
            col("d.departure_time").alias("departure_time"),
            col("d.fetched_date").alias("fetched_date"),
            col("d.fetched_at").alias("fetched_at"),
            "hour",
            "hour_bucket",
            "day_type",
            "traffic_delay_seconds",
            "traffic_impact_pct",
            coalesce(col("w.weather_condition"), lit("clear")).alias("weather_condition"),
            col("w.precipitation_inches").alias("precipitation_inches"),
            col("w.temp_max_f").alias("temp_max_f"),
            col("w.weather_category_detailed").alias("weather_category_detailed"),
            col("w.weather_severity_score").alias("weather_severity_score")
        )
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine Tables
# MAGIC
# MAGIC Tables to capture records that fail data quality checks for auditing and remediation.

# COMMAND ----------

@dlt.table(
    name="stg_weather_daily_quarantine",
    comment="Quarantine table for weather records failing data quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
def stg_weather_daily_quarantine():
    """
    Captures weather records that fail data quality validation.
    Records with NULL observation_date or invalid weather_condition.
    """
    return (
        dlt.read("bronze_openmeteo_weather")
        .filter(
            col("observation_date").isNull() |
            ~col("weather_condition").isin("clear", "rain", "snow")
        )
        .withColumn("quarantine_reason",
            when(col("observation_date").isNull(), "null_observation_date")
            .when(~col("weather_condition").isin("clear", "rain", "snow"), "invalid_weather_condition")
            .otherwise("unknown")
        )
        .withColumn("quarantined_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="stg_mbta_performance_quarantine",
    comment="Quarantine table for MBTA performance records failing data quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
def stg_mbta_performance_quarantine():
    """
    Captures MBTA performance records that fail data quality validation.
    Records where join fails or delay_seconds/scheduled_arrival is NULL.
    """
    predictions = dlt.read_stream("bronze_mbta_predictions").alias("p")
    schedules = dlt.read("bronze_mbta_schedules").alias("s")

    # Left join to find predictions without matching schedules
    joined = (
        predictions
        .join(
            schedules,
            (col("p.trip_id") == col("s.trip_id")) &
            (col("p.stop_id") == col("s.stop_id")),
            "left"
        )
    )

    # Capture records that would be dropped
    return (
        joined
        .filter(
            col("s.arrival_time").isNull() |
            col("p.arrival_time").isNull() |
            col("s.schedule_id").isNull()  # No matching schedule found
        )
        .withColumn("quarantine_reason",
            when(col("s.schedule_id").isNull(), "no_matching_schedule")
            .when(col("s.arrival_time").isNull(), "null_scheduled_arrival")
            .when(col("p.arrival_time").isNull(), "null_predicted_arrival")
            .otherwise("unknown")
        )
        .withColumn("quarantined_at", current_timestamp())
        .select(
            col("p.prediction_id").alias("prediction_id"),
            col("p.trip_id").alias("trip_id"),
            col("p.stop_id").alias("stop_id"),
            col("p.route_id").alias("route_id"),
            col("p.arrival_time").alias("predicted_arrival"),
            col("s.arrival_time").alias("scheduled_arrival"),
            col("p.fetched_at").alias("fetched_at"),
            "quarantine_reason",
            "quarantined_at"
        )
    )

# COMMAND ----------

@dlt.table(
    name="stg_driving_performance_quarantine",
    comment="Quarantine table for driving records failing data quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
def stg_driving_performance_quarantine():
    """
    Captures driving records that fail data quality validation.
    Records with NULL duration_in_traffic_seconds.
    """
    return (
        dlt.read_stream("bronze_driving_routes")
        .filter(col("duration_in_traffic_seconds").isNull())
        .withColumn("quarantine_reason", lit("null_duration_in_traffic"))
        .withColumn("quarantined_at", current_timestamp())
    )
