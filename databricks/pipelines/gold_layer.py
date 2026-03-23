# Databricks notebook source
# MAGIC %md
# MAGIC # Boston Reliability Engine - Gold Layer
# MAGIC
# MAGIC This notebook defines Delta Live Tables for the Gold (analytics) layer.
# MAGIC Contains fact tables with reliability scores and mode comparisons.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table: fct_mbta_route_reliability
# MAGIC
# MAGIC Calculates reliability scores for MBTA routes using the formula:
# MAGIC `reliability_score = 0.6 * on_time_score + 0.4 * stability_score`

# COMMAND ----------

@dlt.table(
    name="fct_mbta_route_reliability",
    comment="Reliability scores and metrics for MBTA routes by stratification",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "stop_h3_index,route_id"
    }
)
@dlt.expect("valid_reliability_score", "reliability_score BETWEEN 0 AND 100")
@dlt.expect("sufficient_sample_size", "trip_count >= 30", on_violation="warn")
def fct_mbta_route_reliability():
    """
    Calculates reliability scores for MBTA routes.
    Migrated from: dbt_project/models/capstone/marts/fct_mbta_route_reliability.sql

    Reliability Formula:
        reliability_score = 0.6 * on_time_score + 0.4 * stability_score
    Where:
        on_time_score = (on_time_count / trip_count) * 100
        stability_score = 100 * max(0, 1 - (p95 - p50) / abs(p50))
    """
    # Read silver table and filter valid records
    performance = (
        dlt.read("stg_mbta_performance")
        .filter(col("delay_seconds").isNotNull())
    )

    # Aggregate by stratification dimensions
    route_stats = (
        performance
        .groupBy(
            "stop_h3_index",
            "route_id",
            "route_long_name",
            "hour_bucket",
            "day_type",
            "weather_condition"
        )
        .agg(
            count("*").alias("trip_count"),
            # Percentile calculations (Spark equivalent of PERCENTILE_CONT)
            expr("percentile_approx(delay_seconds, 0.50)").alias("p50_delay_sec"),
            expr("percentile_approx(delay_seconds, 0.90)").alias("p90_delay_sec"),
            expr("percentile_approx(delay_seconds, 0.95)").alias("p95_delay_sec"),
            avg("delay_seconds").alias("mean_delay_sec"),
            stddev("delay_seconds").alias("stddev_delay_sec"),
            sum(when(col("is_on_time"), 1).otherwise(0)).alias("on_time_count")
        )
        .filter(col("trip_count") >= 30)  # Minimum sample size
    )

    # Calculate reliability scores
    reliability_scores = (
        route_stats
        # On-time rate and score
        .withColumn(
            "on_time_rate",
            col("on_time_count").cast("double") / col("trip_count")
        )
        .withColumn("on_time_score", col("on_time_rate") * 100)
        # Stability score: 100 * max(0, 1 - (p95 - p50) / abs(p50))
        .withColumn(
            "stability_score",
            greatest(
                lit(0.0),
                lit(100.0) * (
                    lit(1.0) - (col("p95_delay_sec") - col("p50_delay_sec")).cast("double") /
                    greatest(abs(col("p50_delay_sec")), lit(0.1))
                )
            )
        )
        # Final reliability score: 0.6 * on_time + 0.4 * stability
        .withColumn(
            "reliability_score",
            least(
                lit(100.0),
                greatest(
                    lit(0.0),
                    (lit(0.6) * col("on_time_score")) + (lit(0.4) * col("stability_score"))
                )
            )
        )
        # Volatility metrics
        .withColumn(
            "transit_volatility_cv",
            when(
                abs(col("mean_delay_sec")) > 0.1,
                (col("stddev_delay_sec").cast("double") / abs(col("mean_delay_sec"))) * 100
            ).otherwise(0.0)
        )
        .withColumn(
            "percentile_spread_pct",
            when(
                abs(col("p50_delay_sec")) > 0.1,
                ((col("p90_delay_sec") - col("p50_delay_sec")).cast("double") /
                 abs(col("p50_delay_sec"))) * 100
            ).otherwise(0.0)
        )
    )

    # Add categorical classifications
    result = (
        reliability_scores
        # Reliability category
        .withColumn(
            "reliability_category",
            when(col("reliability_score") >= 90, "highly_reliable")
            .when(col("reliability_score") >= 75, "moderately_reliable")
            .when(col("reliability_score") >= 60, "unreliable")
            .otherwise("very_unreliable")
        )
        # Volatility category
        .withColumn(
            "volatility_category",
            when(col("transit_volatility_cv") < 15, "low_volatility")
            .when(col("transit_volatility_cv") < 30, "medium_volatility")
            .otherwise("high_volatility")
        )
        # Confidence level based on sample size
        .withColumn(
            "confidence_level",
            when(col("trip_count") < 30, "insufficient_data")
            .when(col("trip_count") < 100, "low_confidence")
            .otherwise("standard_confidence")
        )
        # Recommended buffer (P90 for planning)
        .withColumn("recommended_buffer_min", col("p90_delay_sec") / 60.0)
        # Transportation mode identifier
        .withColumn("transportation_mode", lit("MBTA"))
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table: fct_driving_route_reliability
# MAGIC
# MAGIC Calculates reliability scores for driving routes using the same formula.

# COMMAND ----------

@dlt.table(
    name="fct_driving_route_reliability",
    comment="Reliability scores and metrics for driving routes",
    table_properties={
        "quality": "gold"
    }
)
@dlt.expect("valid_reliability_score", "reliability_score BETWEEN 0 AND 100")
@dlt.expect("sufficient_sample_size", "trip_count >= 20", on_violation="warn")
def fct_driving_route_reliability():
    """
    Calculates reliability scores for driving routes.
    Migrated from: dbt_project/models/capstone/marts/fct_driving_route_reliability.sql

    Uses same reliability formula but based on duration instead of delay.
    On-time defined as: duration_in_traffic <= baseline_duration + 5 minutes
    """
    driving = (
        dlt.read("stg_driving_performance")
        .filter(col("duration_in_traffic_seconds").isNotNull())
    )

    # Aggregate by stratification dimensions
    route_stats = (
        driving
        .groupBy(
            "origin_h3_index",
            "destination_h3_index",
            "route_name",
            "hour_bucket",
            "day_type",
            "weather_condition"
        )
        .agg(
            count("*").alias("trip_count"),
            expr("percentile_approx(duration_in_traffic_seconds, 0.50)").alias("p50_duration_sec"),
            expr("percentile_approx(duration_in_traffic_seconds, 0.90)").alias("p90_duration_sec"),
            expr("percentile_approx(duration_in_traffic_seconds, 0.95)").alias("p95_duration_sec"),
            avg("duration_in_traffic_seconds").alias("mean_duration_sec"),
            stddev("duration_in_traffic_seconds").alias("stddev_duration_sec"),
            avg("duration_seconds").alias("avg_baseline_duration_sec"),
            avg("traffic_delay_seconds").alias("avg_traffic_delay_sec"),
            avg("traffic_impact_pct").alias("avg_traffic_impact_pct"),
            # On-time: within baseline + 5 minutes (300 seconds)
            sum(
                when(col("duration_in_traffic_seconds") <= col("duration_seconds") + 300, 1)
                .otherwise(0)
            ).alias("on_time_count")
        )
        .filter(col("trip_count") >= 20)  # Lower threshold than MBTA
    )

    # Calculate reliability scores (same formula as MBTA)
    reliability_scores = (
        route_stats
        .withColumn(
            "on_time_rate",
            col("on_time_count").cast("double") / col("trip_count")
        )
        .withColumn("on_time_score", col("on_time_rate") * 100)
        .withColumn(
            "stability_score",
            greatest(
                lit(0.0),
                lit(100.0) * (
                    lit(1.0) - (col("p95_duration_sec") - col("p50_duration_sec")).cast("double") /
                    greatest(col("p50_duration_sec"), lit(0.1))
                )
            )
        )
        .withColumn(
            "reliability_score",
            least(
                lit(100.0),
                greatest(
                    lit(0.0),
                    (lit(0.6) * col("on_time_score")) + (lit(0.4) * col("stability_score"))
                )
            )
        )
        .withColumn(
            "transit_volatility_cv",
            when(
                col("mean_duration_sec") > 0.1,
                (col("stddev_duration_sec").cast("double") / col("mean_duration_sec")) * 100
            ).otherwise(0.0)
        )
        .withColumn(
            "percentile_spread_pct",
            when(
                col("p50_duration_sec") > 0.1,
                ((col("p90_duration_sec") - col("p50_duration_sec")).cast("double") /
                 col("p50_duration_sec")) * 100
            ).otherwise(0.0)
        )
    )

    # Add categorical classifications
    result = (
        reliability_scores
        .withColumn(
            "reliability_category",
            when(col("reliability_score") >= 90, "highly_reliable")
            .when(col("reliability_score") >= 75, "moderately_reliable")
            .when(col("reliability_score") >= 60, "unreliable")
            .otherwise("very_unreliable")
        )
        .withColumn(
            "volatility_category",
            when(col("transit_volatility_cv") < 15, "low_volatility")
            .when(col("transit_volatility_cv") < 30, "medium_volatility")
            .otherwise("high_volatility")
        )
        .withColumn(
            "confidence_level",
            when(col("trip_count") < 20, "insufficient_data")
            .when(col("trip_count") < 50, "low_confidence")
            .otherwise("standard_confidence")
        )
        .withColumn("median_duration_min", col("p50_duration_sec") / 60.0)
        .withColumn("recommended_buffer_min", col("p90_duration_sec") / 60.0)
        .withColumn("baseline_duration_min", col("avg_baseline_duration_sec") / 60.0)
        .withColumn("transportation_mode", lit("Driving"))
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table: fct_mode_comparison
# MAGIC
# MAGIC Side-by-side comparison of MBTA and Driving with rankings.

# COMMAND ----------

@dlt.table(
    name="fct_mode_comparison",
    comment="Side-by-side comparison of MBTA and Driving transportation modes",
    table_properties={
        "quality": "gold"
    }
)
def fct_mode_comparison():
    """
    Compares transportation modes with window-based rankings.
    Migrated from: dbt_project/models/capstone/marts/fct_mode_comparison.sql

    Uses FIRST_VALUE, ROW_NUMBER, and other window functions.
    """
    # Read and normalize MBTA data
    mbta = (
        dlt.read("fct_mbta_route_reliability")
        .select(
            col("stop_h3_index").alias("location_h3"),
            col("route_id"),
            col("route_long_name"),
            "hour_bucket",
            "day_type",
            "weather_condition",
            "transportation_mode",
            "reliability_score",
            "reliability_category",
            (col("p50_delay_sec") / 60.0).alias("median_time_min"),
            (col("p90_delay_sec") / 60.0).alias("p90_time_min"),
            "transit_volatility_cv",
            "trip_count",
            "confidence_level"
        )
    )

    # Read and normalize Driving data
    driving = (
        dlt.read("fct_driving_route_reliability")
        .select(
            col("origin_h3_index").alias("location_h3"),
            col("route_name").alias("route_id"),
            col("route_name").alias("route_long_name"),
            "hour_bucket",
            "day_type",
            "weather_condition",
            "transportation_mode",
            "reliability_score",
            "reliability_category",
            col("median_duration_min").alias("median_time_min"),
            col("recommended_buffer_min").alias("p90_time_min"),
            "transit_volatility_cv",
            "trip_count",
            "confidence_level"
        )
    )

    # Union all modes
    all_modes = mbta.union(driving)

    # Define window specifications for ranking
    partition_cols = ["location_h3", "hour_bucket", "day_type", "weather_condition"]

    w_reliability = Window.partitionBy(partition_cols).orderBy(desc("reliability_score"))
    w_speed = Window.partitionBy(partition_cols).orderBy("median_time_min")
    w_predictability = Window.partitionBy(partition_cols).orderBy("transit_volatility_cv")
    w_partition = Window.partitionBy(partition_cols)

    # Add rankings and best mode identifiers
    ranked = (
        all_modes
        .withColumn("rank_by_reliability", row_number().over(w_reliability))
        .withColumn("rank_by_speed", row_number().over(w_speed))
        .withColumn("rank_by_predictability", row_number().over(w_predictability))
        .withColumn("most_reliable_mode", first("transportation_mode").over(w_reliability))
        .withColumn("fastest_mode", first("transportation_mode").over(w_speed))
        .withColumn("most_predictable_mode", first("transportation_mode").over(w_predictability))
    )

    # Add recommendation logic and deltas
    result = (
        ranked
        # Recommendation logic
        .withColumn(
            "is_recommended",
            when(
                (col("rank_by_reliability") == 1) & (col("rank_by_speed") <= 2),
                col("transportation_mode")
            ).when(
                (col("reliability_score") >= 85) & (col("rank_by_speed") == 1),
                col("transportation_mode")
            ).when(
                col("rank_by_reliability") == 1,
                col("transportation_mode")
            ).otherwise(None)
        )
        # Time delta from fastest
        .withColumn(
            "time_delta_from_fastest_min",
            col("median_time_min") - min("median_time_min").over(w_partition)
        )
        # Reliability delta from best
        .withColumn(
            "reliability_delta_from_best",
            col("reliability_score") - max("reliability_score").over(w_partition)
        )
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table: fct_weather_impact
# MAGIC
# MAGIC Analyzes how weather affects each transportation mode.

# COMMAND ----------

@dlt.table(
    name="fct_weather_impact",
    comment="Analysis of weather impact on transportation reliability",
    table_properties={
        "quality": "gold"
    }
)
def fct_weather_impact():
    """
    Analyzes weather impact using LAG window function.
    Migrated from: dbt_project/models/capstone/marts/fct_weather_impact.sql

    Compares each weather condition to clear weather baseline.
    """
    # Read and normalize MBTA data
    mbta = (
        dlt.read("fct_mbta_route_reliability")
        .select(
            lit("MBTA").alias("transportation_mode"),
            col("route_id"),
            col("route_long_name"),
            col("stop_h3_index").alias("location_h3"),
            "hour_bucket",
            "day_type",
            "weather_condition",
            "reliability_score",
            (col("p50_delay_sec") / 60.0).alias("median_time_min"),
            (col("p90_delay_sec") / 60.0).alias("p90_time_min"),
            "transit_volatility_cv",
            "trip_count"
        )
    )

    # Read and normalize Driving data
    driving = (
        dlt.read("fct_driving_route_reliability")
        .select(
            lit("Driving").alias("transportation_mode"),
            col("route_name").alias("route_id"),
            col("route_name").alias("route_long_name"),
            col("origin_h3_index").alias("location_h3"),
            "hour_bucket",
            "day_type",
            "weather_condition",
            "reliability_score",
            col("median_duration_min").alias("median_time_min"),
            col("recommended_buffer_min").alias("p90_time_min"),
            "transit_volatility_cv",
            "trip_count"
        )
    )

    # Union all modes
    all_modes = mbta.union(driving)

    # Order weather conditions for LAG comparison
    weather_ordered = (
        all_modes
        .withColumn(
            "weather_order",
            when(col("weather_condition") == "clear", 1)
            .when(col("weather_condition") == "rain", 2)
            .when(col("weather_condition") == "snow", 3)
            .otherwise(4)
        )
    )

    # Window for LAG comparison (partitioned by mode/route, ordered by weather severity)
    w = Window.partitionBy(
        "transportation_mode",
        "route_id",
        "location_h3",
        "hour_bucket",
        "day_type"
    ).orderBy("weather_order")

    # Calculate deltas from clear weather
    comparisons = (
        weather_ordered
        .withColumn(
            "reliability_delta_from_clear",
            col("reliability_score") - lag("reliability_score", 1).over(w)
        )
        .withColumn(
            "time_delta_from_clear_min",
            col("median_time_min") - lag("median_time_min", 1).over(w)
        )
        .withColumn(
            "volatility_delta_from_clear",
            col("transit_volatility_cv") - lag("transit_volatility_cv", 1).over(w)
        )
    )

    # Categorize impact and calculate resilience
    result = (
        comparisons
        # Weather impact category
        .withColumn(
            "weather_impact_category",
            when(abs(col("reliability_delta_from_clear")) >= 20, "major_impact")
            .when(abs(col("reliability_delta_from_clear")) >= 10, "moderate_impact")
            .when(abs(col("reliability_delta_from_clear")) >= 5, "minor_impact")
            .otherwise("minimal_impact")
        )
        # Weather resilience score (0-100, higher = more resilient)
        .withColumn(
            "weather_resilience_score",
            when(col("weather_condition") == "clear", None)
            .otherwise(
                lit(100.0) - least(lit(100.0), abs(col("reliability_delta_from_clear")))
            )
        )
        .drop("weather_order")
    )

    return result
