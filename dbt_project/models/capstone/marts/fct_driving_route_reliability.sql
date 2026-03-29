{{
    config(
        materialized='table',
        tags=['capstone', 'gold', 'reliability']
    )
}}

WITH route_stats AS (
    SELECT
        -- Stratification dimensions
        origin_h3_index,
        destination_h3_index,
        route_name,
        hour_bucket,
        day_type,
        weather_condition,

        -- Sample statistics
        COUNT(*) AS trip_count,

        -- Travel time distribution (duration in traffic)
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_in_traffic_seconds) AS p50_duration_sec,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_in_traffic_seconds) AS p90_duration_sec,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_in_traffic_seconds) AS p95_duration_sec,
        AVG(duration_in_traffic_seconds) AS mean_duration_sec,
        STDDEV(duration_in_traffic_seconds) AS stddev_duration_sec,

        -- Baseline (no traffic) duration
        AVG(duration_seconds) AS avg_baseline_duration_sec,

        -- Traffic impact
        AVG(traffic_delay_seconds) AS avg_traffic_delay_sec,
        AVG(traffic_impact_pct) AS avg_traffic_impact_pct,

        -- On-time performance (within baseline + 5 minutes)
        SUM(CASE
            WHEN duration_in_traffic_seconds <= (duration_seconds + 300) THEN 1
            ELSE 0
        END) AS on_time_count,
        on_time_count::FLOAT / NULLIF(trip_count, 0) AS on_time_rate

    FROM {{ ref('stg_driving_performance') }}
    WHERE duration_in_traffic_seconds IS NOT NULL
    GROUP BY origin_h3_index, destination_h3_index, route_name, hour_bucket, day_type, weather_condition
    HAVING trip_count >= 20  -- Lower threshold for driving data
),

reliability_scores AS (
    SELECT
        *,

        -- On-time score component (60% weight)
        (on_time_rate * 100) AS on_time_score,

        -- Stability score component (40% weight)
        100 * GREATEST(0, 1 - (p95_duration_sec - p50_duration_sec)::FLOAT / NULLIF(p50_duration_sec, 0.1)) AS stability_score,

        -- Final reliability score (0-100)
        LEAST(100, GREATEST(0,
            (0.6 * on_time_score) + (0.4 * stability_score)
        )) AS reliability_score,

        -- Transit volatility (Coefficient of Variation)
        (stddev_duration_sec::FLOAT / NULLIF(mean_duration_sec, 0.1)) * 100 AS transit_volatility_cv,

        -- Percentile spread ratio
        ((p90_duration_sec - p50_duration_sec)::FLOAT / NULLIF(p50_duration_sec, 0.1)) * 100 AS percentile_spread_pct

    FROM route_stats
)

SELECT
    *,

    -- Reliability category
    CASE
        WHEN reliability_score >= 90 THEN 'highly_reliable'
        WHEN reliability_score >= 75 THEN 'moderately_reliable'
        WHEN reliability_score >= 60 THEN 'unreliable'
        ELSE 'very_unreliable'
    END AS reliability_category,

    -- Volatility category
    CASE
        WHEN transit_volatility_cv < 15 THEN 'low_volatility'
        WHEN transit_volatility_cv < 30 THEN 'medium_volatility'
        ELSE 'high_volatility'
    END AS volatility_category,

    -- Sample confidence level
    CASE
        WHEN trip_count < 20 THEN 'insufficient_data'
        WHEN trip_count < 50 THEN 'low_confidence'
        ELSE 'standard_confidence'
    END AS confidence_level,

    -- Planning recommendations
    p50_duration_sec / 60.0 AS median_duration_min,
    p90_duration_sec / 60.0 AS recommended_buffer_min,
    avg_baseline_duration_sec / 60.0 AS baseline_duration_min,

    'Driving' AS transportation_mode

FROM reliability_scores
ORDER BY origin_h3_index, destination_h3_index, hour_bucket, weather_condition