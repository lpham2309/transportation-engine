{{
    config(
        materialized='table',
        tags=['capstone', 'gold', 'reliability']
    )
}}

WITH route_stats AS (
    SELECT
        -- Stratification dimensions
        stop_h3_index,
        route_id,
        route_long_name,
        hour_bucket,
        day_type,
        weather_condition,

        -- Sample statistics
        COUNT(*) AS trip_count,

        -- Travel time distribution (delay in seconds)
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY delay_seconds) AS p50_delay_sec,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY delay_seconds) AS p90_delay_sec,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_seconds) AS p95_delay_sec,
        AVG(delay_seconds) AS mean_delay_sec,
        STDDEV(delay_seconds) AS stddev_delay_sec,

        -- On-time performance (within 5 minutes)
        SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_count,
        on_time_count::FLOAT / NULLIF(trip_count, 0) AS on_time_rate

    FROM {{ ref('stg_mbta_performance') }}
    WHERE delay_seconds IS NOT NULL
    GROUP BY stop_h3_index, route_id, route_long_name, hour_bucket, day_type, weather_condition
    HAVING trip_count >= 30  -- Minimum sample size for statistical validity
),

reliability_scores AS (
    SELECT
        *,

        -- On-time score component (60% weight)
        (on_time_rate * 100) AS on_time_score,

        -- Stability score component (40% weight)
        -- Based on P95-P50 spread (lower spread = more stable)
        100 * GREATEST(0, 1 - (p95_delay_sec - p50_delay_sec)::FLOAT / NULLIF(ABS(p50_delay_sec), 0.1)) AS stability_score,

        -- Final reliability score (0-100)
        LEAST(100, GREATEST(0,
            (0.6 * on_time_score) + (0.4 * stability_score)
        )) AS reliability_score,

        -- Transit volatility (Coefficient of Variation)
        (stddev_delay_sec::FLOAT / NULLIF(ABS(mean_delay_sec), 0.1)) * 100 AS transit_volatility_cv,

        -- Percentile spread ratio
        ((p90_delay_sec - p50_delay_sec)::FLOAT / NULLIF(ABS(p50_delay_sec), 0.1)) * 100 AS percentile_spread_pct

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
        WHEN trip_count < 30 THEN 'insufficient_data'
        WHEN trip_count < 100 THEN 'low_confidence'
        ELSE 'standard_confidence'
    END AS confidence_level,

    -- Planning recommendations (P90 for 90% confidence)
    p90_delay_sec / 60.0 AS recommended_buffer_min,

    'MBTA' AS transportation_mode

FROM reliability_scores
ORDER BY stop_h3_index, route_id, hour_bucket, weather_condition