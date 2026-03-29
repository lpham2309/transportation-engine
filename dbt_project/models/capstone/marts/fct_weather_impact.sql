{{
    config(
        materialized='table',
        tags=['capstone', 'gold', 'weather']
    )
}}

WITH all_modes_weather AS (
    -- MBTA data by weather
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
    FROM {{ ref('fct_mbta_route_reliability') }}

    UNION ALL

    -- Driving data by weather
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
    FROM {{ ref('fct_driving_route_reliability') }}
),

weather_comparisons AS (
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

        -- Compare to clear weather baseline
        reliability_score - LAG(reliability_score) OVER (
            PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
            ORDER BY
                CASE weather_condition
                    WHEN 'clear' THEN 1
                    WHEN 'rain' THEN 2
                    WHEN 'snow' THEN 3
                    ELSE 4
                END
        ) AS reliability_delta_from_clear,

        median_time_min - LAG(median_time_min) OVER (
            PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
            ORDER BY
                CASE weather_condition
                    WHEN 'clear' THEN 1
                    WHEN 'rain' THEN 2
                    WHEN 'snow' THEN 3
                    ELSE 4
                END
        ) AS time_delta_from_clear_min,

        transit_volatility_cv - LAG(transit_volatility_cv) OVER (
            PARTITION BY transportation_mode, route_id, location_h3, hour_bucket, day_type
            ORDER BY
                CASE weather_condition
                    WHEN 'clear' THEN 1
                    WHEN 'rain' THEN 2
                    WHEN 'snow' THEN 3
                    ELSE 4
                END
        ) AS volatility_delta_from_clear

    FROM all_modes_weather
)

SELECT
    *,

    -- Weather impact severity
    CASE
        WHEN ABS(reliability_delta_from_clear) >= 20 THEN 'major_impact'
        WHEN ABS(reliability_delta_from_clear) >= 10 THEN 'moderate_impact'
        WHEN ABS(reliability_delta_from_clear) >= 5 THEN 'minor_impact'
        ELSE 'minimal_impact'
    END AS weather_impact_category,

    -- Weather resilience score (higher = less affected by weather)
    CASE
        WHEN weather_condition = 'clear' THEN NULL  -- No comparison for baseline
        ELSE 100 - LEAST(100, ABS(reliability_delta_from_clear))
    END AS weather_resilience_score

FROM weather_comparisons
ORDER BY
    transportation_mode,
    location_h3,
    route_id,
    hour_bucket,
    CASE weather_condition
        WHEN 'clear' THEN 1
        WHEN 'rain' THEN 2
        WHEN 'snow' THEN 3
        ELSE 4
    END