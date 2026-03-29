{{
    config(
        materialized='table',
        tags=['capstone', 'gold', 'comparison']
    )
}}

WITH mbta_routes AS (
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
    FROM {{ ref('fct_mbta_route_reliability') }}
),

driving_routes AS (
    SELECT
        origin_h3_index AS location_h3,
        route_name,
        route_name AS route_long_name,  -- For consistency
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
    FROM {{ ref('fct_driving_route_reliability') }}
),

all_modes AS (
    SELECT * FROM mbta_routes
    UNION ALL
    SELECT
        location_h3,
        route_name AS route_id,
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
        confidence_level
    FROM driving_routes
),

mode_rankings AS (
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

        -- Rankings by different criteria
        ROW_NUMBER() OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY reliability_score DESC
        ) AS rank_by_reliability,

        ROW_NUMBER() OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY median_time_min ASC
        ) AS rank_by_speed,

        ROW_NUMBER() OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY transit_volatility_cv ASC
        ) AS rank_by_predictability,

        -- Best modes per location-time-weather
        FIRST_VALUE(transportation_mode) OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY reliability_score DESC
        ) AS most_reliable_mode,

        FIRST_VALUE(transportation_mode) OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY median_time_min ASC
        ) AS fastest_mode,

        FIRST_VALUE(transportation_mode) OVER (
            PARTITION BY location_h3, hour_bucket, day_type, weather_condition
            ORDER BY transit_volatility_cv ASC
        ) AS most_predictable_mode

    FROM all_modes
)

SELECT
    *,

    -- Recommendation logic
    CASE
        -- If mode is both fast and reliable, recommend it
        WHEN rank_by_reliability = 1 AND rank_by_speed <= 2 THEN transportation_mode
        -- If reliability score is high (>= 85) and mode is fastest, recommend it
        WHEN reliability_score >= 85 AND rank_by_speed = 1 THEN transportation_mode
        -- Otherwise recommend most reliable
        WHEN rank_by_reliability = 1 THEN transportation_mode
        ELSE NULL
    END AS is_recommended,

    -- Delta from fastest mode
    median_time_min - MIN(median_time_min) OVER (
        PARTITION BY location_h3, hour_bucket, day_type, weather_condition
    ) AS time_delta_from_fastest_min,

    -- Delta from most reliable mode
    reliability_score - MAX(reliability_score) OVER (
        PARTITION BY location_h3, hour_bucket, day_type, weather_condition
    ) AS reliability_delta_from_best

FROM mode_rankings
ORDER BY location_h3, hour_bucket, day_type, weather_condition, rank_by_reliability
