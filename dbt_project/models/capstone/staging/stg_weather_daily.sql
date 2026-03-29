{{
    config(
        materialized='table',
        tags=['capstone', 'silver', 'weather']
    )
}}

SELECT
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

    -- Enhanced categorization for analysis
    CASE
        WHEN snow_inches > 2 THEN 'heavy_snow'
        WHEN snow_inches > 0 THEN 'light_snow'
        WHEN precipitation_inches > 0.5 THEN 'heavy_rain'
        WHEN precipitation_inches > 0.1 THEN 'light_rain'
        WHEN temp_max_f < 20 THEN 'extreme_cold'
        ELSE 'clear'
    END AS weather_category_detailed,

    -- Weather severity score (0-100, higher = worse conditions)
    LEAST(100,
        COALESCE(precipitation_inches * 10, 0) +
        COALESCE(snow_inches * 20, 0) +
        CASE WHEN temp_max_f < 20 THEN 30 WHEN temp_max_f > 95 THEN 20 ELSE 0 END +
        CASE WHEN wind_speed_mph > 30 THEN 20 WHEN wind_speed_mph > 15 THEN 10 ELSE 0 END
    ) AS weather_severity_score

FROM {{ source('boston_reliability_bronze', 'bronze_noaa_weather') }}
