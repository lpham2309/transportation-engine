{{
    config(
        materialized='incremental',
        unique_key='driving_id',
        on_schema_change='fail',
        tags=['capstone', 'silver', 'driving']
    )
}}

WITH driving_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['route_name', 'departure_time', 'fetched_at']) }} AS driving_id,
        route_name,
        origin_latitude,
        origin_longitude,
        origin_h3_index,
        destination_latitude,
        destination_longitude,
        destination_h3_index,
        distance_meters,
        duration_seconds,
        duration_in_traffic_seconds,
        departure_time,
        fetched_date,
        fetched_at,

        -- Extract time components
        HOUR(departure_time) AS hour,
        CASE
            WHEN HOUR(departure_time) BETWEEN 6 AND 9 THEN 'AM_PEAK'
            WHEN HOUR(departure_time) BETWEEN 16 AND 19 THEN 'PM_PEAK'
            WHEN HOUR(departure_time) BETWEEN 9 AND 16 THEN 'MIDDAY'
            ELSE 'OFF_PEAK'
        END AS hour_bucket,
        CASE
            WHEN DAYOFWEEK(fetched_date) IN (1, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,

        -- Traffic impact
        duration_in_traffic_seconds - duration_seconds AS traffic_delay_seconds,
        CASE
            WHEN duration_seconds > 0 THEN
                ((duration_in_traffic_seconds - duration_seconds)::FLOAT / duration_seconds) * 100
            ELSE 0
        END AS traffic_impact_pct

    FROM {{ source('boston_reliability_bronze', 'bronze_driving_routes') }}
    {% if is_incremental() %}
    WHERE fetched_at > (SELECT MAX(fetched_at) FROM {{ this }})
    {% endif %}
),

with_weather AS (
    SELECT
        d.*,
        w.weather_condition,
        w.precipitation_inches,
        w.temp_max_f,
        w.weather_category_detailed,
        w.weather_severity_score
    FROM driving_data d
    LEFT JOIN {{ ref('stg_weather_daily') }} w
        ON d.fetched_date = w.observation_date
)

SELECT * FROM with_weather
