{{
    config(
        materialized='incremental',
        unique_key='performance_id',
        on_schema_change='fail',
        tags=['capstone', 'silver', 'mbta']
    )
}}

WITH predictions AS (
    SELECT
        prediction_id,
        arrival_time AS predicted_arrival,
        stop_id,
        stop_h3_index,
        route_id,
        route_long_name,
        trip_id,
        fetched_at
    FROM {{ source('boston_reliability_bronze', 'bronze_mbta_predictions') }}
    {% if is_incremental() %}
    WHERE fetched_at > (SELECT MAX(fetched_at) FROM {{ this }})
    {% endif %}
),

schedules AS (
    SELECT
        schedule_id,
        arrival_time AS scheduled_arrival,
        stop_id,
        stop_h3_index,
        route_id,
        trip_id,
        schedule_date,
        fetched_at
    FROM {{ source('boston_reliability_bronze', 'bronze_mbta_schedules') }}
    {% if is_incremental() %}
    WHERE schedule_date >= CURRENT_DATE - 7  -- Keep recent week
    {% endif %}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['p.prediction_id', 's.schedule_id']) }} AS performance_id,
        p.predicted_arrival,
        s.scheduled_arrival,
        TIMESTAMPDIFF(SECOND, s.scheduled_arrival, p.predicted_arrival) AS delay_seconds,
        CASE
            WHEN delay_seconds <= 300 THEN TRUE  -- 5 minutes tolerance
            ELSE FALSE
        END AS is_on_time,
        p.stop_id,
        p.stop_h3_index,
        p.route_id,
        p.route_long_name,
        s.schedule_date,
        HOUR(s.scheduled_arrival) AS hour,
        CASE
            WHEN hour BETWEEN 6 AND 9 THEN 'AM_PEAK'
            WHEN hour BETWEEN 16 AND 19 THEN 'PM_PEAK'
            WHEN hour BETWEEN 9 AND 16 THEN 'MIDDAY'
            ELSE 'OFF_PEAK'
        END AS hour_bucket,
        CASE
            WHEN DAYOFWEEK(s.schedule_date) IN (1, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,
        p.trip_id,
        p.fetched_at
    FROM predictions p
    INNER JOIN schedules s
        ON p.trip_id = s.trip_id
        AND p.stop_id = s.stop_id
    WHERE s.scheduled_arrival IS NOT NULL
        AND p.predicted_arrival IS NOT NULL
),

with_weather AS (
    SELECT
        j.*,
        w.weather_condition,
        w.precipitation_inches,
        w.temp_max_f,
        w.temp_min_f,
        w.snow_inches,
        w.wind_speed_mph
    FROM joined j
    LEFT JOIN {{ ref('stg_weather_daily') }} w
        ON DATE(j.scheduled_arrival) = w.observation_date
)

SELECT * FROM with_weather
