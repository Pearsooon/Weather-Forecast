SELECT
    location_name,
    date,
    min_temperature,
    avg_temperature,
    max_temperature
FROM {{ ref('fct_weather_daily') }}
WHERE NOT (
    min_temperature <= avg_temperature 
    AND avg_temperature <= max_temperature
)