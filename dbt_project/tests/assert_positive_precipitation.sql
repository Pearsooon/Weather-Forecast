SELECT
    location_name,
    date,
    total_precipitation
FROM {{ ref('fct_weather_daily') }}
WHERE total_precipitation < 0