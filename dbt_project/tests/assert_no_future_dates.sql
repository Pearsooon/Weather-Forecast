-- Test that no weather records are dated in the future

SELECT
    location_name,
    date,
    COUNT(*) as record_count
FROM {{ ref('fct_weather_daily') }}
WHERE date > CURRENT_DATE()
GROUP BY location_name, date