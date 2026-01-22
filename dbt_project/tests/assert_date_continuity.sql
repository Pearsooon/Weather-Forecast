-- Test for gaps in date sequence (missing days)

WITH date_gaps AS (
    SELECT
        location_name,
        date,
        LAG(date) OVER (PARTITION BY location_name ORDER BY date) as prev_date,
        DATEDIFF('day', LAG(date) OVER (PARTITION BY location_name ORDER BY date), date) as day_gap
    FROM {{ ref('fct_weather_daily') }}
)

SELECT
    location_name,
    prev_date,
    date,
    day_gap
FROM date_gaps
WHERE day_gap > 1  -- Gap larger than 1 day