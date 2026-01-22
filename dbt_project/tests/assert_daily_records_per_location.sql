-- Test that each location has reasonable number of daily records
-- Should have approximately 365 records per year per location

WITH daily_counts AS (
    SELECT
        location_name,
        COUNT(DISTINCT date) as day_count,
        MIN(date) as first_date,
        MAX(date) as last_date,
        DATEDIFF('day', MIN(date), MAX(date)) as date_range_days
    FROM {{ ref('fct_weather_daily') }}
    GROUP BY location_name
)

SELECT
    location_name,
    day_count,
    date_range_days,
    -- Allow 10% tolerance
    CASE 
        WHEN day_count < date_range_days * 0.9 THEN 'Too few records'
        ELSE 'OK'
    END as status
FROM daily_counts
WHERE day_count < date_range_days * 0.9