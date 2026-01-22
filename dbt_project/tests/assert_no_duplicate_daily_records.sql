-- Test for duplicate daily records per location

WITH duplicate_check AS (
    SELECT
        location_name,
        date,
        COUNT(*) as record_count
    FROM {{ ref('fct_weather_daily') }}
    GROUP BY location_name, date
    HAVING COUNT(*) > 1
)

SELECT * FROM duplicate_check