-- Test that all expected locations have data

WITH expected_locations AS (
    SELECT DISTINCT location_name
    FROM {{ ref('seed_location_metadata') }}
),

actual_locations AS (
    SELECT DISTINCT location_name
    FROM {{ ref('fct_weather_daily') }}
)

SELECT 
    e.location_name
FROM expected_locations e
LEFT JOIN actual_locations a 
    ON e.location_name = a.location_name
WHERE a.location_name IS NULL