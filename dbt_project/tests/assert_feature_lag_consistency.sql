-- Test that lag features match actual historical values

WITH lag_validation AS (
    SELECT
        f.location_name,
        f.date,
        f.temp_lag_1d,
        LAG(f.avg_temperature, 1) OVER (
            PARTITION BY f.location_name 
            ORDER BY f.date
        ) as actual_yesterday_temp,
        ABS(f.temp_lag_1d - LAG(f.avg_temperature, 1) OVER (
            PARTITION BY f.location_name 
            ORDER BY f.date
        )) as difference
    FROM {{ ref('fct_weather_features') }} f
    WHERE f.temp_lag_1d IS NOT NULL
)

SELECT
    location_name,
    date,
    temp_lag_1d,
    actual_yesterday_temp,
    difference
FROM lag_validation
WHERE difference > 0.1  -- Allow small floating point differences