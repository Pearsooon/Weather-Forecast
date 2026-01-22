-- Test that rolling averages are within reasonable bounds

SELECT
    location_name,
    date,
    avg_temperature,
    temp_rolling_7d_prev,
    temp_rolling_30d_prev
FROM {{ ref('fct_weather_features') }}
WHERE 
    -- Rolling average should be close to actual temperature
    ABS(avg_temperature - temp_rolling_7d_prev) > 15
    OR ABS(avg_temperature - temp_rolling_30d_prev) > 20