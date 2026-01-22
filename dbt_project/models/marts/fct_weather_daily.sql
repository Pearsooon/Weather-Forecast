{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'daily']
    )
}}

WITH enriched AS (
    SELECT * FROM {{ ref('int_weather_enriched') }}
),

daily_aggregates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_name', 'date']) }} AS daily_id,
        
        -- Dimensions
        location_name,
        date,
        MIN(day_of_week) AS day_of_week,
        MIN(day_name) AS day_name,
        MIN(month) AS month,
        MIN(month_name) AS month_name,
        MIN(year) AS year,
        MIN(quarter) AS quarter,
        MIN(season) AS season,
        MAX(is_weekend) AS is_weekend,
        
        -- Temperature metrics
        ROUND(AVG(temperature), 2) AS avg_temperature,
        ROUND(MAX(temperature), 2) AS max_temperature,
        ROUND(MIN(temperature), 2) AS min_temperature,
        ROUND(STDDEV(temperature), 2) AS temp_std_dev,
        ROUND(MAX(temperature) - MIN(temperature), 2) AS temp_range,
        
        -- Temperature time-based
        ROUND(AVG(CASE WHEN time_of_day = 'Morning' THEN temperature END), 2) AS morning_temp,
        ROUND(AVG(CASE WHEN time_of_day = 'Afternoon' THEN temperature END), 2) AS afternoon_temp,
        ROUND(AVG(CASE WHEN time_of_day = 'Evening' THEN temperature END), 2) AS evening_temp,
        ROUND(AVG(CASE WHEN time_of_day = 'Night' THEN temperature END), 2) AS night_temp,
        
        -- Precipitation metrics
        ROUND(SUM(precipitation), 2) AS total_precipitation,
        ROUND(MAX(precipitation), 2) AS max_hourly_precipitation,
        ROUND(AVG(precipitation), 2) AS avg_precipitation,
        SUM(is_raining) AS hours_with_rain,
        
        -- Wind metrics
        ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
        ROUND(MAX(wind_speed), 2) AS max_wind_speed,
        ROUND(MIN(wind_speed), 2) AS min_wind_speed,
        ROUND(AVG(wind_direction), 2) AS dominant_wind_direction,
        
        -- Humidity & Pressure
        ROUND(AVG(humidity), 2) AS avg_humidity,
        ROUND(MAX(humidity), 2) AS max_humidity,
        ROUND(MIN(humidity), 2) AS min_humidity,
        ROUND(AVG(pressure), 2) AS avg_pressure,
        ROUND(MAX(pressure), 2) AS max_pressure,
        ROUND(MIN(pressure), 2) AS min_pressure,
        
        -- Cloud cover
        ROUND(AVG(cloud_cover), 2) AS avg_cloud_cover,
        
        -- Weather condition counts
        SUM(is_hot) AS hours_hot,
        SUM(is_cold) AS hours_cold,
        SUM(is_humid) AS hours_humid,
        SUM(is_windy) AS hours_windy,
        SUM(is_cloudy) AS hours_cloudy,
        
        -- Derived metrics
        ROUND(AVG(temp_humidity_index), 2) AS avg_temp_humidity_index,
        ROUND(AVG(wind_chill_index), 2) AS avg_wind_chill_index,
        
        -- Data quality
        ROUND(AVG(data_quality_score), 2) AS avg_data_quality_score,
        COUNT(*) AS total_hourly_records,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM enriched
    WHERE date <= CURRENT_DATE()
    GROUP BY 
        location_name,
        date
)

SELECT * FROM daily_aggregates