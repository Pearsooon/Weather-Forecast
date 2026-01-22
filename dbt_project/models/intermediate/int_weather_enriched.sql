{{
    config(
        materialized='view',
        tags=['intermediate', 'enrichment']
    )
}}

WITH quality_checked AS (
    SELECT * FROM {{ ref('int_weather_quality_checked') }}
),

with_features AS (
    SELECT
        *,
        
        -- Season (Vietnam context)
        CASE 
            WHEN month IN (11, 12, 1, 2, 3, 4) THEN 'Dry Season'
            ELSE 'Rainy Season'
        END AS season,
        
        -- Time of day
        CASE 
            WHEN hour >= 5 AND hour < 12 THEN 'Morning'
            WHEN hour >= 12 AND hour < 17 THEN 'Afternoon'
            WHEN hour >= 17 AND hour < 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        
        -- Day name
        CASE day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        
        -- Month name
        CASE month
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        -- Weather condition flags
        CASE WHEN precipitation > 0 THEN 1 ELSE 0 END AS is_raining,
        CASE WHEN temperature > 32 THEN 1 ELSE 0 END AS is_hot,
        CASE WHEN temperature < 20 THEN 1 ELSE 0 END AS is_cold,
        CASE WHEN humidity > 80 THEN 1 ELSE 0 END AS is_humid,
        CASE WHEN wind_speed > 20 THEN 1 ELSE 0 END AS is_windy,
        CASE WHEN cloud_cover > 70 THEN 1 ELSE 0 END AS is_cloudy,
        
        -- Weekend flag
        CASE WHEN day_of_week IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
        
        -- Derived metrics
        temperature + (humidity / 100.0) * 5 AS temp_humidity_index,
        temperature - (wind_speed / 10.0) AS wind_chill_index,
        
        -- Precipitation intensity
        CASE 
            WHEN precipitation = 0 THEN 'No Rain'
            WHEN precipitation < 2.5 THEN 'Light Rain'
            WHEN precipitation < 10 THEN 'Moderate Rain'
            WHEN precipitation < 50 THEN 'Heavy Rain'
            ELSE 'Very Heavy Rain'
        END AS precipitation_intensity
        
    FROM quality_checked
)

SELECT * FROM with_features