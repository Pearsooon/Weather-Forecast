{{
    config(
        materialized='view',
        tags=['intermediate', 'quality']
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather_raw') }}
),

with_quality_flags AS (
    SELECT
        *,
        
        -- Outlier detection flags
        CASE 
            WHEN temperature < -50 OR temperature > 60 THEN 1 
            ELSE 0 
        END AS temp_outlier_flag,
        
        CASE 
            WHEN humidity < 0 OR humidity > 100 THEN 1 
            ELSE 0 
        END AS humidity_outlier_flag,
        
        CASE 
            WHEN precipitation < 0 THEN 1 
            ELSE 0 
        END AS precip_outlier_flag,
        
        CASE 
            WHEN pressure < 950 OR pressure > 1050 THEN 1 
            ELSE 0 
        END AS pressure_outlier_flag,
        
        CASE 
            WHEN wind_speed < 0 OR wind_speed > 150 THEN 1 
            ELSE 0 
        END AS wind_speed_outlier_flag
        
    FROM staging
),

with_cleaned_values AS (
    SELECT
        *,
        
        -- Interpolate/cap outliers
        CASE 
            WHEN temp_outlier_flag = 1 THEN NULL
            ELSE temperature
        END AS temperature_cleaned,
        
        CASE 
            WHEN humidity_outlier_flag = 1 THEN NULL
            WHEN humidity < 0 THEN 0
            WHEN humidity > 100 THEN 100
            ELSE humidity
        END AS humidity_cleaned,
        
        CASE 
            WHEN precip_outlier_flag = 1 THEN 0
            WHEN precipitation < 0 THEN 0
            ELSE precipitation
        END AS precipitation_cleaned,
        
        CASE 
            WHEN pressure_outlier_flag = 1 THEN NULL
            ELSE pressure
        END AS pressure_cleaned,
        
        CASE 
            WHEN wind_speed_outlier_flag = 1 THEN NULL
            WHEN wind_speed < 0 THEN 0
            ELSE wind_speed
        END AS wind_speed_cleaned
        
    FROM with_quality_flags
),

with_interpolation AS (
    SELECT
        *,
        
        -- Fill missing values with moving average
        COALESCE(
            temperature_cleaned,
            AVG(temperature_cleaned) OVER (
                PARTITION BY location_name, date 
                ORDER BY datetime 
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            )
        ) AS temperature_final,
        
        COALESCE(
            humidity_cleaned,
            AVG(humidity_cleaned) OVER (
                PARTITION BY location_name, date 
                ORDER BY datetime 
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            )
        ) AS humidity_final,
        
        COALESCE(precipitation_cleaned, 0) AS precipitation_final,
        
        COALESCE(
            pressure_cleaned,
            AVG(pressure_cleaned) OVER (
                PARTITION BY location_name, date 
                ORDER BY datetime 
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            )
        ) AS pressure_final,
        
        COALESCE(
            wind_speed_cleaned,
            AVG(wind_speed_cleaned) OVER (
                PARTITION BY location_name, date 
                ORDER BY datetime 
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            )
        ) AS wind_speed_final
        
    FROM with_cleaned_values
),

final AS (
    SELECT
        record_id,
        datetime,
        date,
        hour,
        day,
        month,
        year,
        day_of_week,
        week_of_year,
        quarter,
        location_name,
        latitude,
        longitude,
        
        -- Use cleaned/interpolated values
        temperature_final AS temperature,
        humidity_final AS humidity,
        precipitation_final AS precipitation,
        pressure_final AS pressure,
        wind_speed_final AS wind_speed,
        wind_direction,
        cloud_cover,
        
        -- Keep quality flags for reference
        temp_outlier_flag,
        humidity_outlier_flag,
        precip_outlier_flag,
        pressure_outlier_flag,
        wind_speed_outlier_flag,
        
        -- Calculate overall quality score (0-100)
        100 - (
            (temp_outlier_flag + humidity_outlier_flag + 
             precip_outlier_flag + pressure_outlier_flag + 
             wind_speed_outlier_flag) * 20
        ) AS data_quality_score,
        
        -- Metadata
        extract_date,
        loaded_at,
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM with_interpolation
)

SELECT * FROM final
WHERE data_quality_score >= 60  -- Filter low quality records