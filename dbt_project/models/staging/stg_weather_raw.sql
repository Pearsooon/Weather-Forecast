{{
    config(
        materialized='view',
        tags=['staging', 'hourly']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'WEATHER_RAW') }}
),

cleaned AS (
    SELECT
        -- Primary keys
        record_id,
        
        -- Temporal columns
        datetime,
        DATE(datetime) AS date,
        HOUR(datetime) AS hour,
        DAY(datetime) AS day,
        MONTH(datetime) AS month,
        YEAR(datetime) AS year,
        DAYOFWEEK(datetime) AS day_of_week,
        WEEKOFYEAR(datetime) AS week_of_year,
        QUARTER(datetime) AS quarter,
        
        -- Location columns
        location_name,
        latitude,
        longitude,
        
        -- Weather measurements
        temperature,
        humidity,
        precipitation,
        pressure,
        wind_speed,
        wind_direction,
        cloud_cover,
        
        -- Metadata
        extract_date,
        loaded_at,
        
        -- Data quality flags
        CASE 
            WHEN temperature IS NULL THEN 1 
            ELSE 0 
        END AS has_missing_temperature,
        
        CASE 
            WHEN humidity IS NULL THEN 1 
            ELSE 0 
        END AS has_missing_humidity,
        
        CASE 
            WHEN precipitation IS NULL THEN 1 
            ELSE 0 
        END AS has_missing_precipitation
        
    FROM source
    WHERE datetime IS NOT NULL
        AND location_name IS NOT NULL
)

SELECT * FROM cleaned