{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

WITH raw_locations AS (
    SELECT DISTINCT
        location_name,
        latitude,
        longitude
    FROM {{ source('raw', 'WEATHER_RAW') }}
),

with_metadata AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY location_name) AS location_id,
        location_name,
        latitude,
        longitude,
        
        -- Country (hardcoded for Vietnam cities)
        'Vietnam' AS country,
        
        -- Region
        CASE location_name
            WHEN 'Hanoi' THEN 'Northern Vietnam'
            WHEN 'Ho Chi Minh City' THEN 'Southern Vietnam'
            WHEN 'Da Nang' THEN 'Central Vietnam'
            WHEN 'Can Tho' THEN 'Mekong Delta'
            WHEN 'Hai Phong' THEN 'Northern Vietnam'
            ELSE 'Other'
        END AS region,
        
        -- Timezone
        'Asia/Ho_Chi_Minh' AS timezone,
        
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM raw_locations
)

SELECT * FROM with_metadata