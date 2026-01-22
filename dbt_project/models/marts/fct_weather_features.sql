{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'ml_features']
    )
}}

/*
=================================================================================
WEATHER FORECASTING - ML FEATURES (FIXED - NO CTE COLUMN ISSUES)
=================================================================================
This model creates machine learning features for weather prediction.

KEY PRINCIPLES TO AVOID DATA LEAKAGE:
1. NO current day metrics (max_temp, min_temp, precip, etc.) as features
2. Rolling windows EXCLUDE current row (use "1 PRECEDING" as upper bound)
3. All features must be available BEFORE the target date
4. Change/diff features use only lagged values

TARGET VARIABLE: avg_temperature (to be predicted)
=================================================================================
*/

WITH daily_data AS (
    SELECT * FROM {{ ref('fct_weather_daily') }}
),

-- =============================================================================
-- STEP 1: LAG FEATURES
-- Pass through ALL columns needed for later CTEs
-- =============================================================================
with_lags AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_name', 'date']) }} AS feature_id,
        
        -- Dimensions
        location_name,
        date,
        day_of_week,
        month,
        year,
        quarter,
        season,
        is_weekend,
        
        -- TARGET VARIABLE
        avg_temperature,
        
        -- PASS THROUGH: Need these for rolling calculations
        total_precipitation,
        avg_humidity,
        avg_pressure,
        avg_wind_speed,
        max_wind_speed,
        avg_cloud_cover,
        hours_with_rain,
        
        -- TEMPERATURE LAG FEATURES
        LAG(avg_temperature, 1) OVER (PARTITION BY location_name ORDER BY date) AS temp_lag_1d,
        LAG(avg_temperature, 2) OVER (PARTITION BY location_name ORDER BY date) AS temp_lag_2d,
        LAG(avg_temperature, 3) OVER (PARTITION BY location_name ORDER BY date) AS temp_lag_3d,
        LAG(avg_temperature, 7) OVER (PARTITION BY location_name ORDER BY date) AS temp_lag_7d,
        LAG(avg_temperature, 14) OVER (PARTITION BY location_name ORDER BY date) AS temp_lag_14d,
        LAG(max_temperature, 1) OVER (PARTITION BY location_name ORDER BY date) AS max_temp_lag_1d,
        LAG(min_temperature, 1) OVER (PARTITION BY location_name ORDER BY date) AS min_temp_lag_1d,
        LAG(temp_range, 1) OVER (PARTITION BY location_name ORDER BY date) AS temp_range_lag_1d,
        LAG(temp_std_dev, 1) OVER (PARTITION BY location_name ORDER BY date) AS temp_std_dev_lag_1d,
        
        -- Temperature by time
        LAG(morning_temp, 1) OVER (PARTITION BY location_name ORDER BY date) AS morning_temp_lag_1d,
        LAG(afternoon_temp, 1) OVER (PARTITION BY location_name ORDER BY date) AS afternoon_temp_lag_1d,
        LAG(evening_temp, 1) OVER (PARTITION BY location_name ORDER BY date) AS evening_temp_lag_1d,
        LAG(night_temp, 1) OVER (PARTITION BY location_name ORDER BY date) AS night_temp_lag_1d,
        
        -- PRECIPITATION LAG FEATURES
        LAG(total_precipitation, 1) OVER (PARTITION BY location_name ORDER BY date) AS precip_lag_1d,
        LAG(total_precipitation, 3) OVER (PARTITION BY location_name ORDER BY date) AS precip_lag_3d,
        LAG(total_precipitation, 7) OVER (PARTITION BY location_name ORDER BY date) AS precip_lag_7d,
        LAG(hours_with_rain, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_rain_lag_1d,
        
        -- WIND LAG FEATURES
        LAG(avg_wind_speed, 1) OVER (PARTITION BY location_name ORDER BY date) AS wind_speed_lag_1d,
        LAG(max_wind_speed, 1) OVER (PARTITION BY location_name ORDER BY date) AS max_wind_speed_lag_1d,
        LAG(dominant_wind_direction, 1) OVER (PARTITION BY location_name ORDER BY date) AS wind_direction_lag_1d,
        
        -- HUMIDITY & PRESSURE LAG FEATURES
        LAG(avg_humidity, 1) OVER (PARTITION BY location_name ORDER BY date) AS humidity_lag_1d,
        LAG(max_humidity, 1) OVER (PARTITION BY location_name ORDER BY date) AS max_humidity_lag_1d,
        LAG(avg_pressure, 1) OVER (PARTITION BY location_name ORDER BY date) AS pressure_lag_1d,
        LAG(max_pressure, 1) OVER (PARTITION BY location_name ORDER BY date) AS max_pressure_lag_1d,
        LAG(min_pressure, 1) OVER (PARTITION BY location_name ORDER BY date) AS min_pressure_lag_1d,
        
        -- CLOUD COVER LAG
        LAG(avg_cloud_cover, 1) OVER (PARTITION BY location_name ORDER BY date) AS cloud_cover_lag_1d,
        
        -- WEATHER CONDITION LAG FEATURES
        LAG(hours_hot, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_hot_lag_1d,
        LAG(hours_cold, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_cold_lag_1d,
        LAG(hours_humid, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_humid_lag_1d,
        LAG(hours_windy, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_windy_lag_1d,
        LAG(hours_cloudy, 1) OVER (PARTITION BY location_name ORDER BY date) AS hours_cloudy_lag_1d,
        
        -- DERIVED INDEX LAG FEATURES
        LAG(avg_temp_humidity_index, 1) OVER (PARTITION BY location_name ORDER BY date) AS temp_humidity_index_lag_1d,
        LAG(avg_wind_chill_index, 1) OVER (PARTITION BY location_name ORDER BY date) AS wind_chill_index_lag_1d,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM daily_data
),

-- =============================================================================
-- STEP 2: ROLLING FEATURES (NO LEAKAGE)
-- =============================================================================
with_rolling AS (
    SELECT
        *,
        
        -- TEMPERATURE ROLLING
        AVG(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_7d_prev,
        
        AVG(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_14d_prev,
        
        AVG(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_30d_prev,
        
        STDDEV(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_std_7d_prev,
        
        STDDEV(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_std_30d_prev,
        
        MIN(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_min_7d_prev,
        
        MAX(avg_temperature) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS temp_rolling_max_7d_prev,
        
        -- PRECIPITATION ROLLING
        AVG(total_precipitation) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS precip_rolling_7d_prev,
        
        AVG(total_precipitation) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS precip_rolling_30d_prev,
        
        SUM(total_precipitation) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS precip_sum_7d_prev,
        
        SUM(hours_with_rain) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS total_rain_hours_7d_prev,
        
        -- HUMIDITY ROLLING
        AVG(avg_humidity) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS humidity_rolling_7d_prev,
        
        AVG(avg_humidity) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS humidity_rolling_30d_prev,
        
        -- PRESSURE ROLLING
        AVG(avg_pressure) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS pressure_rolling_7d_prev,
        
        AVG(avg_pressure) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS pressure_rolling_30d_prev,
        
        STDDEV(avg_pressure) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS pressure_rolling_std_7d_prev,
        
        -- WIND ROLLING
        AVG(avg_wind_speed) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS wind_speed_rolling_7d_prev,
        
        MAX(max_wind_speed) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS max_wind_speed_7d_prev,
        
        -- CLOUD COVER ROLLING
        AVG(avg_cloud_cover) OVER (
            PARTITION BY location_name ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS cloud_cover_rolling_7d_prev
        
    FROM with_lags
),

-- =============================================================================
-- STEP 3: CHANGE & TREND FEATURES (NO LEAKAGE)
-- =============================================================================
with_changes AS (
    SELECT
        *,
        
        -- TEMPERATURE CHANGES
        ROUND(temp_lag_1d - temp_lag_2d, 2) AS temp_change_1d_to_2d,
        ROUND(temp_lag_1d - temp_lag_3d, 2) AS temp_change_1d_to_3d,
        ROUND(temp_lag_1d - temp_lag_7d, 2) AS temp_change_1d_to_7d,
        
        -- TEMPERATURE DEVIATIONS
        ROUND(temp_lag_1d - temp_rolling_7d_prev, 2) AS temp_diff_from_7d_avg,
        ROUND(temp_lag_1d - temp_rolling_30d_prev, 2) AS temp_diff_from_30d_avg,
        
        -- PRECIPITATION CHANGES
        ROUND(precip_lag_1d - precip_lag_7d, 2) AS precip_change_1d_to_7d,
        ROUND(precip_lag_1d - precip_rolling_7d_prev, 2) AS precip_diff_from_7d_avg,
        
        -- HUMIDITY CHANGES
        ROUND(humidity_lag_1d - humidity_rolling_7d_prev, 2) AS humidity_diff_from_7d_avg,
        ROUND(humidity_lag_1d - humidity_rolling_30d_prev, 2) AS humidity_diff_from_30d_avg,
        
        -- PRESSURE CHANGES
        ROUND(pressure_lag_1d - pressure_rolling_7d_prev, 2) AS pressure_diff_from_7d_avg,
        ROUND(pressure_lag_1d - pressure_rolling_30d_prev, 2) AS pressure_diff_from_30d_avg,
        
        -- TEMPERATURE TRENDS
        CASE 
            WHEN temp_lag_1d IS NOT NULL AND temp_lag_3d IS NOT NULL THEN
                ROUND((temp_lag_1d - temp_lag_3d) / 2.0, 2)
            ELSE NULL
        END AS temp_trend_3d,
        
        CASE 
            WHEN temp_lag_1d IS NOT NULL AND temp_lag_7d IS NOT NULL THEN
                ROUND((temp_lag_1d - temp_lag_7d) / 6.0, 2)
            ELSE NULL
        END AS temp_trend_7d,
        
        -- VOLATILITY
        ROUND(temp_rolling_std_7d_prev, 2) AS temp_volatility_7d,
        ROUND(temp_rolling_std_30d_prev, 2) AS temp_volatility_30d,
        
        -- WIND CHANGES
        ROUND(wind_speed_lag_1d - wind_speed_rolling_7d_prev, 2) AS wind_speed_diff_from_7d_avg
        
    FROM with_rolling
)

-- =============================================================================
-- FINAL SELECT
-- =============================================================================
SELECT 
    feature_id,
    location_name,
    date,
    
    -- Temporal features
    month,
    year,
    quarter,
    day_of_week,
    is_weekend,
    season,
    
    -- TARGET
    ROUND(avg_temperature, 2) AS avg_temperature,
    
    -- LAG FEATURES - Temperature
    ROUND(temp_lag_1d, 2) AS temp_lag_1d,
    ROUND(temp_lag_2d, 2) AS temp_lag_2d,
    ROUND(temp_lag_3d, 2) AS temp_lag_3d,
    ROUND(temp_lag_7d, 2) AS temp_lag_7d,
    ROUND(temp_lag_14d, 2) AS temp_lag_14d,
    ROUND(max_temp_lag_1d, 2) AS max_temp_lag_1d,
    ROUND(min_temp_lag_1d, 2) AS min_temp_lag_1d,
    ROUND(temp_range_lag_1d, 2) AS temp_range_lag_1d,
    ROUND(temp_std_dev_lag_1d, 2) AS temp_std_dev_lag_1d,
    ROUND(morning_temp_lag_1d, 2) AS morning_temp_lag_1d,
    ROUND(afternoon_temp_lag_1d, 2) AS afternoon_temp_lag_1d,
    ROUND(evening_temp_lag_1d, 2) AS evening_temp_lag_1d,
    ROUND(night_temp_lag_1d, 2) AS night_temp_lag_1d,
    
    -- LAG FEATURES - Precipitation
    ROUND(precip_lag_1d, 2) AS precip_lag_1d,
    ROUND(precip_lag_3d, 2) AS precip_lag_3d,
    ROUND(precip_lag_7d, 2) AS precip_lag_7d,
    hours_rain_lag_1d,
    
    -- LAG FEATURES - Wind
    ROUND(wind_speed_lag_1d, 2) AS wind_speed_lag_1d,
    ROUND(max_wind_speed_lag_1d, 2) AS max_wind_speed_lag_1d,
    ROUND(wind_direction_lag_1d, 2) AS wind_direction_lag_1d,
    
    -- LAG FEATURES - Humidity & Pressure
    ROUND(humidity_lag_1d, 2) AS humidity_lag_1d,
    ROUND(max_humidity_lag_1d, 2) AS max_humidity_lag_1d,
    ROUND(pressure_lag_1d, 2) AS pressure_lag_1d,
    ROUND(max_pressure_lag_1d, 2) AS max_pressure_lag_1d,
    ROUND(min_pressure_lag_1d, 2) AS min_pressure_lag_1d,
    
    -- LAG FEATURES - Cloud & Conditions
    ROUND(cloud_cover_lag_1d, 2) AS cloud_cover_lag_1d,
    hours_hot_lag_1d,
    hours_cold_lag_1d,
    hours_humid_lag_1d,
    hours_windy_lag_1d,
    hours_cloudy_lag_1d,
    
    -- LAG FEATURES - Indices
    ROUND(temp_humidity_index_lag_1d, 2) AS temp_humidity_index_lag_1d,
    ROUND(wind_chill_index_lag_1d, 2) AS wind_chill_index_lag_1d,
    
    -- ROLLING FEATURES - Temperature
    ROUND(temp_rolling_7d_prev, 2) AS temp_rolling_7d_prev,
    ROUND(temp_rolling_14d_prev, 2) AS temp_rolling_14d_prev,
    ROUND(temp_rolling_30d_prev, 2) AS temp_rolling_30d_prev,
    ROUND(temp_rolling_std_7d_prev, 2) AS temp_rolling_std_7d_prev,
    ROUND(temp_rolling_std_30d_prev, 2) AS temp_rolling_std_30d_prev,
    ROUND(temp_rolling_min_7d_prev, 2) AS temp_rolling_min_7d_prev,
    ROUND(temp_rolling_max_7d_prev, 2) AS temp_rolling_max_7d_prev,
    
    -- ROLLING FEATURES - Precipitation
    ROUND(precip_rolling_7d_prev, 2) AS precip_rolling_7d_prev,
    ROUND(precip_rolling_30d_prev, 2) AS precip_rolling_30d_prev,
    ROUND(precip_sum_7d_prev, 2) AS precip_sum_7d_prev,
    total_rain_hours_7d_prev,
    
    -- ROLLING FEATURES - Humidity & Pressure
    ROUND(humidity_rolling_7d_prev, 2) AS humidity_rolling_7d_prev,
    ROUND(humidity_rolling_30d_prev, 2) AS humidity_rolling_30d_prev,
    ROUND(pressure_rolling_7d_prev, 2) AS pressure_rolling_7d_prev,
    ROUND(pressure_rolling_30d_prev, 2) AS pressure_rolling_30d_prev,
    ROUND(pressure_rolling_std_7d_prev, 2) AS pressure_rolling_std_7d_prev,
    
    -- ROLLING FEATURES - Wind & Cloud
    ROUND(wind_speed_rolling_7d_prev, 2) AS wind_speed_rolling_7d_prev,
    ROUND(max_wind_speed_7d_prev, 2) AS max_wind_speed_7d_prev,
    ROUND(cloud_cover_rolling_7d_prev, 2) AS cloud_cover_rolling_7d_prev,
    
    -- CHANGE & TREND FEATURES
    temp_change_1d_to_2d,
    temp_change_1d_to_3d,
    temp_change_1d_to_7d,
    temp_diff_from_7d_avg,
    temp_diff_from_30d_avg,
    precip_change_1d_to_7d,
    precip_diff_from_7d_avg,
    humidity_diff_from_7d_avg,
    humidity_diff_from_30d_avg,
    pressure_diff_from_7d_avg,
    pressure_diff_from_30d_avg,
    temp_trend_3d,
    temp_trend_7d,
    temp_volatility_7d,
    temp_volatility_30d,
    wind_speed_diff_from_7d_avg,
    
    created_at

FROM with_changes
WHERE temp_lag_1d IS NOT NULL
AND date <= CURRENT_DATE()