/*
================================================================================
WEATHER FORECASTING PROJECT - CREATE TABLES 
================================================================================
*/

USE ROLE weather_admin;
USE WAREHOUSE WEATHER_WH;
USE DATABASE WEATHER_DB;

-- ============================================================================
-- SECTION 1: RAW LAYER TABLES (Python loads data here)
-- ============================================================================

-- Drop table if exists (for fresh start)
-- DROP TABLE IF EXISTS RAW.WEATHER_RAW;

CREATE TABLE IF NOT EXISTS RAW.WEATHER_RAW (
    -- Primary identifiers
    record_id VARCHAR(200) NOT NULL,
    
    -- Temporal information
    datetime TIMESTAMP_NTZ NOT NULL,
    
    -- Location information
    location_name VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    
    -- Weather measurements
    temperature FLOAT COMMENT 'Temperature in Celsius',
    humidity FLOAT COMMENT 'Relative humidity in percentage',
    precipitation FLOAT COMMENT 'Precipitation in mm',
    pressure FLOAT COMMENT 'Surface pressure in hPa',
    wind_speed FLOAT COMMENT 'Wind speed in km/h',
    wind_direction FLOAT COMMENT 'Wind direction in degrees',
    cloud_cover FLOAT COMMENT 'Cloud cover in percentage',
    
    -- Metadata
    extract_date DATE NOT NULL,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw weather data from Open-Meteo API - loaded by Python ETL';

-- ============================================================================
-- SECTION 2: ANALYTICS LAYER TABLES (for ML predictions)
-- ============================================================================
-- Note: These are for storing ML predictions, not dbt transformations

-- ML Model Predictions
CREATE TABLE IF NOT EXISTS ANALYTICS.WEATHER_PREDICTIONS (
    prediction_id VARCHAR(200) PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    prediction_date DATE NOT NULL,
    forecast_date DATE NOT NULL,
    days_ahead INTEGER NOT NULL,
    
    -- Predicted values
    predicted_temperature FLOAT,
    predicted_precipitation FLOAT,
    predicted_humidity FLOAT,
    
    -- Actual values (filled later for validation)
    actual_temperature FLOAT,
    actual_precipitation FLOAT,
    actual_humidity FLOAT,
    
    -- Model metadata
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    confidence_score FLOAT,
    
    -- Error metrics (calculated after actual values available)
    temperature_error FLOAT,
    precipitation_error FLOAT,
    absolute_error FLOAT,
    
    -- Metadata
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    UNIQUE (location_name, prediction_date, forecast_date, model_name)
)
COMMENT = 'Weather forecast predictions from ML models (Python/Jupyter)';

-- Model Performance Tracking
CREATE TABLE IF NOT EXISTS ANALYTICS.MODEL_PERFORMANCE (
    performance_id INTEGER AUTOINCREMENT PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50),
    evaluation_date DATE NOT NULL,
    
    -- Performance Metrics
    rmse FLOAT COMMENT 'Root Mean Squared Error',
    mae FLOAT COMMENT 'Mean Absolute Error',
    r_squared FLOAT COMMENT 'R-squared score',
    mape FLOAT COMMENT 'Mean Absolute Percentage Error',
    
    -- Additional metrics
    mse FLOAT COMMENT 'Mean Squared Error',
    median_absolute_error FLOAT,
    max_error FLOAT,
    
    -- Dataset info
    training_records INTEGER,
    test_records INTEGER,
    validation_records INTEGER,
    locations_covered INTEGER,
    date_range_start DATE,
    date_range_end DATE,
    
    -- Model details
    model_type VARCHAR(100),
    hyperparameters TEXT,
    features_used TEXT,
    
    -- Metadata
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    
    UNIQUE (model_name, model_version, evaluation_date)
)
COMMENT = 'ML model performance tracking and evaluation metrics';


-- Show all tables in each schema
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    BYTES,
    COMMENT
FROM WEATHER_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS', 'ANALYTICS')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Count tables per schema
SELECT 
    TABLE_SCHEMA,
    COUNT(*) AS table_count
FROM WEATHER_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS', 'ANALYTICS')
GROUP BY TABLE_SCHEMA
ORDER BY TABLE_SCHEMA;

-- Success message
SELECT 
    'Tables created successfully!' AS status,
    CURRENT_DATABASE() AS database,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_ROLE() AS role,
    CURRENT_TIMESTAMP() AS completed_at;

