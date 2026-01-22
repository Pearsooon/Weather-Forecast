/*
================================================================================
WEATHER FORECASTING PROJECT - CREATE DATABASE & SCHEMAS
================================================================================
Purpose: Create database and schema structure for data layers
Author: Loki
Date: 2025-01-20
================================================================================
*/

-- Switch to weather_admin role and warehouse
USE ROLE weather_admin;
USE WAREHOUSE WEATHER_WH;

-- ============================================================================
-- SECTION 1: CREATE DATABASE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS WEATHER_DB
    DATA_RETENTION_TIME_IN_DAYS = 7        -- Time Travel: 7 days
    COMMENT = 'Weather Forecasting Project Database';

-- Use the database
USE DATABASE WEATHER_DB;

-- ============================================================================
-- SECTION 2: CREATE SCHEMAS (Data Layers)
-- ============================================================================

-- RAW Layer: Direct from source, no transformations
CREATE SCHEMA IF NOT EXISTS RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw data layer - source data without transformations';

-- STAGING Layer: Initial cleaning and type conversions
CREATE SCHEMA IF NOT EXISTS STAGING
    DATA_RETENTION_TIME_IN_DAYS = 3
    COMMENT = 'Staging layer - cleaned and validated source data';

-- INTERMEDIATE Layer: Business logic transformations
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Intermediate layer - transformed data with business logic';

-- MARTS Layer: Final analytics-ready data
CREATE SCHEMA IF NOT EXISTS MARTS
    DATA_RETENTION_TIME_IN_DAYS = 30       -- Keep longer for reporting
    COMMENT = 'Marts layer - analytics-ready dimensional models';

-- ANALYTICS Layer: ML models and predictions
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Analytics layer - ML models, predictions, and forecasts';

-- ============================================================================
-- SECTION 3: GRANT SCHEMA PERMISSIONS
-- ============================================================================

-- Grant all privileges on all schemas to weather_admin
GRANT ALL PRIVILEGES ON SCHEMA RAW TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON SCHEMA STAGING TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON SCHEMA INTERMEDIATE TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON SCHEMA MARTS TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS TO ROLE weather_admin;

-- Grant future table privileges (for tables created later)
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RAW TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STAGING TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA INTERMEDIATE TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA MARTS TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE weather_admin;

-- Grant future view privileges
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA RAW TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA STAGING TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA INTERMEDIATE TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA MARTS TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE weather_admin;

-- ============================================================================
-- SECTION 4: CREATE FILE FORMATS
-- ============================================================================

-- CSV file format for data loading
CREATE OR REPLACE FILE FORMAT WEATHER_DB.PUBLIC.CSV_FORMAT
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '', 'N/A', 'NA');

-- JSON file format (for future use)
CREATE OR REPLACE FILE FORMAT WEATHER_DB.PUBLIC.JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE;

-- ============================================================================
-- SECTION 5: VERIFICATION
-- ============================================================================

-- Show database
SHOW DATABASES LIKE 'WEATHER_DB';

-- Show all schemas
SHOW SCHEMAS IN DATABASE WEATHER_DB;

-- Show file formats
SHOW FILE FORMATS IN DATABASE WEATHER_DB;

-- Display database structure
SELECT 
    'Database WEATHER_DB created successfully!' AS status,
    CURRENT_DATABASE() AS database_name,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_ROLE() AS role;

-- List all schemas with details
SELECT 
    CATALOG_NAME AS database_name,
    SCHEMA_NAME,
    SCHEMA_OWNER,
    RETENTION_TIME,
    COMMENT
FROM WEATHER_DB.INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = 'WEATHER_DB'
ORDER BY SCHEMA_NAME;

/*
================================================================================
DATA LAYER ARCHITECTURE:

1. RAW Layer:
   - Purpose: Store raw data as-is from source
   - Retention: 7 days (for reprocessing)
   - Tables: WEATHER_RAW

2. STAGING Layer:
   - Purpose: Cleaned, typed, deduplicated data
   - Retention: 3 days
   - Tables: WEATHER_CLEANED, STG_*

3. INTERMEDIATE Layer:
   - Purpose: Business transformations, calculations
   - Retention: 1 day (can be recreated)
   - Tables: INT_*

4. MARTS Layer:
   - Purpose: Analytics-ready dimensional models
   - Retention: 30 days (for reporting)
   - Tables: FCT_*, DIM_*

5. ANALYTICS Layer:
   - Purpose: ML predictions, forecasts
   - Retention: 30 days
   - Tables: PREDICTIONS, FORECASTS, MODELS

NEXT STEPS:
Execute: 03_create_tables.sql
================================================================================
*/