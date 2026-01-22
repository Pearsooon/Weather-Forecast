/*
================================================================================
WEATHER FORECASTING PROJECT - CLEANUP SCRIPT
================================================================================
Purpose: Drop all objects (USE WITH CAUTION!)
Author: Loki
Date: 2025-01-20
WARNING: This will delete ALL project data and objects
================================================================================
*/

USE ROLE ACCOUNTADMIN;

-- Confirmation check
SELECT 
    '⚠️  WARNING: This script will delete EVERYTHING!' AS warning,
    'Uncomment the commands below to proceed' AS instruction,
    'Make sure you have backups!' AS reminder;

-- ============================================================================
-- SECTION 1: DROP TABLES (Uncomment to execute)
-- ============================================================================

/*
USE DATABASE WEATHER_DB;

-- Drop Analytics tables
DROP TABLE IF EXISTS ANALYTICS.MODEL_PERFORMANCE;
DROP TABLE IF EXISTS ANALYTICS.WEATHER_PREDICTIONS;

-- Drop Marts tables
DROP TABLE IF EXISTS MARTS.DIM_DATE;
DROP TABLE IF EXISTS MARTS.DIM_LOCATION;
DROP TABLE IF EXISTS MARTS.FCT_WEATHER_FEATURES;
DROP TABLE IF EXISTS MARTS.FCT_WEATHER_DAILY;

-- Drop Intermediate tables
DROP TABLE IF EXISTS INTERMEDIATE.INT_WEATHER_QUALITY_CHECKED;

-- Drop Staging tables
DROP TABLE IF EXISTS STAGING.WEATHER_CLEANED;

-- Drop Raw tables
DROP TABLE IF EXISTS RAW.WEATHER_RAW;
*/

-- ============================================================================
-- SECTION 2: DROP SCHEMAS (Uncomment to execute)
-- ============================================================================

/*
DROP SCHEMA IF EXISTS ANALYTICS;
DROP SCHEMA IF EXISTS MARTS;
DROP SCHEMA IF EXISTS INTERMEDIATE;
DROP SCHEMA IF EXISTS STAGING;
DROP SCHEMA IF EXISTS RAW;
*/

-- ============================================================================
-- SECTION 3: DROP DATABASE (Uncomment to execute)
-- ============================================================================

/*
DROP DATABASE IF EXISTS WEATHER_DB;
*/

-- ============================================================================
-- SECTION 4: DROP WAREHOUSE (Uncomment to execute)
-- ============================================================================

/*
DROP WAREHOUSE IF EXISTS WEATHER_WH;
*/

-- ============================================================================
-- SECTION 5: DROP ROLES (Uncomment to execute)
-- ============================================================================

/*
DROP ROLE IF EXISTS weather_scientist;
DROP ROLE IF EXISTS weather_analyst;
DROP ROLE IF EXISTS weather_engineer;
DROP ROLE IF EXISTS weather_admin;
*/

-- ============================================================================
-- SECTION 6: VERIFICATION
-- ============================================================================

/*
-- Check remaining objects
SHOW DATABASES LIKE 'WEATHER%';
SHOW WAREHOUSES LIKE 'WEATHER%';
SHOW ROLES LIKE 'weather%';

SELECT 'Cleanup completed!' AS status;
*/

/*
================================================================================
TO USE THIS CLEANUP SCRIPT:

1. Uncomment the sections you want to execute
2. Run them in order (tables → schemas → database → warehouse → roles)
3. Verify each step before proceeding to the next

SAFER ALTERNATIVE:
Instead of dropping, consider:
- TRUNCATE tables to keep structure
- Use separate dev/test environments
- Implement proper backup strategy

================================================================================
*/