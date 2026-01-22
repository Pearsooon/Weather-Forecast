/*
================================================================================
WEATHER FORECASTING PROJECT - CREATE WAREHOUSE
================================================================================
Purpose: Create dedicated warehouse for weather data processing
Author: Loki
Date: 2025-01-20
================================================================================
*/

-- Switch to weather_admin role
USE ROLE weather_admin;

-- ============================================================================
-- SECTION 1: CREATE WAREHOUSE
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS WEATHER_WH
WITH
    -- Warehouse size
    WAREHOUSE_SIZE = 'X-SMALL'           -- Start small, can scale up
    
    -- Auto-suspend and auto-resume
    AUTO_SUSPEND = 300                    -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE                    -- Auto-resume when queries submitted
    
    -- Initial state
    INITIALLY_SUSPENDED = TRUE            -- Don't start immediately
    
    -- Scaling policy
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2                 -- Can scale to 2 clusters if needed
    SCALING_POLICY = 'STANDARD'
    
    -- Resource monitor (optional - set budget limit)
    -- RESOURCE_MONITOR = WEATHER_MONITOR
    
    COMMENT = 'Warehouse for Weather Forecasting ETL and Analytics';

-- ============================================================================
-- SECTION 2: CONFIGURE WAREHOUSE PARAMETERS
-- ============================================================================

-- Set warehouse parameters for optimization
ALTER WAREHOUSE WEATHER_WH SET
    STATEMENT_TIMEOUT_IN_SECONDS = 3600    -- 1 hour max per query
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 0; -- No queue timeout

-- ============================================================================
-- SECTION 3: GRANT PERMISSIONS
-- ============================================================================

-- Grant all privileges to weather_admin role
GRANT ALL PRIVILEGES ON WAREHOUSE WEATHER_WH TO ROLE weather_admin;

-- Grant usage to PUBLIC (if needed for reporting users)
-- GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE PUBLIC;

-- ============================================================================
-- SECTION 4: VERIFICATION
-- ============================================================================

-- Show warehouse details
SHOW WAREHOUSES LIKE 'WEATHER_WH';

-- Show warehouse parameters
SHOW PARAMETERS FOR WAREHOUSE WEATHER_WH;

-- Describe warehouse
DESCRIBE WAREHOUSE WEATHER_WH;

-- Verify grants
SHOW GRANTS ON WAREHOUSE WEATHER_WH;

-- Test warehouse
USE WAREHOUSE WEATHER_WH;
SELECT 
    'Warehouse WEATHER_WH created successfully!' AS status,
    CURRENT_WAREHOUSE() AS warehouse_name,
    CURRENT_TIMESTAMP() AS created_at;

/*
================================================================================
WAREHOUSE SIZING GUIDE:
- X-SMALL: Development, small datasets (<1GB)
- SMALL: Regular ETL, medium datasets (1-10GB)
- MEDIUM: Heavy ETL, large datasets (10-100GB)
- LARGE+: Production workloads, very large datasets

COST OPTIMIZATION:
- AUTO_SUSPEND: Minimize costs when not in use
- AUTO_RESUME: Ensure availability when needed
- SCALING_POLICY: Handle concurrent workloads

NEXT STEPS:
Execute: 02_create_database.sql
================================================================================
*/