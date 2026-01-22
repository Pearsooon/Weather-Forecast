
-- Switch to ACCOUNTADMIN role to create role and user
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 1: CREATE CUSTOM ROLE
-- ============================================================================

-- Create weather_admin role
CREATE ROLE IF NOT EXISTS weather_admin
    COMMENT = 'Admin role for Weather Forecasting project';

-- Grant role to SYSADMIN (role hierarchy)
GRANT ROLE weather_admin TO ROLE SYSADMIN;

-- Grant role to current user (PEARSON1411)
GRANT ROLE weather_admin TO USER PEARSON1411;

-- Display created role
SHOW ROLES LIKE 'weather_admin';

-- ============================================================================
-- SECTION 2: GRANT SYSTEM PRIVILEGES TO ROLE
-- ============================================================================

-- Grant warehouse creation privilege
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE weather_admin;

-- Grant database creation privilege
GRANT CREATE DATABASE ON ACCOUNT TO ROLE weather_admin;

-- Grant usage on default warehouse (if needed)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE weather_admin;

-- ============================================================================
-- SECTION 3: VERIFICATION
-- ============================================================================

-- Show grants to role
SHOW GRANTS TO ROLE weather_admin;

-- Show grants to user
SHOW GRANTS TO USER PEARSON1411;

-- Success message
SELECT 
    'Environment setup completed successfully!' AS status,
    CURRENT_TIMESTAMP() AS completed_at,
    CURRENT_USER() AS created_by,
    CURRENT_ROLE() AS using_role;

/*
================================================================================
NEXT STEPS:
1. Run: USE ROLE weather_admin;
2. Execute: 01_create_warehouse.sql
================================================================================
*/