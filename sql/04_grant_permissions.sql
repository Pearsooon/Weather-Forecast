/*
================================================================================
WEATHER FORECASTING PROJECT - GRANT PERMISSIONS
================================================================================
Purpose: Set up granular permissions for different roles
Author: Loki
Date: 2025-01-20
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WEATHER_WH;
USE DATABASE WEATHER_DB;

-- ============================================================================
-- SECTION 1: CREATE ADDITIONAL ROLES (Optional)
-- ============================================================================

-- Role for data engineers (can write to RAW, STAGING)
CREATE ROLE IF NOT EXISTS weather_engineer
    COMMENT = 'Data engineers - can load and transform data';

-- Role for analysts (read-only access to MARTS)
CREATE ROLE IF NOT EXISTS weather_analyst
    COMMENT = 'Data analysts - read-only access to marts';

-- Role for data scientists (read MARTS, write to ANALYTICS)
CREATE ROLE IF NOT EXISTS weather_scientist
    COMMENT = 'Data scientists - can create ML models';

-- Grant roles to SYSADMIN
GRANT ROLE weather_engineer TO ROLE SYSADMIN;
GRANT ROLE weather_analyst TO ROLE SYSADMIN;
GRANT ROLE weather_scientist TO ROLE SYSADMIN;

-- Grant roles to weather_admin (admin has all sub-roles)
GRANT ROLE weather_engineer TO ROLE weather_admin;
GRANT ROLE weather_analyst TO ROLE weather_admin;
GRANT ROLE weather_scientist TO ROLE weather_admin;

-- ============================================================================
-- SECTION 2: GRANT WAREHOUSE USAGE
-- ============================================================================

-- Admin: All privileges
GRANT ALL PRIVILEGES ON WAREHOUSE WEATHER_WH TO ROLE weather_admin;

-- Engineers: Usage + monitoring
GRANT USAGE, OPERATE ON WAREHOUSE WEATHER_WH TO ROLE weather_engineer;

-- Analysts: Usage only
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE weather_analyst;

-- Scientists: Usage + monitoring
GRANT USAGE, OPERATE ON WAREHOUSE WEATHER_WH TO ROLE weather_scientist;

-- ============================================================================
-- SECTION 3: GRANT DATABASE ACCESS
-- ============================================================================

-- All roles need database usage
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE weather_admin;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE weather_engineer;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE weather_analyst;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE weather_scientist;

-- ============================================================================
-- SECTION 4: GRANT SCHEMA PERMISSIONS
-- ============================================================================

-- -------------------------
-- WEATHER_ADMIN: Full access to all schemas
-- -------------------------
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE WEATHER_DB TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE WEATHER_DB TO ROLE weather_admin;

-- -------------------------
-- WEATHER_ENGINEER: Write access to RAW, STAGING, INTERMEDIATE
-- -------------------------
GRANT USAGE ON SCHEMA RAW TO ROLE weather_engineer;
GRANT USAGE ON SCHEMA STAGING TO ROLE weather_engineer;
GRANT USAGE ON SCHEMA INTERMEDIATE TO ROLE weather_engineer;
GRANT USAGE ON SCHEMA MARTS TO ROLE weather_engineer;

-- Write permissions
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA RAW TO ROLE weather_engineer;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA STAGING TO ROLE weather_engineer;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA INTERMEDIATE TO ROLE weather_engineer;

-- -------------------------
-- WEATHER_ANALYST: Read-only access to MARTS
-- -------------------------
GRANT USAGE ON SCHEMA MARTS TO ROLE weather_analyst;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE weather_analyst;

-- -------------------------
-- WEATHER_SCIENTIST: Read MARTS, Write ANALYTICS
-- -------------------------
GRANT USAGE ON SCHEMA MARTS TO ROLE weather_scientist;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA ANALYTICS TO ROLE weather_scientist;

-- ============================================================================
-- SECTION 5: GRANT TABLE PERMISSIONS
-- ============================================================================

-- -------------------------
-- WEATHER_ADMIN: All privileges on all tables
-- -------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RAW TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA INTERMEDIATE TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MARTS TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE weather_admin;

-- Future tables
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RAW TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STAGING TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA INTERMEDIATE TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA MARTS TO ROLE weather_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE weather_admin;

-- -------------------------
-- WEATHER_ENGINEER: Full access to RAW, STAGING, INTERMEDIATE
-- -------------------------
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA RAW TO ROLE weather_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA STAGING TO ROLE weather_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA INTERMEDIATE TO ROLE weather_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA MARTS TO ROLE weather_engineer;

-- Future tables
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA RAW TO ROLE weather_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA STAGING TO ROLE weather_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA INTERMEDIATE TO ROLE weather_engineer;

-- -------------------------
-- WEATHER_ANALYST: Read-only access to MARTS and ANALYTICS
-- -------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA MARTS TO ROLE weather_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE weather_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS TO ROLE weather_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE weather_analyst;

-- -------------------------
-- WEATHER_SCIENTIST: Read MARTS, Full access to ANALYTICS
-- -------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA MARTS TO ROLE weather_scientist;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE weather_scientist;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS TO ROLE weather_scientist;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE weather_scientist;

-- ============================================================================
-- SECTION 6: GRANT VIEW PERMISSIONS
-- ============================================================================

GRANT SELECT ON ALL VIEWS IN SCHEMA MARTS TO ROLE weather_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE weather_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARTS TO ROLE weather_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE weather_analyst;

-- ============================================================================
-- SECTION 7: ASSIGN ROLES TO USERS
-- ============================================================================

-- Grant weather_admin to PEARSON1411
GRANT ROLE weather_admin TO USER PEARSON1411;

-- Optional: Grant other roles to specific users
-- GRANT ROLE weather_engineer TO USER engineer_user;
-- GRANT ROLE weather_analyst TO USER analyst_user;
-- GRANT ROLE weather_scientist TO USER scientist_user;

-- ============================================================================
-- SECTION 8: VERIFICATION
-- ============================================================================

-- Show all custom roles
SHOW ROLES LIKE 'weather%';

-- Show grants to weather_admin
SHOW GRANTS TO ROLE weather_admin;

-- Show grants to weather_engineer
SHOW GRANTS TO ROLE weather_engineer;

-- Show grants to weather_analyst
SHOW GRANTS TO ROLE weather_analyst;

-- Show grants to weather_scientist
SHOW GRANTS TO ROLE weather_scientist;

-- Show grants to current user
SHOW GRANTS TO USER PEARSON1411;

-- Summary
SELECT 
    'Permissions granted successfully!' AS status,
    CURRENT_ROLE() AS current_role,
    CURRENT_USER() AS current_user,
    CURRENT_TIMESTAMP() AS completed_at;

/*
================================================================================
ROLE HIERARCHY:

ACCOUNTADMIN (System)
    └── SYSADMIN (System)
        └── WEATHER_ADMIN (Project Owner)
            ├── WEATHER_ENGINEER (ETL Developer)
            ├── WEATHER_ANALYST (BI Analyst)
            └── WEATHER_SCIENTIST (Data Scientist)

PERMISSION MATRIX:

┌──────────────────┬──────────┬──────────────┬───────────────┬─────────────────┐
│ Role             │ Warehouse│ RAW/STAGING  │ MARTS         │ ANALYTICS       │
├──────────────────┼──────────┼──────────────┼───────────────┼─────────────────┤
│ weather_admin    │ ALL      │ ALL          │ ALL           │ ALL             │
│ weather_engineer │ USAGE    │ READ/WRITE   │ READ          │ READ            │
│ weather_analyst  │ USAGE    │ NONE         │ READ          │ READ            │
│ weather_scientist│ USAGE    │ NONE         │ READ          │ READ/WRITE      │
└──────────────────┴──────────┴──────────────┴───────────────┴─────────────────┘

SETUP COMPLETE!
================================================================================
*/