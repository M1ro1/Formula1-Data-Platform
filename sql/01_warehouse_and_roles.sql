-- =====================================================================
-- 01: warehouse, resource monitor, roles
-- Run as ACCOUNTADMIN (Snowsight worksheet).
-- Replace YOUR_SNOWFLAKE_USER below with your own user name.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- ---- Roles (least privilege per stage) ----
CREATE ROLE IF NOT EXISTS F1_LOADER      COMMENT = 'Airflow: loads raw data into RAW';
CREATE ROLE IF NOT EXISTS F1_TRANSFORMER COMMENT = 'dbt: reads RAW, builds all other layers';
CREATE ROLE IF NOT EXISTS F1_REPORTER    COMMENT = 'BI tools: read-only access to MARTS';

-- Keep the role hierarchy tidy: SYSADMIN can manage everything below it
GRANT ROLE F1_LOADER      TO ROLE SYSADMIN;
GRANT ROLE F1_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE F1_REPORTER    TO ROLE SYSADMIN;

-- Give the roles to your user
SET my_user = 'YOUR_SNOWFLAKE_USER';
GRANT ROLE F1_LOADER      TO USER IDENTIFIER($my_user);
GRANT ROLE F1_TRANSFORMER TO USER IDENTIFIER($my_user);
GRANT ROLE F1_REPORTER    TO USER IDENTIFIER($my_user);

-- ---- Warehouse: XSMALL, suspends after 60 seconds idle ----
CREATE WAREHOUSE IF NOT EXISTS F1_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Warehouse for the F1 data warehouse project';

GRANT USAGE, OPERATE ON WAREHOUSE F1_WH TO ROLE F1_LOADER;
GRANT USAGE, OPERATE ON WAREHOUSE F1_WH TO ROLE F1_TRANSFORMER;
GRANT USAGE          ON WAREHOUSE F1_WH TO ROLE F1_REPORTER;

-- ---- Cost guard: notify at 80%, suspend at 100% of the monthly quota ----
CREATE RESOURCE MONITOR IF NOT EXISTS F1_MONITOR
    WITH CREDIT_QUOTA = 10
         FREQUENCY = MONTHLY
         START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80  PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE F1_WH SET RESOURCE_MONITOR = F1_MONITOR;
