-- =====================================================================
-- snowflake/06_security.sql
-- Day 9: Least-privilege analyst role - Gold access only.
-- Depends on 01_setup.sql through 04_pipeline.sql having already run.
-- Safe to re-run: CREATE ROLE uses IF NOT EXISTS; grants are idempotent.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS analyst_role;

GRANT USAGE ON WAREHOUSE training_wh TO ROLE analyst_role;
GRANT USAGE ON DATABASE training TO ROLE analyst_role;
GRANT USAGE ON SCHEMA training.gold TO ROLE analyst_role;

-- ALL TABLES covers what exists now; FUTURE TABLES covers anything
-- created in gold later, so a new Gold table doesn't need a manual
-- grant added every time.
GRANT SELECT ON ALL TABLES IN SCHEMA training.gold TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA training.gold TO ROLE analyst_role;

-- Deliberately no grants on training.raw or training.clean - default
-- deny means analyst_role has zero access to either.

SHOW GRANTS TO ROLE analyst_role;

-- ---------------------------------------------------------------------
-- Attach the role to the account's user and prove the restriction holds
-- ---------------------------------------------------------------------
GRANT ROLE analyst_role TO USER Hafsa;

USE ROLE analyst_role;
USE WAREHOUSE training_wh;

-- Expect: fails - no grant on raw
SELECT * FROM training.raw.products LIMIT 1;

-- Expect: succeeds - SELECT granted on gold
SELECT * FROM training.gold.product_sales_snowpark LIMIT 1;

-- ---------------------------------------------------------------------
-- Secondary roles can silently widen access beyond what was explicitly
-- granted; confirm the restriction holds with secondary roles off too.
-- ---------------------------------------------------------------------
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE();
SELECT CURRENT_SECONDARY_ROLES();
SELECT * FROM training.raw.products LIMIT 1;  -- expect: still fails

-- ---------------------------------------------------------------------
-- Switch back to admin for any further setup work
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
