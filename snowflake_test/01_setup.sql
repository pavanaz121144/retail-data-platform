-- =====================================================================
-- 01_setup.sql : test database, schemas, tables, and BATCH 1 of raw data
-- Run in a Snowsight worksheet (or SnowSQL) with a role that can create a database.
-- =====================================================================
USE ROLE SYSADMIN;                       -- or any role with CREATE DATABASE / CREATE WAREHOUSE

CREATE WAREHOUSE IF NOT EXISTS FRAMEWORK_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE FRAMEWORK_WH;

CREATE OR REPLACE DATABASE FRAMEWORK_TEST;
CREATE SCHEMA FRAMEWORK_TEST.STG_INVY;          -- {{ SCHEMA_STG_INVY }}      : watermark state, temp tables
CREATE SCHEMA FRAMEWORK_TEST.RAW_INV_WEEKLY;    -- {{ SCHEMA_RAW_INV_WEEKLY }}: raw source
CREATE SCHEMA FRAMEWORK_TEST.CON_INV_WEEKLY;    -- {{ SCHEMA_CON_INV_WEEKLY }}: SCD2 target
USE DATABASE FRAMEWORK_TEST;

-- ---------------------------------------------------------------------
-- SOURCE: raw instrument-level holdings (arrives in weekly files)
-- NOTE: keep NUMBER scales identical in source and target - the hash compares values as text.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_INV_WEEKLY.TCFD_INSTRUMENT_LEVEL_RAW (
    FILE_AS_OF_DATE                           TIMESTAMP_NTZ,   -- watermark column
    FIL_FMR_ID                                VARCHAR,
    FUND_CODE                                 VARCHAR,
    HOLDING_DATE                              DATE,
    PCAF_VALUATION_USD                        NUMBER(18,2),
    PCAF_VALUATION_PERCENTAGE                 NUMBER(9,4),
    VALUATION_METHOD_MARKET_VALUE_USD_AMOUNT  NUMBER(18,2),
    VALUATION_METHOD_HEAD_WEIGHTS_PERCENTAGE  NUMBER(9,4)
);

-- ---------------------------------------------------------------------
-- TARGET: SCD2 table. Starts empty. SK is filled by AUTOINCREMENT on insert.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE CON_INV_WEEKLY.TCFD_ENTITY_INSTRUMENT_HOLDING_METRIC (
    SK_TCFD_ENTITY_INSTRUMENT_HOLDING         NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    UNIQUE_ACTIVE_BUSINESS_KEY                VARCHAR NOT NULL,
    FIL_FMR_IDENTIFIER                        VARCHAR,
    VALUATION_METHOD_MARKET_VALUE_USD_AMOUNT  NUMBER(18,2),
    VALUATION_METHOD_HEAD_WEIGHTS_PERCENTAGE  NUMBER(9,4),
    BATCH_ID                                  VARCHAR,
    SUB_ID                                    VARCHAR,
    IS_ACTIVE                                 BOOLEAN NOT NULL DEFAULT TRUE,
    CREATED_DT                                TIMESTAMP_NTZ,
    CREATED_BY                                VARCHAR,
    UPDATED_DT                                TIMESTAMP_NTZ,
    UPDATED_BY                                VARCHAR
);

-- The watermark state table (STG_INVY.PIPELINE_WATERMARK_STATE) is created by the framework on first run.

-- ---------------------------------------------------------------------
-- BATCH 1  (FILE_AS_OF_DATE = 3 days ago, one older duplicate = 4 days ago)
-- ---------------------------------------------------------------------
INSERT INTO RAW_INV_WEEKLY.TCFD_INSTRUMENT_LEVEL_RAW
SELECT DATEADD(day, -3, SYSDATE()), 'FMR001', 'FUNDA', DATE '2026-09-30',  900.00, 0.0900, 1000.00, 0.1000 UNION ALL
SELECT DATEADD(day, -3, SYSDATE()), 'FMR001', 'FUNDB', DATE '2026-09-30',   -5.00, 0.0000, 2500.50, 0.2500 UNION ALL  -- negative PCAF -> WARNING only
SELECT DATEADD(day, -3, SYSDATE()), 'FMR002', 'FUNDA', DATE '2026-09-30',  650.00, 0.0650,  700.00, 0.0700 UNION ALL
SELECT DATEADD(day, -4, SYSDATE()), 'FMR002', 'FUNDA', DATE '2026-09-30',  600.00, 0.0600,  650.00, 0.0650 UNION ALL  -- older duplicate: must LOSE
SELECT DATEADD(day, -3, SYSDATE()), NULL::VARCHAR, 'FUNDZ', DATE '2026-09-30', 1.00, 0.0100, 1.00, 0.0100;               -- null FMR id: dropped by source filter

-- ---------------------------------------------------------------------
-- CHECK QUERIES (run any time)
-- ---------------------------------------------------------------------
-- SELECT * FROM RAW_INV_WEEKLY.TCFD_INSTRUMENT_LEVEL_RAW ORDER BY FILE_AS_OF_DATE, FIL_FMR_ID;
-- SELECT UNIQUE_ACTIVE_BUSINESS_KEY, VALUATION_METHOD_MARKET_VALUE_USD_AMOUNT AS MV, IS_ACTIVE,
--        CREATED_DT, UPDATED_DT, BATCH_ID
--   FROM CON_INV_WEEKLY.TCFD_ENTITY_INSTRUMENT_HOLDING_METRIC ORDER BY 1, CREATED_DT;
-- SELECT * FROM STG_INVY.PIPELINE_WATERMARK_STATE;
