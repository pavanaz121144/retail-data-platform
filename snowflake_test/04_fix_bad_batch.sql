-- Fix the bad row, then re-run the pipeline: the watermark never moved, so batch 3 is picked up again
USE DATABASE FRAMEWORK_TEST; USE WAREHOUSE FRAMEWORK_WH;
UPDATE RAW_INV_WEEKLY.TCFD_INSTRUMENT_LEVEL_RAW SET FUND_CODE = 'FUNDD' WHERE FIL_FMR_ID = 'FMR004';
