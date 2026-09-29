-- Attach the masks and row filters from 01_functions.sql.
--
-- Set with ALTER rather than in the pipeline definition, so all governance lives in
-- this directory. Since pipelines release 2025.29, pipeline updates keep masks and
-- filters set this way instead of removing them.
--
-- Two consequences, both in ARCHITECTURE.md:
-- - On refresh the functions run as the pipeline owner. The owner must be in
--   gl_pii_readers, or silver is built from masked bronze, and in gl_engineers, or gold
--   is built from filtered silver.
-- - A table created by the pipeline is unprotected until this job runs.

ALTER STREAMING TABLE silver.accounts ALTER COLUMN account_number SET MASK silver.mask_account_number;
ALTER STREAMING TABLE silver.accounts ALTER COLUMN holder_name SET MASK silver.mask_pii;
ALTER STREAMING TABLE silver.accounts ALTER COLUMN holder_email SET MASK silver.mask_pii;
ALTER STREAMING TABLE silver.account_history ALTER COLUMN account_number SET MASK silver.mask_account_number;
ALTER STREAMING TABLE silver.account_history ALTER COLUMN holder_name SET MASK silver.mask_pii;
ALTER STREAMING TABLE silver.account_history ALTER COLUMN holder_email SET MASK silver.mask_pii;

ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN before SET MASK silver.mask_account_row;
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN after SET MASK silver.mask_account_row;
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN _corrupt_record SET MASK silver.mask_pii;
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN _rescued_data SET MASK silver.mask_pii;

ALTER STREAMING TABLE silver.quarantine_events ALTER COLUMN payload
  SET MASK silver.mask_payload USING COLUMNS (source_table);

-- journal_entries has no business_unit column and analysts have no silver access, so it
-- gets no filter. The unit comes from the account, and gold carries it.
ALTER STREAMING TABLE silver.accounts SET ROW FILTER silver.bu_filter ON (business_unit);
ALTER STREAMING TABLE silver.account_history SET ROW FILTER silver.bu_filter ON (business_unit);
ALTER MATERIALIZED VIEW gold.account_balances SET ROW FILTER silver.bu_filter ON (business_unit);
ALTER MATERIALIZED VIEW gold.daily_trial_balance SET ROW FILTER silver.bu_filter ON (business_unit);
