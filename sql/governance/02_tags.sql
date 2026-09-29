-- Table classification and PII column tags.
--
-- Pipeline tables take ALTER STREAMING TABLE or ALTER MATERIALIZED VIEW, not ALTER
-- TABLE. Setting a tag to the value it already has is a no-op, so this file re-runs
-- cleanly.
--
-- confidential: tables that hold PII (the account tables, bronze raw, quarantine
--   payloads) and journal_entries, which is the ledger itself at line level.
-- internal: gold aggregates. No PII, and business units are separated by the row filter.

ALTER STREAMING TABLE bronze.accounts_cdc_raw SET TAGS ('classification' = 'confidential');
ALTER STREAMING TABLE bronze.journal_entries_cdc_raw SET TAGS ('classification' = 'confidential');
ALTER STREAMING TABLE silver.accounts SET TAGS ('classification' = 'confidential');
ALTER STREAMING TABLE silver.account_history SET TAGS ('classification' = 'confidential');
ALTER STREAMING TABLE silver.journal_entries SET TAGS ('classification' = 'confidential');
ALTER STREAMING TABLE silver.quarantine_events SET TAGS ('classification' = 'confidential');
ALTER MATERIALIZED VIEW gold.account_balances SET TAGS ('classification' = 'internal');
ALTER MATERIALIZED VIEW gold.daily_trial_balance SET TAGS ('classification' = 'internal');

ALTER STREAMING TABLE silver.accounts ALTER COLUMN account_number SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.accounts ALTER COLUMN holder_name SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.accounts ALTER COLUMN holder_email SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.account_history ALTER COLUMN account_number SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.account_history ALTER COLUMN holder_name SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.account_history ALTER COLUMN holder_email SET TAGS ('pii' = 'true');

-- Raw layers. The row structs carry all three PII fields. _corrupt_record and
-- _rescued_data are raw text and can too. quarantine payloads are raw account events.
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN before SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN after SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN _corrupt_record SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE bronze.accounts_cdc_raw ALTER COLUMN _rescued_data SET TAGS ('pii' = 'true');
ALTER STREAMING TABLE silver.quarantine_events ALTER COLUMN payload SET TAGS ('pii' = 'true');
