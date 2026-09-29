-- What each group actually sees. Run by hand in the SQL editor, signed in as the test
-- user, once per step below. governance_check.py proves the masks and filters are
-- attached. This proves they behave.
--
-- Group changes are made in the account console by an account admin. A change can take
-- a minute or two to reach a running warehouse. Wait for the first query to reflect the
-- new membership before recording anything.
--
-- Steps and expected results for the test user:
--   1. no groups
--        every query fails: no USE CATALOG on gl_dev
--   2. gl_analysts_treasury
--        gold: TREASURY rows only, no NULL business_unit rows
--        silver: fails, no USE SCHEMA
--   3. gl_analysts_treasury + gl_engineers
--        gold: every unit, including NULL business_unit
--        silver and bronze: account_number '********1234', name and email 'REDACTED',
--        account quarantine payloads 'REDACTED', journal payloads readable
--   4. gl_analysts_treasury + gl_engineers + gl_pii_readers
--        everything in clear
-- Then remove the test user from every group again.
--
-- Run the queries one at a time. In steps 1 and 2 some are expected to fail, and a failure
-- stops a run-all at that point.

USE CATALOG gl_dev;

SELECT
  current_user() AS user,
  is_account_group_member('gl_analysts_treasury') AS treasury,
  is_account_group_member('gl_engineers') AS engineer,
  is_account_group_member('gl_pii_readers') AS pii_reader;

-- Gold: which units are visible, and how much of each.
SELECT business_unit, count(*) AS rows, sum(line_count) AS lines
FROM gold.daily_trial_balance
GROUP BY business_unit
ORDER BY business_unit;

SELECT business_unit, count(*) AS accounts
FROM gold.account_balances
GROUP BY business_unit
ORDER BY business_unit;

-- Silver: masked PII and filtered units.
SELECT account_id, account_number, holder_name, holder_email, business_unit
FROM silver.accounts
ORDER BY account_id
LIMIT 5;

SELECT business_unit, count(*) AS versions
FROM silver.account_history
GROUP BY business_unit
ORDER BY business_unit;

-- Quarantine: account payloads masked, journal payloads readable.
SELECT source_table, left(payload, 80) AS payload
FROM silver.quarantine_events
QUALIFY row_number() OVER (PARTITION BY source_table ORDER BY _lsn) = 1;

-- Bronze: the row structs with PII fields masked.
SELECT after.account_number, after.holder_name, after.holder_email, after.business_unit
FROM bronze.accounts_cdc_raw
WHERE after IS NOT NULL
LIMIT 5;

-- Pipeline backing tables hold unmasked, unfiltered copies of the data. Unity Catalog
-- reports no effective privilege on them for any group, and these queries confirm it:
-- both must fail with a permission error at every step. Names embed the pipeline id.
-- If the pipeline is recreated, list the new names as the owner with:
--   SELECT table_schema, table_name FROM gl_dev.information_schema.tables
--   WHERE startswith(table_name, '__materialization_');
SELECT * FROM gold.__materialization_mat_75bb9baf_ffe1_49e4_8869_28c924a7057e_daily_trial_balance_1 LIMIT 5;
SELECT * FROM silver.__materialization_mat_75bb9baf_ffe1_49e4_8869_28c924a7057e_accounts_1 LIMIT 5;
