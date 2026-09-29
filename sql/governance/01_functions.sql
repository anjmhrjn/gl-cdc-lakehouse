-- Column mask and row filter functions, all in the silver schema.
--
-- Group checks use is_account_group_member because the gl_* groups are account-level
-- groups. is_member only sees workspace-local groups.
--
-- Each function is one flat expression and calls no other UDF, even where that repeats
-- the account number rule. The docs list "nesting" among the policy features MERGE does
-- not support without defining it, and AUTO CDC writes silver with MERGE, so these stay
-- as plain as possible.
--
-- A NULL input stays NULL for everyone: a missing value is not PII, and hiding it would
-- hide data quality problems from engineers.

-- Everyone outside gl_pii_readers sees only the last four characters. A value of four
-- characters or fewer is masked completely, because its last four would be all of it.
CREATE OR REPLACE FUNCTION silver.mask_account_number(account_number STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('gl_pii_readers') THEN account_number
  WHEN length(account_number) <= 4 THEN repeat('*', length(account_number))
  ELSE concat(repeat('*', length(account_number) - 4), right(account_number, 4))
END;

-- Holder name and email, and raw bronze text that may contain them.
CREATE OR REPLACE FUNCTION silver.mask_pii(value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('gl_pii_readers') OR value IS NULL THEN value
  ELSE 'REDACTED'
END;

-- quarantine_events.payload holds the raw event for both source tables. Only account
-- events carry PII. Journal payloads stay readable so engineers can debug quarantine.
CREATE OR REPLACE FUNCTION silver.mask_payload(payload STRING, source_table STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('gl_pii_readers') OR payload IS NULL THEN payload
  WHEN source_table = 'journal_entries' THEN payload
  ELSE 'REDACTED'
END;

-- bronze.accounts_cdc_raw.before and .after. A mask on a STRUCT column must return
-- the column's exact type, so the three PII fields are masked and the other fields
-- are copied through in the bronze field order.
CREATE OR REPLACE FUNCTION silver.mask_account_row(
  account_row STRUCT<
    account_id: STRING, account_number: STRING, holder_name: STRING,
    holder_email: STRING, business_unit: STRING, account_type: STRING,
    currency: STRING, status: STRING, opened_at: STRING, updated_at: STRING
  >
)
RETURNS STRUCT<
  account_id: STRING, account_number: STRING, holder_name: STRING,
  holder_email: STRING, business_unit: STRING, account_type: STRING,
  currency: STRING, status: STRING, opened_at: STRING, updated_at: STRING
>
RETURN CASE
  WHEN is_account_group_member('gl_pii_readers') OR account_row IS NULL THEN account_row
  ELSE named_struct(
    'account_id', account_row.account_id,
    'account_number', CASE
      WHEN length(account_row.account_number) <= 4
        THEN repeat('*', length(account_row.account_number))
      ELSE concat(
        repeat('*', length(account_row.account_number) - 4),
        right(account_row.account_number, 4)
      )
    END,
    'holder_name', CASE WHEN account_row.holder_name IS NULL THEN NULL ELSE 'REDACTED' END,
    'holder_email', CASE WHEN account_row.holder_email IS NULL THEN NULL ELSE 'REDACTED' END,
    'business_unit', account_row.business_unit,
    'account_type', account_row.account_type,
    'currency', account_row.currency,
    'status', account_row.status,
    'opened_at', account_row.opened_at,
    'updated_at', account_row.updated_at
  )
END;

-- Row filter on business_unit. Engineers see every row. Each analyst group sees its own
-- unit. A row with a NULL business_unit (a journal line whose account has not landed)
-- is visible to engineers only, since no analyst unit can claim it yet.
CREATE OR REPLACE FUNCTION silver.bu_filter(business_unit STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('gl_engineers')
  OR (business_unit = 'TREASURY' AND is_account_group_member('gl_analysts_treasury'))
  OR (business_unit = 'CORP_BANKING' AND is_account_group_member('gl_analysts_corp_banking'))
  OR (business_unit = 'ASSET_MGMT' AND is_account_group_member('gl_analysts_asset_mgmt'));
