"""Gold: materialized views over silver.

  account_balances     debits, credits and normal-side balance per account and currency
  daily_trial_balance  debits against credits per business unit, currency and entry date

Materialized views rather than streaming tables: both are aggregates over SCD1 tables
that change in place (updates, deletes), and a late account changes rows that were
already aggregated. A materialized view is always the result of its query over current
silver, so late arrivals reconcile on the next update without extra logic.
"""

from pyspark import pipelines as dp

from transforms import gold

CATALOG = spark.conf.get("gl.catalog")


def silver_inputs():
    return (
        spark.read.table(f"{CATALOG}.silver.journal_entries"),
        spark.read.table(f"{CATALOG}.silver.account_history"),
    )


# Warn only. A journal line whose account has not landed yet is usually a late account,
# so the row is kept (NULL business_unit and account_type) and the count shows in the
# event log. It resolves by itself once the account lands.
@dp.materialized_view(
    name=f"{CATALOG}.gold.account_balances",
    comment="Debits, credits and normal-side balance per account and currency.",
)
@dp.expect("account_known", "account_type IS NOT NULL")
def account_balances():
    return gold.account_balances(*silver_inputs())


@dp.materialized_view(
    name=f"{CATALOG}.gold.daily_trial_balance",
    comment="Debits against credits per business unit, currency and entry date.",
)
def daily_trial_balance():
    return gold.daily_trial_balance(*silver_inputs())
