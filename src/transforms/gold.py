"""Gold aggregates over silver: account balances and the daily trial balance.

The pipeline's materialized views call these on the silver tables, and the tests and
the state check call them on the reference silver, so both sides compute gold the same
way.

Account attributes come from the latest version in account_history, not from the
current accounts table. A deleted account's journal lines stay live, and its last
version still says which business unit and type it had. Only an account that has not
landed at all has no version, and its lines keep a NULL business_unit and account_type
until it does.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# The side that increases each account type. A balance is reported in that direction,
# so a healthy liability shows as positive, the way a ledger reads.
DEBIT_NORMAL = ("ASSET", "EXPENSE")
CREDIT_NORMAL = ("LIABILITY", "EQUITY", "REVENUE")


def latest_accounts(account_history: DataFrame) -> DataFrame:
    """One row per account id: its most recent version, open or closed."""
    newest = Window.partitionBy("account_id").orderBy(F.col("__START_AT").desc())
    return (
        account_history.withColumn("_rank", F.row_number().over(newest))
        .filter("_rank = 1")
        .select("account_id", "business_unit", "account_type")
    )


def _lines(journal_entries: DataFrame, account_history: DataFrame) -> DataFrame:
    """Journal lines with their account's unit and type, and the amount split by side.

    Left join: an orphan line keeps its amount in gold instead of disappearing.
    """
    zero = F.lit(0).cast("DECIMAL(18,2)")
    return journal_entries.join(latest_accounts(account_history), "account_id", "left").select(
        "*",
        F.when(F.col("side") == "DEBIT", F.col("amount")).otherwise(zero).alias("_debit"),
        F.when(F.col("side") == "CREDIT", F.col("amount")).otherwise(zero).alias("_credit"),
    )


def _totals() -> list:
    return [
        F.count(F.lit(1)).alias("line_count"),
        F.sum("_debit").alias("debit_total"),
        F.sum("_credit").alias("credit_total"),
    ]


def account_balances(journal_entries: DataFrame, account_history: DataFrame) -> DataFrame:
    """Debits, credits and normal-side balance per account and currency.

    balance is NULL for an orphan: its sign depends on an account type nobody knows yet.
    """
    debit_normal = ", ".join(f"'{t}'" for t in DEBIT_NORMAL)
    credit_normal = ", ".join(f"'{t}'" for t in CREDIT_NORMAL)
    balance = F.expr(
        f"CASE WHEN account_type IN ({debit_normal}) THEN debit_total - credit_total"
        f" WHEN account_type IN ({credit_normal}) THEN credit_total - debit_total END"
    )
    return (
        _lines(journal_entries, account_history)
        .groupBy("account_id", "business_unit", "account_type", "currency")
        .agg(*_totals())
        .withColumn("balance", balance)
    )


def daily_trial_balance(journal_entries: DataFrame, account_history: DataFrame) -> DataFrame:
    """Debits against credits per business unit, currency and entry date.

    net is debits minus credits and is zero when the books balance. Orphan lines sit
    under a NULL business_unit, so while an account is late both that group and the
    account's real unit show a non-zero net.
    """
    return (
        _lines(journal_entries, account_history)
        .groupBy("business_unit", "currency", "entry_date")
        .agg(*_totals())
        .withColumn("net", F.col("debit_total") - F.col("credit_total"))
    )
