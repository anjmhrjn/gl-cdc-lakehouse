"""Silver and gold match the reference state computed straight from the landing files.

Runs as the `silver_state_check` bundle job on serverless compute:

  databricks bundle run silver_state_check -t dev

It reads the landing NDJSON directly, not bronze, so a bug in bronze cannot hide
itself. For every silver and gold table and for quarantine_events it prints a row count
and an order-independent checksum. Run it after a pipeline update and again after a
full refresh: identical checksums show the rebuild is idempotent.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Ingest time and file path differ on every rebuild, so they are left out of the
# quarantine comparison and checksum.
QUARANTINE_COMPARED = ["source_table", "reasons", "payload", "_op", "_lsn"]

# quarantine_events is a log of deliveries. A re-delivered unparseable line always adds
# a row, since it skips the dedupe, and a re-delivered rejected event adds one once the
# stream dedupe no longer holds its original. Silver and gold must not change, but the
# quarantine row count legitimately does, so quarantine is compared as a set of
# distinct rows.
SET_COMPARED = {"silver.quarantine_events"}


def checksum(df: DataFrame) -> int:
    """Sum of per-row hashes. Independent of row order and file layout."""
    total = df.select(F.sum(F.xxhash64(*df.columns).cast("DECIMAL(38,0)"))).first()[0]
    return int(total or 0)


def compare(name: str, want: DataFrame, got: DataFrame) -> bool:
    """Row-for-row comparison. Prints counts and the checksum of the actual table.

    The expected count is derived from actual, missing and extra rather than counted.
    Counting the unparseable reference rows directly would read only _corrupt_record
    from the raw JSON, which Spark refuses, and serverless cannot cache around it.
    exceptAll reads every column, so it is safe.
    """
    missing = want.exceptAll(got).count()
    extra = got.exceptAll(want).count()
    actual = got.count()
    print(
        f"{name:26} expected={actual - extra + missing:>7} actual={actual:>7} "
        f"missing={missing:>5} extra={extra:>5} checksum={checksum(got)}"
    )
    return not (missing or extra)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    args = p.parse_args()

    sys.path.insert(0, args.src)
    from transforms import cdc, gold

    spark = SparkSession.builder.getOrCreate()

    flat = {}
    for table in cdc.TABLES:
        raw = cdc.read_landing(spark.read, table).json(f"{args.landing_root}/{table}/")
        flat[table] = cdc.flatten(raw, table)
    events = {table: cdc.clean_events(df, table) for table, df in flat.items()}

    silver = {
        "accounts": cdc.expected_scd1(events["accounts"], "accounts"),
        "account_history": cdc.expected_scd2(events["accounts"], "accounts"),
        "journal_entries": cdc.expected_scd1(events["journal_entries"], "journal_entries"),
    }
    quarantine = cdc.quarantined(flat["accounts"], "accounts").unionByName(
        cdc.quarantined(flat["journal_entries"], "journal_entries")
    )
    gold_inputs = (silver["journal_entries"], silver["account_history"])
    expected = {
        **{f"silver.{name}": df for name, df in silver.items()},
        "silver.quarantine_events": quarantine.select(*QUARANTINE_COMPARED),
        "gold.account_balances": gold.account_balances(*gold_inputs),
        "gold.daily_trial_balance": gold.daily_trial_balance(*gold_inputs),
    }

    failed = []
    for name, want in expected.items():
        got = spark.table(f"{args.catalog}.{name}").select(*want.columns)
        if name in SET_COMPARED:
            want, got = want.distinct(), got.distinct()
        if not compare(name, want, got):
            failed.append(name)

    print("\nquarantine reasons:")
    reasons = (
        spark.table(f"{args.catalog}.silver.quarantine_events")
        .select("source_table", F.explode("reasons").alias("reason"))
        .groupBy("source_table", "reason")
        .count()
        .orderBy("source_table", "reason")
    )
    for row in reasons.collect():
        print(f"  {row['source_table']:16} {row['reason']:20} {row['count']:>5}")

    trial = spark.table(f"{args.catalog}.gold.daily_trial_balance")
    orphans = trial.filter("business_unit IS NULL").count()
    unbalanced = trial.filter("net != 0").count()
    print(f"\ntrial balance: {unbalanced} unbalanced groups, {orphans} orphan groups")

    if failed:
        raise SystemExit(f"differs from the reference state: {', '.join(failed)}")
    print("silver, quarantine and gold match the reference state")
    return 0


if __name__ == "__main__":
    main()
