"""Silver matches the reference state computed straight from the landing files.

Runs as the `silver_state_check` bundle job on serverless compute:

  databricks bundle run silver_state_check -t dev

It reads the landing NDJSON directly, not bronze, so a bug in bronze cannot hide
itself. It prints a row count and an order-independent checksum per table. Run it
after a pipeline update and again after a full refresh: identical checksums show the
rebuild is idempotent.
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def checksum(df: DataFrame) -> int:
    """Sum of per-row hashes. Independent of row order and file layout."""
    total = df.select(F.sum(F.xxhash64(*df.columns).cast("DECIMAL(38,0)"))).first()[0]
    return int(total or 0)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    args = p.parse_args()

    sys.path.insert(0, args.src)
    from transforms import cdc

    spark = SparkSession.builder.getOrCreate()

    events = {}
    for table in cdc.TABLES:
        raw = cdc.read_landing(spark.read, table).json(f"{args.landing_root}/{table}/")
        events[table] = cdc.clean_events(cdc.flatten(raw, table), table)

    expected = {
        "accounts": cdc.expected_scd1(events["accounts"], "accounts"),
        "account_history": cdc.expected_scd2(events["accounts"], "accounts"),
        "journal_entries": cdc.expected_scd1(events["journal_entries"], "journal_entries"),
    }

    failed = []
    for name, want in expected.items():
        got = spark.table(f"{args.catalog}.silver.{name}").select(*want.columns)
        missing = want.exceptAll(got).count()
        extra = got.exceptAll(want).count()
        print(
            f"{name:16} expected={want.count():>7} actual={got.count():>7} "
            f"missing={missing:>5} extra={extra:>5} checksum={checksum(got)}"
        )
        if missing or extra:
            failed.append(name)

    if failed:
        raise SystemExit(f"silver differs from the reference state: {', '.join(failed)}")
    print("silver matches the reference state")
    return 0


if __name__ == "__main__":
    main()
