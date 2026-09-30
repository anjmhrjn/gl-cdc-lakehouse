"""Fails when debits and credits do not net to zero per business unit, currency and day.

Runs after every scheduled pipeline update, and by hand:

  databricks bundle run trial_balance_check -t dev
  databricks bundle run trial_balance_check -t dev --params max_orphan_hours=0

Gaps caused by late accounts are tolerated with a warning while the orphan lines are
younger than --max-orphan-hours (see transforms/checks.py). Anything else exits
non-zero, which fails the run and sends the failure email. A failure here is a data
problem, so the task never retries.

Runs as the job owner, who is in gl_engineers, so the business_unit row filter on gold
hides nothing, including the NULL-unit orphan groups.
"""

import argparse
import sys
from datetime import UTC, datetime


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    p.add_argument("--max-orphan-hours", type=float, default=48.0)
    args = p.parse_args()

    sys.path.insert(0, args.src)
    from pyspark.sql import SparkSession

    from transforms import checks

    spark = SparkSession.builder.getOrCreate()
    tb = spark.table(f"{args.catalog}.gold.daily_trial_balance")
    groups = checks.unbalanced_groups(tb).orderBy("entry_date", "currency").collect()
    stale = checks.stale_orphans(
        spark.table(f"{args.catalog}.silver.journal_entries"),
        spark.table(f"{args.catalog}.silver.account_history"),
        datetime.now(UTC),
        args.max_orphan_hours,
    ).collect()

    for g in groups:
        label = "explained by late account" if g["explained"] else "UNBALANCED"
        print(f"{g['entry_date']} {g['currency']} {g['business_unit']}: net {g['net']} ({label})")
    for row in stale[:20]:
        print(f"stale orphan {row['entry_id']} ({row['account_id']}) posted {row['posted_at']}")
    if len(stale) > 20:
        print(f"... and {len(stale) - 20} more stale orphan lines")

    explained = sum(g["explained"] for g in groups)
    verdict = checks.trial_balance_verdict(
        unexplained=len(groups) - explained, explained=explained, stale_orphans=len(stale)
    )
    print(
        f"trial balance: {len(groups) - explained} unexplained groups, {explained} explained "
        f"by late accounts, {len(stale)} orphan lines older than {args.max_orphan_hours}h"
    )
    if verdict == "fail":
        raise SystemExit("trial balance check failed")
    print(f"trial balance check: {verdict}")


if __name__ == "__main__":
    main()
