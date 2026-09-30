"""Fails when gold.daily_trial_balance lags the landing files by more than the SLA.

Runs every 15 minutes in prod (schedule paused by default), and by hand:

  databricks bundle run freshness_check -t dev
  databricks bundle run freshness_check -t dev --params sla_minutes=1

A landing file is covered once a completed pipeline update that refreshed gold started
after the file landed. The lag is the age of the oldest file not yet covered. Landing
time is the S3 modification time, the moment the file actually arrived, not the dt=/hh=
partition, which the generator names after simulated time.

Update history comes from the Pipelines API rather than the table, because a
materialized view has no history of its own to read.
"""

import argparse
import sys
from datetime import UTC, datetime, timedelta

# Newest updates to inspect. A 30 minute schedule makes 48 a day, so 25 covers about
# 12 hours; if none of them refreshed gold, every recent file counts as uncovered.
UPDATES_INSPECTED = 25


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--pipeline-id", required=True)
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    p.add_argument("--sla-minutes", type=float, default=60.0)
    args = p.parse_args()

    sys.path.insert(0, args.src)
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.runtime import dbutils

    from transforms import cdc, checks

    now = datetime.now(UTC)
    gold_table = f"{args.catalog}.gold.daily_trial_balance"
    w = WorkspaceClient()
    response = w.pipelines.list_updates(args.pipeline_id, max_results=UPDATES_INSPECTED)
    updates = [
        {
            "creation_time": datetime.fromtimestamp(u.creation_time / 1000, UTC),
            "state": u.state.value if u.state else None,
            "selection": [*(u.refresh_selection or []), *(u.full_refresh_selection or [])],
            "validate_only": bool(u.validate_only),
        }
        for u in response.updates or []
    ]
    last_refresh = checks.last_gold_refresh(updates, gold_table)

    # Partitions are named by landing time and the generator backdates them by at most
    # its --minutes window, so starting a day before the last refresh finds every file
    # that could have landed after it.
    since = (last_refresh - timedelta(days=1)).date() if last_refresh else None
    file_times = [
        mtime
        for table in cdc.TABLES
        for _, mtime in checks.landing_files(
            dbutils.fs.ls, f"{args.landing_root}/{table}/", from_day=since
        )
    ]

    fresh, lag = checks.freshness_verdict(file_times, last_refresh, now, args.sla_minutes)
    print(f"last gold refresh started {last_refresh}")
    print(f"landing files checked: {len(file_times)}, newest {max(file_times, default=None)}")
    if lag is None:
        print("every landing file is covered by a gold refresh")
    else:
        print(f"oldest uncovered file has waited {lag}, SLA {args.sla_minutes} minutes")
    if not fresh:
        raise SystemExit(f"{gold_table} is stale")
    print("freshness check passed")


if __name__ == "__main__":
    main()
