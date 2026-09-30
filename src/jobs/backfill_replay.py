"""Reprocesses landing files for a date range, or full-refreshes selected tables.

Runs as the first task of the `backfill_replay` job, followed by the governance job and
governance_check, so masks and filters are back on any table a refresh recreated.
See docs/RUNBOOK.md for when to use which mode.

  range    Copies every original landing file with from <= dt <= to into the current
           dt=/hh= partition as replay-<run id>-<name>, then runs a normal update.
           Auto Loader tracks files by path, so the copies are ingested again: a
           re-delivery of the same events. Silver must end up unchanged, which is what
           the re-run proof checks.
  refresh  Full refresh. `all` resets every table. Otherwise a space separated list of
           silver and gold tables (schema.table) is reset, and a normal update follows
           so gold catches up with them. Spaces, because `bundle run --params` already
           splits on commas.

Bronze can only be reset with `all`. Silver streams read bronze from a checkpoint, and
resetting bronze under them would leave that checkpoint pointing into a table that
has been rewritten.

The pipeline must be idle: the API refuses to start an update while another runs.
Pause the schedule first in prod.
"""

import argparse
import sys
import time
from datetime import UTC, date, datetime

POLL_SECONDS = 30
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}


def run_update(w, pipeline_id: str, **selection) -> None:
    """Start a pipeline update and wait for it. Exits non-zero unless it completes."""
    update_id = w.pipelines.start_update(pipeline_id, **selection).update_id
    print(f"update {update_id} started {selection or '(full graph)'}")
    while True:
        state = w.pipelines.get_update(pipeline_id, update_id).update.state.value
        if state in TERMINAL:
            break
        time.sleep(POLL_SECONDS)
    print(f"update {update_id} {state}")
    if state != "COMPLETED":
        raise SystemExit(f"pipeline update {update_id} ended {state}")


def replay_range(args, cdc, checks) -> None:
    from databricks.sdk.runtime import dbutils

    if not (args.from_day and args.to_day):
        raise SystemExit("range mode needs --from and --to (YYYY-MM-DD)")
    from_day, to_day = date.fromisoformat(args.from_day), date.fromisoformat(args.to_day)
    now = datetime.now(UTC)
    copies = []
    for table in cdc.TABLES:
        files = checks.landing_files(
            dbutils.fs.ls, f"{args.landing_root}/{table}/", from_day, to_day
        )
        try:
            paths = [p for p, _ in files]
            copies += checks.replay_copies(paths, from_day, to_day, args.run_id, now)
        except ValueError as e:
            raise SystemExit(str(e)) from e
    if not copies:
        raise SystemExit(f"no landing files between {from_day} and {to_day}")
    for src, dst in copies:
        dbutils.fs.cp(src, dst)
    print(f"re-delivered {len(copies)} files from {from_day}..{to_day} as replay-{args.run_id}-*")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--pipeline-id", required=True)
    p.add_argument("--src", required=True, help="workspace path of src/, for transforms")
    p.add_argument("--run-id", required=True, help="job run id, names the replay copies")
    p.add_argument("--mode", required=True, choices=["range", "refresh"])
    p.add_argument("--from", dest="from_day", default="")
    p.add_argument("--to", dest="to_day", default="")
    p.add_argument("--tables", default="", help="refresh mode: `all` or schema.table ...")
    args = p.parse_args()

    sys.path.insert(0, args.src)
    from databricks.sdk import WorkspaceClient

    from transforms import cdc, checks

    w = WorkspaceClient()

    if args.mode == "range":
        replay_range(args, cdc, checks)
        run_update(w, args.pipeline_id)
        return

    tables = args.tables.split()
    if tables == ["all"]:
        run_update(w, args.pipeline_id, full_refresh=True)
        return
    if not tables:
        raise SystemExit("refresh mode needs --tables: `all` or schema.table ...")
    if any(t.startswith("bronze.") for t in tables):
        raise SystemExit("bronze tables can only be refreshed with --tables all")
    run_update(
        w, args.pipeline_id, full_refresh_selection=[f"{args.catalog}.{t}" for t in tables]
    )
    run_update(w, args.pipeline_id)


if __name__ == "__main__":
    main()
