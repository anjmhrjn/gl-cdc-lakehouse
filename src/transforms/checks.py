"""Decisions behind the reliability jobs, kept pure so the unit tests can pin them down.

  trial_balance_check  unbalanced_groups, stale_orphans, trial_balance_verdict
  freshness_check      last_gold_refresh, freshness_verdict, landing_files
  backfill_replay      replay_copies, landing_files

The jobs in src/jobs/ only read tables, list files and call these.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from transforms import gold


def unbalanced_groups(trial_balance: DataFrame) -> DataFrame:
    """Trial balance groups whose net is not zero, each marked explained or not.

    A late account moves its lines to a NULL business_unit, so its real unit is short
    by exactly what the NULL group holds. A gap counts as explained when a NULL group
    exists for the same currency and entry date and all groups for that currency and
    date net to zero together. Without a NULL group, a zero total across units still
    means two real units are wrong, for example a journal whose lines crossed units.
    """
    day = Window.partitionBy("currency", "entry_date")
    has_orphans = F.max(F.col("business_unit").isNull().cast("int")).over(day) == 1
    return (
        trial_balance.withColumn("_day_net", F.sum("net").over(day))
        .withColumn("_has_orphans", has_orphans)
        .filter("net != 0")
        .withColumn("explained", F.col("_has_orphans") & (F.col("_day_net") == 0))
        .select("business_unit", "currency", "entry_date", "net", "explained")
    )


def stale_orphans(
    journal_entries: DataFrame, account_history: DataFrame, now: datetime, max_hours: float
) -> DataFrame:
    """Journal lines whose account has not landed and that were posted too long ago.

    Age comes from posted_at, the source's time, not _ingested_at: ingest time resets on
    a full refresh, which would make an old orphan look new. A NULL posted_at (a failed
    try_cast) has no age, so it counts as stale rather than being waited on forever.
    """
    known = gold.latest_accounts(account_history).select("account_id")
    cutoff = F.lit(now - timedelta(hours=max_hours))
    return (
        journal_entries.join(known, "account_id", "left_anti")
        .filter(F.col("posted_at").isNull() | (F.col("posted_at") < cutoff))
        .select("entry_id", "account_id", "posted_at")
    )


def trial_balance_verdict(unexplained: int, explained: int, stale_orphans: int) -> str:
    """fail on any unexplained gap or stale orphan; warn when only late accounts are
    pending; pass when every group nets to zero."""
    if unexplained or stale_orphans:
        return "fail"
    if explained:
        return "warn"
    return "pass"


def last_gold_refresh(updates: list[dict], gold_table: str) -> datetime | None:
    """Start time of the newest completed pipeline update that refreshed gold_table.

    Each update is a dict with creation_time, state, selection (the tables named in
    refresh_selection and full_refresh_selection) and validate_only. An empty selection
    is a full graph update. A validate-only update materializes nothing.
    """
    times = [
        u["creation_time"]
        for u in updates
        if u["state"] == "COMPLETED"
        and not u["validate_only"]
        and (not u["selection"] or gold_table in u["selection"])
    ]
    return max(times, default=None)


def freshness_verdict(
    file_times: list[datetime], last_refresh: datetime | None, now: datetime, sla_minutes: float
) -> tuple[bool, timedelta | None]:
    """(fresh, lag): lag is how long the oldest file not yet covered by a gold refresh
    has waited, None if every file is covered.

    A file is covered only if the update started strictly after it landed. The update
    lists landing files some time after it starts, so a file landing at the same moment
    may or may not be in it, and the check assumes it is not.
    """
    pending = [t for t in file_times if last_refresh is None or t >= last_refresh]
    if not pending:
        return True, None
    lag = now - min(pending)
    return lag <= timedelta(minutes=sla_minutes), lag


def landing_files(
    ls, table_root: str, from_day: date | None = None, to_day: date | None = None
) -> list[tuple[str, datetime]]:
    """(path, modification time) of every file under table_root/dt=*/hh=*/, limited to
    dt partitions in [from_day, to_day] when given.

    `ls` is dbutils.fs.ls, passed in so the tests can fake it. Only the matching dt
    partitions are listed, so the cost follows the range, not the whole history.
    """
    files = []
    for day_dir in ls(table_root):
        m = re.fullmatch(r"dt=(\d{4}-\d{2}-\d{2})/?", day_dir.name)
        if not m:
            continue
        day = date.fromisoformat(m[1])
        if (from_day and day < from_day) or (to_day and day > to_day):
            continue
        for hour_dir in ls(day_dir.path):
            if not hour_dir.name.startswith("hh="):
                continue
            for f in ls(hour_dir.path):
                if not f.name.endswith("/"):
                    mtime = datetime.fromtimestamp(f.modificationTime / 1000, UTC)
                    files.append((f.path, mtime))
    return files


# <root>/<table>/dt=YYYY-MM-DD/hh=HH/<name>, the layout generator/emit.py writes.
_LANDING_PATH = re.compile(
    r"^(?P<root>.+)/(?P<table>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hh=\d{2}/(?P<name>[^/]+)$"
)
REPLAY_PREFIX = "replay-"

# How far back a range replay may start. silver.py keeps delete tombstones for 7 days.
# A file landed on from_day holds events at most 48h older than that, and any delete
# that supersedes one of them happened after it. Starting no more than 5 days back keeps
# every such delete inside the 7 days, so a re-delivered event for a deleted key meets
# its tombstone instead of re-inserting the row. Older ranges need a full refresh.
REPLAY_MAX_AGE_DAYS = 5


def replay_copies(
    paths: list[str], from_day: date, to_day: date, run_id: str, now: datetime
) -> list[tuple[str, str]]:
    """(source, destination) for every landing file with from_day <= dt <= to_day.

    A copy is a re-delivery, so it lands in the current dt=/hh= partition, the same
    landing-time rule the generator follows. Earlier copies are skipped: replaying a
    range re-delivers the original files once, not every copy made since.
    """
    if from_day > to_day:
        raise ValueError(f"from {from_day} is after to {to_day}")
    if (now.date() - from_day).days > REPLAY_MAX_AGE_DAYS:
        raise ValueError(
            f"from {from_day} is more than {REPLAY_MAX_AGE_DAYS} days back; replaying it could"
            " re-insert deleted rows after their tombstones expired. Use a full refresh."
        )
    copies = []
    for path in paths:
        m = _LANDING_PATH.match(path)
        if not m or m["name"].startswith(REPLAY_PREFIX):
            continue
        if not from_day <= date.fromisoformat(m["dt"]) <= to_day:
            continue
        dest = (
            f"{m['root']}/{m['table']}/dt={now:%Y-%m-%d}/hh={now:%H}/"
            f"{REPLAY_PREFIX}{run_id}-{m['name']}"
        )
        copies.append((path, dest))
    return copies
