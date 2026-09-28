"""CDC event generator CLI.

  uv run python -m generator --env dev --minutes 10 --rate 50

Time is simulated, not slept through: a run covers the `--minutes` window ending
now, one file per table per simulated minute, all uploaded immediately. So a
10 minute run takes seconds and lands files in the current and recent partitions.
"""

from __future__ import annotations

import argparse
import random
from collections import Counter
from datetime import UTC, datetime, timedelta

from generator import mess, model
from generator.emit import LocalSink, S3Sink

# The run's lsn counter starts here. Late arrivals draw from the band below it so
# their lsn is genuinely older than events already processed, not just their ts_ms.
START_LSN = 5_000_000
PREHISTORY_LO = 1_000_000

ACCOUNT_EVENT_SHARE = 0.15


class Lsn:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.value = START_LSN

    def next(self) -> int:
        self.value += self.rng.randint(1, 64)
        return self.value

    def backdated(self, hours_late: float, late_hours: float) -> int:
        """An lsn inside the prehistory band, older the further back the event sits."""
        span = START_LSN - PREHISTORY_LO
        fraction = 1.0 - min(hours_late / late_hours, 1.0)
        return PREHISTORY_LO + int(span * fraction)


def build_groups(accounts: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """Accounts bucketed by (business_unit, currency). Journals never cross a bucket."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for acct in accounts:
        groups.setdefault((acct["business_unit"], acct["currency"]), []).append(acct)
    return {k: v for k, v in groups.items() if len(v) >= 2}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="generator", description=__doc__)
    p.add_argument("--env", required=True, choices=["dev", "prod"])
    p.add_argument("--minutes", type=int, default=10, help="length of the simulated window")
    p.add_argument("--rate", type=int, default=50, help="events per simulated minute")
    p.add_argument("--seed", type=int, default=42, help="same seed gives identical output")
    p.add_argument("--accounts", type=int, default=200)
    p.add_argument("--hot-share", type=float, default=0.30, help="journal lines on the hot account")
    p.add_argument("--late-hours", type=float, default=48.0)
    p.add_argument("--bucket", default="gl-cdc-lakehouse-anuj")
    p.add_argument("--profile", default="gl-lakehouse")
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--out-dir", help="write locally instead of S3")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    if args.out_dir:
        sink = LocalSink(args.out_dir)
    else:
        sink = S3Sink(args.bucket, args.profile, args.region)

    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=args.minutes)
    lsn = Lsn(rng)
    stats: Counter[str] = Counter()

    accounts = [model.make_account(rng, i, start) for i in range(1, args.accounts + 1)]
    by_id = {a["account_id"]: a for a in accounts}
    groups = build_groups(accounts)
    if not groups:
        raise SystemExit("not enough accounts to form a balanced journal, raise --accounts")

    hot_group_key = max(groups, key=lambda k: len(groups[k]))
    hot_account = groups[hot_group_key][0]
    # Two dials that multiply out to --hot-share: how often a journal comes from the
    # hot account's group, and how often a line inside such a journal is the hot account.
    p_hot_group = min(1.0, args.hot_share / 0.6)
    p_hot_line = min(1.0, args.hot_share / p_hot_group) if p_hot_group else 0.0

    # A few accounts are held back so their journal entries land before they exist.
    # That is the "warn only" orphan FK case in the data quality rules.
    late_accounts = {a["account_id"] for a in rng.sample(accounts, k=min(3, len(accounts)))}
    late_queue: dict[int, list[tuple[str, dict]]] = {}

    def queue_late(file_index: int, table: str, event: dict) -> None:
        late_queue.setdefault(file_index, []).append((table, event))

    # File 0 is the snapshot: op = r for every account that is not held back.
    snapshot = [
        model.envelope("r", "accounts", lsn.next(), 0, start, None, a)
        for a in accounts
        if a["account_id"] not in late_accounts
    ]
    for acct in accounts:
        if acct["account_id"] in late_accounts:
            release = rng.randint(max(1, args.minutes // 2), max(1, args.minutes - 1))
            hours_late = rng.uniform(1.0, args.late_hours)
            event = model.envelope(
                "c",
                "accounts",
                lsn.backdated(hours_late, args.late_hours),
                rng.randint(1, 10**6),
                start - timedelta(hours=hours_late),
                None,
                acct,
            )
            queue_late(release, "accounts", event)
            stats["late"] += 1

    entry_seq = 1
    journal_seq = 1
    recent_journals: list[list[dict]] = []
    files = 0

    for minute in range(args.minutes):
        landed = start + timedelta(minutes=minute)
        batch: dict[str, list] = {"accounts": [], "journal_entries": []}
        if minute == 0:
            batch["accounts"].extend(snapshot)
            stats["snapshot"] += len(snapshot)

        n_account_events = max(1, round(args.rate * ACCOUNT_EVENT_SHARE))
        n_entries = max(1, args.rate - n_account_events)

        for _ in range(n_account_events):
            tx = rng.randint(1, 10**6)
            roll = rng.random()
            if roll < 0.05:
                idx = len(by_id) + 1
                acct = model.make_account(rng, idx, landed)
                by_id[acct["account_id"]] = acct
                accounts.append(acct)
                batch["accounts"].append(
                    model.envelope("c", "accounts", lsn.next(), tx, landed, None, acct)
                )
            elif roll < 0.06:
                candidates = [
                    a for a in accounts if a["account_id"] != hot_account["account_id"]
                ]
                victim = rng.choice(candidates)
                batch["accounts"].append(
                    model.envelope("d", "accounts", lsn.next(), tx, landed, victim, None)
                )
            else:
                before = rng.choice(accounts)
                after = model.mutate_account(rng, before, landed)
                by_id[after["account_id"]] = after
                event = model.envelope("u", "accounts", lsn.next(), tx, landed, before, after)
                if rng.random() < mess.LATE_RATE and minute < args.minutes - 1:
                    hours_late = rng.uniform(1.0, args.late_hours)
                    event["source"]["lsn"] = lsn.backdated(hours_late, args.late_hours)
                    event["ts_ms"] = model.ms(landed - timedelta(hours=hours_late))
                    queue_late(rng.randint(minute + 1, args.minutes - 1), "accounts", event)
                    stats["late"] += 1
                else:
                    batch["accounts"].append(event)

        produced = 0
        while produced < n_entries:
            tx = rng.randint(1, 10**6)
            if recent_journals and rng.random() < 0.03:
                # Delete a whole journal, never a single line, so debits stay equal to credits.
                victim = recent_journals.pop(rng.randrange(len(recent_journals)))
                for row in victim:
                    batch["journal_entries"].append(
                        model.envelope("d", "journal_entries", lsn.next(), tx, landed, row, None)
                    )
                    produced += 1
                continue
            if recent_journals and rng.random() < 0.05:
                # Amendments only touch free text. Changing an amount would break the invariant.
                row = rng.choice(rng.choice(recent_journals))
                after = dict(
                    row,
                    description=rng.choice(model.DESCRIPTIONS),
                    updated_at=model.iso(landed),
                )
                batch["journal_entries"].append(
                    model.envelope("u", "journal_entries", lsn.next(), tx, landed, row, after)
                )
                produced += 1
                continue

            if rng.random() < p_hot_group or len(groups) == 1:
                key = hot_group_key
            else:
                key = rng.choice([k for k in groups if k != hot_group_key])
            entry_day = (landed - timedelta(days=rng.randint(0, 2))).date()
            rows = model.make_journal(
                rng, f"JRN-{journal_seq:08d}", groups[key], landed, entry_day, entry_seq
            )
            journal_seq += 1
            entry_seq += len(rows)
            if key == hot_group_key:
                for row in rows:
                    if rng.random() < p_hot_line:
                        row["account_id"] = hot_account["account_id"]
            if any(row["account_id"] in late_accounts for row in rows):
                stats["orphan_fk"] += 1
            recent_journals.append(rows)
            if len(recent_journals) > 200:
                recent_journals.pop(0)
            for row in rows:
                batch["journal_entries"].append(
                    model.envelope("c", "journal_entries", lsn.next(), tx, landed, None, row)
                )
                produced += 1

        for table, event in late_queue.pop(minute, []):
            batch[table].append(event)

        for table, events in batch.items():
            if not events:
                continue
            before_dupes = len(events)
            events = mess.duplicate(rng, events)
            stats["duplicates"] += len(events) - before_dupes
            for _ in range(len(events)):
                if rng.random() < mess.MALFORMED_RATE:
                    events.append(mess.malformed_record(rng, table, lsn.next()))
                    stats["malformed"] += 1
            events = mess.shuffle_within_file(rng, events)
            sink.write(args.env, table, events, landed, minute)
            files += 1
            stats[table] += len(events)

    # Anything still queued past the end of the window lands in the final file.
    leftover: dict[str, list] = {"accounts": [], "journal_entries": []}
    for pending in late_queue.values():
        for table, event in pending:
            leftover[table].append(event)
    for table, events in leftover.items():
        if events:
            sink.write(args.env, table, events, end, args.minutes)
            files += 1
            stats[table] += len(events)

    print(f"sink            {sink.describe()}/{args.env}/cdc/")
    print(f"window          {model.iso(start)} .. {model.iso(end)}")
    print(f"files           {files}")
    print(f"accounts        {stats['accounts']} events (snapshot {stats['snapshot']})")
    print(f"journal_entries {stats['journal_entries']} events")
    print(f"duplicates      {stats['duplicates']}")
    print(f"malformed       {stats['malformed']}")
    print(f"late arrivals   {stats['late']}")
    print(f"hot account     {hot_account['account_id']} ({hot_group_key[0]}/{hot_group_key[1]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
