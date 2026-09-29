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

# lsn is anchored to wall-clock time: LSN_PER_MS slots per millisecond. Real log sequence
# numbers only ever grow, across runs as well as within one. A fixed starting value would
# give two runs the same (key, lsn) with different rows, which no ordering can resolve.
# A run's events take far fewer than LSN_PER_MS * (time to the next run) slots, so a
# later run always sequences after an earlier one.
LSN_PER_MS = 100

ACCOUNT_EVENT_SHARE = 0.15


def run_tag(now: datetime) -> str:
    """Identifies one run: its start time in milliseconds, not rounded to the minute.

    Journal ids, entry ids, ids of accounts opened during the run and landing file
    names all carry it. With plain counters every run restarted at JE-000000001, so a
    new run overwrote the previous run's journal lines by key instead of adding to them.
    The base accounts keep stable ids: each run re-reads the same accounts, as a real
    source would.
    """
    return str(model.ms(now))


class Lsn:
    def __init__(self, rng: random.Random, now: datetime):
        self.rng = rng
        self.value = model.ms(now) * LSN_PER_MS

    def next(self) -> int:
        self.value += self.rng.randint(1, 64)
        return self.value

    def backdated(self, at: datetime) -> int:
        """The lsn a change made at `at` would have had. Always below this run's lsns."""
        return model.ms(at) * LSN_PER_MS + self.rng.randint(0, LSN_PER_MS - 1)


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
    # Two halves of one test: the late accounts' journal lines land in one pipeline
    # update and the accounts themselves in the next. Use the same --seed and --accounts.
    late = p.add_mutually_exclusive_group()
    late.add_argument(
        "--withhold-late-accounts",
        action="store_true",
        help="leave the late accounts out entirely; their journal lines are still written",
    )
    late.add_argument(
        "--only-late-accounts",
        action="store_true",
        help="write only the late accounts' create events, then stop",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    if args.out_dir:
        sink = LocalSink(args.out_dir)
    else:
        sink = S3Sink(args.bucket, args.profile, args.region)

    now = datetime.now(UTC)
    run = run_tag(now)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=args.minutes)
    lsn = Lsn(rng, now)
    stats: Counter[str] = Counter()

    accounts = [
        model.make_account(rng, f"ACC-{i:06d}", start) for i in range(1, args.accounts + 1)
    ]
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
    late_creates = []
    for acct in accounts:
        if acct["account_id"] in late_accounts:
            release = rng.randint(max(1, args.minutes // 2), max(1, args.minutes - 1))
            created = start - timedelta(hours=rng.uniform(1.0, args.late_hours))
            event = model.envelope(
                "c", "accounts", lsn.backdated(created), rng.randint(1, 10**6), created, None, acct
            )
            late_creates.append((release, event))

    # Everything above draws from rng identically in every mode, so the same seed picks
    # the same late accounts in a --withhold-late-accounts run and in the follow-up.
    if args.only_late_accounts:
        sink.write(args.env, "accounts", [e for _, e in late_creates], end, run, 0)
        print(f"sink            {sink.describe()}/{args.env}/cdc/")
        print(f"late accounts   {', '.join(sorted(late_accounts))}")
        return 0

    withheld = late_accounts if args.withhold_late_accounts else set()
    for release, event in late_creates:
        if not withheld:
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
                acct = model.make_account(rng, f"ACC-{run}-{idx:06d}", landed)
                by_id[acct["account_id"]] = acct
                accounts.append(acct)
                batch["accounts"].append(
                    model.envelope("c", "accounts", lsn.next(), tx, landed, None, acct)
                )
            elif roll < 0.06:
                candidates = [
                    a
                    for a in accounts
                    if a["account_id"] != hot_account["account_id"]
                    and a["account_id"] not in withheld
                ]
                victim = rng.choice(candidates)
                batch["accounts"].append(
                    model.envelope("d", "accounts", lsn.next(), tx, landed, victim, None)
                )
            else:
                # A withheld account gets no events at all, or an update would create it.
                before = rng.choice([a for a in accounts if a["account_id"] not in withheld])
                after = model.mutate_account(rng, before, landed)
                by_id[after["account_id"]] = after
                event = model.envelope("u", "accounts", lsn.next(), tx, landed, before, after)
                if rng.random() < mess.LATE_RATE and minute < args.minutes - 1:
                    changed = landed - timedelta(hours=rng.uniform(1.0, args.late_hours))
                    event["source"]["lsn"] = lsn.backdated(changed)
                    event["ts_ms"] = model.ms(changed)
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
                rng,
                f"JRN-{run}-{journal_seq:06d}",
                groups[key],
                landed,
                entry_day,
                f"JE-{run}",
                entry_seq,
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
            sink.write(args.env, table, events, landed, run, minute)
            files += 1
            stats[table] += len(events)

    # Anything still queued past the end of the window lands in the final file.
    leftover: dict[str, list] = {"accounts": [], "journal_entries": []}
    for pending in late_queue.values():
        for table, event in pending:
            leftover[table].append(event)
    for table, events in leftover.items():
        if events:
            sink.write(args.env, table, events, end, run, args.minutes)
            files += 1
            stats[table] += len(events)

    print(f"sink            {sink.describe()}/{args.env}/cdc/")
    print(f"run             {run}")
    print(f"window          {model.iso(start)} .. {model.iso(end)}")
    print(f"files           {files}")
    print(f"accounts        {stats['accounts']} events (snapshot {stats['snapshot']})")
    print(f"journal_entries {stats['journal_entries']} events")
    print(f"duplicates      {stats['duplicates']}")
    print(f"malformed       {stats['malformed']}")
    print(f"late arrivals   {stats['late']}")
    if withheld:
        print(f"withheld        {', '.join(sorted(withheld))}, land with --only-late-accounts")
    print(f"hot account     {hot_account['account_id']} ({hot_group_key[0]}/{hot_group_key[1]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
