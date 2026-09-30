"""trial_balance_check: debits net to zero per business unit, currency and entry date.

A late account puts its lines under a NULL business_unit and leaves its real unit short
by the same amount. That is tolerated (warn) while the lines are young and the NULL
group cancels the shortfall for the same currency and day. Everything else fails.
"""

from datetime import UTC, datetime

import generator.__main__ as gen
from conftest import account, entry, event, generated_events
from transforms import cdc, checks, gold

JE = "journal_entries"
NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)  # entry() lines are posted at 00:00 that day


def acct(account_id, business_unit="TREASURY"):
    return dict(account(account_id), business_unit=business_unit)


def line(entry_id, account_id, side, amount, journal="JRN-1", posted_at=None):
    row = dict(entry(entry_id, amount=amount, side=side), account_id=account_id, journal_id=journal)
    if posted_at is not None:
        row["posted_at"] = posted_at
    return row


def verdict(load_events, account_events, lines, max_orphan_hours=48):
    accounts = load_events("accounts", account_events)
    entries = load_events(JE, [event("c", 100 + i, table=JE, after=r) for i, r in enumerate(lines)])
    journal_entries = cdc.expected_scd1(entries, JE)
    account_history = cdc.expected_scd2(accounts, "accounts")
    return _verdict(journal_entries, account_history, max_orphan_hours)


def _verdict(journal_entries, account_history, max_orphan_hours):
    tb = gold.daily_trial_balance(journal_entries, account_history)
    groups = checks.unbalanced_groups(tb).collect()
    stale = checks.stale_orphans(journal_entries, account_history, NOW, max_orphan_hours)
    return checks.trial_balance_verdict(
        unexplained=sum(not g["explained"] for g in groups),
        explained=sum(g["explained"] for g in groups),
        stale_orphans=stale.count(),
    )


def test_balanced_books_pass(load_events):
    assert (
        verdict(
            load_events,
            [event("r", 1, after=acct("A1")), event("r", 2, after=acct("A2"))],
            [line("JE-1", "A1", "DEBIT", "10.00"), line("JE-2", "A2", "CREDIT", "10.00")],
        )
        == "pass"
    )


def test_young_orphan_that_explains_the_gap_warns(load_events):
    # A2 has not landed. TREASURY is 10.00 short and the NULL group holds the 10.00.
    assert (
        verdict(
            load_events,
            [event("r", 1, after=acct("A1"))],
            [line("JE-1", "A1", "DEBIT", "10.00"), line("JE-2", "A2", "CREDIT", "10.00")],
        )
        == "warn"
    )


def test_orphan_older_than_the_limit_fails(load_events):
    # Posted 12 hours before NOW, limit 6 hours: the account should have landed by now.
    assert (
        verdict(
            load_events,
            [event("r", 1, after=acct("A1"))],
            [line("JE-1", "A1", "DEBIT", "10.00"), line("JE-2", "A2", "CREDIT", "10.00")],
            max_orphan_hours=6,
        )
        == "fail"
    )


def test_orphan_with_unknown_posting_time_counts_as_stale(load_events):
    assert (
        verdict(
            load_events,
            [event("r", 1, after=acct("A1"))],
            [
                line("JE-1", "A1", "DEBIT", "10.00"),
                line("JE-2", "A2", "CREDIT", "10.00", posted_at="not a time"),
            ],
        )
        == "fail"
    )


def test_journal_across_two_units_fails(load_events):
    # The day nets to zero, but no orphan explains it: two real units are off.
    assert (
        verdict(
            load_events,
            [
                event("r", 1, after=acct("T1")),
                event("r", 2, after=acct("C1", business_unit="CORP_BANKING")),
            ],
            [line("JE-1", "T1", "DEBIT", "10.00"), line("JE-2", "C1", "CREDIT", "10.00")],
        )
        == "fail"
    )


def test_orphan_does_not_cover_an_unrelated_gap(load_events):
    # The orphan explains 10.00 of TREASURY's gap, but a one-sided 5.00 line remains.
    assert (
        verdict(
            load_events,
            [event("r", 1, after=acct("A1"))],
            [
                line("JE-1", "A1", "DEBIT", "10.00"),
                line("JE-2", "A2", "CREDIT", "10.00"),
                line("JE-3", "A1", "DEBIT", "5.00", journal="JRN-2"),
            ],
        )
        == "fail"
    )


def test_unbalanced_groups_marks_each_group(load_events):
    accounts = load_events("accounts", [event("r", 1, after=acct("A1"))])
    entries = load_events(
        JE,
        [
            event("c", 100, table=JE, after=line("JE-1", "A1", "DEBIT", "10.00")),
            event("c", 101, table=JE, after=line("JE-2", "A2", "CREDIT", "10.00")),
        ],
    )
    tb = gold.daily_trial_balance(
        cdc.expected_scd1(entries, JE), cdc.expected_scd2(accounts, "accounts")
    )
    groups = {r["business_unit"]: r["explained"] for r in checks.unbalanced_groups(tb).collect()}
    assert groups == {"TREASURY": True, None: True}


def test_generator_withheld_accounts_warn_then_pass(spark, tmp_path, capsys):
    """Real generator output: the three withheld accounts warn, and their arrival clears it."""
    common = ["--env", "dev", "--minutes", "5", "--rate", "200", "--out-dir", str(tmp_path)]

    def run_verdict():
        events = generated_events(spark, tmp_path)
        clean = {t: cdc.clean_events(e, t) for t, e in events.items()}
        je = cdc.expected_scd1(clean[JE], JE)
        history = cdc.expected_scd2(clean["accounts"], "accounts")
        # Generated lines are posted just now, so a 48 hour limit keeps them young.
        tb = gold.daily_trial_balance(je, history)
        groups = checks.unbalanced_groups(tb).collect()
        stale = checks.stale_orphans(je, history, datetime.now(UTC), 48).count()
        return checks.trial_balance_verdict(
            unexplained=sum(not g["explained"] for g in groups),
            explained=sum(g["explained"] for g in groups),
            stale_orphans=stale,
        )

    gen.main([*common, "--withhold-late-accounts"])
    capsys.readouterr()
    assert run_verdict() == "warn"

    gen.main([*common, "--only-late-accounts"])
    capsys.readouterr()
    # generated_events caches its reads; without this the second read sees run 1 only.
    spark.catalog.clearCache()
    assert run_verdict() == "pass"
