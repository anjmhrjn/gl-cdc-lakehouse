"""Gold reference: account balances and the daily trial balance, computed from silver."""

from decimal import Decimal

import pytest

import generator.__main__ as gen
from conftest import account, entry, event, generated_events
from transforms import cdc, gold

JE = "journal_entries"


def acct(account_id, account_type="ASSET", business_unit="TREASURY"):
    return dict(account(account_id), account_type=account_type, business_unit=business_unit)


def line(entry_id, account_id, side, amount, journal="JRN-1", day="2026-09-01"):
    return dict(
        entry(entry_id, amount=amount, side=side),
        account_id=account_id,
        journal_id=journal,
        entry_date=day,
    )


def silver(load_events, account_events, entry_events):
    """Reference silver: (journal_entries SCD1, account_history SCD2)."""
    accounts = load_events("accounts", account_events)
    entries = load_events(JE, entry_events)
    return cdc.expected_scd1(entries, JE), cdc.expected_scd2(accounts, "accounts")


def balances(journal_entries, account_history):
    rows = gold.account_balances(journal_entries, account_history).collect()
    return {r["account_id"]: r.asDict() for r in rows}


def trial(journal_entries, account_history):
    rows = gold.daily_trial_balance(journal_entries, account_history).collect()
    return {(r["business_unit"], r["currency"], str(r["entry_date"])): r.asDict() for r in rows}


def je(lines):
    return [event("c", 100 + i, table=JE, after=row) for i, row in enumerate(lines)]


@pytest.mark.parametrize(
    "account_type, balance",
    [
        ("ASSET", Decimal("70.00")),
        ("EXPENSE", Decimal("70.00")),
        ("LIABILITY", Decimal("-70.00")),
        ("EQUITY", Decimal("-70.00")),
        ("REVENUE", Decimal("-70.00")),
    ],
)
def test_balance_uses_the_normal_side(load_events, account_type, balance):
    # 100 debited, 30 credited: +70 on a debit-normal account, -70 on a credit-normal one.
    entries, history = silver(
        load_events,
        [event("r", 1, after=acct("A1", account_type)), event("r", 2, after=acct("A2"))],
        je(
            [
                line("JE-1", "A1", "DEBIT", "100.00"),
                line("JE-2", "A1", "CREDIT", "30.00", journal="JRN-2"),
                line("JE-3", "A2", "CREDIT", "100.00"),
                line("JE-4", "A2", "DEBIT", "30.00", journal="JRN-2"),
            ]
        ),
    )
    row = balances(entries, history)["A1"]
    assert (row["debit_total"], row["credit_total"]) == (Decimal("100.00"), Decimal("30.00"))
    assert row["balance"] == balance
    assert row["line_count"] == 2


def test_orphan_lines_are_kept_with_null_unit(load_events):
    entries, history = silver(
        load_events,
        [event("r", 1, after=acct("A1"))],
        je([line("JE-1", "A1", "DEBIT", "100.00"), line("JE-2", "A9", "CREDIT", "100.00")]),
    )
    orphan = balances(entries, history)["A9"]
    assert orphan["business_unit"] is None
    assert orphan["account_type"] is None
    assert orphan["credit_total"] == Decimal("100.00")
    # The sign depends on the account type, which is unknown until the account lands.
    assert orphan["balance"] is None

    tb = trial(entries, history)
    assert tb[(None, "USD", "2026-09-01")]["credit_total"] == Decimal("100.00")
    assert tb[("TREASURY", "USD", "2026-09-01")]["net"] == Decimal("100.00")


def test_late_account_reconciles(load_events):
    lines = je([line("JE-1", "A1", "DEBIT", "100.00"), line("JE-2", "A2", "CREDIT", "100.00")])
    a1 = event("r", 1, after=acct("A1"))

    entries, history = silver(load_events, [a1], lines)
    assert (None, "USD", "2026-09-01") in trial(entries, history)

    # A2's create lands later, with an lsn older than the journal lines that reference it.
    late = event("c", 2, after=acct("A2", "LIABILITY"))
    entries, history = silver(load_events, [a1, late], lines)
    tb = trial(entries, history)
    assert list(tb) == [("TREASURY", "USD", "2026-09-01")]
    assert tb[("TREASURY", "USD", "2026-09-01")]["net"] == Decimal("0.00")
    assert balances(entries, history)["A2"]["balance"] == Decimal("100.00")


def test_deleted_account_keeps_its_unit(load_events):
    # Lines stay live after their account is deleted; they must not turn into orphans.
    entries, history = silver(
        load_events,
        [
            event("r", 1, after=acct("A1", business_unit="ASSET_MGMT")),
            event("r", 2, after=acct("A2", business_unit="ASSET_MGMT")),
            event("d", 200, before=acct("A2", business_unit="ASSET_MGMT")),
        ],
        je([line("JE-1", "A1", "DEBIT", "5.00"), line("JE-2", "A2", "CREDIT", "5.00")]),
    )
    assert balances(entries, history)["A2"]["business_unit"] == "ASSET_MGMT"
    assert list(trial(entries, history)) == [("ASSET_MGMT", "USD", "2026-09-01")]


def test_latest_account_type_sets_the_sign(load_events):
    entries, history = silver(
        load_events,
        [
            event("r", 1, after=acct("A1", "ASSET")),
            event("u", 50, after=acct("A1", "LIABILITY")),
            event("r", 2, after=acct("A2")),
        ],
        je([line("JE-1", "A1", "DEBIT", "10.00"), line("JE-2", "A2", "CREDIT", "10.00")]),
    )
    row = balances(entries, history)["A1"]
    assert row["account_type"] == "LIABILITY"
    assert row["balance"] == Decimal("-10.00")


def test_trial_balance_groups_by_unit_currency_and_day(load_events):
    entries, history = silver(
        load_events,
        [
            event("r", 1, after=acct("T1")),
            event("r", 2, after=acct("T2")),
            event("r", 3, after=acct("C1", business_unit="CORP_BANKING")),
            event("r", 4, after=acct("C2", business_unit="CORP_BANKING")),
        ],
        je(
            [
                line("JE-1", "T1", "DEBIT", "10.00", journal="J1"),
                line("JE-2", "T2", "CREDIT", "10.00", journal="J1"),
                line("JE-3", "C1", "DEBIT", "7.50", journal="J2", day="2026-09-02"),
                line("JE-4", "C2", "CREDIT", "2.50", journal="J2", day="2026-09-02"),
                line("JE-5", "C2", "CREDIT", "5.00", journal="J2", day="2026-09-02"),
            ]
        ),
    )
    tb = trial(entries, history)
    assert set(tb) == {("TREASURY", "USD", "2026-09-01"), ("CORP_BANKING", "USD", "2026-09-02")}
    corp = tb[("CORP_BANKING", "USD", "2026-09-02")]
    assert (corp["debit_total"], corp["credit_total"], corp["net"]) == (
        Decimal("7.50"),
        Decimal("7.50"),
        Decimal("0.00"),
    )
    assert corp["line_count"] == 3


def test_deleted_lines_leave_the_balance(load_events):
    entries, history = silver(
        load_events,
        [event("r", 1, after=acct("A1")), event("r", 2, after=acct("A2"))],
        je([line("JE-1", "A1", "DEBIT", "10.00"), line("JE-2", "A2", "CREDIT", "10.00")])
        + [
            event("d", 300, table=JE, before=line("JE-1", "A1", "DEBIT", "10.00")),
            event("d", 301, table=JE, before=line("JE-2", "A2", "CREDIT", "10.00")),
        ],
    )
    assert balances(entries, history) == {}
    assert trial(entries, history) == {}


def generated_silver(spark, root):
    events = generated_events(spark, root)
    clean = {t: cdc.clean_events(e, t) for t, e in events.items()}
    return cdc.expected_scd1(clean[JE], JE), cdc.expected_scd2(clean["accounts"], "accounts")


def test_generator_output_nets_to_zero(spark, tmp_path, capsys):
    """The journal invariant, held all the way to gold: debits equal credits per
    business_unit, currency and entry_date, with no orphans once every account landed."""
    gen.main(["--env", "dev", "--minutes", "5", "--rate", "200", "--out-dir", str(tmp_path)])
    capsys.readouterr()

    tb = gold.daily_trial_balance(*generated_silver(spark, tmp_path))
    assert tb.count() > 0
    assert tb.filter("business_unit IS NULL").count() == 0
    assert tb.filter("net != 0").count() == 0


def test_withheld_accounts_reconcile_on_the_next_run(spark, tmp_path, capsys):
    common = ["--env", "dev", "--minutes", "5", "--rate", "200", "--out-dir", str(tmp_path)]
    gen.main([*common, "--withhold-late-accounts"])
    capsys.readouterr()

    tb = gold.daily_trial_balance(*generated_silver(spark, tmp_path))
    assert tb.filter("business_unit IS NULL").count() > 0
    assert tb.filter("net != 0").count() > 0

    gen.main([*common, "--only-late-accounts"])
    capsys.readouterr()
    # generated_events caches its reads; without this the second read sees run 1 only.
    spark.catalog.clearCache()

    tb = gold.daily_trial_balance(*generated_silver(spark, tmp_path))
    assert tb.filter("business_unit IS NULL").count() == 0
    assert tb.filter("net != 0").count() == 0
