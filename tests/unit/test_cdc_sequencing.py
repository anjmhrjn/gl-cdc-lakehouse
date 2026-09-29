"""SCD1 expected state: the reference silver is checked against in integration."""

from decimal import Decimal

from conftest import account, entry, event
from transforms import cdc


def state(df, table):
    pk = cdc.TABLES[table]["pk"]
    return {r[pk]: r.asDict() for r in cdc.expected_scd1(df, table).collect()}


def test_out_of_order_within_file_takes_highest_lsn(load_events):
    events = load_events(
        "accounts",
        [
            event("u", 30, after=account("A1", status="CLOSED")),
            event("r", 10, after=account("A1", status="OPEN")),
            event("u", 20, after=account("A1", status="FROZEN")),
        ],
    )
    assert state(events, "accounts")["A1"]["status"] == "CLOSED"


def test_exact_duplicates_collapse(load_events):
    dup = event("c", 10, table="journal_entries", after=entry("JE-1"))
    events = load_events("journal_entries", [dup, dup, dup])
    assert events.count() == 1
    assert len(state(events, "journal_entries")) == 1


def test_delete_takes_key_from_before(load_events):
    events = load_events(
        "journal_entries",
        [
            event("c", 10, table="journal_entries", after=entry("JE-1")),
            event("c", 11, table="journal_entries", after=entry("JE-2")),
            event("d", 12, table="journal_entries", before=entry("JE-1")),
        ],
    )
    assert set(state(events, "journal_entries")) == {"JE-2"}


def test_late_older_update_does_not_resurrect_deleted_row(load_events):
    events = load_events(
        "journal_entries",
        [
            event("c", 10, table="journal_entries", after=entry("JE-1")),
            event("d", 30, table="journal_entries", before=entry("JE-1")),
            # Arrives last, sequenced before the delete.
            event("u", 20, table="journal_entries", after=entry("JE-1", description="late")),
        ],
    )
    assert state(events, "journal_entries") == {}


def test_late_older_update_is_ignored(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 5_000_001, after=account("A1", status="OPEN")),
            event("u", 1_200_000, after=account("A1", status="FROZEN")),
        ],
    )
    assert state(events, "accounts")["A1"]["status"] == "OPEN"


def test_update_before_late_create_keeps_update(load_events):
    # Held back account: an update lands first, its create arrives later with an older lsn.
    events = load_events(
        "accounts",
        [
            event("u", 5_000_100, after=account("A1", status="FROZEN")),
            event("c", 1_500_000, after=account("A1", status="OPEN")),
        ],
    )
    assert state(events, "accounts")["A1"]["status"] == "FROZEN"


def test_update_after_delete_recreates_row(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("d", 20, before=account("A1")),
            event("u", 30, after=account("A1", status="CLOSED")),
        ],
    )
    assert state(events, "accounts")["A1"]["status"] == "CLOSED"


def test_drop_rules(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("u", 11, after={"business_unit": "TREASURY"}),  # missing pk
            event("u", None, after=account("A2")),  # null lsn
            event("x", 12, after=account("A3")),  # unknown op
            event("", 13, after=account("A4")),  # empty op
            '{"op":"u","ts_ms":0,"sour',  # truncated
            "<<corrupt record lsn=14>>",  # not json
        ],
    )
    assert [r["account_id"] for r in events.collect()] == ["A1"]


def test_types_are_cast(load_events):
    events = load_events(
        "journal_entries",
        [event("c", 10, table="journal_entries", after=entry("JE-1", amount="1234.56"))],
    )
    row = state(events, "journal_entries")["JE-1"]
    assert row["amount"] == Decimal("1234.56")
    assert str(row["entry_date"]) == "2026-09-01"
    assert row["posted_at"].year == 2026


def test_bad_amount_string_becomes_null_not_error(load_flat):
    # The NULL amount is then quarantined; see test_quarantine.py.
    events = load_flat(
        "journal_entries",
        [event("c", 10, table="journal_entries", after=entry("JE-1", amount="abc"))],
    )
    assert events.collect()[0]["amount"] is None


def test_scd1_columns_match_contract(load_events):
    events = load_events("accounts", [event("r", 10, after=account("A1"))])
    assert cdc.expected_scd1(events, "accounts").columns == list(
        cdc.TABLES["accounts"]["columns"]
    )
