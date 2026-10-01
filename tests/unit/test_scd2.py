"""SCD2 expected history for accounts. __START_AT and __END_AT are lsn values."""

from conftest import account, event
from transforms import cdc


def history(df):
    rows = cdc.expected_scd2(df, "accounts").orderBy("account_id", "__START_AT").collect()
    return [(r["account_id"], r["status"], r["__START_AT"], r["__END_AT"]) for r in rows]


def test_versions_chain_by_lsn(load_events):
    events = load_events(
        "accounts",
        [
            event("u", 30, after=account("A1", status="CLOSED")),
            event("r", 10, after=account("A1", status="OPEN")),
            event("u", 20, after=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [
        ("A1", "OPEN", 10, 20),
        ("A1", "FROZEN", 20, 30),
        ("A1", "CLOSED", 30, None),
    ]


def test_delete_closes_current_version(load_events):
    events = load_events(
        "accounts",
        [event("r", 10, after=account("A1")), event("d", 20, before=account("A1"))],
    )
    assert history(events) == [("A1", "OPEN", 10, 20)]


def test_update_after_delete_opens_new_version(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("d", 20, before=account("A1")),
            event("u", 30, after=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [("A1", "OPEN", 10, 20), ("A1", "FROZEN", 30, None)]


def test_late_event_is_inserted_into_the_past(load_events):
    # The late update lands after the current version but belongs between two old ones.
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1", status="OPEN")),
            event("u", 30, after=account("A1", status="CLOSED")),
            event("u", 20, after=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [
        ("A1", "OPEN", 10, 20),
        ("A1", "FROZEN", 20, 30),
        ("A1", "CLOSED", 30, None),
    ]


def test_duplicates_do_not_create_versions(load_events):
    ev = event("u", 20, after=account("A1", status="FROZEN"))
    events = load_events("accounts", [event("r", 10, after=account("A1")), ev, ev])
    assert history(events) == [("A1", "OPEN", 10, 20), ("A1", "FROZEN", 20, None)]


def test_unchanged_update_does_not_open_a_version(load_events):
    # A source can emit an update that changes nothing (UPDATE ... SET x = x). AUTO CDC
    # opens a new version only when a tracked column changes, so the first version
    # stays open until the real change at lsn 30.
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("u", 20, after=account("A1")),
            event("u", 30, after=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [("A1", "OPEN", 10, 30), ("A1", "FROZEN", 30, None)]


def test_unchanged_updates_in_a_row_collapse(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("u", 20, after=account("A1")),
            event("u", 25, after=account("A1")),
        ],
    )
    assert history(events) == [("A1", "OPEN", 10, None)]


def test_same_values_after_a_delete_open_a_version(load_events):
    # The delete closed the version, so identical values re-insert the key.
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("d", 20, before=account("A1")),
            event("u", 30, after=account("A1")),
        ],
    )
    assert history(events) == [("A1", "OPEN", 10, 20), ("A1", "OPEN", 30, None)]


def test_second_delete_in_a_row_writes_a_closed_row_with_no_start(load_events):
    # Observed on dev, not documented: a delete that finds no open version makes AUTO
    # CDC write a history row from the delete's before image, with a NULL __START_AT
    # and the delete's lsn as __END_AT.
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("d", 20, before=account("A1")),
            event("d", 30, before=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [("A1", "FROZEN", None, 30), ("A1", "OPEN", 10, 20)]


def test_late_unchanged_update_is_absorbed(load_events):
    # Arrives last but sequences between 10 and 30, with the values already open at 10.
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("u", 30, after=account("A1", status="FROZEN")),
            event("u", 20, after=account("A1")),
        ],
    )
    assert history(events) == [("A1", "OPEN", 10, 30), ("A1", "FROZEN", 30, None)]


def test_keys_are_independent(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("r", 11, after=account("A2")),
            event("u", 12, after=account("A1", status="FROZEN")),
        ],
    )
    assert history(events) == [
        ("A1", "OPEN", 10, 12),
        ("A1", "FROZEN", 12, None),
        ("A2", "OPEN", 11, None),
    ]


def test_one_current_version_per_live_key(load_events):
    events = load_events(
        "accounts",
        [
            event("r", 10, after=account("A1")),
            event("u", 20, after=account("A1", status="FROZEN")),
            event("r", 11, after=account("A2")),
        ],
    )
    current = cdc.expected_scd2(events, "accounts").filter("__END_AT IS NULL")
    assert sorted(r["account_id"] for r in current.collect()) == ["A1", "A2"]
