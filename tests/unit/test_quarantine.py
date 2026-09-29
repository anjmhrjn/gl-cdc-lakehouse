"""Quarantine routing: rows that cannot be trusted go to quarantine_events, not silver.

Order per event: unparseable -> quarantine, else a failed drop rule -> dropped, else a
failed quarantine rule -> quarantine, else AUTO CDC.
"""

import json

import generator.__main__ as gen
from conftest import account, entry, event, generated_events
from generator import model
from transforms import cdc

JE = "journal_entries"


def route(load_flat, table, records):
    flat = load_flat(table, records)
    return cdc.clean_events(flat, table), cdc.quarantined(flat, table)


def keys(df, table):
    return sorted(r[cdc.TABLES[table]["pk"]] for r in df.collect())


def reasons(quarantine):
    return sorted((r["_lsn"], tuple(r["reasons"])) for r in quarantine.collect())


def test_currency_allowlist_matches_generator():
    assert set(cdc.CURRENCIES) == set(model.CURRENCIES)


def test_non_positive_amount_is_quarantined(load_flat):
    valid, quarantine = route(
        load_flat,
        JE,
        [
            event("c", 10, table=JE, after=entry("JE-1", amount="0.00")),
            event("c", 11, table=JE, after=entry("JE-2", amount="-42.50")),
            event("c", 12, table=JE, after=entry("JE-3", amount="0.01")),
        ],
    )
    assert keys(valid, JE) == ["JE-3"]
    assert reasons(quarantine) == [(10, ("amount_positive",)), (11, ("amount_positive",))]


def test_unreadable_amount_is_quarantined(load_flat):
    # try_cast turns "abc" into NULL; a NULL amount is not a usable ledger line.
    valid, quarantine = route(
        load_flat, JE, [event("c", 10, table=JE, after=entry("JE-1", amount="abc"))]
    )
    assert valid.count() == 0
    assert reasons(quarantine) == [(10, ("amount_positive",))]


def test_unknown_currency_is_quarantined_in_both_tables(load_flat):
    valid, quarantine = route(
        load_flat,
        "accounts",
        [
            event("r", 10, after=dict(account("A1"), currency="XXX")),
            event("r", 11, after=dict(account("A2"), currency="us_dollar")),
            event("r", 12, after=dict(account("A3"), currency=None)),
            event("r", 13, after=dict(account("A4"), currency="EUR")),
        ],
    )
    assert keys(valid, "accounts") == ["A4"]
    assert [r for _, r in reasons(quarantine)] == [("currency_known",)] * 3

    valid, quarantine = route(
        load_flat, JE, [event("c", 10, table=JE, after=dict(entry("JE-1"), currency="ZZ"))]
    )
    assert valid.count() == 0
    assert reasons(quarantine) == [(10, ("currency_known",))]


def test_every_failed_rule_is_listed(load_flat):
    _, quarantine = route(
        load_flat,
        JE,
        [event("c", 10, table=JE, after=dict(entry("JE-1", amount="0.00"), currency="ZZ"))],
    )
    assert reasons(quarantine) == [(10, ("amount_positive", "currency_known"))]


def test_unparseable_lines_are_quarantined_with_their_text(load_flat):
    truncated = '{"op":"u","ts_ms":0,"sour'
    not_json = "<<corrupt record lsn=14>>"
    valid, quarantine = route(
        load_flat,
        "accounts",
        [event("r", 10, after=account("A1")), truncated, not_json],
    )
    assert keys(valid, "accounts") == ["A1"]
    rows = quarantine.collect()
    assert sorted(r["payload"] for r in rows) == sorted([truncated, not_json])
    assert all(r["reasons"] == [cdc.UNPARSEABLE] for r in rows)
    assert all(r["source_table"] == "accounts" for r in rows)


def test_unparseable_lines_are_not_collapsed(load_flat):
    # They have no key or lsn, so a dedupe on (pk, lsn) would fold them into one row.
    lines = [f"<<corrupt record lsn={n}>>" for n in range(5)]
    _, quarantine = route(load_flat, "accounts", lines)
    assert quarantine.count() == 5


def test_drop_rules_win_over_quarantine_rules(load_flat):
    valid, quarantine = route(
        load_flat,
        JE,
        [
            event("c", 10, table=JE, after={"amount": "0.00"}),  # missing pk
            event("c", None, table=JE, after=dict(entry("JE-2"), currency="ZZ")),  # null lsn
            event("x", 12, table=JE, after=entry("JE-3", amount="-1.00")),  # unknown op
        ],
    )
    assert valid.count() == 0
    assert quarantine.count() == 0


def test_duplicates_are_quarantined_once(load_flat):
    bad = event("c", 10, table=JE, after=entry("JE-1", amount="0.00"))
    _, quarantine = route(load_flat, JE, [bad, bad, bad])
    assert quarantine.count() == 1


def test_delete_is_checked_against_its_before_image(load_flat):
    # For op = d, after is null. The rules must read the row from before, like flatten does.
    valid, quarantine = route(
        load_flat,
        JE,
        [
            event("c", 10, table=JE, after=entry("JE-1")),
            event("d", 11, table=JE, before=entry("JE-1")),
        ],
    )
    assert valid.count() == 2
    assert quarantine.count() == 0


def test_payload_keeps_the_event(load_flat):
    _, quarantine = route(
        load_flat, JE, [event("u", 10, table=JE, after=entry("JE-1", amount="0.00"))]
    )
    row = quarantine.collect()[0]
    payload = json.loads(row["payload"])
    assert payload["op"] == "u"
    assert payload["source"]["lsn"] == 10
    assert payload["after"]["entry_id"] == "JE-1"
    assert (row["_op"], row["_lsn"]) == ("u", 10)


def test_quarantine_columns(load_flat):
    _, quarantine = route(load_flat, "accounts", ["not json"])
    assert quarantine.columns == cdc.QUARANTINE_COLUMNS


def test_generator_output_routes_as_labelled(spark, tmp_path, capsys):
    # The generator gives every structurally valid bad record a BAD-<lsn> key.
    gen.main(["--env", "dev", "--minutes", "5", "--rate", "200", "--out-dir", str(tmp_path)])
    capsys.readouterr()

    total = 0
    for table, flat in generated_events(spark, tmp_path).items():
        pk = cdc.TABLES[table]["pk"]
        valid = cdc.clean_events(flat, table)
        quarantine = cdc.quarantined(flat, table)
        assert valid.filter(f"{pk} LIKE 'BAD-%'").count() == 0

        for row in quarantine.collect():
            if row["reasons"] != [cdc.UNPARSEABLE]:
                assert json.loads(row["payload"])["after"][pk].startswith("BAD-")
        total += quarantine.count()
    assert total > 0
