"""Real generator output through the reference, to catch schema drift between the two."""

import generator.__main__ as gen
from conftest import generated_events
from transforms import cdc


def test_generator_files_parse_and_sequence(spark, tmp_path, capsys):
    gen.main(["--env", "dev", "--minutes", "5", "--rate", "200", "--out-dir", str(tmp_path)])
    capsys.readouterr()

    events = {}
    corrupt = 0
    for table in cdc.TABLES:
        path = str(tmp_path / "dev" / "cdc" / table)
        # Cached because Spark refuses a query on raw JSON that reads only _corrupt_record.
        raw = cdc.read_landing(spark.read, table).json(path).cache()
        events[table] = cdc.clean_events(cdc.flatten(raw, table), table)
        pk = cdc.TABLES[table]["pk"]

        # Malformed lines are read and then dropped; nothing fails the read.
        corrupt += raw.filter(f"{cdc.CORRUPT_COLUMN} IS NOT NULL").count()
        assert raw.count() > events[table].count() >= raw.count() * 0.9

        current = cdc.expected_scd1(events[table], table)
        assert current.filter(f"{pk} IS NULL").count() == 0
        assert current.count() == current.select(pk).distinct().count()

    # Unparseable lines are rare (half of ~0.5%) but present in a run this size.
    assert corrupt > 0

    # Every live account has exactly one open version, and deleted ones have none.
    history = cdc.expected_scd2(events["accounts"], "accounts")
    open_ids = history.filter("__END_AT IS NULL").select("account_id")
    live_ids = cdc.expected_scd1(events["accounts"], "accounts").select("account_id")
    assert open_ids.count() == live_ids.count()
    assert open_ids.exceptAll(live_ids).count() == 0


def account_ids(events, table):
    """Account ids touched by a table's events, from after or before."""
    pk = "account_id"
    return {r[pk] for r in events[table].select(pk).filter(f"{pk} IS NOT NULL").collect()}


def test_withheld_accounts_arrive_only_in_the_follow_up_run(spark, tmp_path, capsys):
    first, second = tmp_path / "first", tmp_path / "second"
    common = ["--env", "dev", "--minutes", "5", "--rate", "200"]
    gen.main([*common, "--withhold-late-accounts", "--out-dir", str(first)])
    gen.main([*common, "--only-late-accounts", "--out-dir", str(second)])
    capsys.readouterr()

    run1 = generated_events(spark, first)
    run2 = generated_events(spark, second)

    # The follow-up run holds only creates, for accounts the first run never mentioned
    # but did post journal lines against.
    late = account_ids(run2, "accounts")
    assert len(late) == 3
    assert {r["_op"] for r in run2["accounts"].collect()} == {"c"}
    assert late.isdisjoint(account_ids(run1, "accounts"))
    assert late & account_ids(run1, "journal_entries")
    assert not (second / "dev" / "cdc" / "journal_entries").exists()


def test_runs_add_rows_instead_of_overwriting_them(spark, tmp_path, capsys):
    """Journal lines, journals, new accounts and landing files are unique per run.

    Only the base accounts share ids across runs: each run re-reads the same accounts.
    """
    common = ["--env", "dev", "--minutes", "3", "--rate", "100", "--out-dir", str(tmp_path)]
    gen.main(common)
    first_files = set((tmp_path / "dev").rglob("*.json"))
    first = generated_events(spark, tmp_path)
    first_ids = {
        column: {r[0] for r in first["journal_entries"].select(column).distinct().collect()}
        for column in ("entry_id", "journal_id")
    }
    first_accounts = account_ids(first, "accounts")
    spark.catalog.clearCache()

    gen.main(common)
    capsys.readouterr()
    both = generated_events(spark, tmp_path)

    # Same minute, same seed: the second run still lands next to the first, not over it.
    assert first_files < set((tmp_path / "dev").rglob("*.json"))
    for column, ids in first_ids.items():
        rows = both["journal_entries"].filter(f"{column} IS NOT NULL")
        assert rows.select(column).distinct().count() > len(ids)

    base = {f"ACC-{i:06d}" for i in range(1, 201)}
    new_accounts = account_ids(both, "accounts") - base
    assert first_accounts - base < new_accounts
