"""Real generator output through the reference, to catch schema drift between the two."""

import generator.__main__ as gen
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
