"""backfill_replay, range mode: which landing files are re-delivered, and to where.

A re-delivered file lands now, so it goes into the current dt=/hh= partition under a
new name. Auto Loader tracks files by path, so the new name is what makes it ingest the
file again.
"""

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from transforms import checks

ROOT = "s3://gl-cdc-lakehouse-anuj/dev/cdc"
NOW = datetime(2026, 9, 30, 14, 5, tzinfo=UTC)


def path(table, day, hour, name):
    return f"{ROOT}/{table}/dt={day}/hh={hour}/{name}"


def test_copies_files_in_range_into_the_current_partition():
    paths = [
        path("accounts", "2026-09-27", "23", "part-1-r1-0000.json"),
        path("accounts", "2026-09-28", "01", "part-2-r1-0001.json"),
        path("journal_entries", "2026-09-29", "10", "part-3-r1-0002.json"),
        path("journal_entries", "2026-09-30", "09", "part-4-r2-0000.json"),
    ]
    copies = checks.replay_copies(paths, date(2026, 9, 28), date(2026, 9, 29), "777", NOW)
    assert copies == [
        (paths[1], f"{ROOT}/accounts/dt=2026-09-30/hh=14/replay-777-part-2-r1-0001.json"),
        (paths[2], f"{ROOT}/journal_entries/dt=2026-09-30/hh=14/replay-777-part-3-r1-0002.json"),
    ]


def test_earlier_replays_are_not_replayed_again():
    # Copying a copy would deliver the same events a third time and grow the name.
    paths = [
        path("accounts", "2026-09-30", "10", "part-1-r1-0000.json"),
        path("accounts", "2026-09-30", "11", "replay-555-part-1-r1-0000.json"),
    ]
    copies = checks.replay_copies(paths, date(2026, 9, 30), date(2026, 9, 30), "777", NOW)
    assert [src for src, _ in copies] == [paths[0]]


def test_paths_outside_the_landing_layout_are_ignored():
    paths = [f"{ROOT}/accounts/_checkpoint/x.json", f"{ROOT}/accounts/dt=2026-09-30/stray.json"]
    assert checks.replay_copies(paths, date(2026, 9, 30), date(2026, 9, 30), "777", NOW) == []


def fake_ls(tree):
    """dbutils.fs.ls over a dict of path -> [(name, modificationTime ms)]."""

    def ls(p):
        p = p.rstrip("/")
        return [
            SimpleNamespace(path=f"{p}/{name}", name=name, modificationTime=mtime)
            for name, mtime in tree.get(p, [])
        ]

    return ls


def test_landing_files_lists_only_the_requested_days():
    t = f"{ROOT}/accounts"
    ls = fake_ls(
        {
            t: [("dt=2026-09-28/", 0), ("dt=2026-09-29/", 0), ("_schemas/", 0)],
            f"{t}/dt=2026-09-28": [("hh=23/", 0)],
            f"{t}/dt=2026-09-28/hh=23": [("part-a.json", 1_000)],
            f"{t}/dt=2026-09-29": [("hh=00/", 0)],
            f"{t}/dt=2026-09-29/hh=00": [("part-b.json", 2_000), ("sub/", 0)],
        }
    )
    assert checks.landing_files(ls, t, from_day=date(2026, 9, 29)) == [
        (f"{t}/dt=2026-09-29/hh=00/part-b.json", datetime(1970, 1, 1, 0, 0, 2, tzinfo=UTC))
    ]
    assert len(checks.landing_files(ls, t)) == 2


def test_range_older_than_the_tombstones_is_refused():
    # NOW is 2026-09-30. Five days back is allowed, six is not.
    assert checks.replay_copies([], date(2026, 9, 25), date(2026, 9, 25), "777", NOW) == []
    with pytest.raises(ValueError, match="full refresh"):
        checks.replay_copies([], date(2026, 9, 24), date(2026, 9, 30), "777", NOW)


def test_range_must_not_be_reversed():
    with pytest.raises(ValueError):
        checks.replay_copies([], date(2026, 9, 30), date(2026, 9, 29), "777", NOW)
