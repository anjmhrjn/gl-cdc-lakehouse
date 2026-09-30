"""freshness_check: gold.daily_trial_balance refreshes within 60 minutes of a file landing.

A file counts as covered once a pipeline update that refreshed gold started after it
landed. The lag is how long the oldest uncovered file has waited.
"""

from datetime import UTC, datetime, timedelta

from transforms import checks

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
GOLD = "gl_dev.gold.daily_trial_balance"


def ago(minutes):
    return NOW - timedelta(minutes=minutes)


def test_no_files_is_fresh():
    assert checks.freshness_verdict([], ago(5), NOW, 60) == (True, None)


def test_files_covered_by_the_last_refresh_are_fresh():
    assert checks.freshness_verdict([ago(300), ago(90)], ago(80), NOW, 60) == (True, None)


def test_lag_is_measured_from_the_oldest_uncovered_file():
    fresh, lag = checks.freshness_verdict([ago(200), ago(50), ago(10)], ago(100), NOW, 60)
    assert fresh
    assert lag == timedelta(minutes=50)


def test_sixty_minutes_is_still_within_the_sla():
    assert checks.freshness_verdict([ago(60)], ago(120), NOW, 60) == (True, timedelta(minutes=60))


def test_past_the_sla_is_stale():
    fresh, lag = checks.freshness_verdict([ago(61), ago(5)], ago(120), NOW, 60)
    assert not fresh
    assert lag == timedelta(minutes=61)


def test_never_refreshed_counts_every_file():
    assert checks.freshness_verdict([ago(70)], None, NOW, 60) == (False, timedelta(minutes=70))


def test_file_landing_as_the_update_starts_is_not_covered():
    # The update lists files some time after it starts. Counting a file that landed at
    # the same instant as covered could hide it for a whole schedule interval.
    assert checks.freshness_verdict([ago(90)], ago(90), NOW, 60) == (False, timedelta(minutes=90))


def update(minutes_ago, state="COMPLETED", selection=(), validate_only=False):
    return {
        "creation_time": ago(minutes_ago),
        "state": state,
        "selection": list(selection),
        "validate_only": validate_only,
    }


def test_last_gold_refresh_is_the_latest_completed_update():
    updates = [update(10, state="FAILED"), update(40), update(70)]
    assert checks.last_gold_refresh(updates, GOLD) == ago(40)


def test_updates_that_skip_gold_do_not_count():
    updates = [
        update(10, selection=["gl_dev.silver.accounts"]),
        update(20, validate_only=True),
        update(30, selection=["gl_dev.silver.accounts", GOLD]),
    ]
    assert checks.last_gold_refresh(updates, GOLD) == ago(30)


def test_no_gold_refresh_at_all():
    assert checks.last_gold_refresh([update(5, state="RUNNING")], GOLD) is None
