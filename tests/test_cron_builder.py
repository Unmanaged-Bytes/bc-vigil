from __future__ import annotations

import pytest

from bc_vigil.integrity.cron_builder import build_cron, next_occurrences


def test_every_minutes():
    r = build_cron("every_minutes", interval_minutes="15")
    assert r.error is None
    assert r.cron == "*/15 * * * *"
    assert "15 minutes" in r.description


def test_hourly():
    r = build_cron("hourly", minute_of_hour="30")
    assert r.error is None
    assert r.cron == "30 * * * *"


def test_daily():
    r = build_cron("daily", time="03:00")
    assert r.error is None
    assert r.cron == "0 3 * * *"
    assert "03:00" in r.description


def test_weekly_multiple_days():
    r = build_cron("weekly", time="09:30", days=["mon", "wed", "fri"])
    assert r.error is None
    assert r.cron == "30 9 * * 1,3,5"


def test_weekly_sunday_maps_to_zero():
    r = build_cron("weekly", time="08:00", days=["sun"])
    assert r.error is None
    assert r.cron == "0 8 * * 0"


def test_weekly_requires_at_least_one_day():
    r = build_cron("weekly", time="09:00", days=[])
    assert r.cron is None
    assert "jour" in r.error


def test_monthly():
    r = build_cron("monthly", time="02:15", day_of_month="1")
    assert r.error is None
    assert r.cron == "15 2 1 * *"


def test_cron_expert_valid():
    r = build_cron("cron", cron_expr="*/15 * * * *")
    assert r.error is None
    assert r.cron == "*/15 * * * *"


def test_cron_expert_invalid():
    r = build_cron("cron", cron_expr="nonsense")
    assert r.cron is None
    assert "invalide" in r.error


def test_daily_rejects_bad_time():
    r = build_cron("daily", time="25:00")
    assert r.cron is None
    assert r.error


def test_every_minutes_rejects_out_of_range():
    r = build_cron("every_minutes", interval_minutes="0")
    assert r.cron is None


def test_unknown_mode():
    r = build_cron("foo")
    assert r.cron is None
    assert "mode" in r.error


def test_next_occurrences_count():
    assert len(next_occurrences("0 3 * * *", 5)) == 5


def test_next_occurrences_are_increasing():
    occs = next_occurrences("*/5 * * * *", 4)
    for a, b in zip(occs, occs[1:]):
        assert a < b
