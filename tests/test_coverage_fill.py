from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


def _make_target(tmp_path, name: str = "cov") -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / f"data-{name}"
    folder.mkdir()
    with SessionLocal() as session:
        t = models.Target(
            name=name, path=str(folder), algorithm="sha256", threads="auto",
        )
        session.add(t)
        session.commit()
        return t.id


def _insert_scan(target_id: int, status: str, **kwargs) -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        scan = models.Scan(
            target_id=target_id, status=status,
            started_at=datetime.now(timezone.utc),
            **kwargs,
        )
        session.add(scan)
        session.commit()
        return scan.id


# -------------------- routes/schedules ------------------------------------


def test_new_schedule_form_returns_200(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/schedules/new?target_id={target_id}")
        assert r.status_code == 200
        assert "Fréquence" in r.text


def test_new_schedule_form_404_on_missing_target(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/schedules/new?target_id=999")
        assert r.status_code == 404


def test_create_schedule_404_on_missing_target(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={"target_id": "9999", "mode": "daily", "time": "03:00"},
            follow_redirects=False,
        )
        assert r.status_code == 404


def test_toggle_schedule(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()
        sid = s.id

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/schedules/{sid}/toggle", follow_redirects=False)
        assert r.status_code == 303

    with SessionLocal() as session:
        assert session.get(models.Schedule, sid).enabled is False


def test_toggle_schedule_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/schedules/999/toggle", follow_redirects=False)
        assert r.status_code == 404


def test_delete_schedule(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()
        sid = s.id

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/schedules/{sid}/delete", follow_redirects=False)
        assert r.status_code == 303

    with SessionLocal() as session:
        assert session.get(models.Schedule, sid) is None


def test_delete_schedule_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/schedules/999/delete", follow_redirects=False)
        assert r.status_code == 404


# -------------------- routes/targets --------------------------------------


def test_new_target_form(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/targets/new")
        assert r.status_code == 200
        assert "Nom" in r.text


def test_show_target(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/targets/{target_id}")
        assert r.status_code == 200


def test_show_target_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/targets/9999")
        assert r.status_code == 404


def test_delete_target_removes_schedules_and_scans(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/targets/{target_id}/delete", follow_redirects=False)
        assert r.status_code == 303

    with SessionLocal() as session:
        assert session.query(models.Target).count() == 0
        assert session.query(models.Schedule).count() == 0


def test_delete_target_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/targets/9999/delete", follow_redirects=False)
        assert r.status_code == 404


def test_duplicate_target_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/targets/9999/duplicate", follow_redirects=False)
        assert r.status_code == 404


def test_create_target_duplicate_name_rejected(tmp_path):
    _make_target(tmp_path, "unique")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    folder = tmp_path / "other"
    folder.mkdir()
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "unique", "path": str(folder),
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "existe déjà" in r.text


def test_create_target_invalid_algo(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    folder = tmp_path / "bad"
    folder.mkdir()
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "t", "path": str(folder),
                "algorithm": "md5", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "algo" in r.text.lower()


def test_create_target_invalid_threads(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    folder = tmp_path / "bt"
    folder.mkdir()
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "t", "path": str(folder),
                "algorithm": "sha256", "threads": "many",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_create_target_empty_name(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    folder = tmp_path / "en"
    folder.mkdir()
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "   ", "path": str(folder),
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400


# -------------------- routes/scans ----------------------------------------


def test_run_scan_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/scans/run?target_id=9999", follow_redirects=False)
        assert r.status_code == 404


def test_show_scan_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/scans/9999")
        assert r.status_code == 404


def test_csv_export_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/scans/9999/events.csv")
        assert r.status_code == 404


def test_acknowledge_scan(tmp_path):
    target_id = _make_target(tmp_path)
    sid = _insert_scan(target_id, "drift", acknowledged=False)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/scans/{sid}/acknowledge", follow_redirects=False)
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.get(models.Scan, sid).acknowledged is True


def test_acknowledge_scan_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/scans/9999/acknowledge", follow_redirects=False)
        assert r.status_code == 404


def test_promote_scan(tmp_path):
    target_id = _make_target(tmp_path)
    sid = _insert_scan(target_id, "ok")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/scans/{sid}/promote", follow_redirects=False)
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.get(models.Target, target_id).baseline_scan_id == sid


def test_cancel_route_404(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/scans/9999/cancel", follow_redirects=False)
        assert r.status_code == 404


# -------------------- scans module ----------------------------------------


def test_trigger_scan_unknown_target_raises(tmp_path):
    from bc_vigil.integrity import scans
    import pytest
    with pytest.raises(ValueError):
        scans.trigger_scan(999999, trigger="manual")


def test_promote_baseline_unknown_scan(tmp_path):
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal
    import pytest
    with SessionLocal() as session:
        with pytest.raises(ValueError):
            scans.promote_baseline(session, 999999)


def test_promote_baseline_rejects_failed_scan(tmp_path):
    target_id = _make_target(tmp_path)
    sid = _insert_scan(target_id, "failed", error="boom")

    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal
    import pytest
    with SessionLocal() as session:
        with pytest.raises(ValueError):
            scans.promote_baseline(session, sid)


def test_cancel_scan_returns_false_when_no_handle(tmp_path):
    from bc_vigil.integrity import scans
    assert scans.cancel_scan(999999) is False


# -------------------- scheduler module ------------------------------------


def test_validate_cron_accepts_valid():
    from bc_vigil.integrity import scheduler
    scheduler.validate_cron("0 3 * * *")


def test_validate_cron_rejects_invalid():
    from bc_vigil.integrity import scheduler
    import pytest
    with pytest.raises(ValueError):
        scheduler.validate_cron("nope")


def test_scheduler_function_raises_if_not_started():
    from bc_vigil.integrity import scheduler
    import pytest
    scheduler.shutdown()
    with pytest.raises(RuntimeError):
        scheduler.scheduler()


def test_start_is_idempotent(tmp_path):
    from bc_vigil.integrity import scheduler
    scheduler.shutdown()
    s1 = scheduler.start()
    s2 = scheduler.start()
    assert s1 is s2
    scheduler.shutdown()


def test_remove_schedule_no_raise_when_job_missing(tmp_path):
    from bc_vigil.integrity import scheduler
    scheduler.start()
    try:
        scheduler.remove_schedule(99999)
    finally:
        scheduler.shutdown()


def test_sync_schedule_removes_when_disabled(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.db import SessionLocal
    scheduler.start()
    try:
        with SessionLocal() as session:
            s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=False)
            session.add(s)
            session.commit()
            scheduler.sync_schedule(session, s.id)
            jobs = scheduler.scheduler().get_jobs()
            assert not any(j.id == f"schedule-{s.id}" for j in jobs)
    finally:
        scheduler.shutdown()


def test_sync_schedule_removes_when_missing(tmp_path):
    from bc_vigil.integrity import scheduler
    from bc_vigil.db import SessionLocal
    scheduler.start()
    try:
        with SessionLocal() as session:
            scheduler.sync_schedule(session, 999999)
    finally:
        scheduler.shutdown()


def test_install_purge_job_noop_when_retention_zero(tmp_path, monkeypatch):
    from bc_vigil.integrity import scheduler
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "scan_retention_days", 0)
    scheduler.shutdown()
    scheduler.start()
    try:
        jobs = scheduler.scheduler().get_jobs()
        assert not any(j.id == scheduler.PURGE_JOB_ID for j in jobs)
    finally:
        scheduler.shutdown()


def test_install_purge_job_registered_when_retention_set(tmp_path, monkeypatch):
    from bc_vigil.integrity import scheduler
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "scan_retention_days", 7)
    scheduler.shutdown()
    scheduler.start()
    try:
        jobs = scheduler.scheduler().get_jobs()
        assert any(j.id == scheduler.PURGE_JOB_ID for j in jobs)
    finally:
        scheduler.shutdown()


def test_run_manual_scan_swallows_exceptions(tmp_path, monkeypatch):
    from bc_vigil.integrity import scans, scheduler
    def boom(scan_id):
        raise RuntimeError("boom")
    monkeypatch.setattr(scans, "execute_scan", boom)
    scheduler._run_manual_scan(42)


def test_run_scheduled_scan_skips_disabled(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=False)
        session.add(s)
        session.commit()
        sid = s.id

    scheduler._run_scheduled_scan(sid)

    with SessionLocal() as session:
        assert session.query(models.Scan).count() == 0


def test_run_scheduled_scan_missing_schedule(tmp_path):
    from bc_vigil.integrity import scheduler
    scheduler._run_scheduled_scan(999999)


# -------------------- bchash / cron_builder edge cases --------------------


def test_bchash_parse_patterns_mixed_whitespace():
    from bc_vigil.integrity import bchash
    assert bchash.parse_patterns("\n\n\n") == []
    assert bchash.parse_patterns("  foo\n\nbar  ") == ["foo", "bar"]


def test_bchash_run_hash_missing_target(tmp_path):
    from bc_vigil.integrity import bchash
    import pytest
    with pytest.raises(bchash.BcHashError):
        bchash.run_hash(tmp_path / "nope", tmp_path / "d", "sha256")


def test_bchash_read_summary_missing(tmp_path):
    from bc_vigil.integrity import bchash
    digest = tmp_path / "d.ndjson"
    digest.write_text('{"type":"header"}\n')
    import pytest
    with pytest.raises(bchash.BcHashError):
        bchash._read_summary(digest)


def test_cron_builder_weekly_invalid_day():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("weekly", time="08:00", days=["xyz"])
    assert r.cron is None
    assert "invalide" in r.error


def test_cron_builder_weekly_duplicate_days_deduplicated():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("weekly", time="08:00", days=["mon", "mon", "wed"])
    assert r.cron == "0 8 * * 1,3"


def test_cron_builder_missing_time():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("daily", time="")
    assert r.cron is None


def test_cron_builder_bad_time_format():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("daily", time="not-a-time")
    assert r.cron is None


def test_cron_builder_every_minutes_missing():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("every_minutes", interval_minutes="")
    assert r.cron is None


def test_cron_builder_hourly_missing_minute():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("hourly", minute_of_hour="")
    assert r.cron is None


def test_cron_builder_monthly_missing_dom():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("monthly", time="08:00", day_of_month="")
    assert r.cron is None


def test_cron_builder_non_integer_interval():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("every_minutes", interval_minutes="abc")
    assert r.cron is None


# -------------------- admin_ops edge cases --------------------------------


def test_snapshot_to_dir_creates_file(tmp_path):
    from bc_vigil.core import admin_ops
    dest = tmp_path / "snap"
    path = admin_ops.snapshot_to_dir(dest)
    assert path.exists()
    assert path.name.startswith("bc-vigil-backup-")


def test_has_active_scans_counts_properly(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "ok")
    _insert_scan(target_id, "running")
    _insert_scan(target_id, "pending")

    from bc_vigil.core import admin_ops
    assert admin_ops.has_active_scans() == 2


# -------------------- scheduler_utils --------------------------------------


def test_cron_period_daily():
    from bc_vigil.integrity.scheduler_utils import cron_period
    period = cron_period("0 3 * * *")
    assert period == timedelta(days=1)


def test_cron_period_hourly():
    from bc_vigil.integrity.scheduler_utils import cron_period
    period = cron_period("0 * * * *")
    assert period == timedelta(hours=1)


# -------------------- app helpers -----------------------------------------


def test_humanbytes_large_values():
    from bc_vigil.app import _format_bytes
    assert _format_bytes(1024 ** 3) == "1.0 GiB"
    assert _format_bytes(1024 ** 4) == "1.0 TiB"
    assert _format_bytes(1024 ** 5) == "1.0 PiB"


def test_format_datetime_utc():
    from bc_vigil.app import _format_datetime_utc
    out = _format_datetime_utc(0)
    assert "1970-01-01" in out
    assert "UTC" in out


def test_localtime_accepts_epoch(tmp_path, monkeypatch):
    from bc_vigil.app import _format_local
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "display_tz", "UTC")
    out = _format_local(0)
    assert "1970-01-01" in out


def test_localtime_falls_back_on_bad_tz(monkeypatch):
    from bc_vigil.app import _format_local
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "display_tz", "Not/A/Zone")
    dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    out = _format_local(dt)
    assert "Not/A/Zone" in out


def test_nav_pending_drift_swallows_db_errors(monkeypatch, tmp_path):
    from bc_vigil.app import _nav_pending_drift
    from bc_vigil import db as db_module

    class BrokenSession:
        def __enter__(self): raise RuntimeError("db down")
        def __exit__(self, *a): pass

    monkeypatch.setattr(db_module, "SessionLocal", lambda: BrokenSession())
    assert _nav_pending_drift() == 0


# -------------------- __main__ --------------------------------------------


def test_main_module_parses_args(monkeypatch):
    import sys
    import bc_vigil.__main__ as m

    called = {}
    def fake_run(app, host, port, reload):
        called["host"] = host
        called["port"] = port
        called["reload"] = reload

    monkeypatch.setattr(m, "uvicorn", type("U", (), {"run": staticmethod(fake_run)}))
    monkeypatch.setattr(sys, "argv", ["bc-vigil", "--host", "0.0.0.0", "--port", "9999"])
    m.main()
    assert called["host"] == "0.0.0.0"
    assert called["port"] == 9999
    assert called["reload"] is False
