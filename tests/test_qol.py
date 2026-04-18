from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


def _make_target(tmp_path, name: str = "qol") -> int:
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


def test_display_tz_filter_converts():
    from bc_vigil.app import _format_local
    from bc_vigil.config import settings
    dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    settings.display_tz = "UTC"
    assert "12:00:00 UTC" in _format_local(dt)

    settings.display_tz = "Europe/Paris"
    out = _format_local(dt)
    assert "Europe/Paris" in out
    assert "13:00:00" in out


def test_display_tz_filter_handles_none():
    from bc_vigil.app import _format_local
    assert _format_local(None) == "—"


def test_nav_badge_shows_pending_drift_count(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "drift", acknowledged=False)
    _insert_scan(target_id, "drift", acknowledged=False)
    _insert_scan(target_id, "drift", acknowledged=True)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert 'class="nav-badge">2<' in r.text


def test_scans_filter_by_status(tmp_path):
    target_id = _make_target(tmp_path)
    ok_id = _insert_scan(target_id, "ok")
    drift_id = _insert_scan(target_id, "drift", acknowledged=False)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/scans?status=drift")
        assert r.status_code == 200
        assert f"#{drift_id}" in r.text
        assert f"#{ok_id}" not in r.text


def test_scans_filter_by_target(tmp_path):
    t1 = _make_target(tmp_path, "t1")
    t2 = _make_target(tmp_path, "t2")
    s1 = _insert_scan(t1, "ok")
    s2 = _insert_scan(t2, "ok")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/scans?target_id={t1}")
        assert f"#{s1}" in r.text
        assert f"#{s2}" not in r.text


def test_acknowledge_all_global(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "drift", acknowledged=False)
    _insert_scan(target_id, "drift", acknowledged=False)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/scans/acknowledge-all", follow_redirects=False)
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        not_ack = session.query(models.Scan).filter_by(
            status="drift", acknowledged=False,
        ).count()
        assert not_ack == 0


def test_acknowledge_all_for_target(tmp_path):
    t1 = _make_target(tmp_path, "t1")
    t2 = _make_target(tmp_path, "t2")
    _insert_scan(t1, "drift", acknowledged=False)
    _insert_scan(t2, "drift", acknowledged=False)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        client.post(f"/scans/acknowledge-all?target_id={t1}", follow_redirects=False)

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s_t1 = session.query(models.Scan).filter_by(target_id=t1).one()
        s_t2 = session.query(models.Scan).filter_by(target_id=t2).one()
        assert s_t1.acknowledged is True
        assert s_t2.acknowledged is False


def test_duplicate_target(tmp_path):
    target_id = _make_target(tmp_path, "orig")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        t = session.get(models.Target, target_id)
        t.excludes = ".git\n*.log"
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/targets/{target_id}/duplicate", follow_redirects=False)
        assert r.status_code == 303

    with SessionLocal() as session:
        targets = session.query(models.Target).order_by(models.Target.id).all()
        assert len(targets) == 2
        assert targets[1].name == "orig (copie)"
        assert targets[1].excludes == ".git\n*.log"
        assert targets[1].path == targets[0].path


def test_duplicate_target_name_collision(tmp_path):
    target_id = _make_target(tmp_path, "dup")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        client.post(f"/targets/{target_id}/duplicate", follow_redirects=False)
        client.post(f"/targets/{target_id}/duplicate", follow_redirects=False)

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        names = {t.name for t in session.query(models.Target).all()}
        assert names == {"dup", "dup (copie)", "dup (copie) 2"}


def test_csv_export_events(tmp_path):
    target_id = _make_target(tmp_path)
    scan_id = _insert_scan(target_id, "drift", acknowledged=False)

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        session.add_all([
            models.IntegrityEvent(scan_id=scan_id, event_type="added", path="new.txt"),
            models.IntegrityEvent(
                scan_id=scan_id, event_type="modified", path="a.txt",
                old_digest="aa", new_digest="bb",
            ),
        ])
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/scans/{scan_id}/events.csv")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        body = r.text
        assert "event_type,path" in body
        assert "added,new.txt" in body
        assert "modified,a.txt,aa,bb" in body


def test_is_schedule_stuck_daily():
    from bc_vigil.integrity.scheduler_utils import is_schedule_stuck
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    recent = now - timedelta(hours=12)
    old = now - timedelta(days=5)

    assert is_schedule_stuck("0 3 * * *", recent, now=now) is False
    assert is_schedule_stuck("0 3 * * *", old, now=now) is True


def test_is_schedule_stuck_never_ran():
    from bc_vigil.integrity.scheduler_utils import is_schedule_stuck
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    assert is_schedule_stuck("0 3 * * *", None, now=now) is False


def test_purge_removes_old_scans_keeps_baseline(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.config import settings
    from bc_vigil.db import SessionLocal

    monkeypatch.setattr(settings, "scan_retention_days", 7)

    old_time = datetime.now(timezone.utc) - timedelta(days=30)
    recent_time = datetime.now(timezone.utc) - timedelta(days=1)

    with SessionLocal() as session:
        baseline = models.Scan(
            target_id=target_id, status="ok",
            started_at=old_time, finished_at=old_time,
            digest_path=str(tmp_path / "baseline.ndjson"),
        )
        (tmp_path / "baseline.ndjson").write_text("x")
        old_scan = models.Scan(
            target_id=target_id, status="ok",
            started_at=old_time, finished_at=old_time,
            digest_path=str(tmp_path / "old.ndjson"),
        )
        (tmp_path / "old.ndjson").write_text("x")
        recent_scan = models.Scan(
            target_id=target_id, status="ok",
            started_at=recent_time, finished_at=recent_time,
        )
        session.add_all([baseline, old_scan, recent_scan])
        session.commit()
        baseline_id = baseline.id
        old_scan_id = old_scan.id
        recent_id = recent_scan.id

        target = session.get(models.Target, target_id)
        target.baseline_scan_id = baseline_id
        session.commit()

    removed = scheduler.purge_old_scans()
    assert removed == 1

    with SessionLocal() as session:
        remaining_ids = {s.id for s in session.query(models.Scan).all()}
        assert remaining_ids == {baseline_id, recent_id}

    assert not (tmp_path / "old.ndjson").exists()
    assert (tmp_path / "baseline.ndjson").exists()


def test_purge_disabled_when_retention_zero(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.config import settings
    from bc_vigil.db import SessionLocal

    monkeypatch.setattr(settings, "scan_retention_days", 0)
    with SessionLocal() as session:
        session.add(models.Scan(
            target_id=target_id, status="ok",
            started_at=datetime.now(timezone.utc) - timedelta(days=365),
        ))
        session.commit()

    assert scheduler.purge_old_scans() == 0

    with SessionLocal() as session:
        assert session.query(models.Scan).count() == 1


def test_scans_list_has_auto_refresh_when_running(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "running")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/scans")
        assert 'hx-trigger="every 3s"' in r.text


def test_scans_list_no_refresh_when_idle(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "ok")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/scans")
        assert 'hx-trigger="every 3s"' not in r.text
