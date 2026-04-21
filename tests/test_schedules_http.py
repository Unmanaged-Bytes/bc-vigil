from __future__ import annotations

from pathlib import Path


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))
    from bc_vigil.config import settings as cfg
    cfg.data_dir = tmp_path / "var"
    from bc_vigil.db import init_db
    init_db()


def _make_target(tmp_path) -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / "data"
    folder.mkdir()
    with SessionLocal() as session:
        target = models.Target(
            name="sched-t", path=str(folder), algorithm="sha256", threads="auto",
        )
        session.add(target)
        session.commit()
        return target.id


def test_create_schedule_daily(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={
                "target_id": str(target_id),
                "mode": "daily",
                "time": "03:00",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        schedules = session.query(models.Schedule).all()
        assert len(schedules) == 1
        assert schedules[0].cron == "0 3 * * *"


def test_create_schedule_weekly_multi_days(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={
                "target_id": str(target_id),
                "mode": "weekly",
                "time": "09:30",
                "days": ["mon", "wed", "fri"],
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        schedule = session.query(models.Schedule).one()
        assert schedule.cron == "30 9 * * 1,3,5"


def test_create_schedule_expert_cron(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={
                "target_id": str(target_id),
                "mode": "cron",
                "cron_expr": "*/15 * * * *",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        schedule = session.query(models.Schedule).one()
        assert schedule.cron == "*/15 * * * *"


def test_create_schedule_invalid_cron_returns_form(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={
                "target_id": str(target_id),
                "mode": "cron",
                "cron_expr": "not a cron",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "invalide" in r.text

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.query(models.Schedule).count() == 0


def test_preview_endpoint(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules/preview",
            data={"mode": "daily", "time": "08:15"},
        )
        assert r.status_code == 200
        assert "15 8 * * *" in r.text
        assert "08:15" in r.text
        assert r.text.count("<li>") == 5


def test_schedule_form_uses_24h_time_picker(tmp_path, monkeypatch):
    # Custom time-picker macro (pair of <select> + hidden input) instead of
    # <input type="time">, which would otherwise display AM/PM in en-US
    # browser locales.
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.get(f"/schedules/new?target_id={target_id}")
        assert r.status_code == 200
        assert '<input type="time"' not in r.text
        assert 'data-time-picker' in r.text
        assert 'data-time-hour' in r.text
        assert 'data-time-minute' in r.text
        assert '<input type="hidden" name="time"' in r.text


def test_preview_invalid_weekly_shows_error(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules/preview",
            data={"mode": "weekly", "time": "08:00"},
        )
        assert r.status_code == 200
        assert "jour" in r.text


def test_edit_integrity_target_form_and_update(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models

    with TestClient(create_app()) as client:
        r = client.get(f"/targets/{target_id}/edit")
        assert r.status_code == 200
        assert "sched-t" in r.text
        assert 'readonly' in r.text

        r = client.post(
            f"/targets/{target_id}/update",
            data={
                "name": "renamed",
                "algorithm": "sha256",
                "threads": "4",
                "includes": "*.c",
                "excludes": ".git",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == f"/targets/{target_id}"

    with SessionLocal() as session:
        target = session.get(models.Target, target_id)
        assert target.name == "renamed"
        assert target.threads == "4"
        assert target.includes == "*.c"
        assert target.excludes == ".git"


def test_edit_integrity_target_missing(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/targets/9999/edit")
        assert r.status_code == 404
        r = client.post(
            "/targets/9999/update",
            data={"name": "x", "algorithm": "sha256", "threads": "auto"},
        )
        assert r.status_code == 404


def test_edit_integrity_target_rejects_duplicate_name(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    other = tmp_path / "other"
    other.mkdir()
    with SessionLocal() as session:
        session.add(models.Target(name="other-t", path=str(other), algorithm="sha256"))
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/targets/{target_id}/update",
            data={"name": "other-t", "algorithm": "sha256", "threads": "auto"},
        )
        assert r.status_code == 400


def test_edit_integrity_target_invalid_threads(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/targets/{target_id}/update",
            data={"name": "x", "algorithm": "sha256", "threads": "bogus"},
        )
        assert r.status_code == 400


def test_edit_integrity_schedule_form_and_update(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models

    with TestClient(create_app()) as client:
        r = client.post(
            "/schedules",
            data={"target_id": str(target_id), "mode": "cron", "cron_expr": "0 3 * * *"},
            follow_redirects=False,
        )
        assert r.status_code == 303

        with SessionLocal() as session:
            sched_id = session.scalars(__import__("sqlalchemy").select(models.Schedule)).first().id

        r = client.get(f"/schedules/{sched_id}/edit")
        assert r.status_code == 200
        assert "0 3 * * *" in r.text

        r = client.post(
            f"/schedules/{sched_id}/update",
            data={"mode": "cron", "cron_expr": "30 4 * * *", "enabled": "true"},
            follow_redirects=False,
        )
        assert r.status_code == 303

    with SessionLocal() as session:
        sched = session.get(models.Schedule, sched_id)
        assert sched.cron == "30 4 * * *"


def test_edit_integrity_schedule_missing_and_invalid(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/schedules/9999/edit")
        assert r.status_code == 404
        r = client.post("/schedules/9999/update", data={"mode": "daily", "time": "03:00"})
        assert r.status_code == 404

        r = client.post(
            "/schedules",
            data={"target_id": str(target_id), "mode": "cron", "cron_expr": "0 3 * * *"},
            follow_redirects=False,
        )
        from bc_vigil.db import SessionLocal
        from bc_vigil import models
        with SessionLocal() as session:
            sched_id = session.scalars(__import__("sqlalchemy").select(models.Schedule)).first().id

        r = client.post(
            f"/schedules/{sched_id}/update",
            data={"mode": "cron", "cron_expr": "garbage"},
        )
        assert r.status_code == 400
