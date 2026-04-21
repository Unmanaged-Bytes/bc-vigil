"""Regression guards for UI bugs fixed in 0.5.4 / 0.5.5.

Catches a regression if someone reintroduces hx-target="body" on the
live-refresh tables (causes the "page noire" flicker), the old
"Cibles" label without the integrity/dedup disambiguation, or the
native <input type="time"> in the schedule forms (AM/PM on en-US
browsers).
"""
from __future__ import annotations


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))
    from bc_vigil.config import settings as cfg
    cfg.data_dir = tmp_path / "var"
    from bc_vigil.db import init_db
    init_db()


def _make_target(tmp_path, name="reg-t"):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / name
    folder.mkdir(exist_ok=True)
    with SessionLocal() as session:
        t = models.Target(
            name=name, path=str(folder), algorithm="sha256", threads="auto",
        )
        session.add(t)
        session.commit()
        return t.id


def test_no_hx_target_body_in_scans_views(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        for url in ("/scans", "/dedup/scans"):
            r = client.get(url)
            assert r.status_code == 200
            assert 'hx-target="body"' not in r.text, (
                f"{url} reintroduced hx-target=body (reverts the 'page noire' fix)"
            )


def test_nav_labels_disambiguate_integrity_and_dedup(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert "Cibles intégrité" in r.text
        assert "Cibles doublons" in r.text
        assert "Scans intégrité" in r.text
        assert "Scans doublons" in r.text

        client.cookies.set("bcv_lang", "en")
        r = client.get("/")
        assert r.status_code == 200
        assert "Integrity targets" in r.text
        assert "Dedup targets" in r.text
        assert "Integrity scans" in r.text
        assert "Dedup scans" in r.text


def test_schedule_form_has_no_native_time_input(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/schedules/new?target_id={target_id}")
        assert r.status_code == 200
        assert '<input type="time"' not in r.text
        assert 'data-time-picker' in r.text


def test_favicon_served(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/static/favicon.svg")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("image/")


def test_cron_builder_label_honors_display_tz(tmp_path, monkeypatch):
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "display_tz", "Africa/Casablanca")
    from bc_vigil.integrity import cron_builder
    r = cron_builder.build_cron("daily", time="03:00")
    assert r.cron == "0 3 * * *"
    assert "Africa/Casablanca" in r.description
    assert "UTC" not in r.description
