from __future__ import annotations

import os
from pathlib import Path


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))
    from bc_vigil.config import settings as cfg
    cfg.data_dir = tmp_path / "var"
    from bc_vigil.db import init_db
    init_db()


def test_tilde_path_is_expanded(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    home = tmp_path / "fakehome"
    (home / "docs").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "tilde", "path": "~/docs",
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text

    from bc_vigil.db import SessionLocal
    from bc_vigil import models
    with SessionLocal() as session:
        target = session.query(models.Target).filter_by(name="tilde").one()
        assert target.path == str(home / "docs")
        assert Path(target.path).is_absolute()


def test_relative_path_rejected(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "rel", "path": "./some-rel",
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "absolu" in r.text.lower()


def test_nonexistent_path_rejected_with_resolved_hint(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path / "fakehome"))
    (tmp_path / "fakehome").mkdir()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "nope", "path": "~/nonexistent",
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        resolved = str(tmp_path / "fakehome" / "nonexistent")
        assert resolved in r.text, f"expected resolved path {resolved!r} in error"
