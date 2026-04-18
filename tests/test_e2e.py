from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest


def _fresh_env(tmp_path: Path) -> None:
    os.environ["BC_VIGIL_DATA_DIR"] = str(tmp_path / "var")


@pytest.mark.requires_bchash
def test_scan_baseline_then_drift(tmp_path):
    _fresh_env(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.config import settings
    settings.data_dir = tmp_path / "var"
    from bc_vigil.db import SessionLocal, init_db
    init_db()

    source = tmp_path / "data"
    source.mkdir()
    (source / "a.txt").write_text("alpha\n")
    (source / "b.txt").write_text("beta\n")

    with SessionLocal() as session:
        target = models.Target(
            name="e2e", path=str(source), algorithm="sha256", threads="auto",
        )
        session.add(target)
        session.commit()
        target_id = target.id

    first_scan_id = scans.trigger_scan(target_id, trigger="manual")
    scans.execute_scan(first_scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, first_scan_id)
        assert scan.status == models.SCAN_OK, scan.error
        assert scan.files_total == 2
        target = session.get(models.Target, target_id)
        assert target.baseline_scan_id == first_scan_id
        assert Path(scan.digest_path).exists()

    (source / "a.txt").write_text("ALPHA CHANGED\n")
    (source / "c.txt").write_text("gamma\n")
    (source / "b.txt").unlink()

    second_scan_id = scans.trigger_scan(target_id, trigger="manual")
    scans.execute_scan(second_scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, second_scan_id)
        assert scan.status == models.SCAN_DRIFT, scan.error
        events_by_type = {}
        for ev in scan.events:
            events_by_type.setdefault(ev.event_type, []).append(ev)
        assert set(events_by_type) == {"added", "removed", "modified"}
        assert len(events_by_type["added"]) == 1
        assert len(events_by_type["removed"]) == 1
        assert len(events_by_type["modified"]) == 1
        mod = events_by_type["modified"][0]
        assert mod.old_digest and mod.new_digest
        assert mod.old_digest != mod.new_digest

        target = session.get(models.Target, target_id)
        assert target.baseline_scan_id == first_scan_id


def test_http_flow(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))

    from bc_vigil.config import settings as cfg
    cfg.data_dir = tmp_path / "var"

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    source = tmp_path / "http-data"
    source.mkdir()
    (source / "one.txt").write_text("hello\n")

    app = create_app()
    with TestClient(app) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert "BitCrafts Vigil" in r.text

        r = client.post(
            "/targets",
            data={
                "name": "http",
                "path": str(source),
                "algorithm": "sha256",
                "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303

        r = client.get("/targets")
        assert "http" in r.text
        assert str(source) in r.text
