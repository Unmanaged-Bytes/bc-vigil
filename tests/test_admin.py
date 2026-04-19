from __future__ import annotations

import io
import tarfile
from pathlib import Path


def _make_seed(tmp_path: Path):
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    source = tmp_path / "tree"
    source.mkdir()
    (source / "a.txt").write_text("alpha\n")
    with SessionLocal() as session:
        target = models.Target(
            name="seed", path=str(source), algorithm="sha256", threads="auto",
        )
        session.add(target)
        session.commit()
        return target.id


def test_help_page_returns_200(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/help")
        assert r.status_code == 200
        assert "Prise en main" in r.text
        assert "Include / exclude" in r.text
        assert "Doublons" in r.text
        assert "Corbeille" in r.text


def test_admin_page_returns_200(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/admin")
        assert r.status_code == 200
        assert "Sauvegarde" in r.text


def test_backup_download_contains_db(tmp_path):
    _make_seed(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/admin/backup")
        assert r.status_code == 200
        assert r.headers["content-type"] == "application/gzip"
        assert "attachment" in r.headers["content-disposition"]
        body = r.content

    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
        names = tar.getnames()
        assert "bc-vigil.sqlite" in names


def test_reset_requires_confirmation(tmp_path):
    _make_seed(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/admin/reset", data={"confirm": "no"}, follow_redirects=False)
        assert r.status_code == 400

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.query(models.Target).count() == 1


def test_reset_clears_db_and_creates_snapshot(tmp_path):
    _make_seed(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/reset", data={"confirm": "RESET"}, follow_redirects=False,
        )
        assert r.status_code == 303

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.query(models.Target).count() == 0
        assert session.query(models.Scan).count() == 0

    from bc_vigil.config import settings
    snap_dir = settings.data_dir / "snapshots"
    assert snap_dir.exists()
    snaps = list(snap_dir.iterdir())
    assert len(snaps) == 1


def test_reset_blocked_when_scan_running(tmp_path):
    _make_seed(tmp_path)
    from datetime import datetime, timezone
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        target = session.query(models.Target).first()
        scan = models.Scan(
            target_id=target.id, status=models.SCAN_RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        session.add(scan)
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/reset", data={"confirm": "RESET"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "err=" in r.headers["location"]

    with SessionLocal() as session:
        assert session.query(models.Target).count() == 1


def test_restore_roundtrip(tmp_path):
    target_id = _make_seed(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        backup_resp = client.get("/admin/backup")
        archive = backup_resp.content

        client.post("/admin/reset", data={"confirm": "RESET"}, follow_redirects=False)

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        assert session.query(models.Target).count() == 0

    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/restore",
            data={"confirm": "RESTORE"},
            files={"archive": ("b.tar.gz", archive, "application/gzip")},
            follow_redirects=False,
        )
        assert r.status_code == 303, r.text
        assert "msg=" in r.headers["location"]

    with SessionLocal() as session:
        targets = session.query(models.Target).all()
        assert len(targets) == 1
        assert targets[0].name == "seed"


def test_restore_rejects_bad_archive(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/restore",
            data={"confirm": "RESTORE"},
            files={"archive": ("x.tar.gz", b"not a gzip", "application/gzip")},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert "err=" in r.headers["location"]


def test_restore_requires_confirmation(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/restore",
            data={"confirm": "no"},
            files={"archive": ("x.tar.gz", b"whatever", "application/gzip")},
            follow_redirects=False,
        )
        assert r.status_code == 400
