from __future__ import annotations

import os
import stat
import threading
import time
from pathlib import Path

import pytest


def _install_fake_bchash(tmp_path: Path, sleep_secs: int = 30) -> Path:
    script = tmp_path / "bc-hash-fake"
    script.write_text(
        "#!/usr/bin/env bash\n"
        f"exec sleep {sleep_secs}\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return script


def _make_target(tmp_path: Path) -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / "tree"
    folder.mkdir()
    (folder / "a.txt").write_text("x")
    with SessionLocal() as session:
        target = models.Target(
            name="cancel-t", path=str(folder), algorithm="sha256", threads="auto",
        )
        session.add(target)
        session.commit()
        return target.id


def _wait_handle_registered(scan_id: int, timeout: float = 5.0) -> bool:
    from bc_vigil.integrity import scans
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with scans._handles_lock:
            if scan_id in scans._cancel_handles:
                return True
        time.sleep(0.02)
    return False


def test_cancel_sends_sigterm_and_marks_cancelled(tmp_path):
    fake = _install_fake_bchash(tmp_path, sleep_secs=30)
    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(fake)

    target_id = _make_target(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    runner = threading.Thread(target=scans.execute_scan, args=(scan_id,))
    runner.start()
    try:
        assert _wait_handle_registered(scan_id), "handle never registered"

        t0 = time.monotonic()
        assert scans.cancel_scan(scan_id) is True

        runner.join(timeout=10)
        assert not runner.is_alive(), "runner still alive after cancel"
        elapsed = time.monotonic() - t0
        assert elapsed < 8, f"cancel took too long: {elapsed:.1f}s"
    finally:
        if runner.is_alive():
            scans.cancel_scan(scan_id, force=True)
            runner.join(timeout=5)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_CANCELLED
        assert scan.finished_at is not None

    digest_file = cfg.digests_dir / f"target-{target_id}" / f"scan-{scan_id}.ndjson"
    assert not digest_file.exists(), "partial digest should be cleaned up"


def test_cancel_force_uses_sigkill(tmp_path):
    trap_fake = tmp_path / "bc-hash-trap"
    trap_fake.write_text(
        "#!/usr/bin/env bash\n"
        "trap 'echo ignored' TERM\n"
        "while true; do sleep 1; done\n"
    )
    trap_fake.chmod(trap_fake.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(trap_fake)

    target_id = _make_target(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    runner = threading.Thread(target=scans.execute_scan, args=(scan_id,))
    runner.start()
    try:
        assert _wait_handle_registered(scan_id)

        scans.cancel_scan(scan_id)
        runner.join(timeout=2)
        assert runner.is_alive(), "SIGTERM trap should keep it alive"

        assert scans.cancel_scan(scan_id, force=True) is True
        runner.join(timeout=5)
        assert not runner.is_alive(), "SIGKILL should have killed it"
    finally:
        if runner.is_alive():
            scans.cancel_scan(scan_id, force=True)
            runner.join(timeout=5)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_CANCELLED


def test_cancel_route_rejects_finished_scan(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        from datetime import datetime, timezone
        scan = models.Scan(
            target_id=target_id,
            status=models.SCAN_OK,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        )
        session.add(scan)
        session.commit()
        scan_id = scan.id

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    with TestClient(create_app()) as client:
        r = client.post(f"/scans/{scan_id}/cancel", follow_redirects=False)
        assert r.status_code == 409


def test_cancel_route_on_running_scan(tmp_path):
    fake = _install_fake_bchash(tmp_path, sleep_secs=30)
    from bc_vigil.config import settings as cfg
    cfg.bc_hash_binary = str(fake)

    target_id = _make_target(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    runner = threading.Thread(target=scans.execute_scan, args=(scan_id,))
    runner.start()
    try:
        assert _wait_handle_registered(scan_id)

        from bc_vigil.app import create_app
        from fastapi.testclient import TestClient
        with TestClient(create_app()) as client:
            r = client.post(
                f"/scans/{scan_id}/cancel", follow_redirects=False,
            )
            assert r.status_code == 303

        runner.join(timeout=10)
        assert not runner.is_alive()
    finally:
        if runner.is_alive():
            scans.cancel_scan(scan_id, force=True)
            runner.join(timeout=5)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_CANCELLED
