from __future__ import annotations

import io
import stat
import subprocess
import tarfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _make_target(tmp_path: Path, name: str = "c100") -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    folder = tmp_path / f"tree-{name}"
    folder.mkdir(exist_ok=True)
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
            started_at=kwargs.pop("started_at", datetime.now(timezone.utc)),
            **kwargs,
        )
        session.add(scan)
        session.commit()
        return scan.id


# -------------------- bchash: CancelHandle edge cases ----------------------


def test_cancel_handle_signal_when_proc_none():
    from bc_vigil.integrity.bchash import CancelHandle
    h = CancelHandle()
    assert h.cancel() is False


def test_cancel_handle_attach_when_already_cancelled(tmp_path):
    from bc_vigil.integrity.bchash import CancelHandle
    script = tmp_path / "sleep.sh"
    script.write_text("#!/usr/bin/env bash\nexec sleep 30\n")
    script.chmod(0o755)

    proc = subprocess.Popen([str(script)])
    try:
        h = CancelHandle()
        h.cancel()
        h.attach(proc)
        proc.wait(timeout=5)
        assert proc.returncode != 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_cancel_handle_signal_proc_already_dead(tmp_path):
    from bc_vigil.integrity.bchash import CancelHandle
    proc = subprocess.Popen(["/usr/bin/true"])
    proc.wait(timeout=5)
    h = CancelHandle()
    h.attach(proc)
    assert h.cancel() is False


def test_cancel_handle_signal_lookup_error(monkeypatch):
    from bc_vigil.integrity.bchash import CancelHandle

    class FakeProc:
        def poll(self): return None

        def send_signal(self, sig):
            raise ProcessLookupError()

    h = CancelHandle()
    h._proc = FakeProc()
    assert h._signal_locked() is False


# -------------------- bchash: binary not found, diff error, read_summary ---


def test_bchash_binary_not_found(monkeypatch):
    from bc_vigil.integrity import bchash
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", "bc-hash-does-not-exist-xyz")
    with pytest.raises(bchash.BcHashError, match="binary not found"):
        bchash._binary()


def test_bchash_run_hash_exits_nonzero(tmp_path, monkeypatch):
    fake = tmp_path / "bc-hash-fail"
    fake.write_text("#!/usr/bin/env bash\necho 'oops' >&2\nexit 2\n")
    fake.chmod(0o755)

    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", str(fake))

    from bc_vigil.integrity import bchash
    source = tmp_path / "data"
    source.mkdir()
    with pytest.raises(bchash.BcHashError, match="hash failed"):
        bchash.run_hash(source, tmp_path / "d.json", "sha256")


def test_bchash_run_diff_exits_unexpected(tmp_path, monkeypatch):
    fake = tmp_path / "bc-hash-difffail"
    fake.write_text("#!/usr/bin/env bash\necho 'bad' >&2\nexit 3\n")
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", str(fake))
    from bc_vigil.integrity import bchash
    a = tmp_path / "a.json"
    b = tmp_path / "b.json"
    a.write_text("")
    b.write_text("")
    with pytest.raises(bchash.BcHashError, match="diff failed"):
        bchash.run_diff(a, b)


def test_bchash_run_diff_no_summary_line_fallback(tmp_path, monkeypatch):
    fake = tmp_path / "bc-hash-plain"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "echo 'bc-hash: running'\n"
        "echo 'ADDED file1.txt'\n"
        "echo 'REMOVED file2.txt'\n"
        "exit 1\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", str(fake))
    from bc_vigil.integrity import bchash
    a = tmp_path / "a.json"
    b = tmp_path / "b.json"
    a.write_text(""); b.write_text("")
    result = bchash.run_diff(a, b)
    assert result.added == 1
    assert result.removed == 1
    assert result.unchanged == 0


def test_bchash_run_diff_parses_modified_and_summary(tmp_path, monkeypatch):
    fake = tmp_path / "bc-hash-diff"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "echo 'MODIFIED file1.txt aaaaaaaa -> bbbbbbbb'\n"
        "echo 'ADDED file2.txt'\n"
        "echo 'bc-hash: 1 added, 0 removed, 1 modified, 5 unchanged' >&2\n"
        "exit 1\n"
    )
    fake.chmod(0o755)

    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", str(fake))

    from bc_vigil.integrity import bchash
    a = tmp_path / "a.ndjson"; a.write_text("")
    b = tmp_path / "b.ndjson"; b.write_text("")
    result = bchash.run_diff(a, b)

    assert result.added == 1
    assert result.modified == 1
    assert result.unchanged == 5
    modified = [e for e in result.events if e.event_type == "modified"]
    assert len(modified) == 1
    assert modified[0].path == "file1.txt"
    assert modified[0].old_digest == "aaaaaaaa"
    assert modified[0].new_digest == "bbbbbbbb"


def test_execute_scan_full_drift_cycle(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path, "drift-cycle")

    from bc_vigil.integrity import bchash, scans

    def fake_run_hash(target_path, digest_path, algo, threads,
                     includes=None, excludes=None, cancel=None):
        digest_path.parent.mkdir(parents=True, exist_ok=True)
        digest_path.write_text(
            '{"type":"header"}\n'
            '{"type":"summary","files_total":1,"bytes_total":10,"wall_ms":5}\n'
        )
        return bchash.HashResult(
            digest_path=digest_path, files_total=1, bytes_total=10,
            wall_ms=5, files_error=0, peak_rss_bytes=1000,
        )

    monkeypatch.setattr(bchash, "run_hash", fake_run_hash)

    first_scan = scans.trigger_scan(target_id)
    scans.execute_scan(first_scan)

    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        target = session.get(models.Target, target_id)
        assert target.baseline_scan_id == first_scan

    def fake_run_diff(a, b):
        return bchash.DiffResult(
            added=1, removed=0, modified=1, unchanged=5,
            events=[
                bchash.DiffEvent("added", "new.txt"),
                bchash.DiffEvent("modified", "a.txt", "oldhash", "newhash"),
            ],
        )

    monkeypatch.setattr(bchash, "run_diff", fake_run_diff)

    second_scan = scans.trigger_scan(target_id)
    scans.execute_scan(second_scan)

    with SessionLocal() as session:
        scan = session.get(models.Scan, second_scan)
        assert scan.status == models.SCAN_DRIFT
        events = list(scan.events)
        assert len(events) == 2
        types = {ev.event_type for ev in events}
        assert types == {"added", "modified"}


def test_bchash_read_summary_skips_empty_lines(tmp_path):
    from bc_vigil.integrity import bchash
    digest = tmp_path / "d.ndjson"
    digest.write_text(
        '{"type":"header"}\n'
        '\n'
        '   \n'
        '{"type":"summary","files_total":1,"files_ok":1,"files_error":0,'
        '"bytes_total":1,"wall_ms":1}\n'
    )
    summary = bchash._read_summary(digest)
    assert summary["files_total"] == 1


# -------------------- scans.py: edge paths ---------------------------------


def test_execute_scan_with_missing_scan_record(tmp_path, monkeypatch):
    from bc_vigil.integrity import scans
    scans.execute_scan(999999)


def test_finalize_failure_when_scan_missing():
    from bc_vigil.integrity import scans
    scans._finalize_failure(999999, "boom")


def test_finalize_cancelled_when_scan_missing():
    from bc_vigil.integrity import scans
    scans._finalize_cancelled(999999)


def test_execute_scan_handles_bchash_error(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    fake = tmp_path / "bc-hash-err"
    fake.write_text("#!/usr/bin/env bash\necho 'nope' >&2\nexit 4\n")
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_hash_binary", str(fake))

    from bc_vigil import models
    from bc_vigil.integrity import scans
    from bc_vigil.db import SessionLocal
    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_FAILED
        assert "hash failed" in scan.error


def test_execute_scan_handles_unexpected_exception(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)

    from bc_vigil import models
    from bc_vigil.integrity import bchash, scans
    from bc_vigil.db import SessionLocal

    def boom(*a, **kw):
        raise RuntimeError("unexpected kaboom")
    monkeypatch.setattr(bchash, "run_hash", boom)

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.Scan, scan_id)
        assert scan.status == models.SCAN_FAILED
        assert "unexpected" in scan.error


def _fake_run_hash(target_path, digest_path, algorithm, threads,
                   includes=None, excludes=None, cancel=None):
    from bc_vigil.integrity.bchash import HashResult
    digest_path.parent.mkdir(parents=True, exist_ok=True)
    digest_path.write_text('{"path":"x.txt","digest":"abc"}\n')
    return HashResult(
        digest_path=digest_path,
        files_total=1,
        bytes_total=1,
        wall_ms=1,
        files_error=0,
    )


def test_execute_scan_with_diff_error(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    (Path(tmp_path) / f"tree-c100" / "x.txt").write_text("x")

    from bc_vigil import models
    from bc_vigil.integrity import bchash, scans
    from bc_vigil.db import SessionLocal

    monkeypatch.setattr(bchash, "run_hash", _fake_run_hash)

    first_scan = scans.trigger_scan(target_id)
    scans.execute_scan(first_scan)

    def diff_boom(*a, **kw):
        raise bchash.BcHashError("diff kaboom")
    monkeypatch.setattr(bchash, "run_diff", diff_boom)

    second_scan = scans.trigger_scan(target_id)
    scans.execute_scan(second_scan)

    with SessionLocal() as session:
        scan = session.get(models.Scan, second_scan)
        assert scan.status == models.SCAN_FAILED
        assert "diff kaboom" in scan.error


def test_execute_scan_persists_even_if_scan_vanished_post_hash(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    (Path(tmp_path) / f"tree-c100" / "y.txt").write_text("y")

    from bc_vigil import models
    from bc_vigil.integrity import bchash, scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)

    def run_then_delete_scan(*a, **kw):
        result = _fake_run_hash(*a, **kw)
        with SessionLocal() as session:
            session.query(models.Scan).filter_by(id=scan_id).delete()
            session.commit()
        return result

    monkeypatch.setattr(bchash, "run_hash", run_then_delete_scan)

    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        assert session.get(models.Scan, scan_id) is None


# -------------------- scheduler.py: paths ----------------------------------


def test_run_scheduled_scan_full_path(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()
        sid = s.id

    scheduler._run_scheduled_scan(sid)

    with SessionLocal() as session:
        scans = session.query(models.Scan).filter_by(target_id=target_id).all()
        assert len(scans) == 1
        assert scans[0].trigger == "scheduled"


def test_run_scheduled_scan_trigger_fails(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scans as scans_mod, scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()
        sid = s.id

    def boom(*a, **kw):
        raise RuntimeError("trigger boom")
    monkeypatch.setattr(scans_mod, "trigger_scan", boom)

    scheduler._run_scheduled_scan(sid)


def test_run_scheduled_scan_execute_fails(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scans as scans_mod, scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()
        sid = s.id

    def boom_exec(*a, **kw):
        raise RuntimeError("exec boom")
    monkeypatch.setattr(scans_mod, "execute_scan", boom_exec)

    scheduler._run_scheduled_scan(sid)


def test_run_scan_async_registers_job(tmp_path):
    from bc_vigil.integrity import scheduler
    scheduler.shutdown()
    scheduler.start()
    try:
        scheduler.run_scan_async(42)
        job_ids = {j.id for j in scheduler.scheduler().get_jobs()}
        assert "scan-42" in job_ids
    finally:
        scheduler.shutdown()


def test_purge_swallows_oserror_on_unlink(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.integrity import scheduler
    from bc_vigil.config import settings
    from bc_vigil.db import SessionLocal
    import pathlib

    monkeypatch.setattr(settings, "scan_retention_days", 1)
    old = datetime.now(timezone.utc) - timedelta(days=30)
    with SessionLocal() as session:
        session.add(models.Scan(
            target_id=target_id, status="ok",
            started_at=old, finished_at=old,
            digest_path=str(tmp_path / "wont-matter.ndjson"),
        ))
        session.commit()

    original_unlink = pathlib.Path.unlink

    def failing_unlink(self, *a, **kw):
        raise OSError("simulated I/O error")

    monkeypatch.setattr(pathlib.Path, "unlink", failing_unlink)
    removed = scheduler.purge_old_scans()
    monkeypatch.setattr(pathlib.Path, "unlink", original_unlink)
    assert removed == 1


# -------------------- routes/targets: hard-to-reach branches ---------------


def test_show_target_with_schedule_stats(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        s = models.Schedule(target_id=target_id, cron="0 3 * * *", enabled=True)
        session.add(s)
        session.commit()

    _insert_scan(target_id, "ok", trigger="scheduled")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/targets/{target_id}")
        assert r.status_code == 200


def test_create_target_whitespace_path():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={"name": "x", "path": "   ", "algorithm": "sha256", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "requis" in r.text


def test_create_target_path_resolve_oserror(tmp_path, monkeypatch):
    import pathlib
    original = pathlib.Path.resolve

    def bad_resolve(self, strict=False):
        if "trigger-oserror" in str(self):
            raise OSError("simulated")
        return original(self, strict=strict)

    monkeypatch.setattr(pathlib.Path, "resolve", bad_resolve)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "bad-resolve",
                "path": "/trigger-oserror/here",
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "illisible" in r.text


def test_create_target_path_is_special_device(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient

    special = "/dev/null"
    if not Path(special).exists():
        pytest.skip("no /dev/null available")

    with TestClient(create_app()) as client:
        r = client.post(
            "/targets",
            data={
                "name": "special", "path": special,
                "algorithm": "sha256", "threads": "auto",
            },
            follow_redirects=False,
        )
        assert r.status_code in (303, 400)


# -------------------- routes/scans + admin: remaining paths ----------------


def test_run_scan_triggers_async(tmp_path):
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/scans/run?target_id={target_id}", follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"].startswith("/scans/")


def test_csv_export_contains_full_row(tmp_path):
    target_id = _make_target(tmp_path)
    sid = _insert_scan(target_id, "drift")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        session.add(models.IntegrityEvent(
            scan_id=sid, event_type="modified", path="a/b.txt",
            old_digest="old", new_digest="new",
        ))
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/scans/{sid}/events.csv")
        assert r.status_code == 200
        assert "modified,a/b.txt,old,new" in r.text


def test_admin_empty_archive_rejected(tmp_path):
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/admin/restore",
            data={"confirm": "RESTORE"},
            files={"archive": ("empty.tar.gz", b"", "application/gzip")},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_admin_page_lists_snapshots(tmp_path):
    from bc_vigil.config import settings
    snap_dir = settings.data_dir / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    (snap_dir / "bc-vigil-backup-20260101-000000Z.tar.gz").write_bytes(b"stub")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/admin")
        assert r.status_code == 200
        assert "bc-vigil-backup-20260101" in r.text


# -------------------- admin_ops: remaining branches ------------------------


def test_reset_blocked_when_active_scan(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "running")

    from bc_vigil.core import admin_ops
    with pytest.raises(admin_ops.AdminError, match="scans sont en cours"):
        admin_ops.reset_database()


def test_restore_blocked_when_active_scan(tmp_path):
    target_id = _make_target(tmp_path)
    _insert_scan(target_id, "pending")

    from bc_vigil.core import admin_ops
    with pytest.raises(admin_ops.AdminError, match="scans sont en cours"):
        admin_ops.restore_from_archive(b"irrelevant")


def test_reset_removes_sibling_sqlite_files(tmp_path):
    from bc_vigil.config import settings
    from bc_vigil import db as db_module
    db_module.engine.dispose()

    journal = settings.data_dir / "bc-vigil.sqlite-journal"
    wal = settings.data_dir / "bc-vigil.sqlite-wal"
    shm = settings.data_dir / "bc-vigil.sqlite-shm"
    journal.write_text("j")
    wal.write_text("w")
    shm.write_text("s")

    from bc_vigil.core import admin_ops
    admin_ops.reset_database()

    assert not journal.exists()
    assert not wal.exists()
    assert not shm.exists()


def test_restore_removes_sibling_and_missing_digests(tmp_path):
    from bc_vigil.config import settings
    from bc_vigil import db as db_module
    from bc_vigil.core import admin_ops
    import shutil

    db_module.engine.dispose()

    archive_buf = io.BytesIO()
    with tarfile.open(fileobj=archive_buf, mode="w:gz") as tar:
        db_src = settings.data_dir / "bc-vigil.sqlite"
        tar.add(db_src, arcname="bc-vigil.sqlite")

    if settings.digests_dir.exists():
        shutil.rmtree(settings.digests_dir)

    journal = settings.data_dir / "bc-vigil.sqlite-journal"
    wal = settings.data_dir / "bc-vigil.sqlite-wal"
    shm = settings.data_dir / "bc-vigil.sqlite-shm"
    journal.write_text("j"); wal.write_text("w"); shm.write_text("s")

    admin_ops.restore_from_archive(archive_buf.getvalue())
    assert not journal.exists()
    assert not wal.exists()
    assert not shm.exists()
    assert settings.digests_dir.exists()


def test_restore_rejects_archive_without_db(tmp_path):
    from bc_vigil.core import admin_ops

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        data = b"not a db"
        info = tarfile.TarInfo(name="other.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))

    with pytest.raises(admin_ops.AdminError, match="absent de l'archive"):
        admin_ops.restore_from_archive(buf.getvalue())


def test_show_scan_existing(tmp_path):
    target_id = _make_target(tmp_path)
    sid = _insert_scan(target_id, "ok")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/scans/{sid}")
        assert r.status_code == 200
        assert f"Scan #{sid}" in r.text


def test_safe_extract_rejects_path_traversal(tmp_path):
    from bc_vigil.core import admin_ops

    evil = io.BytesIO()
    with tarfile.open(fileobj=evil, mode="w:gz") as tar:
        data = b"evil"
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
        info2 = tarfile.TarInfo(name="bc-vigil.sqlite")
        info2.size = len(data)
        tar.addfile(info2, io.BytesIO(data))

    with pytest.raises(admin_ops.AdminError, match="douteux"):
        admin_ops.restore_from_archive(evil.getvalue())


# -------------------- cron_builder: remaining branches --------------------


def test_cron_builder_weekly_bad_time():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("weekly", time="99:00", days=["mon"])
    assert r.cron is None


def test_cron_builder_monthly_bad_time():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("monthly", time="99:00", day_of_month="1")
    assert r.cron is None


def test_cron_builder_cron_mode_empty_expr():
    from bc_vigil.integrity.cron_builder import build_cron
    r = build_cron("cron", cron_expr="")
    assert r.cron is None
    assert "requise" in r.error


def test_cron_builder_hhmm_non_integer():
    from bc_vigil.integrity.cron_builder import _parse_hhmm
    msg, _ = _parse_hhmm("aa:bb")
    assert isinstance(msg, str)
    assert "format" in msg


# -------------------- scheduler_utils: remaining branches -----------------


def test_is_schedule_stuck_zero_period():
    from bc_vigil.integrity.scheduler_utils import is_schedule_stuck
    import bc_vigil.integrity.scheduler_utils as su
    from datetime import timedelta

    original = su.cron_period

    def zero_period(*a, **kw):
        return timedelta(0)

    su.cron_period = zero_period
    try:
        assert is_schedule_stuck("0 0 * * *", datetime.now(timezone.utc)) is False
    finally:
        su.cron_period = original


def test_is_schedule_stuck_naive_last_run():
    from bc_vigil.integrity.scheduler_utils import is_schedule_stuck
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    naive_old = datetime(2026, 4, 1, 12, 0, 0)
    assert is_schedule_stuck("0 3 * * *", naive_old, now=now) is True


# -------------------- db.py: remaining branches ---------------------------


def test_session_scope_rolls_back_on_exception():
    from bc_vigil.db import session_scope
    with pytest.raises(RuntimeError):
        with session_scope() as session:
            raise RuntimeError("trigger rollback")


def test_add_missing_columns_skips_tables_not_in_db(tmp_path):
    from bc_vigil import db as db_module
    from sqlalchemy import MetaData, Table, Column, Integer

    extra_metadata = MetaData()
    Table(
        "not_in_real_db", extra_metadata,
        Column("id", Integer, primary_key=True),
    )

    class Fake:
        metadata = extra_metadata

    db_module._add_missing_columns(Fake())


# -------------------- __main__ entrypoint ---------------------------------


def test_run_module_as_script(monkeypatch):
    import runpy
    import sys

    calls = {}

    class FakeUvicorn:
        @staticmethod
        def run(app, host, port, reload):
            calls["host"] = host

    monkeypatch.setattr(sys, "argv", ["bc-vigil", "--reload"])

    sys.modules.pop("bc_vigil.__main__", None)
    import bc_vigil.__main__ as m
    monkeypatch.setattr(m, "uvicorn", FakeUvicorn)
    m.main()
    assert "host" in calls
