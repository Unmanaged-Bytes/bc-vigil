from __future__ import annotations

import json
import stat
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


# =========================================================================
# Helpers
# =========================================================================


def _make_tree(tmp_path: Path, name: str = "dedup-tree") -> Path:
    folder = tmp_path / name
    folder.mkdir(exist_ok=True)
    (folder / "a.txt").write_text("hello")
    (folder / "b.txt").write_text("hello")
    (folder / "c.txt").write_text("world")
    return folder


def _make_target(tmp_path: Path, name: str = "dedup-t") -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    folder = _make_tree(tmp_path, f"tree-{name}")
    with SessionLocal() as session:
        target = models.DedupTarget(
            name=name, path=str(folder), algorithm="xxh3", threads="auto",
        )
        session.add(target)
        session.commit()
        return target.id


def _insert_scan(
    target_id: int, status: str, **kwargs
) -> int:
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        scan = models.DedupScan(
            target_id=target_id, status=status,
            started_at=kwargs.pop("started_at", datetime.now(timezone.utc)),
            **kwargs,
        )
        session.add(scan)
        session.commit()
        return scan.id


def _install_fake_bcduplicate(
    tmp_path: Path,
    duplicates: bool = True,
    name: str = "bc-duplicate-fake",
) -> Path:
    script = tmp_path / name
    if duplicates:
        json_body = (
            '{"version":"1.0.0","tool":"bc-duplicate","algorithm":"xxh3",'
            '"stats":{"files_scanned":3,"directories_scanned":0,'
            '"files_skipped":0,"hardlinks_collapsed":0,"size_candidates":1,'
            '"files_hashed_fast":3,"files_hashed_full":2,'
            '"duplicate_groups":1,"duplicate_files":1,'
            '"wasted_bytes":6,"wall_ms":10},'
            '"groups":[{"size":6,"files":["/tmp/a.txt","/tmp/b.txt"]}]}'
        )
        stats_line = (
            "bc-duplicate: 1 duplicate group(s), 1 duplicate file(s), "
            "6 wasted byte(s) in 10 ms"
        )
    else:
        json_body = (
            '{"version":"1.0.0","tool":"bc-duplicate","algorithm":"xxh3",'
            '"stats":{"files_scanned":3,"directories_scanned":0,'
            '"files_skipped":0,"hardlinks_collapsed":0,"size_candidates":0,'
            '"files_hashed_fast":0,"files_hashed_full":0,'
            '"duplicate_groups":0,"duplicate_files":0,'
            '"wasted_bytes":0,"wall_ms":4},"groups":[]}'
        )
        stats_line = (
            "bc-duplicate: 0 duplicate group(s), 0 duplicate file(s), "
            "0 wasted byte(s) in 4 ms"
        )
    script.write_text(
        "#!/usr/bin/env bash\n"
        "OUT=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in --output=*) OUT=\"${arg#--output=}\" ;; esac\n"
        "done\n"
        f"if [ -n \"$OUT\" ]; then\n  cat > \"$OUT\" <<'JSON'\n{json_body}\nJSON\nfi\n"
        f"echo '{stats_line}' >&2\n"
        "exit 0\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return script


def _wait_handle_registered(scan_id: int, timeout: float = 5.0) -> bool:
    from bc_vigil.dedup import scans
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with scans._handles_lock:
            if scan_id in scans._cancel_handles:
                return True
        time.sleep(0.02)
    return False


# =========================================================================
# bcduplicate: CancelHandle
# =========================================================================


def test_cancel_handle_signal_when_proc_none():
    from bc_vigil.dedup.bcduplicate import CancelHandle
    h = CancelHandle()
    assert h.cancel() is False


def test_cancel_handle_attach_when_already_cancelled(tmp_path):
    from bc_vigil.dedup.bcduplicate import CancelHandle
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


def test_cancel_handle_signal_proc_already_dead():
    from bc_vigil.dedup.bcduplicate import CancelHandle
    proc = subprocess.Popen(["/usr/bin/true"])
    proc.wait(timeout=5)
    h = CancelHandle()
    h.attach(proc)
    assert h.cancel() is False


def test_cancel_handle_signal_lookup_error():
    from bc_vigil.dedup.bcduplicate import CancelHandle

    class FakeProc:
        def poll(self):
            return None

        def send_signal(self, sig):
            raise ProcessLookupError()

    h = CancelHandle()
    h._proc = FakeProc()
    assert h._signal_locked() is False


def test_cancel_handle_cancelled_property():
    from bc_vigil.dedup.bcduplicate import CancelHandle
    h = CancelHandle()
    assert h.cancelled is False
    h.cancel()
    assert h.cancelled is True


def test_cancel_handle_force_flag():
    from bc_vigil.dedup.bcduplicate import CancelHandle
    h = CancelHandle()
    assert h.cancel(force=True) is False
    assert h._forced is True


def test_nav_pending_duplicates_swallows_exception(monkeypatch):
    from bc_vigil import app as app_mod
    from bc_vigil import db as db_module

    def boom():
        raise RuntimeError("no db")

    monkeypatch.setattr(db_module, "SessionLocal", boom)
    assert app_mod._nav_pending_duplicates() == 0


# =========================================================================
# bcduplicate: _binary / parse_patterns
# =========================================================================


def test_binary_not_found(monkeypatch):
    from bc_vigil.config import settings
    from bc_vigil.dedup import bcduplicate
    monkeypatch.setattr(
        settings, "bc_duplicate_binary", "bc-duplicate-xyz-not-found",
    )
    with pytest.raises(bcduplicate.BcDuplicateError, match="binary not found"):
        bcduplicate._binary()


def test_parse_patterns_variants():
    from bc_vigil.dedup.bcduplicate import parse_patterns
    assert parse_patterns(None) == []
    assert parse_patterns("") == []
    assert parse_patterns("  \n  \n") == []
    assert parse_patterns("a\n b \n\nc") == ["a", "b", "c"]


# =========================================================================
# bcduplicate: run_scan
# =========================================================================


def test_run_scan_target_missing(tmp_path):
    from bc_vigil.dedup import bcduplicate
    with pytest.raises(bcduplicate.BcDuplicateError, match="does not exist"):
        bcduplicate.run_scan(
            tmp_path / "does-not-exist", tmp_path / "out.json",
        )


def test_run_scan_non_zero_exit(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-fail"
    fake.write_text("#!/usr/bin/env bash\necho 'bad' >&2\nexit 2\n")
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = tmp_path / "data"
    source.mkdir()
    from bc_vigil.dedup import bcduplicate
    with pytest.raises(bcduplicate.BcDuplicateError, match="scan failed"):
        bcduplicate.run_scan(source, tmp_path / "out.json")


def test_run_scan_output_missing(tmp_path, monkeypatch):
    # bc-duplicate omits the JSON output entirely when discovery yields
    # zero files. Treat as a successful empty scan, not an error.
    fake = tmp_path / "bc-duplicate-no-output"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "echo 'bc-duplicate: 0 duplicate group(s), 0 duplicate file(s), "
        "0 wasted byte(s) in 1 ms' >&2\n"
        "exit 0\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = tmp_path / "data"
    source.mkdir()
    from bc_vigil.dedup import bcduplicate
    result = bcduplicate.run_scan(source, tmp_path / "out.json")
    assert result.duplicate_groups == 0
    assert result.duplicate_files == 0
    assert result.wasted_bytes == 0
    assert result.groups == []


def test_run_scan_invalid_json(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-bad-json"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "OUT=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in --output=*) OUT=\"${arg#--output=}\" ;; esac\n"
        "done\n"
        "printf 'not-a-json' > \"$OUT\"\n"
        "echo 'bc-duplicate: 0 duplicate group(s), 0 duplicate file(s), "
        "0 wasted byte(s) in 1 ms' >&2\n"
        "exit 0\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = tmp_path / "data"
    source.mkdir()
    from bc_vigil.dedup import bcduplicate
    with pytest.raises(bcduplicate.BcDuplicateError, match="invalid"):
        bcduplicate.run_scan(source, tmp_path / "out.json")


def test_run_scan_json_not_object(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-list"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "OUT=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in --output=*) OUT=\"${arg#--output=}\" ;; esac\n"
        "done\n"
        "printf '[]' > \"$OUT\"\n"
        "exit 0\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = tmp_path / "data"
    source.mkdir()
    from bc_vigil.dedup import bcduplicate
    with pytest.raises(bcduplicate.BcDuplicateError, match="not a JSON object"):
        bcduplicate.run_scan(source, tmp_path / "out.json")


def test_run_scan_filters_invalid_groups(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-mixed"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "OUT=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in --output=*) OUT=\"${arg#--output=}\" ;; esac\n"
        "done\n"
        "cat > \"$OUT\" <<'JSON'\n"
        "{\"stats\":{},\"groups\":[\"not-a-dict\","
        "{\"size\":1,\"files\":\"not-a-list\"},"
        "{\"size\":10,\"files\":[\"/a\", 42]}]}\n"
        "JSON\n"
        "exit 0\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = tmp_path / "data"
    source.mkdir()
    from bc_vigil.dedup import bcduplicate
    result = bcduplicate.run_scan(source, tmp_path / "out.json")
    assert len(result.groups) == 1
    assert result.groups[0].size == 10
    assert result.groups[0].files == ["/a"]


def test_run_scan_success_with_all_flags(tmp_path, monkeypatch):
    fake = _install_fake_bcduplicate(tmp_path, duplicates=True)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = _make_tree(tmp_path)
    out = tmp_path / "nested" / "out.json"
    from bc_vigil.dedup import bcduplicate
    result = bcduplicate.run_scan(
        source, out,
        algorithm="xxh3", threads="2",
        includes=["*.txt"], excludes=[".git"],
        minimum_size=0, include_hidden=True, follow_symlinks=True,
        match_hardlinks=True, one_file_system=True,
    )
    assert result.duplicate_groups == 1
    assert result.duplicate_files == 1
    assert result.wasted_bytes == 6
    assert result.wall_ms == 10
    assert result.files_scanned == 3
    assert result.algorithm == "xxh3"
    assert len(result.groups) == 1


def test_run_scan_cancellation(tmp_path, monkeypatch):
    sleep_fake = tmp_path / "bc-duplicate-sleep"
    sleep_fake.write_text("#!/usr/bin/env bash\nexec sleep 30\n")
    sleep_fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(sleep_fake))

    source = _make_tree(tmp_path, "sleep-tree")
    out = tmp_path / "cancel-out.json"
    from bc_vigil.dedup import bcduplicate

    handle = bcduplicate.CancelHandle()

    def canceller():
        time.sleep(0.1)
        handle.cancel()

    threading.Thread(target=canceller, daemon=True).start()
    with pytest.raises(bcduplicate.BcDuplicateCancelled):
        bcduplicate.run_scan(source, out, cancel=handle)
    assert not out.exists()


def test_run_scan_no_stats_line_falls_back_to_json(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-quiet"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "OUT=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  case \"$arg\" in --output=*) OUT=\"${arg#--output=}\" ;; esac\n"
        "done\n"
        "cat > \"$OUT\" <<'JSON'\n"
        "{\"stats\":{\"duplicate_groups\":2,\"duplicate_files\":3,"
        "\"wasted_bytes\":100,\"wall_ms\":50,\"files_scanned\":5},"
        "\"groups\":[]}\n"
        "JSON\n"
        "exit 0\n"
    )
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    source = _make_tree(tmp_path, "quiet-tree")
    from bc_vigil.dedup import bcduplicate
    result = bcduplicate.run_scan(source, tmp_path / "q.json")
    assert result.duplicate_groups == 2
    assert result.duplicate_files == 3
    assert result.wasted_bytes == 100
    assert result.wall_ms == 50
    assert result.files_scanned == 5


# =========================================================================
# dedup.scans orchestration
# =========================================================================


def test_trigger_scan_missing_target():
    from bc_vigil.dedup import scans
    with pytest.raises(ValueError, match="not found"):
        scans.trigger_scan(999999)


def test_execute_scan_missing_scan_record():
    from bc_vigil.dedup import scans
    scans.execute_scan(999999)


def test_execute_scan_with_duplicates(tmp_path, monkeypatch):
    fake = _install_fake_bcduplicate(tmp_path, duplicates=True)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    target_id = _make_target(tmp_path, "with-dupes")
    from bc_vigil import models
    from bc_vigil.dedup import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_DUPLICATES
        assert scan.duplicate_groups == 1
        assert scan.wasted_bytes == 6
        groups = list(scan.groups)
        assert len(groups) == 1
        assert groups[0].size == 6
        assert groups[0].file_count == 2
        target = session.get(models.DedupTarget, target_id)
        assert target.last_scan_id == scan_id


def test_execute_scan_without_duplicates(tmp_path, monkeypatch):
    fake = _install_fake_bcduplicate(tmp_path, duplicates=False)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    target_id = _make_target(tmp_path, "no-dupes")
    from bc_vigil import models
    from bc_vigil.dedup import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_OK
        assert scan.duplicate_groups == 0
        assert len(list(scan.groups)) == 0


def test_execute_scan_bcduplicate_error(tmp_path, monkeypatch):
    fake = tmp_path / "bc-duplicate-fail"
    fake.write_text("#!/usr/bin/env bash\necho 'boom' >&2\nexit 4\n")
    fake.chmod(0o755)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    target_id = _make_target(tmp_path, "fail")
    from bc_vigil import models
    from bc_vigil.dedup import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_FAILED
        assert "scan failed" in scan.error


def test_execute_scan_unexpected_exception(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path, "unexpected")
    from bc_vigil import models
    from bc_vigil.dedup import bcduplicate, scans
    from bc_vigil.db import SessionLocal

    def boom(*a, **kw):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(bcduplicate, "run_scan", boom)
    scan_id = scans.trigger_scan(target_id)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_FAILED
        assert "unexpected" in scan.error


def test_execute_scan_vanishes_post_run(tmp_path, monkeypatch):
    fake = _install_fake_bcduplicate(tmp_path, duplicates=False)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    target_id = _make_target(tmp_path, "vanish")
    from bc_vigil import models
    from bc_vigil.dedup import bcduplicate, scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    original = bcduplicate.run_scan

    def run_then_delete_scan(*a, **kw):
        res = original(*a, **kw)
        with SessionLocal() as session:
            session.query(models.DedupScan).filter_by(id=scan_id).delete()
            session.commit()
        return res

    monkeypatch.setattr(bcduplicate, "run_scan", run_then_delete_scan)
    scans.execute_scan(scan_id)

    with SessionLocal() as session:
        assert session.get(models.DedupScan, scan_id) is None


def test_finalize_failure_missing_scan():
    from bc_vigil.dedup import scans
    scans._finalize_failure(999999, "boom")


def test_finalize_cancelled_missing_scan():
    from bc_vigil.dedup import scans
    scans._finalize_cancelled(999999)


def test_cancel_scan_no_handle():
    from bc_vigil.dedup import scans
    assert scans.cancel_scan(999999) is False


def test_cancel_sigterm_marks_cancelled(tmp_path):
    sleep_fake = tmp_path / "bc-duplicate-sleep"
    sleep_fake.write_text("#!/usr/bin/env bash\nexec sleep 30\n")
    sleep_fake.chmod(0o755)
    from bc_vigil.config import settings
    settings.bc_duplicate_binary = str(sleep_fake)

    target_id = _make_target(tmp_path, "cancel")
    from bc_vigil import models
    from bc_vigil.dedup import scans
    from bc_vigil.db import SessionLocal

    scan_id = scans.trigger_scan(target_id)
    runner = threading.Thread(target=scans.execute_scan, args=(scan_id,))
    runner.start()
    try:
        assert _wait_handle_registered(scan_id), "handle never registered"
        t0 = time.monotonic()
        assert scans.cancel_scan(scan_id) is True
        runner.join(timeout=10)
        assert not runner.is_alive()
        elapsed = time.monotonic() - t0
        assert elapsed < 8
    finally:
        if runner.is_alive():
            scans.cancel_scan(scan_id, force=True)
            runner.join(timeout=5)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_CANCELLED
        assert scan.finished_at is not None

    out_file = settings.dedup_dir / f"target-{target_id}" / f"scan-{scan_id}.json"
    assert not out_file.exists()


def test_parse_group_paths():
    from bc_vigil.dedup.scans import parse_group_paths
    assert parse_group_paths(json.dumps(["/a", "/b"])) == ["/a", "/b"]
    assert parse_group_paths(json.dumps(["/a", 42, None, "/b"])) == ["/a", "/b"]
    assert parse_group_paths("not-json") == []
    assert parse_group_paths(json.dumps({"not": "a list"})) == []


# =========================================================================
# dedup.scheduler
# =========================================================================


def test_validate_cron_valid_and_invalid():
    from bc_vigil.dedup.scheduler import validate_cron
    validate_cron("0 3 * * *")
    with pytest.raises(ValueError):
        validate_cron("not a cron")


def test_run_scheduled_scan_full_path(tmp_path, monkeypatch):
    fake = _install_fake_bcduplicate(tmp_path, duplicates=False)
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "bc_duplicate_binary", str(fake))

    target_id = _make_target(tmp_path, "sched")
    from bc_vigil import models
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal

    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
        sid = sched.id

    scheduler._run_scheduled_scan(sid)

    with SessionLocal() as session:
        scans = session.query(models.DedupScan).filter_by(target_id=target_id).all()
        assert len(scans) == 1
        assert scans[0].trigger == "scheduled"


def test_run_scheduled_scan_disabled_returns_early(tmp_path):
    target_id = _make_target(tmp_path, "disabled")
    from bc_vigil import models
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=False,
        )
        session.add(sched)
        session.commit()
        sid = sched.id

    scheduler._run_scheduled_scan(sid)

    with SessionLocal() as session:
        scans = session.query(models.DedupScan).filter_by(target_id=target_id).all()
        assert scans == []


def test_run_scheduled_scan_missing_schedule():
    from bc_vigil.dedup import scheduler
    scheduler._run_scheduled_scan(999999)


def test_run_scheduled_scan_trigger_fails(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path, "trig-fail")
    from bc_vigil import models
    from bc_vigil.dedup import scans as scans_mod, scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
        sid = sched.id

    monkeypatch.setattr(
        scans_mod, "trigger_scan", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
    )
    scheduler._run_scheduled_scan(sid)


def test_run_scheduled_scan_execute_fails(tmp_path, monkeypatch):
    target_id = _make_target(tmp_path, "exec-fail")
    from bc_vigil import models
    from bc_vigil.dedup import scans as scans_mod, scheduler
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
        sid = sched.id

    monkeypatch.setattr(
        scans_mod, "execute_scan",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")),
    )
    scheduler._run_scheduled_scan(sid)


def test_run_manual_scan_swallows_exception(monkeypatch):
    from bc_vigil.dedup import scans as scans_mod, scheduler
    monkeypatch.setattr(
        scans_mod, "execute_scan",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")),
    )
    scheduler._run_manual_scan(1)


def test_scheduler_start_idempotent_and_shutdown():
    from bc_vigil.dedup import scheduler
    scheduler.shutdown()
    s1 = scheduler.start()
    s2 = scheduler.start()
    assert s1 is s2
    scheduler.shutdown()


def test_scheduler_not_started_raises():
    from bc_vigil.dedup import scheduler
    scheduler.shutdown()
    with pytest.raises(RuntimeError):
        scheduler.scheduler()


def test_install_purge_with_retention(monkeypatch):
    from bc_vigil.config import settings
    monkeypatch.setattr(settings, "scan_retention_days", 1)
    from bc_vigil.dedup import scheduler
    scheduler.shutdown()
    scheduler.start()
    try:
        job_ids = {j.id for j in scheduler.scheduler().get_jobs()}
        assert scheduler.PURGE_JOB_ID in job_ids
    finally:
        scheduler.shutdown()


def test_purge_old_scans_retention_zero():
    from bc_vigil.dedup import scheduler
    assert scheduler.purge_old_scans() == 0


def test_purge_old_scans_removes_and_keeps_protected(tmp_path, monkeypatch):
    from bc_vigil import models
    from bc_vigil.config import settings
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal

    monkeypatch.setattr(settings, "scan_retention_days", 1)
    target_id = _make_target(tmp_path, "purge-t")

    old = datetime.now(timezone.utc) - timedelta(days=30)
    with SessionLocal() as session:
        old_scan = models.DedupScan(
            target_id=target_id, status=models.DEDUP_OK,
            started_at=old, finished_at=old,
            output_path=str(tmp_path / "old-out.json"),
        )
        (tmp_path / "old-out.json").write_text("{}")
        protected = models.DedupScan(
            target_id=target_id, status=models.DEDUP_OK,
            started_at=old, finished_at=old,
        )
        session.add(old_scan)
        session.add(protected)
        session.flush()
        target = session.get(models.DedupTarget, target_id)
        target.last_scan_id = protected.id
        session.commit()

    removed = scheduler.purge_old_scans()
    assert removed == 1


def test_purge_old_scans_swallows_oserror(tmp_path, monkeypatch):
    import pathlib
    from bc_vigil import models
    from bc_vigil.config import settings
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal

    monkeypatch.setattr(settings, "scan_retention_days", 1)
    target_id = _make_target(tmp_path, "purge-oserror")

    old = datetime.now(timezone.utc) - timedelta(days=30)
    with SessionLocal() as session:
        session.add(models.DedupScan(
            target_id=target_id, status=models.DEDUP_OK,
            started_at=old, finished_at=old,
            output_path=str(tmp_path / "stale.json"),
        ))
        session.commit()

    original_unlink = pathlib.Path.unlink

    def failing_unlink(self, *a, **kw):
        raise OSError("simulated")

    monkeypatch.setattr(pathlib.Path, "unlink", failing_unlink)
    try:
        removed = scheduler.purge_old_scans()
    finally:
        monkeypatch.setattr(pathlib.Path, "unlink", original_unlink)
    assert removed == 1


def test_sync_schedule_and_remove(tmp_path):
    from bc_vigil import models
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal
    scheduler.shutdown()
    scheduler.start()
    try:
        target_id = _make_target(tmp_path, "sync-t")
        with SessionLocal() as session:
            sched = models.DedupSchedule(
                target_id=target_id, cron="0 3 * * *", enabled=True,
            )
            session.add(sched)
            session.commit()
            scheduler.sync_schedule(session, sched.id)
            assert scheduler._job_id(sched.id) in {
                j.id for j in scheduler.scheduler().get_jobs()
            }

            sched.enabled = False
            session.commit()
            scheduler.sync_schedule(session, sched.id)
            assert scheduler._job_id(sched.id) not in {
                j.id for j in scheduler.scheduler().get_jobs()
            }

            scheduler.sync_schedule(session, 999999)  # missing
            scheduler.remove_schedule(999999)  # missing job handled
    finally:
        scheduler.shutdown()


def test_run_scan_async_registers_job():
    from bc_vigil.dedup import scheduler
    scheduler.shutdown()
    scheduler.start()
    try:
        scheduler.run_scan_async(42)
        job_ids = {j.id for j in scheduler.scheduler().get_jobs()}
        assert "dedup-scan-42" in job_ids
    finally:
        scheduler.shutdown()


def test_reload_jobs_iterates_enabled(tmp_path):
    from bc_vigil import models
    from bc_vigil.dedup import scheduler
    from bc_vigil.db import SessionLocal

    target_id = _make_target(tmp_path, "reload-t")
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
        schedule_id = sched.id

    scheduler.shutdown()
    scheduler.start()
    try:
        assert scheduler._job_id(schedule_id) in {
            j.id for j in scheduler.scheduler().get_jobs()
        }
    finally:
        scheduler.shutdown()


# =========================================================================
# dedup.scheduler_utils
# =========================================================================


def test_is_schedule_stuck_none_last_run():
    from bc_vigil.dedup.scheduler_utils import is_schedule_stuck
    assert is_schedule_stuck("0 3 * * *", None) is False


def test_is_schedule_stuck_zero_period(monkeypatch):
    from datetime import timedelta as td
    from bc_vigil.dedup import scheduler_utils as su
    monkeypatch.setattr(su, "cron_period", lambda *a, **k: td(0))
    assert su.is_schedule_stuck(
        "0 3 * * *", datetime.now(timezone.utc)
    ) is False


def test_is_schedule_stuck_naive_last_run():
    from bc_vigil.dedup.scheduler_utils import is_schedule_stuck
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    naive_old = datetime(2026, 4, 1, 12, 0, 0)
    assert is_schedule_stuck("0 3 * * *", naive_old, now=now) is True


def test_is_schedule_stuck_recent_is_not_stuck():
    from bc_vigil.dedup.scheduler_utils import is_schedule_stuck
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    recent = datetime(2026, 4, 18, 11, 30, 0, tzinfo=timezone.utc)
    assert is_schedule_stuck("0 3 * * *", recent, now=now) is False


# =========================================================================
# dedup.cron_builder
# =========================================================================


def test_cron_builder_every_modes():
    from bc_vigil.dedup.cron_builder import build_cron, next_occurrences
    r = build_cron("every_minutes", interval_minutes="15")
    assert r.cron == "*/15 * * * *"
    assert build_cron("hourly", minute_of_hour="5").cron == "5 * * * *"
    assert build_cron("daily", time="03:30").cron == "30 3 * * *"

    r = build_cron("weekly", time="04:00", days=["mon", "wed", "mon"])
    assert r.cron == "0 4 * * 1,3"
    r = build_cron("monthly", time="05:00", day_of_month="15")
    assert r.cron == "0 5 15 * *"
    r = build_cron("cron", cron_expr="*/10 * * * *")
    assert r.cron == "*/10 * * * *"

    assert len(next_occurrences(r.cron, 3)) == 3


def test_cron_builder_error_paths():
    from bc_vigil.dedup.cron_builder import build_cron, _parse_hhmm
    assert build_cron("unknown").error
    assert build_cron("every_minutes", interval_minutes="").error
    assert build_cron("every_minutes", interval_minutes="abc").error
    assert build_cron("every_minutes", interval_minutes="0").error
    assert build_cron("hourly", minute_of_hour="").error
    assert build_cron("daily", time="").error
    assert build_cron("daily", time="aa:bb").error
    assert build_cron("daily", time="25:00").error
    assert build_cron("daily", time="no-colon").error
    assert build_cron("weekly", time="03:00", days=[]).error
    assert build_cron("weekly", time="03:00", days=["xxx"]).error
    assert build_cron("monthly", time="03:00", day_of_month="").error
    assert build_cron("cron", cron_expr="").error
    assert build_cron("cron", cron_expr="not a cron").error
    assert build_cron("weekly", time="99:00", days=["mon"]).error
    assert build_cron("monthly", time="99:00", day_of_month="1").error
    msg, _ = _parse_hhmm("aa:bb")
    assert isinstance(msg, str)


# =========================================================================
# Routes: targets
# =========================================================================


def test_route_list_targets_empty():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/dedup/targets")
        assert r.status_code == 200


def test_route_new_target_form():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/dedup/targets/new")
        assert r.status_code == 200


def test_route_create_target_valid(tmp_path):
    folder = _make_tree(tmp_path, "new-target-tree")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={
                "name": "ok-target", "path": str(folder),
                "algorithm": "xxh3", "threads": "auto",
                "includes": "*.txt\n",
                "excludes": ".git\n",
                "minimum_size": "0",
                "include_hidden": "true",
                "follow_symlinks": "true",
                "match_hardlinks": "true",
                "one_file_system": "true",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == "/dedup/targets"


def test_route_create_target_invalid_name_whitespace():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": " ", "path": "/tmp",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "nom requis" in r.text


def test_route_create_target_invalid_algorithm():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "/tmp",
                  "algorithm": "md5", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "algo invalide" in r.text


def test_route_create_target_invalid_threads():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "/tmp",
                  "algorithm": "xxh3", "threads": "abc"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_create_target_path_whitespace():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "   ",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_create_target_path_relative():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "relative/path",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "absolu" in r.text


def test_route_create_target_path_not_exist():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "/no-such-path-xyz-12345",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_create_target_path_resolve_oserror(monkeypatch):
    import pathlib
    original = pathlib.Path.resolve

    def bad_resolve(self, strict=False):
        if "trigger-oserror-dedup" in str(self):
            raise OSError("simulated")
        return original(self, strict=strict)

    monkeypatch.setattr(pathlib.Path, "resolve", bad_resolve)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "x", "path": "/trigger-oserror-dedup/here",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code == 400
        assert "illisible" in r.text


def test_route_create_target_special_device():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    if not Path("/dev/null").exists():
        pytest.skip("no /dev/null")
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "spe", "path": "/dev/null",
                  "algorithm": "xxh3", "threads": "auto"},
            follow_redirects=False,
        )
        assert r.status_code in (303, 400)


def test_route_create_target_invalid_minimum_size(tmp_path):
    folder = _make_tree(tmp_path, "min-size")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/targets",
            data={"name": "min1", "path": str(folder),
                  "algorithm": "xxh3", "threads": "auto",
                  "minimum_size": "abc"},
            follow_redirects=False,
        )
        assert r.status_code == 400

        r = client.post(
            "/dedup/targets",
            data={"name": "min2", "path": str(folder),
                  "algorithm": "xxh3", "threads": "auto",
                  "minimum_size": "-5"},
            follow_redirects=False,
        )
        assert r.status_code == 400


def test_route_create_target_duplicate_name(tmp_path):
    folder = _make_tree(tmp_path, "dup-name-tree")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        payload = {
            "name": "dup", "path": str(folder),
            "algorithm": "xxh3", "threads": "auto",
        }
        client.post("/dedup/targets", data=payload, follow_redirects=False)
        r = client.post("/dedup/targets", data=payload, follow_redirects=False)
        assert r.status_code == 400
        assert "existe déjà" in r.text


def test_route_show_target(tmp_path):
    target_id = _make_target(tmp_path, "show-t")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
    _insert_scan(target_id, models.DEDUP_OK, trigger="scheduled")

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/targets/{target_id}")
        assert r.status_code == 200


def test_route_show_target_missing():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/dedup/targets/999999")
        assert r.status_code == 404


def test_route_duplicate_target(tmp_path):
    target_id = _make_target(tmp_path, "dup-src")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/targets/{target_id}/duplicate", follow_redirects=False,
        )
        assert r.status_code == 303
        r = client.post(
            f"/dedup/targets/{target_id}/duplicate", follow_redirects=False,
        )
        assert r.status_code == 303
        r = client.post(
            "/dedup/targets/999999/duplicate", follow_redirects=False,
        )
        assert r.status_code == 404


def test_route_delete_target(tmp_path):
    target_id = _make_target(tmp_path, "delete-t")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/targets/{target_id}/delete", follow_redirects=False,
        )
        assert r.status_code == 303
        r = client.post(
            "/dedup/targets/999999/delete", follow_redirects=False,
        )
        assert r.status_code == 404


# =========================================================================
# Routes: scans
# =========================================================================


def test_route_list_scans_with_filters(tmp_path):
    target_id = _make_target(tmp_path, "filter-t")
    from bc_vigil import models
    _insert_scan(target_id, models.DEDUP_PENDING)
    _insert_scan(target_id, models.DEDUP_OK)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans?status=ok&target_id={target_id}")
        assert r.status_code == 200
        r = client.get("/dedup/scans?status=unknown-status")
        assert r.status_code == 200


def test_route_run_scan(tmp_path):
    target_id = _make_target(tmp_path, "run-t")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/scans/run?target_id={target_id}", follow_redirects=False,
        )
        assert r.status_code == 303

        r = client.post(
            "/dedup/scans/run?target_id=999999", follow_redirects=False,
        )
        assert r.status_code == 404


def test_route_show_scan(tmp_path):
    target_id = _make_target(tmp_path, "show-s")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    scan_id = _insert_scan(target_id, models.DEDUP_DUPLICATES)
    with SessionLocal() as session:
        session.add(models.DedupGroup(
            scan_id=scan_id, size=100, file_count=2,
            paths_json=json.dumps(["/a", "/b"]),
        ))
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}")
        assert r.status_code == 200
        r = client.get("/dedup/scans/999999")
        assert r.status_code == 404


def test_route_groups_csv(tmp_path):
    target_id = _make_target(tmp_path, "csv-t")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    scan_id = _insert_scan(target_id, models.DEDUP_DUPLICATES)
    with SessionLocal() as session:
        session.add(models.DedupGroup(
            scan_id=scan_id, size=200, file_count=2,
            paths_json=json.dumps(["/x", "/y"]),
        ))
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/scans/{scan_id}/groups.csv")
        assert r.status_code == 200
        assert "/x" in r.text
        assert "/y" in r.text

        r = client.get("/dedup/scans/999999/groups.csv")
        assert r.status_code == 404


def test_route_acknowledge_scan(tmp_path):
    target_id = _make_target(tmp_path, "ack-t")
    from bc_vigil import models
    scan_id = _insert_scan(target_id, models.DEDUP_DUPLICATES)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/dedup/scans/{scan_id}/acknowledge",
                        follow_redirects=False)
        assert r.status_code == 303
        r = client.post("/dedup/scans/999999/acknowledge",
                        follow_redirects=False)
        assert r.status_code == 404


def test_route_acknowledge_all(tmp_path):
    target_id = _make_target(tmp_path, "ack-all")
    from bc_vigil import models
    _insert_scan(target_id, models.DEDUP_DUPLICATES)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/dedup/scans/acknowledge-all",
                        follow_redirects=False)
        assert r.status_code == 303
        r = client.post(
            f"/dedup/scans/acknowledge-all?target_id={target_id}",
            follow_redirects=False,
        )
        assert r.status_code == 303


def test_route_cancel_scan_finished_rejected(tmp_path):
    target_id = _make_target(tmp_path, "cancel-finished")
    from bc_vigil import models
    scan_id = _insert_scan(
        target_id, models.DEDUP_OK,
        finished_at=datetime.now(timezone.utc),
    )
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/dedup/scans/{scan_id}/cancel",
                        follow_redirects=False)
        assert r.status_code == 409


def test_route_cancel_scan_missing():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/dedup/scans/999999/cancel", follow_redirects=False)
        assert r.status_code == 404


def test_route_cancel_scan_running(tmp_path):
    sleep_fake = tmp_path / "bc-duplicate-slow"
    sleep_fake.write_text("#!/usr/bin/env bash\nexec sleep 30\n")
    sleep_fake.chmod(0o755)
    from bc_vigil.config import settings
    settings.bc_duplicate_binary = str(sleep_fake)

    target_id = _make_target(tmp_path, "cancel-running")
    from bc_vigil.dedup import scans
    from bc_vigil import models
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
                f"/dedup/scans/{scan_id}/cancel", follow_redirects=False,
            )
            assert r.status_code == 303
        runner.join(timeout=10)
        assert not runner.is_alive()
    finally:
        if runner.is_alive():
            scans.cancel_scan(scan_id, force=True)
            runner.join(timeout=5)

    with SessionLocal() as session:
        scan = session.get(models.DedupScan, scan_id)
        assert scan.status == models.DEDUP_CANCELLED


# =========================================================================
# Routes: schedules
# =========================================================================


def test_route_new_schedule_form(tmp_path):
    target_id = _make_target(tmp_path, "sched-new")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/schedules/new?target_id={target_id}")
        assert r.status_code == 200
        r = client.get("/dedup/schedules/new?target_id=999999")
        assert r.status_code == 404


def test_route_preview_schedule():
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/schedules/preview",
            data={"mode": "daily", "time": "03:00"},
        )
        assert r.status_code == 200
        assert "03:00" in r.text

        r = client.post(
            "/dedup/schedules/preview",
            data={"mode": "daily", "time": "99:00"},
        )
        assert r.status_code == 200
        assert "plage" in r.text or "format" in r.text


def test_route_create_schedule_valid_and_invalid(tmp_path):
    target_id = _make_target(tmp_path, "sched-crea")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/schedules",
            data={
                "target_id": str(target_id), "mode": "daily", "time": "03:00",
                "enabled": "true",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303

        r = client.post(
            "/dedup/schedules",
            data={
                "target_id": str(target_id), "mode": "cron",
                "cron_expr": "",
            },
            follow_redirects=False,
        )
        assert r.status_code == 400

        r = client.post(
            "/dedup/schedules",
            data={"target_id": "999999", "mode": "daily", "time": "03:00"},
            follow_redirects=False,
        )
        assert r.status_code == 404


def test_route_toggle_and_delete_schedule(tmp_path):
    target_id = _make_target(tmp_path, "sched-mg")
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        sched = models.DedupSchedule(
            target_id=target_id, cron="0 3 * * *", enabled=True,
        )
        session.add(sched)
        session.commit()
        sid = sched.id

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/dedup/schedules/{sid}/toggle",
                        follow_redirects=False)
        assert r.status_code == 303
        r = client.post("/dedup/schedules/999999/toggle",
                        follow_redirects=False)
        assert r.status_code == 404

        r = client.post(f"/dedup/schedules/{sid}/delete",
                        follow_redirects=False)
        assert r.status_code == 303
        r = client.post("/dedup/schedules/999999/delete",
                        follow_redirects=False)
        assert r.status_code == 404


# =========================================================================
# bcduplicate: _RssSampler sample helpers
# =========================================================================


def test_rss_sampler_missing_pid():
    from bc_vigil.dedup.bcduplicate import _RssSampler
    s = _RssSampler(99999999)
    s._sample_once()
    assert s._peak_kb == 0


def test_rss_sampler_parses_vmhwm(tmp_path, monkeypatch):
    from bc_vigil.dedup import bcduplicate

    class FakeOpen:
        def __init__(self, content: str) -> None:
            self._content = content

        def __call__(self, path: str):
            from io import StringIO
            return StringIO(self._content)

    monkeypatch.setattr(bcduplicate, "open", FakeOpen(
        "Name:\tfoo\nVmPeak:\t1024 kB\nVmHWM:\t4096 kB\n"
    ), raising=False)
    s = bcduplicate._RssSampler(1)
    s._sample_once()
    assert s._peak_kb == 4096


def test_rss_sampler_lifecycle(tmp_path):
    script = tmp_path / "sleepy"
    script.write_text("#!/usr/bin/env bash\nexec sleep 1\n")
    script.chmod(0o755)
    proc = subprocess.Popen([str(script)])
    try:
        from bc_vigil.dedup.bcduplicate import _RssSampler
        s = _RssSampler(proc.pid, interval=0.01)
        s.start()
        time.sleep(0.05)
        s.stop()
    finally:
        proc.wait(timeout=5)


# =========================================================================
# Edit target / schedule + retry failed deletion (0.5.6)
# =========================================================================


def _prepare_app(tmp_path, monkeypatch):
    monkeypatch.setenv("BC_VIGIL_DATA_DIR", str(tmp_path / "var"))
    from bc_vigil.config import settings
    settings.data_dir = tmp_path / "var"
    from bc_vigil.db import init_db
    init_db()


def test_edit_dedup_target_form_and_update(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models

    with TestClient(create_app()) as client:
        r = client.get(f"/dedup/targets/{target_id}/edit")
        assert r.status_code == 200
        assert "dedup-t" in r.text
        assert "readonly" in r.text

        r = client.post(
            f"/dedup/targets/{target_id}/update",
            data={
                "name": "renamed-dedup",
                "algorithm": "xxh3",
                "threads": "2",
                "includes": "*.mp4",
                "excludes": ".git",
                "minimum_size": "1024",
                "one_file_system": "true",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == f"/dedup/targets/{target_id}"

    with SessionLocal() as session:
        target = session.get(models.DedupTarget, target_id)
        assert target.name == "renamed-dedup"
        assert target.threads == "2"
        assert target.minimum_size == 1024
        assert target.one_file_system is True


def test_edit_dedup_target_missing(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.get("/dedup/targets/9999/edit")
        assert r.status_code == 404
        r = client.post(
            "/dedup/targets/9999/update",
            data={"name": "x", "algorithm": "xxh3", "threads": "auto"},
        )
        assert r.status_code == 404


def test_edit_dedup_target_rejects_duplicate_name(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path, name="first-dt")
    _make_target(tmp_path, name="second-dt")
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/targets/{target_id}/update",
            data={"name": "second-dt", "algorithm": "xxh3", "threads": "auto"},
        )
        assert r.status_code == 400


def test_edit_dedup_target_invalid_minsize(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/targets/{target_id}/update",
            data={
                "name": "dedup-t", "algorithm": "xxh3", "threads": "auto",
                "minimum_size": "not-a-number",
            },
        )
        assert r.status_code == 400


def test_edit_dedup_schedule_form_and_update(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models
    from sqlalchemy import select

    with TestClient(create_app()) as client:
        r = client.post(
            "/dedup/schedules",
            data={
                "target_id": str(target_id),
                "mode": "cron", "cron_expr": "0 3 * * *",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        with SessionLocal() as session:
            sched_id = session.scalars(select(models.DedupSchedule)).first().id

        r = client.get(f"/dedup/schedules/{sched_id}/edit")
        assert r.status_code == 200
        assert "0 3 * * *" in r.text

        r = client.post(
            f"/dedup/schedules/{sched_id}/update",
            data={"mode": "cron", "cron_expr": "30 4 * * *", "enabled": "true"},
            follow_redirects=False,
        )
        assert r.status_code == 303

    with SessionLocal() as session:
        sched = session.get(models.DedupSchedule, sched_id)
        assert sched.cron == "30 4 * * *"


def test_edit_dedup_schedule_missing_and_invalid(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models
    from sqlalchemy import select

    with TestClient(create_app()) as client:
        r = client.get("/dedup/schedules/9999/edit")
        assert r.status_code == 404
        r = client.post("/dedup/schedules/9999/update", data={"mode": "daily", "time": "03:00"})
        assert r.status_code == 404

        client.post(
            "/dedup/schedules",
            data={
                "target_id": str(target_id),
                "mode": "cron", "cron_expr": "0 3 * * *",
            },
            follow_redirects=False,
        )
        with SessionLocal() as session:
            sched_id = session.scalars(select(models.DedupSchedule)).first().id

        r = client.post(
            f"/dedup/schedules/{sched_id}/update",
            data={"mode": "cron", "cron_expr": "garbage"},
        )
        assert r.status_code == 400


def _make_failed_deletion(tmp_path: Path, target_id: int) -> int:
    """Create a scan with a duplicate pair, insert a failed DedupDeletion
    (no trash_path) for one of them, return the deletion id."""
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    import hashlib

    src_dir = tmp_path / f"dup-src-{target_id}"
    src_dir.mkdir()
    f1 = src_dir / "a.txt"
    f1.write_text("dup content")
    f2 = src_dir / "b.txt"
    f2.write_text("dup content")

    hash_hex = hashlib.sha256(b"dup content").hexdigest()
    with SessionLocal() as session:
        scan = models.DedupScan(
            target_id=target_id, status=models.DEDUP_DUPLICATES,
            trigger="manual",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        )
        session.add(scan)
        session.flush()
        group = models.DedupGroup(
            scan_id=scan.id, size=11, file_count=2,
            paths_json=json.dumps([str(f1), str(f2)]),
        )
        session.add(group)
        session.flush()
        deletion = models.DedupDeletion(
            scan_id=scan.id, group_id=group.id,
            original_path=str(f2),
            trash_path=None,
            size=11, hash_algo="sha256", hash_hex=hash_hex,
            stored_mode=models.STORED_MODE_COPY_UNLINK,
            status=models.DELETION_FAILED,
            error="simulated EROFS",
            triggered_by="test",
            deleted_at=datetime.now(timezone.utc),
        )
        session.add(deletion)
        session.commit()
        return deletion.id


def test_retry_failed_deletion_moves_file_to_trash(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    deletion_id = _make_failed_deletion(tmp_path, target_id)

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    from bc_vigil.db import SessionLocal
    from bc_vigil import models

    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/trash/{deletion_id}/retry", follow_redirects=False,
        )
        assert r.status_code == 303

    with SessionLocal() as session:
        old = session.get(models.DedupDeletion, deletion_id)
        assert old is None
        rows = session.query(models.DedupDeletion).all()
        assert len(rows) == 1
        assert rows[0].status == models.DELETION_QUARANTINED
        assert rows[0].trash_path is not None


def test_retry_failed_deletion_rejects_wrong_status(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    deletion_id = _make_failed_deletion(tmp_path, target_id)
    from bc_vigil.db import SessionLocal
    from bc_vigil import models
    with SessionLocal() as session:
        d = session.get(models.DedupDeletion, deletion_id)
        d.status = models.DELETION_QUARANTINED
        session.commit()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(f"/dedup/trash/{deletion_id}/retry")
        assert r.status_code == 400


def test_retry_failed_deletion_missing(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post("/dedup/trash/9999/retry")
        assert r.status_code == 400


def test_retry_failed_deletion_keeps_failed_row_if_source_gone(tmp_path, monkeypatch):
    _prepare_app(tmp_path, monkeypatch)
    target_id = _make_target(tmp_path)
    deletion_id = _make_failed_deletion(tmp_path, target_id)
    from bc_vigil import models
    from bc_vigil.db import SessionLocal
    with SessionLocal() as session:
        d = session.get(models.DedupDeletion, deletion_id)
        Path(d.original_path).unlink()

    from bc_vigil.app import create_app
    from fastapi.testclient import TestClient
    with TestClient(create_app()) as client:
        r = client.post(
            f"/dedup/trash/{deletion_id}/retry", follow_redirects=False,
        )
        assert r.status_code == 303

    with SessionLocal() as session:
        rows = session.query(models.DedupDeletion).all()
        assert len(rows) == 1
        assert rows[0].id == deletion_id
        assert rows[0].status == models.DELETION_FAILED
        assert "disappeared" in (rows[0].error or "")
