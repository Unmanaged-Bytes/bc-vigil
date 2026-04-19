from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore, Lock

from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import session_scope
from bc_vigil.integrity import bchash

log = logging.getLogger(__name__)

_run_slots = BoundedSemaphore(settings.max_parallel_scans)
_cancel_handles: dict[int, bchash.CancelHandle] = {}
_handles_lock = Lock()


def trigger_scan(target_id: int, trigger: str = "manual") -> int:
    with session_scope() as session:
        target = session.get(models.Target, target_id)
        if target is None:
            raise ValueError(f"target {target_id} not found")
        scan = models.Scan(
            target_id=target.id,
            trigger=trigger,
            status=models.SCAN_PENDING,
        )
        session.add(scan)
        session.flush()
        return scan.id


def execute_scan(scan_id: int) -> None:
    acquired = _run_slots.acquire(blocking=True)
    try:
        _execute_locked(scan_id)
    finally:
        if acquired:
            _run_slots.release()


def _execute_locked(scan_id: int) -> None:
    with session_scope() as session:
        scan = session.get(models.Scan, scan_id)
        if scan is None:
            log.warning("scan %s disappeared before execution", scan_id)
            return
        scan.status = models.SCAN_RUNNING
        scan.started_at = datetime.now(timezone.utc)
        target = scan.target
        target_id = target.id
        target_path = Path(target.path)
        algorithm = target.algorithm
        threads = target.threads
        includes = bchash.parse_patterns(target.includes)
        excludes = bchash.parse_patterns(target.excludes)
        baseline = target.baseline_scan
        baseline_has_entries = bool(baseline and baseline.files_total)
        baseline_digest = (
            Path(baseline.digest_path)
            if baseline_has_entries and baseline.digest_path
            else None
        )

    digest_file = (
        settings.digests_dir
        / f"target-{target_id}"
        / f"scan-{scan_id}.ndjson"
    )

    handle = bchash.CancelHandle()
    with _handles_lock:
        _cancel_handles[scan_id] = handle

    try:
        try:
            result = bchash.run_hash(
                target_path, digest_file, algorithm, threads,
                includes=includes, excludes=excludes, cancel=handle,
            )
        except bchash.BcHashCancelled:
            log.info("scan %s cancelled by user", scan_id)
            _finalize_cancelled(scan_id)
            return
        except bchash.BcHashError as exc:
            log.exception("hash failed for scan %s", scan_id)
            _finalize_failure(scan_id, str(exc))
            return
        except Exception as exc:
            log.exception("unexpected error hashing scan %s", scan_id)
            _finalize_failure(scan_id, f"unexpected: {exc}")
            return
    finally:
        with _handles_lock:
            _cancel_handles.pop(scan_id, None)

    events: list[bchash.DiffEvent] = []
    diff_error: str | None = None
    if baseline_digest is not None and baseline_digest.exists():
        try:
            diff = bchash.run_diff(baseline_digest, digest_file)
            events = diff.events
        except bchash.BcHashError as exc:
            log.exception("diff failed for scan %s", scan_id)
            diff_error = str(exc)

    with session_scope() as session:
        scan = session.get(models.Scan, scan_id)
        if scan is None:
            return
        scan.digest_path = str(result.digest_path) if result.digest_path else None
        scan.files_total = result.files_total
        scan.bytes_total = result.bytes_total
        scan.peak_rss_bytes = result.peak_rss_bytes
        scan.duration_ms = result.wall_ms
        scan.finished_at = datetime.now(timezone.utc)
        if diff_error is not None:
            scan.status = models.SCAN_FAILED
            scan.error = diff_error
        elif events:
            scan.status = models.SCAN_DRIFT
        else:
            scan.status = models.SCAN_OK

        for ev in events:
            session.add(models.IntegrityEvent(
                scan_id=scan.id,
                event_type=ev.event_type,
                path=ev.path,
                old_digest=ev.old_digest,
                new_digest=ev.new_digest,
            ))

        target = scan.target
        current_baseline = target.baseline_scan
        baseline_usable = bool(
            current_baseline
            and current_baseline.digest_path
            and current_baseline.files_total
        )
        if (
            scan.status in (models.SCAN_OK, models.SCAN_DRIFT)
            and scan.digest_path is not None
            and not baseline_usable
        ):
            target.baseline_scan_id = scan.id


def _finalize_failure(scan_id: int, message: str) -> None:
    with session_scope() as session:
        scan = session.get(models.Scan, scan_id)
        if scan is None:
            return
        scan.status = models.SCAN_FAILED
        scan.error = message
        scan.finished_at = datetime.now(timezone.utc)


def _finalize_cancelled(scan_id: int) -> None:
    with session_scope() as session:
        scan = session.get(models.Scan, scan_id)
        if scan is None:
            return
        scan.status = models.SCAN_CANCELLED
        scan.finished_at = datetime.now(timezone.utc)


def cancel_scan(scan_id: int, force: bool = False) -> bool:
    with _handles_lock:
        handle = _cancel_handles.get(scan_id)
    if handle is None:
        return False
    return handle.cancel(force=force)


def promote_baseline(session: Session, scan_id: int) -> None:
    scan = session.get(models.Scan, scan_id)
    if scan is None:
        raise ValueError(f"scan {scan_id} not found")
    if scan.status not in (models.SCAN_OK, models.SCAN_DRIFT):
        raise ValueError("cannot promote a failed/running scan as baseline")
    scan.target.baseline_scan_id = scan.id
