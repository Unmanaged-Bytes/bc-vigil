from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore, Lock

from sqlalchemy import insert

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import session_scope
from bc_vigil.dedup import bcduplicate

log = logging.getLogger(__name__)

_run_slots = BoundedSemaphore(settings.max_parallel_scans)
_cancel_handles: dict[int, bcduplicate.CancelHandle] = {}
_handles_lock = Lock()


class ScanAlreadyRunningError(RuntimeError):
    def __init__(self, scan_id: int):
        super().__init__(
            f"another scan is already pending/running for this target: "
            f"#{scan_id}"
        )
        self.active_scan_id = scan_id


def trigger_scan(target_id: int, trigger: str = "manual") -> int:
    with session_scope() as session:
        target = session.get(models.DedupTarget, target_id)
        if target is None:
            raise ValueError(f"dedup target {target_id} not found")
        active = session.query(models.DedupScan).filter(
            models.DedupScan.target_id == target.id,
            models.DedupScan.status.in_(
                [models.DEDUP_PENDING, models.DEDUP_RUNNING]
            ),
        ).first()
        if active is not None:
            raise ScanAlreadyRunningError(active.id)
        scan = models.DedupScan(
            target_id=target.id,
            trigger=trigger,
            status=models.DEDUP_PENDING,
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
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            log.warning("dedup scan %s disappeared before execution", scan_id)
            return
        scan.status = models.DEDUP_RUNNING
        scan.started_at = datetime.now(timezone.utc)
        target = scan.target
        target_id = target.id
        target_path = Path(target.path)
        algorithm = target.algorithm
        threads = target.threads
        includes = bcduplicate.parse_patterns(target.includes)
        excludes = bcduplicate.parse_patterns(target.excludes)
        minimum_size = target.minimum_size
        include_hidden = bool(target.include_hidden)
        follow_symlinks = bool(target.follow_symlinks)
        match_hardlinks = bool(target.match_hardlinks)
        one_file_system = bool(target.one_file_system)

    output_file = (
        settings.dedup_dir
        / f"target-{target_id}"
        / f"scan-{scan_id}.json"
    )

    handle = bcduplicate.CancelHandle()
    with _handles_lock:
        _cancel_handles[scan_id] = handle

    try:
        try:
            result = bcduplicate.run_scan(
                target_path,
                output_file,
                algorithm=algorithm,
                threads=threads,
                includes=includes,
                excludes=excludes,
                minimum_size=minimum_size,
                include_hidden=include_hidden,
                follow_symlinks=follow_symlinks,
                match_hardlinks=match_hardlinks,
                one_file_system=one_file_system,
                cancel=handle,
            )
        except bcduplicate.BcDuplicateCancelled:
            log.info("dedup scan %s cancelled by user", scan_id)
            _finalize_cancelled(scan_id)
            return
        except bcduplicate.BcDuplicateError as exc:
            log.exception("dedup scan %s failed", scan_id)
            _finalize_failure(scan_id, str(exc))
            return
        except Exception as exc:
            log.exception("unexpected error in dedup scan %s", scan_id)
            _finalize_failure(scan_id, f"unexpected: {exc}")
            return
    finally:
        with _handles_lock:
            _cancel_handles.pop(scan_id, None)

    with session_scope() as session:
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            return
        scan.output_path = str(output_file) if output_file.exists() else None
        scan.files_scanned = result.files_scanned
        scan.duplicate_groups = result.duplicate_groups
        scan.duplicate_files = result.duplicate_files
        scan.wasted_bytes = result.wasted_bytes
        scan.wall_ms = result.wall_ms
        scan.duration_ms = result.wall_ms
        scan.peak_rss_bytes = result.peak_rss_bytes
        scan.finished_at = datetime.now(timezone.utc)
        scan.status = (
            models.DEDUP_DUPLICATES if result.duplicate_groups > 0
            else models.DEDUP_OK
        )
        target = scan.target
        target.last_scan_id = scan.id

    _bulk_insert_groups(scan_id, result.groups)


_GROUP_INSERT_CHUNK = 2000


def _bulk_insert_groups(
    scan_id: int, groups: list[bcduplicate.DuplicateGroup],
) -> None:
    if not groups:
        return
    rows = [
        {
            "scan_id": scan_id,
            "size": int(g.size),
            "file_count": len(g.files),
            "paths_json": json.dumps(g.files, ensure_ascii=False),
        }
        for g in groups
    ]
    for start in range(0, len(rows), _GROUP_INSERT_CHUNK):
        chunk = rows[start:start + _GROUP_INSERT_CHUNK]
        with session_scope() as session:
            session.execute(insert(models.DedupGroup), chunk)


def _finalize_failure(scan_id: int, message: str) -> None:
    with session_scope() as session:
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            return
        scan.status = models.DEDUP_FAILED
        scan.error = message
        scan.finished_at = datetime.now(timezone.utc)


def _finalize_cancelled(scan_id: int) -> None:
    with session_scope() as session:
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            return
        scan.status = models.DEDUP_CANCELLED
        scan.finished_at = datetime.now(timezone.utc)


def cancel_scan(scan_id: int, force: bool = False) -> bool:
    with _handles_lock:
        handle = _cancel_handles.get(scan_id)
    if handle is None:
        return False
    return handle.cancel(force=force)


def parse_group_paths(paths_json: str) -> list[str]:
    try:
        data = json.loads(paths_json)
    except ValueError:
        return []
    if not isinstance(data, list):
        return []
    return [str(p) for p in data if isinstance(p, str)]
