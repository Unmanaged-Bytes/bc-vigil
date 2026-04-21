from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import stat as _stat
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import session_scope
from bc_vigil.dedup import scans as dedup_scans

log = logging.getLogger(__name__)


class QuarantineError(RuntimeError):
    pass


class BulkThresholdExceeded(QuarantineError):
    pass


AUTO_RULE_SHORTEST_PATH = "shortest_path"
AUTO_RULE_OLDEST_MTIME = "oldest_mtime"
AUTO_RULE_NEWEST_MTIME = "newest_mtime"
AUTO_RULE_PRIORITY_FOLDER = "priority_folder"

AUTO_RULES = (
    AUTO_RULE_SHORTEST_PATH,
    AUTO_RULE_OLDEST_MTIME,
    AUTO_RULE_NEWEST_MTIME,
    AUTO_RULE_PRIORITY_FOLDER,
)


def build_auto_selection(
    scan_id: int,
    rule: str,
    priority_path: str | None = None,
) -> dict[int, list[str]]:
    """
    For each group of a scan, apply the rule to pick a survivor and return
    the list of other paths to quarantine. Returns a mapping
    group_id -> [paths_to_delete].
    """
    if rule not in AUTO_RULES:
        raise QuarantineError(f"unknown auto rule: {rule!r}")
    if rule == AUTO_RULE_PRIORITY_FOLDER and not priority_path:
        raise QuarantineError("priority_path is required for priority_folder")

    selection: dict[int, list[str]] = {}
    with session_scope() as session:
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            raise QuarantineError(f"scan {scan_id} not found")
        if scan.status != models.DEDUP_DUPLICATES:
            raise QuarantineError(
                f"scan {scan_id} status is {scan.status}, not duplicates"
            )

        for group in scan.groups:
            paths = dedup_scans.parse_group_paths(group.paths_json)
            if len(paths) < 2:
                continue
            survivor = _pick_survivor(paths, rule, priority_path)
            if survivor is None:
                continue
            victims = [p for p in paths if p != survivor]
            if victims:
                selection[group.id] = victims
    return selection


def _pick_survivor(
    paths: list[str], rule: str, priority_path: str | None,
) -> str | None:
    if rule == AUTO_RULE_SHORTEST_PATH:
        return min(paths, key=lambda p: (len(p), p))
    if rule == AUTO_RULE_OLDEST_MTIME:
        return _pick_by_mtime(paths, newest=False)
    if rule == AUTO_RULE_NEWEST_MTIME:
        return _pick_by_mtime(paths, newest=True)
    assert rule == AUTO_RULE_PRIORITY_FOLDER
    assert priority_path is not None
    pref = priority_path.rstrip("/") + "/"
    under = [p for p in paths if p.startswith(pref)]
    if under:
        return min(under, key=lambda p: (len(p), p))
    return None


def _pick_by_mtime(paths: list[str], newest: bool) -> str | None:
    scored: list[tuple[float, str]] = []
    for p in paths:
        try:
            mtime = Path(p).stat().st_mtime
        except OSError:
            continue
        scored.append((mtime, p))
    if not scored:
        return None
    scored.sort(reverse=newest)
    return scored[0][1]


@dataclass
class PlannedItem:
    group_id: int
    original_path: str
    size: int
    stored_mode: str
    trash_path: str
    hash_algo: str
    hash_hex: str


@dataclass
class SkippedItem:
    group_id: int
    original_path: str
    reason: str


@dataclass
class DeletionPlan:
    scan_id: int
    items: list[PlannedItem] = field(default_factory=list)
    skipped: list[SkippedItem] = field(default_factory=list)
    total_size: int = 0
    cross_fs_count: int = 0
    requires_bulk_opt_in: bool = False


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _trash_root() -> Path:
    return settings.dedup_trash_dir_resolved


def _build_trash_path(scan_id: int, group_id: int, source: Path) -> Path:
    sub = uuid.uuid4().hex
    return (
        _trash_root()
        / f"scan-{scan_id}"
        / f"group-{group_id}"
        / sub
        / source.name
    )


def _same_device(a: Path, b: Path) -> bool:
    return os.stat(a).st_dev == os.stat(b).st_dev


def plan_deletion(
    scan_id: int,
    selection: dict[int, list[str]],
    bulk_opt_in: bool = False,
) -> DeletionPlan:
    plan = DeletionPlan(scan_id=scan_id)
    _trash_root().mkdir(parents=True, exist_ok=True)

    with session_scope() as session:
        scan = session.get(models.DedupScan, scan_id)
        if scan is None:
            raise QuarantineError(f"scan {scan_id} not found")
        if scan.status != models.DEDUP_DUPLICATES:
            raise QuarantineError(
                f"scan {scan_id} status is {scan.status}, not duplicates"
            )

        groups_by_id = {g.id: g for g in scan.groups}

        for group_id, paths_to_delete in selection.items():
            group = groups_by_id.get(group_id)
            if group is None:
                for p in paths_to_delete:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=p,
                        reason="group not found in scan",
                    ))
                continue

            all_paths = dedup_scans.parse_group_paths(group.paths_json)
            remaining = [p for p in all_paths if p not in paths_to_delete]
            if not remaining:
                for p in paths_to_delete:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=p,
                        reason="must leave at least one survivor in group",
                    ))
                continue

            survivor = Path(remaining[0])
            try:
                survivor_stat = os.lstat(survivor)
            except OSError as exc:
                for p in paths_to_delete:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=p,
                        reason=f"survivor missing: {exc}",
                    ))
                continue
            if _stat.S_ISLNK(survivor_stat.st_mode):
                for p in paths_to_delete:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=p,
                        reason="survivor is a symlink",
                    ))
                continue
            try:
                survivor_hash = sha256_file(survivor)
            except OSError as exc:
                for p in paths_to_delete:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=p,
                        reason=f"cannot read survivor: {exc}",
                    ))
                continue

            trash_root = _trash_root()
            for raw_path in paths_to_delete:
                victim = Path(raw_path)
                reason = _validate_victim(
                    victim, survivor, survivor_stat, group.size,
                )
                if reason:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=raw_path,
                        reason=reason,
                    ))
                    continue
                try:
                    victim_hash = sha256_file(victim)
                except OSError as exc:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=raw_path,
                        reason=f"cannot read victim: {exc}",
                    ))
                    continue
                if victim_hash != survivor_hash:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=raw_path,
                        reason="hash mismatch vs survivor",
                    ))
                    continue

                try:
                    stored_mode = (
                        models.STORED_MODE_RENAME
                        if _same_device(victim, trash_root)
                        else models.STORED_MODE_COPY_UNLINK
                    )
                except OSError as exc:
                    plan.skipped.append(SkippedItem(
                        group_id=group_id,
                        original_path=raw_path,
                        reason=f"cannot stat for fs detection: {exc}",
                    ))
                    continue
                if stored_mode == models.STORED_MODE_COPY_UNLINK:
                    plan.cross_fs_count += 1

                trash_path = _build_trash_path(scan_id, group_id, victim)
                plan.items.append(PlannedItem(
                    group_id=group_id,
                    original_path=str(victim),
                    size=victim.stat().st_size,
                    stored_mode=stored_mode,
                    trash_path=str(trash_path),
                    hash_algo="sha256",
                    hash_hex=victim_hash,
                ))
                plan.total_size += victim.stat().st_size

    if len(plan.items) > settings.dedup_deletion_bulk_threshold:
        plan.requires_bulk_opt_in = True
        if not bulk_opt_in:
            raise BulkThresholdExceeded(
                f"selection exceeds threshold of "
                f"{settings.dedup_deletion_bulk_threshold} files; "
                f"confirmation required"
            )

    return plan


def _validate_victim(
    victim: Path,
    survivor: Path,
    survivor_stat: os.stat_result,
    group_size: int,
) -> str | None:
    try:
        victim_stat = os.lstat(victim)
    except FileNotFoundError:
        return "file no longer exists"
    except OSError as exc:
        return f"stat failed: {exc}"
    if _stat.S_ISLNK(victim_stat.st_mode):
        return "path is a symlink"
    if not _stat.S_ISREG(victim_stat.st_mode):
        return "path is not a regular file"
    if (
        victim_stat.st_dev == survivor_stat.st_dev
        and victim_stat.st_ino == survivor_stat.st_ino
    ):
        return "same inode as survivor (hardlink, nothing to free)"
    if victim_stat.st_size != group_size:
        return "size no longer matches scan"
    if victim == survivor:
        return "cannot delete survivor"
    return None


def execute_deletion(
    plan: DeletionPlan, triggered_by: str | None = None,
) -> list[int]:
    executed_ids: list[int] = []
    for item in plan.items:
        deletion_id = _execute_one(plan.scan_id, item, triggered_by)
        if deletion_id is not None:
            executed_ids.append(deletion_id)
    return executed_ids


def _execute_one(
    scan_id: int, item: PlannedItem, triggered_by: str | None,
) -> int | None:
    source = Path(item.original_path)
    dest = Path(item.trash_path)
    moved = False
    failure_reason: str | None = None
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            dest.parent.chmod(0o700)
        except OSError:
            pass
        if not source.exists() or source.is_symlink():
            failure_reason = "source disappeared or became symlink"
        else:
            try:
                current_hash = sha256_file(source)
            except OSError as exc:
                failure_reason = f"cannot re-read source: {exc}"
            else:
                if current_hash != item.hash_hex:
                    failure_reason = "source changed since plan"
                else:
                    try:
                        if item.stored_mode == models.STORED_MODE_RENAME:
                            os.rename(source, dest)
                        else:
                            shutil.copy2(source, dest)
                            with dest.open("rb") as fh:
                                os.fsync(fh.fileno())
                            os.unlink(source)
                        moved = True
                    except OSError as exc:
                        failure_reason = f"move failed: {exc}"
    except OSError as exc:
        failure_reason = f"setup failed: {exc}"

    with session_scope() as session:
        if moved:
            deletion = models.DedupDeletion(
                scan_id=scan_id,
                group_id=item.group_id,
                original_path=item.original_path,
                trash_path=item.trash_path,
                size=item.size,
                hash_algo=item.hash_algo,
                hash_hex=item.hash_hex,
                stored_mode=item.stored_mode,
                status=models.DELETION_QUARANTINED,
                triggered_by=triggered_by,
            )
            session.add(deletion)
            session.flush()
            _write_meta(dest, deletion)
            return deletion.id

        deletion = models.DedupDeletion(
            scan_id=scan_id,
            group_id=item.group_id,
            original_path=item.original_path,
            trash_path=None,
            size=item.size,
            hash_algo=item.hash_algo,
            hash_hex=item.hash_hex,
            stored_mode=item.stored_mode,
            status=models.DELETION_FAILED,
            triggered_by=triggered_by,
            error=failure_reason,
        )
        session.add(deletion)
        session.flush()
        return None


def _write_meta(dest: Path, deletion: models.DedupDeletion) -> None:
    try:
        st = os.stat(dest)
    except OSError:
        return
    meta = {
        "deletion_id": deletion.id,
        "scan_id": deletion.scan_id,
        "group_id": deletion.group_id,
        "original_path": deletion.original_path,
        "size": deletion.size,
        "mode": st.st_mode,
        "mtime": st.st_mtime,
        "uid": st.st_uid,
        "gid": st.st_gid,
        "hash_algo": deletion.hash_algo,
        "hash_hex": deletion.hash_hex,
        "stored_mode": deletion.stored_mode,
    }
    try:
        (dest.parent / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2)
        )
    except OSError:
        pass


def restore(deletion_id: int) -> None:
    with session_scope() as session:
        d = session.get(models.DedupDeletion, deletion_id)
        if d is None:
            raise QuarantineError(f"deletion {deletion_id} not found")
        if d.status != models.DELETION_QUARANTINED:
            raise QuarantineError(
                f"deletion {deletion_id} status is {d.status}, "
                "cannot restore"
            )
        if d.trash_path is None:
            raise QuarantineError("no trash path recorded")
        trash = Path(d.trash_path)
        target = Path(d.original_path)
        if not trash.exists():
            raise QuarantineError(f"trash file missing: {trash}")
        if target.exists():
            raise QuarantineError(
                f"target path already exists: {target}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            same_fs = _same_device(trash, target.parent)
        except OSError:
            same_fs = False
        if same_fs:
            os.rename(trash, target)
        else:
            shutil.copy2(trash, target)
            with target.open("rb") as fh:
                os.fsync(fh.fileno())
            os.unlink(trash)
        _cleanup_trash_parent(trash)
        d.status = models.DELETION_RESTORED
        d.restored_at = datetime.now(timezone.utc)


def retry(deletion_id: int) -> int | None:
    """Retry a previously-failed deletion. Returns the id of the new
    DedupDeletion row (quarantined) on success, or None on failure (the
    original failed row is kept with an updated error message)."""
    with session_scope() as session:
        original = session.get(models.DedupDeletion, deletion_id)
        if original is None:
            raise QuarantineError(f"deletion {deletion_id} not found")
        if original.status != models.DELETION_FAILED:
            raise QuarantineError(
                f"deletion {deletion_id} status is {original.status}, "
                f"retry only allowed from {models.DELETION_FAILED}"
            )
        source = Path(original.original_path)
        new_trash = _build_trash_path(
            original.scan_id, original.group_id, source,
        )
        item = PlannedItem(
            group_id=original.group_id,
            original_path=original.original_path,
            size=original.size,
            stored_mode=original.stored_mode,
            trash_path=str(new_trash),
            hash_algo=original.hash_algo,
            hash_hex=original.hash_hex,
        )
        scan_id = original.scan_id
        triggered_by = original.triggered_by

    _trash_root().mkdir(parents=True, exist_ok=True)
    new_id = _execute_one(scan_id, item, triggered_by)

    with session_scope() as session:
        if new_id is not None:
            old = session.get(models.DedupDeletion, deletion_id)
            if old is not None:
                session.delete(old)
            return new_id
        # _execute_one returned None: it inserted a fresh FAILED row for
        # this attempt. Fold its error message into the original row and
        # drop the duplicate so the trash view stays clean.
        new_failed = session.query(models.DedupDeletion).filter(
            models.DedupDeletion.scan_id == scan_id,
            models.DedupDeletion.original_path == item.original_path,
            models.DedupDeletion.status == models.DELETION_FAILED,
            models.DedupDeletion.id != deletion_id,
        ).order_by(models.DedupDeletion.id.desc()).first()
        if new_failed is not None:
            old = session.get(models.DedupDeletion, deletion_id)
            if old is not None:
                old.error = new_failed.error
                old.deleted_at = new_failed.deleted_at
            session.delete(new_failed)
        return None


def purge_one(deletion_id: int) -> None:
    with session_scope() as session:
        d = session.get(models.DedupDeletion, deletion_id)
        if d is None:
            raise QuarantineError(f"deletion {deletion_id} not found")
        if d.status != models.DELETION_QUARANTINED:
            raise QuarantineError(
                f"deletion {deletion_id} status is {d.status}, "
                "cannot purge"
            )
        _purge_record(d)


def purge_expired() -> int:
    retention = settings.dedup_trash_retention_days
    if retention <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention)
    removed = 0
    with session_scope() as session:
        expired = session.query(models.DedupDeletion).filter(
            models.DedupDeletion.status == models.DELETION_QUARANTINED,
            models.DedupDeletion.deleted_at < cutoff,
        ).all()
        for d in expired:
            _purge_record(d)
            removed += 1
    return removed


def _purge_record(d: models.DedupDeletion) -> None:
    if d.trash_path:
        trash = Path(d.trash_path)
        try:
            if trash.exists():
                trash.unlink()
        except OSError:
            pass
        _cleanup_trash_parent(trash)
    d.status = models.DELETION_PURGED
    d.purged_at = datetime.now(timezone.utc)


def _cleanup_trash_parent(trash: Path) -> None:
    parent = trash.parent
    meta = parent / "meta.json"
    try:
        meta.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        return
    try:
        parent.rmdir()
    except OSError:
        pass
