from __future__ import annotations

import io
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import SessionLocal


class AdminError(RuntimeError):
    pass


DB_FILENAME = "bc-vigil.sqlite"
DIGESTS_DIRNAME = "digests"
DEDUP_DIRNAME = "dedup"


def has_active_scans() -> int:
    with SessionLocal() as session:
        integrity_active = session.scalar(
            select(func.count()).select_from(models.Scan).where(
                models.Scan.status.in_([models.SCAN_PENDING, models.SCAN_RUNNING])
            )
        ) or 0
        dedup_active = session.scalar(
            select(func.count()).select_from(models.DedupScan).where(
                models.DedupScan.status.in_(
                    [models.DEDUP_PENDING, models.DEDUP_RUNNING]
                )
            )
        ) or 0
        return int(integrity_active) + int(dedup_active)


def _checkpoint_wal() -> None:
    from bc_vigil.db import engine
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass


def build_backup_archive() -> bytes:
    buf = io.BytesIO()
    db_path = settings.data_dir / DB_FILENAME
    digests_dir = settings.digests_dir
    dedup_dir = settings.dedup_dir

    _checkpoint_wal()

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        if db_path.exists():
            tar.add(db_path, arcname=DB_FILENAME)
        if digests_dir.exists():
            tar.add(digests_dir, arcname=DIGESTS_DIRNAME)
        if dedup_dir.exists():
            tar.add(dedup_dir, arcname=DEDUP_DIRNAME)
    return buf.getvalue()


def backup_filename() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    return f"bc-vigil-backup-{ts}.tar.gz"


def snapshot_to_dir(dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = backup_filename()
    snapshot_path = dest_dir / filename
    snapshot_path.write_bytes(build_backup_archive())
    return snapshot_path


def reset_database() -> Path:
    from bc_vigil import db as db_module

    if has_active_scans():
        raise AdminError(
            "des scans sont en cours ou en attente — annulez-les avant de réinitialiser"
        )

    snapshot = snapshot_to_dir(settings.data_dir / "snapshots")

    from bc_vigil.integrity import scheduler
    from bc_vigil.dedup import scheduler as dedup_scheduler
    scheduler.shutdown()
    dedup_scheduler.shutdown()

    db_path = settings.data_dir / DB_FILENAME
    if db_path.exists():
        db_path.unlink()
    for sibling in (f"{DB_FILENAME}-journal", f"{DB_FILENAME}-wal", f"{DB_FILENAME}-shm"):
        p = settings.data_dir / sibling
        if p.exists():
            p.unlink()

    digests_dir = settings.digests_dir
    if digests_dir.exists():
        shutil.rmtree(digests_dir)
    dedup_dir = settings.dedup_dir
    if dedup_dir.exists():
        shutil.rmtree(dedup_dir)
    trash_dir = settings.dedup_trash_dir_resolved
    if trash_dir.exists() and trash_dir != dedup_dir:
        shutil.rmtree(trash_dir, ignore_errors=True)

    db_module.reset_engine()
    db_module.init_db()
    scheduler.start()
    dedup_scheduler.start()
    return snapshot


def restore_from_archive(archive_bytes: bytes) -> Path:
    if has_active_scans():
        raise AdminError(
            "des scans sont en cours ou en attente — annulez-les avant de restaurer"
        )

    with tempfile.TemporaryDirectory(prefix="bc-vigil-restore-") as tmp:
        tmp_path = Path(tmp)
        try:
            with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
                _safe_extract(tar, tmp_path)
        except tarfile.TarError as exc:
            raise AdminError(f"archive invalide: {exc}") from exc

        extracted_db = tmp_path / DB_FILENAME
        if not extracted_db.exists():
            raise AdminError(
                f"archive invalide: {DB_FILENAME} absent de l'archive"
            )

        snapshot = snapshot_to_dir(settings.data_dir / "snapshots")

        from bc_vigil import db as db_module
        from bc_vigil.integrity import scheduler
        from bc_vigil.dedup import scheduler as dedup_scheduler
        scheduler.shutdown()
        dedup_scheduler.shutdown()

        db_path = settings.data_dir / DB_FILENAME
        if db_path.exists():
            db_path.unlink()
        for sibling in (f"{DB_FILENAME}-journal", f"{DB_FILENAME}-wal", f"{DB_FILENAME}-shm"):
            p = settings.data_dir / sibling
            if p.exists():
                p.unlink()

        digests_dir = settings.digests_dir
        if digests_dir.exists():
            shutil.rmtree(digests_dir)
        dedup_dir = settings.dedup_dir
        if dedup_dir.exists():
            shutil.rmtree(dedup_dir)
        trash_dir = settings.dedup_trash_dir_resolved
        if trash_dir.exists() and trash_dir != dedup_dir:
            shutil.rmtree(trash_dir, ignore_errors=True)

        shutil.copy2(extracted_db, db_path)
        extracted_digests = tmp_path / DIGESTS_DIRNAME
        if extracted_digests.exists():
            shutil.copytree(extracted_digests, digests_dir)
        else:
            digests_dir.mkdir(parents=True, exist_ok=True)
        extracted_dedup = tmp_path / DEDUP_DIRNAME
        if extracted_dedup.exists():
            shutil.copytree(extracted_dedup, dedup_dir)
        else:
            dedup_dir.mkdir(parents=True, exist_ok=True)

        db_module.reset_engine()
        db_module.init_db()
        scheduler.start()
        dedup_scheduler.start()
        return snapshot


def _safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    dest_resolved = dest.resolve()
    for member in tar.getmembers():
        target = (dest / member.name).resolve()
        if not str(target).startswith(str(dest_resolved)):
            raise AdminError(
                f"chemin d'archive douteux: {member.name}"
            )
    tar.extractall(dest, filter="data")
