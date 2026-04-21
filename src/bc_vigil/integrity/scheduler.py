from __future__ import annotations

import logging
import os

from datetime import datetime, timedelta, timezone

import psutil
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from croniter import croniter
from sqlalchemy import delete as sa_delete
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import SessionLocal, session_scope
from bc_vigil.integrity import scans
from bc_vigil.integrity.cron_builder import display_tz


PURGE_JOB_ID = "bc-vigil-purge"
VACUUM_JOB_ID = "bc-vigil-vacuum"

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _job_id(schedule_id: int) -> str:
    return f"schedule-{schedule_id}"


def validate_cron(expr: str) -> None:
    if not croniter.is_valid(expr):
        raise ValueError(f"invalid cron expression: {expr!r}")


def _run_scheduled_scan(schedule_id: int) -> None:
    with session_scope() as session:
        schedule = session.get(models.Schedule, schedule_id)
        if schedule is None or not schedule.enabled:
            return
        target_id = schedule.target_id

    try:
        scan_id = scans.trigger_scan(target_id, trigger="scheduled")
    except Exception:
        log.exception("failed to trigger scheduled scan for schedule %s", schedule_id)
        return

    try:
        scans.execute_scan(scan_id)
    except Exception:
        log.exception("scheduled scan %s failed", scan_id)


def _run_manual_scan(scan_id: int) -> None:
    try:
        scans.execute_scan(scan_id)
    except Exception:
        log.exception("manual scan %s failed", scan_id)


def start() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    cleaned = _cleanup_stale_scans()
    if cleaned:
        log.info(
            "cleaned up %d stale integrity scan(s) "
            "from previous service start", cleaned,
        )
    _scheduler = BackgroundScheduler(timezone=display_tz())
    _scheduler.start()
    _reload_jobs()
    _install_purge_job()
    _install_vacuum_job()
    return _scheduler


def _install_vacuum_job() -> None:
    scheduler().add_job(
        vacuum_db,
        trigger=CronTrigger.from_crontab("0 4 1 * *", timezone=display_tz()),
        id=VACUUM_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )


def vacuum_db() -> None:
    from sqlalchemy import text
    try:
        with SessionLocal() as session:
            session.execute(text("VACUUM"))
            session.commit()
        log.info("sqlite VACUUM completed")
    except Exception:
        log.exception("sqlite VACUUM failed")


def _cleanup_stale_scans() -> int:
    """Mark scans left in pending/running by a previous service death as
    failed, so the UI does not show them as perpetually in-progress.
    Only scans older than the current process start time are considered
    stale — scans initiated by this process are left untouched."""
    proc_start = datetime.fromtimestamp(
        psutil.Process(os.getpid()).create_time(), tz=timezone.utc,
    )
    with session_scope() as session:
        stale = session.query(models.Scan).filter(
            models.Scan.status.in_([models.SCAN_PENDING, models.SCAN_RUNNING]),
            models.Scan.started_at < proc_start,
        ).all()
        now = datetime.now(timezone.utc)
        for scan in stale:
            scan.status = models.SCAN_FAILED
            scan.finished_at = now
            scan.error = "interrompu par un redémarrage du service"
        return len(stale)


def _install_purge_job() -> None:
    if settings.scan_retention_days <= 0:
        return
    scheduler().add_job(
        purge_old_scans,
        trigger=IntervalTrigger(hours=24),
        id=PURGE_JOB_ID,
        replace_existing=True,
        max_instances=1,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=5),
    )


def purge_old_scans() -> int:
    if settings.scan_retention_days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.scan_retention_days)
    from pathlib import Path
    with session_scope() as session:
        old = session.query(models.Scan).filter(
            models.Scan.started_at < cutoff,
            models.Scan.status.notin_([models.SCAN_PENDING, models.SCAN_RUNNING]),
        ).all()
        protected_ids = {
            t.baseline_scan_id for t in session.query(models.Target).all()
            if t.baseline_scan_id is not None
        }
        removed = 0
        for scan in old:
            if scan.id in protected_ids:
                continue
            if scan.digest_path:
                try:
                    Path(scan.digest_path).unlink(missing_ok=True)
                except OSError:
                    pass
            session.delete(scan)
            removed += 1
        return removed


def shutdown() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def scheduler() -> BackgroundScheduler:
    if _scheduler is None:
        raise RuntimeError("scheduler not started")
    return _scheduler


def _reload_jobs() -> None:
    with SessionLocal() as session:
        schedules = session.query(models.Schedule).filter_by(enabled=True).all()
        for schedule in schedules:
            _upsert_job(schedule)


def _upsert_job(schedule: models.Schedule) -> None:
    scheduler().add_job(
        _run_scheduled_scan,
        trigger=CronTrigger.from_crontab(schedule.cron, timezone=display_tz()),
        args=[schedule.id],
        id=_job_id(schedule.id),
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )


def sync_schedule(session: Session, schedule_id: int) -> None:
    schedule = session.get(models.Schedule, schedule_id)
    if schedule is None:
        remove_schedule(schedule_id)
        return
    if not schedule.enabled:
        remove_schedule(schedule_id)
        return
    _upsert_job(schedule)


def remove_schedule(schedule_id: int) -> None:
    job_id = _job_id(schedule_id)
    try:
        scheduler().remove_job(job_id)
    except Exception:
        pass


def run_scan_async(scan_id: int) -> None:
    scheduler().add_job(
        _run_manual_scan,
        args=[scan_id],
        id=f"scan-{scan_id}",
        replace_existing=True,
        max_instances=1,
    )
