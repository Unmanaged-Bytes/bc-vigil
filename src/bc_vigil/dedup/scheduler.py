from __future__ import annotations

import logging

from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from croniter import croniter
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.db import SessionLocal, session_scope
from bc_vigil.dedup import quarantine, scans


PURGE_JOB_ID = "bc-vigil-dedup-purge"
TRASH_PURGE_JOB_ID = "bc-vigil-dedup-trash-purge"

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _job_id(schedule_id: int) -> str:
    return f"dedup-schedule-{schedule_id}"


def validate_cron(expr: str) -> None:
    if not croniter.is_valid(expr):
        raise ValueError(f"invalid cron expression: {expr!r}")


def _run_scheduled_scan(schedule_id: int) -> None:
    with session_scope() as session:
        schedule = session.get(models.DedupSchedule, schedule_id)
        if schedule is None or not schedule.enabled:
            return
        target_id = schedule.target_id

    try:
        scan_id = scans.trigger_scan(target_id, trigger="scheduled")
    except Exception:
        log.exception(
            "failed to trigger scheduled dedup scan for schedule %s", schedule_id,
        )
        return

    try:
        scans.execute_scan(scan_id)
    except Exception:
        log.exception("scheduled dedup scan %s failed", scan_id)


def _run_manual_scan(scan_id: int) -> None:
    try:
        scans.execute_scan(scan_id)
    except Exception:
        log.exception("manual dedup scan %s failed", scan_id)


def start() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.start()
    _reload_jobs()
    _install_purge_job()
    _install_trash_purge_job()
    return _scheduler


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


def _install_trash_purge_job() -> None:
    if settings.dedup_trash_retention_days <= 0:
        return
    scheduler().add_job(
        _purge_trash,
        trigger=IntervalTrigger(hours=24),
        id=TRASH_PURGE_JOB_ID,
        replace_existing=True,
        max_instances=1,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=10),
    )


def _purge_trash() -> int:
    try:
        return quarantine.purge_expired()
    except Exception:
        log.exception("trash purge failed")
        return 0


def purge_old_scans() -> int:
    if settings.scan_retention_days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.scan_retention_days)
    from pathlib import Path
    with session_scope() as session:
        old = session.query(models.DedupScan).filter(
            models.DedupScan.started_at < cutoff,
            models.DedupScan.status.notin_(
                [models.DEDUP_PENDING, models.DEDUP_RUNNING]
            ),
        ).all()
        protected_ids = {
            t.last_scan_id for t in session.query(models.DedupTarget).all()
            if t.last_scan_id is not None
        }
        removed = 0
        for scan in old:
            if scan.id in protected_ids:
                continue
            if scan.output_path:
                try:
                    Path(scan.output_path).unlink(missing_ok=True)
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
        raise RuntimeError("dedup scheduler not started")
    return _scheduler


def _reload_jobs() -> None:
    with SessionLocal() as session:
        schedules = session.query(models.DedupSchedule).filter_by(enabled=True).all()
        for schedule in schedules:
            _upsert_job(schedule)


def _upsert_job(schedule: models.DedupSchedule) -> None:
    scheduler().add_job(
        _run_scheduled_scan,
        trigger=CronTrigger.from_crontab(schedule.cron, timezone="UTC"),
        args=[schedule.id],
        id=_job_id(schedule.id),
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )


def sync_schedule(session: Session, schedule_id: int) -> None:
    schedule = session.get(models.DedupSchedule, schedule_id)
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
        id=f"dedup-scan-{scan_id}",
        replace_existing=True,
        max_instances=1,
    )
