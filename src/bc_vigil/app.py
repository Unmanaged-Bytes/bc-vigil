from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from importlib import metadata, resources
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from bc_vigil import i18n
from bc_vigil.config import settings
from bc_vigil.core.routes import admin as admin_routes
from bc_vigil.core.routes import dashboard
from bc_vigil.core.routes import help as help_routes
from bc_vigil.core.routes import lang as lang_routes
from bc_vigil.db import init_db
from bc_vigil.dedup import scheduler as dedup_scheduler
from bc_vigil.dedup.routes import scans as dedup_scans_routes
from bc_vigil.dedup.routes import schedules as dedup_schedules_routes
from bc_vigil.dedup.routes import targets as dedup_targets_routes
from bc_vigil.dedup.routes import trash as dedup_trash_routes
from bc_vigil.integrity import scheduler
from bc_vigil.integrity.routes import scans as scans_routes
from bc_vigil.integrity.routes import schedules as schedules_routes
from bc_vigil.integrity.routes import targets as targets_routes
from bc_vigil.storage import routes as storage_routes

log = logging.getLogger(__name__)


def _package_path(sub: str) -> Path:
    return Path(resources.files("bc_vigil").joinpath(sub))


templates = Jinja2Templates(directory=str(_package_path("templates")))


def _format_bytes(n: int | None) -> str:
    if n is None:
        return "—"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if value < 1024.0 or unit == "PiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024.0
    raise AssertionError("unreachable")  # pragma: no cover


def _format_datetime_utc(epoch: int | float) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def _format_local(value, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if value is None:
        return "—"
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    dt = value
    if isinstance(dt, (int, float)):
        dt = datetime.fromtimestamp(int(dt), tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        tz = ZoneInfo(settings.display_tz)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    local = dt.astimezone(tz)
    return local.strftime(fmt) + f" {settings.display_tz}"


def _nav_pending_drift() -> int:
    from sqlalchemy import func, select
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    try:
        with SessionLocal() as session:
            return session.scalar(
                select(func.count()).select_from(models.Scan).where(
                    models.Scan.status == models.SCAN_DRIFT,
                    models.Scan.acknowledged.is_(False),
                )
            ) or 0
    except Exception:
        return 0


def _nav_pending_duplicates() -> int:
    from sqlalchemy import func, select
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    try:
        with SessionLocal() as session:
            return session.scalar(
                select(func.count()).select_from(models.DedupScan).where(
                    models.DedupScan.status == models.DEDUP_DUPLICATES,
                    models.DedupScan.acknowledged.is_(False),
                )
            ) or 0
    except Exception:
        return 0


def _nav_trash_count() -> int:
    from sqlalchemy import func, select
    from bc_vigil import models
    from bc_vigil.db import SessionLocal

    try:
        with SessionLocal() as session:
            return session.scalar(
                select(func.count()).select_from(models.DedupDeletion).where(
                    models.DedupDeletion.status == models.DELETION_QUARANTINED,
                )
            ) or 0
    except Exception:
        return 0


templates.env.filters["humanbytes"] = _format_bytes
templates.env.filters["datetime_utc"] = _format_datetime_utc
templates.env.filters["localtime"] = _format_local
templates.env.globals["nav_pending_drift"] = _nav_pending_drift
templates.env.globals["nav_pending_duplicates"] = _nav_pending_duplicates
templates.env.globals["nav_trash_count"] = _nav_trash_count
templates.env.globals["display_tz"] = lambda: settings.display_tz
templates.env.globals["t"] = i18n.translate
templates.env.globals["current_lang"] = i18n.current_lang
templates.env.globals["supported_langs"] = i18n.SUPPORTED


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_db()
        scheduler.start()
        dedup_scheduler.start()
        try:
            yield
        finally:
            dedup_scheduler.shutdown()
            scheduler.shutdown()

    app = FastAPI(title="BitCrafts Vigil", lifespan=lifespan)
    app.state.templates = templates

    static_dir = _package_path("static")
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/health", include_in_schema=False)
    def health() -> JSONResponse:
        from sqlalchemy import text
        from bc_vigil.db import SessionLocal

        try:
            version = metadata.version("bc-vigil")
        except metadata.PackageNotFoundError:
            version = "unknown"
        db_ok = True
        try:
            with SessionLocal() as session:
                session.execute(text("select 1")).scalar()
        except Exception:
            db_ok = False
        sched_integ = scheduler._scheduler is not None and scheduler._scheduler.running
        sched_dedup = (
            dedup_scheduler._scheduler is not None
            and dedup_scheduler._scheduler.running
        )
        ok = db_ok and sched_integ and sched_dedup
        payload = {
            "status": "ok" if ok else "degraded",
            "version": version,
            "db": "ok" if db_ok else "down",
            "scheduler_integrity": "running" if sched_integ else "stopped",
            "scheduler_dedup": "running" if sched_dedup else "stopped",
        }
        return JSONResponse(payload, status_code=200 if ok else 503)

    @app.get("/metrics", include_in_schema=False, response_class=PlainTextResponse)
    def metrics() -> PlainTextResponse:
        from sqlalchemy import func, select, text
        from bc_vigil.db import SessionLocal
        from bc_vigil import models

        try:
            version = metadata.version("bc-vigil")
        except metadata.PackageNotFoundError:
            version = "unknown"
        db_ok = 1
        integrity_counts: dict[str, int] = {}
        dedup_counts: dict[str, int] = {}
        deletion_counts: dict[str, int] = {}
        try:
            with SessionLocal() as session:
                session.execute(text("select 1")).scalar()
                for row in session.execute(
                    select(models.Scan.status, func.count()).group_by(
                        models.Scan.status,
                    )
                ).all():
                    integrity_counts[row[0]] = int(row[1])
                for row in session.execute(
                    select(models.DedupScan.status, func.count()).group_by(
                        models.DedupScan.status,
                    )
                ).all():
                    dedup_counts[row[0]] = int(row[1])
                for row in session.execute(
                    select(models.DedupDeletion.status, func.count()).group_by(
                        models.DedupDeletion.status,
                    )
                ).all():
                    deletion_counts[row[0]] = int(row[1])
        except Exception:
            db_ok = 0
        sched_integ = 1 if (
            scheduler._scheduler is not None and scheduler._scheduler.running
        ) else 0
        sched_dedup = 1 if (
            dedup_scheduler._scheduler is not None
            and dedup_scheduler._scheduler.running
        ) else 0
        up = 1 if (db_ok and sched_integ and sched_dedup) else 0

        lines: list[str] = []
        lines.append("# HELP bc_vigil_up 1 if service is fully up (DB + both schedulers), else 0")
        lines.append("# TYPE bc_vigil_up gauge")
        lines.append(f"bc_vigil_up {up}")
        lines.append("# HELP bc_vigil_info Static info about the running service")
        lines.append("# TYPE bc_vigil_info gauge")
        lines.append(f'bc_vigil_info{{version="{version}"}} 1')
        lines.append("# HELP bc_vigil_db_up 1 if the SQLite DB answered a select 1")
        lines.append("# TYPE bc_vigil_db_up gauge")
        lines.append(f"bc_vigil_db_up {db_ok}")
        lines.append("# HELP bc_vigil_scheduler_up 1 if the APScheduler for a module is running")
        lines.append("# TYPE bc_vigil_scheduler_up gauge")
        lines.append(f'bc_vigil_scheduler_up{{module="integrity"}} {sched_integ}')
        lines.append(f'bc_vigil_scheduler_up{{module="dedup"}} {sched_dedup}')
        lines.append("# HELP bc_vigil_scans_total Number of scans by module and status")
        lines.append("# TYPE bc_vigil_scans_total gauge")
        for status, count in integrity_counts.items():
            lines.append(
                f'bc_vigil_scans_total{{module="integrity",status="{status}"}} {count}'
            )
        for status, count in dedup_counts.items():
            lines.append(
                f'bc_vigil_scans_total{{module="dedup",status="{status}"}} {count}'
            )
        lines.append("# HELP bc_vigil_dedup_deletions_total Number of dedup deletions by status")
        lines.append("# TYPE bc_vigil_dedup_deletions_total gauge")
        for status, count in deletion_counts.items():
            lines.append(
                f'bc_vigil_dedup_deletions_total{{status="{status}"}} {count}'
            )
        return PlainTextResponse(
            "\n".join(lines) + "\n",
            media_type="text/plain; version=0.0.4",
        )

    app.include_router(dashboard.router)
    app.include_router(targets_routes.router)
    app.include_router(schedules_routes.router)
    app.include_router(scans_routes.router)
    app.include_router(dedup_targets_routes.router)
    app.include_router(dedup_schedules_routes.router)
    app.include_router(dedup_scans_routes.router)
    app.include_router(dedup_trash_routes.router)
    app.include_router(storage_routes.router)
    app.include_router(admin_routes.router)
    app.include_router(help_routes.router)
    app.include_router(lang_routes.router)
    return app


app = create_app()
