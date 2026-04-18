from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from importlib import resources
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from bc_vigil import i18n
from bc_vigil.config import settings
from bc_vigil.core.routes import admin as admin_routes
from bc_vigil.core.routes import dashboard
from bc_vigil.core.routes import help as help_routes
from bc_vigil.core.routes import lang as lang_routes
from bc_vigil.db import init_db
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


templates.env.filters["humanbytes"] = _format_bytes
templates.env.filters["datetime_utc"] = _format_datetime_utc
templates.env.filters["localtime"] = _format_local
templates.env.globals["nav_pending_drift"] = _nav_pending_drift
templates.env.globals["display_tz"] = lambda: settings.display_tz
templates.env.globals["t"] = i18n.translate
templates.env.globals["current_lang"] = i18n.current_lang
templates.env.globals["supported_langs"] = i18n.SUPPORTED


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_db()
        scheduler.start()
        try:
            yield
        finally:
            scheduler.shutdown()

    app = FastAPI(title="BitCrafts Vigil", lifespan=lifespan)
    app.state.templates = templates

    static_dir = _package_path("static")
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(dashboard.router)
    app.include_router(targets_routes.router)
    app.include_router(schedules_routes.router)
    app.include_router(scans_routes.router)
    app.include_router(storage_routes.router)
    app.include_router(admin_routes.router)
    app.include_router(help_routes.router)
    app.include_router(lang_routes.router)
    return app


app = create_app()
