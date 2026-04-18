from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def index(request: Request, session: Session = Depends(get_session)):
    templates = request.app.state.templates
    targets_count = session.scalar(select(func.count()).select_from(models.Target)) or 0
    schedules_count = session.scalar(
        select(func.count()).select_from(models.Schedule).where(models.Schedule.enabled.is_(True))
    ) or 0
    pending_drift = session.scalar(
        select(func.count()).select_from(models.Scan).where(
            models.Scan.status == models.SCAN_DRIFT,
            models.Scan.acknowledged.is_(False),
        )
    ) or 0
    recent_scans = session.scalars(
        select(models.Scan).order_by(models.Scan.started_at.desc()).limit(10)
    ).all()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "targets_count": targets_count,
            "schedules_count": schedules_count,
            "pending_drift": pending_drift,
            "recent_scans": recent_scans,
        },
    )
