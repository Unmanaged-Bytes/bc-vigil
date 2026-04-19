from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session
from bc_vigil.dedup import cron_builder, scheduler

router = APIRouter(prefix="/dedup/schedules", tags=["dedup-schedules"])


def _default_form_state() -> dict:
    return {
        "mode": "daily",
        "interval_minutes": "15",
        "minute_of_hour": "0",
        "time": "03:00",
        "days": [],
        "day_of_month": "1",
        "cron_expr": "",
    }


def _state_from_form(
    mode: str,
    interval_minutes: str,
    minute_of_hour: str,
    time: str,
    days: list[str],
    day_of_month: str,
    cron_expr: str,
) -> dict:
    return {
        "mode": mode,
        "interval_minutes": interval_minutes,
        "minute_of_hour": minute_of_hour,
        "time": time,
        "days": days,
        "day_of_month": day_of_month,
        "cron_expr": cron_expr,
    }


@router.get("/new", response_class=HTMLResponse)
def new_schedule_form(
    target_id: int, request: Request, session: Session = Depends(get_session),
):
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    return request.app.state.templates.TemplateResponse(
        request, "dedup/schedules/form.html",
        {"target": target, "state": _default_form_state(), "error": None},
    )


@router.post("/preview", response_class=HTMLResponse)
def preview_schedule(
    request: Request,
    mode: str = Form("daily"),
    interval_minutes: str = Form("15"),
    minute_of_hour: str = Form("0"),
    time: str = Form("03:00"),
    days: list[str] = Form(default_factory=list),
    day_of_month: str = Form("1"),
    cron_expr: str = Form(""),
):
    result = cron_builder.build_cron(
        mode,
        interval_minutes=interval_minutes,
        minute_of_hour=minute_of_hour,
        time=time,
        days=days,
        day_of_month=day_of_month,
        cron_expr=cron_expr,
    )
    occurrences = (
        cron_builder.next_occurrences(result.cron, 5) if result.cron else []
    )
    return request.app.state.templates.TemplateResponse(
        request, "dedup/schedules/_preview.html",
        {"result": result, "occurrences": occurrences},
    )


@router.post("")
def create_schedule(
    request: Request,
    target_id: int = Form(...),
    mode: str = Form("daily"),
    interval_minutes: str = Form("15"),
    minute_of_hour: str = Form("0"),
    time: str = Form("03:00"),
    days: list[str] = Form(default_factory=list),
    day_of_month: str = Form("1"),
    cron_expr: str = Form(""),
    enabled: bool = Form(True),
    session: Session = Depends(get_session),
):
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    result = cron_builder.build_cron(
        mode,
        interval_minutes=interval_minutes,
        minute_of_hour=minute_of_hour,
        time=time,
        days=days,
        day_of_month=day_of_month,
        cron_expr=cron_expr,
    )
    if result.error is not None or result.cron is None:
        state = _state_from_form(
            mode, interval_minutes, minute_of_hour, time, days,
            day_of_month, cron_expr,
        )
        return request.app.state.templates.TemplateResponse(
            request, "dedup/schedules/form.html",
            {"target": target, "state": state, "error": result.error},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    schedule = models.DedupSchedule(
        target_id=target_id, cron=result.cron, enabled=enabled,
    )
    session.add(schedule)
    session.commit()
    session.refresh(schedule)
    scheduler.sync_schedule(session, schedule.id)
    return RedirectResponse(f"/dedup/targets/{target_id}", status_code=303)


@router.post("/{schedule_id}/toggle")
def toggle_schedule(schedule_id: int, session: Session = Depends(get_session)):
    schedule = session.get(models.DedupSchedule, schedule_id)
    if schedule is None:
        raise HTTPException(404)
    schedule.enabled = not schedule.enabled
    session.commit()
    scheduler.sync_schedule(session, schedule_id)
    return RedirectResponse(
        f"/dedup/targets/{schedule.target_id}", status_code=303,
    )


@router.post("/{schedule_id}/delete")
def delete_schedule(schedule_id: int, session: Session = Depends(get_session)):
    schedule = session.get(models.DedupSchedule, schedule_id)
    if schedule is None:
        raise HTTPException(404)
    target_id = schedule.target_id
    scheduler.remove_schedule(schedule_id)
    session.delete(schedule)
    session.commit()
    return RedirectResponse(f"/dedup/targets/{target_id}", status_code=303)
