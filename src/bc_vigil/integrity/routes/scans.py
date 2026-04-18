from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session
from bc_vigil.integrity import scans, scheduler

router = APIRouter(prefix="/scans", tags=["scans"])


_ALL_STATUSES = (
    models.SCAN_PENDING, models.SCAN_RUNNING, models.SCAN_OK,
    models.SCAN_DRIFT, models.SCAN_FAILED, models.SCAN_CANCELLED,
)


@router.get("", response_class=HTMLResponse)
def list_scans(
    request: Request,
    status: str | None = None,
    target_id: int | None = None,
    session: Session = Depends(get_session),
):
    stmt = select(models.Scan).order_by(models.Scan.started_at.desc()).limit(200)
    if status and status in _ALL_STATUSES:
        stmt = stmt.where(models.Scan.status == status)
    if target_id is not None:
        stmt = stmt.where(models.Scan.target_id == target_id)
    rows = session.scalars(stmt).all()

    has_live = any(s.status in (models.SCAN_PENDING, models.SCAN_RUNNING) for s in rows)
    targets = session.scalars(select(models.Target).order_by(models.Target.name)).all()

    return request.app.state.templates.TemplateResponse(
        request, "scans/list.html",
        {
            "scans": rows,
            "statuses": _ALL_STATUSES,
            "selected_status": status or "",
            "selected_target_id": target_id,
            "targets": targets,
            "has_live": has_live,
        },
    )


@router.post("/run")
def run_scan(target_id: int, session: Session = Depends(get_session)):
    target = session.get(models.Target, target_id)
    if target is None:
        raise HTTPException(404)
    scan_id = scans.trigger_scan(target_id, trigger="manual")
    scheduler.run_scan_async(scan_id)
    return RedirectResponse(f"/scans/{scan_id}", status_code=303)


@router.post("/acknowledge-all")
def acknowledge_all(
    target_id: int | None = None,
    session: Session = Depends(get_session),
):
    stmt = (
        update(models.Scan)
        .where(models.Scan.status == models.SCAN_DRIFT)
        .where(models.Scan.acknowledged.is_(False))
        .values(acknowledged=True)
    )
    if target_id is not None:
        stmt = stmt.where(models.Scan.target_id == target_id)
    session.execute(stmt)
    session.commit()
    redirect = f"/targets/{target_id}" if target_id else "/scans"
    return RedirectResponse(redirect, status_code=303)


@router.get("/{scan_id}", response_class=HTMLResponse)
def show_scan(scan_id: int, request: Request, session: Session = Depends(get_session)):
    scan = session.get(models.Scan, scan_id)
    if scan is None:
        raise HTTPException(404)
    return request.app.state.templates.TemplateResponse(
        request, "scans/detail.html", {"scan": scan, "events": scan.events},
    )


@router.get("/{scan_id}/events.csv")
def export_scan_events_csv(scan_id: int, session: Session = Depends(get_session)):
    scan = session.get(models.Scan, scan_id)
    if scan is None:
        raise HTTPException(404)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["scan_id", "target", "event_type", "path", "old_digest", "new_digest"])
    for ev in scan.events:
        writer.writerow([
            scan.id, scan.target.name, ev.event_type, ev.path,
            ev.old_digest or "", ev.new_digest or "",
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": (
                f'attachment; filename="scan-{scan.id}-events.csv"'
            ),
        },
    )


@router.post("/{scan_id}/acknowledge")
def acknowledge_scan(scan_id: int, session: Session = Depends(get_session)):
    scan = session.get(models.Scan, scan_id)
    if scan is None:
        raise HTTPException(404)
    scan.acknowledged = True
    session.commit()
    return RedirectResponse(f"/scans/{scan_id}", status_code=303)


@router.post("/{scan_id}/promote")
def promote_scan(scan_id: int, session: Session = Depends(get_session)):
    scans.promote_baseline(session, scan_id)
    session.commit()
    scan = session.get(models.Scan, scan_id)
    return RedirectResponse(f"/targets/{scan.target_id}", status_code=303)


@router.post("/{scan_id}/cancel")
def cancel_scan_route(
    scan_id: int,
    force: bool = False,
    session: Session = Depends(get_session),
):
    scan = session.get(models.Scan, scan_id)
    if scan is None:
        raise HTTPException(404)
    if scan.status not in (models.SCAN_PENDING, models.SCAN_RUNNING):
        raise HTTPException(409, f"scan déjà {scan.status}")
    scans.cancel_scan(scan_id, force=force)
    return RedirectResponse(f"/scans/{scan_id}", status_code=303)
