from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session
from bc_vigil.dedup import scans, scheduler

router = APIRouter(prefix="/dedup/scans", tags=["dedup-scans"])


_ALL_STATUSES = (
    models.DEDUP_PENDING, models.DEDUP_RUNNING, models.DEDUP_OK,
    models.DEDUP_DUPLICATES, models.DEDUP_FAILED, models.DEDUP_CANCELLED,
)


@router.get("", response_class=HTMLResponse)
def list_scans(
    request: Request,
    status: str | None = None,
    target_id: int | None = None,
    session: Session = Depends(get_session),
):
    stmt = (
        select(models.DedupScan)
        .order_by(models.DedupScan.started_at.desc())
        .limit(200)
    )
    if status and status in _ALL_STATUSES:
        stmt = stmt.where(models.DedupScan.status == status)
    if target_id is not None:
        stmt = stmt.where(models.DedupScan.target_id == target_id)
    rows = session.scalars(stmt).all()

    has_live = any(
        s.status in (models.DEDUP_PENDING, models.DEDUP_RUNNING) for s in rows
    )
    targets = session.scalars(
        select(models.DedupTarget).order_by(models.DedupTarget.name)
    ).all()

    return request.app.state.templates.TemplateResponse(
        request, "dedup/scans/list.html",
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
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    scan_id = scans.trigger_scan(target_id, trigger="manual")
    scheduler.run_scan_async(scan_id)
    return RedirectResponse(f"/dedup/scans/{scan_id}", status_code=303)


@router.post("/acknowledge-all")
def acknowledge_all(
    target_id: int | None = None,
    session: Session = Depends(get_session),
):
    stmt = (
        update(models.DedupScan)
        .where(models.DedupScan.status == models.DEDUP_DUPLICATES)
        .where(models.DedupScan.acknowledged.is_(False))
        .values(acknowledged=True)
    )
    if target_id is not None:
        stmt = stmt.where(models.DedupScan.target_id == target_id)
    session.execute(stmt)
    session.commit()
    redirect = (
        f"/dedup/targets/{target_id}" if target_id else "/dedup/scans"
    )
    return RedirectResponse(redirect, status_code=303)


@router.get("/{scan_id}", response_class=HTMLResponse)
def show_scan(
    scan_id: int, request: Request, session: Session = Depends(get_session),
):
    scan = session.get(models.DedupScan, scan_id)
    if scan is None:
        raise HTTPException(404)
    groups = []
    for group in scan.groups:
        groups.append({
            "size": group.size,
            "file_count": group.file_count,
            "paths": scans.parse_group_paths(group.paths_json),
        })
    return request.app.state.templates.TemplateResponse(
        request, "dedup/scans/detail.html",
        {"scan": scan, "groups": groups},
    )


@router.get("/{scan_id}/groups.csv")
def export_scan_groups_csv(
    scan_id: int, session: Session = Depends(get_session),
):
    scan = session.get(models.DedupScan, scan_id)
    if scan is None:
        raise HTTPException(404)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["scan_id", "target", "group_id", "size", "path"])
    for group in scan.groups:
        paths = scans.parse_group_paths(group.paths_json)
        for path in paths:
            writer.writerow([
                scan.id, scan.target.name, group.id, group.size, path,
            ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": (
                f'attachment; filename="dedup-scan-{scan.id}-groups.csv"'
            ),
        },
    )


@router.post("/{scan_id}/acknowledge")
def acknowledge_scan(scan_id: int, session: Session = Depends(get_session)):
    scan = session.get(models.DedupScan, scan_id)
    if scan is None:
        raise HTTPException(404)
    scan.acknowledged = True
    session.commit()
    return RedirectResponse(f"/dedup/scans/{scan_id}", status_code=303)


@router.post("/{scan_id}/cancel")
def cancel_scan_route(
    scan_id: int,
    force: bool = False,
    session: Session = Depends(get_session),
):
    scan = session.get(models.DedupScan, scan_id)
    if scan is None:
        raise HTTPException(404)
    if scan.status not in (models.DEDUP_PENDING, models.DEDUP_RUNNING):
        raise HTTPException(409, f"scan déjà {scan.status}")
    scans.cancel_scan(scan_id, force=force)
    return RedirectResponse(f"/dedup/scans/{scan_id}", status_code=303)
