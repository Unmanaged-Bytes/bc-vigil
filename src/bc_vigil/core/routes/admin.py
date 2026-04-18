from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.config import settings
from bc_vigil.core import admin_ops
from bc_vigil.db import get_session

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("", response_class=HTMLResponse)
def admin_page(request: Request, session: Session = Depends(get_session)):
    targets_count = session.scalar(select(func.count()).select_from(models.Target)) or 0
    scans_count = session.scalar(select(func.count()).select_from(models.Scan)) or 0
    active = admin_ops.has_active_scans()
    snapshots_dir = settings.data_dir / "snapshots"
    snapshots = []
    if snapshots_dir.exists():
        snapshots = sorted(
            (p for p in snapshots_dir.iterdir() if p.is_file()),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )[:10]
    return request.app.state.templates.TemplateResponse(
        request, "admin.html",
        {
            "targets_count": targets_count,
            "scans_count": scans_count,
            "active_scans": active,
            "snapshots": snapshots,
            "message": request.query_params.get("msg"),
            "error": request.query_params.get("err"),
        },
    )


@router.get("/backup")
def backup_download():
    data = admin_ops.build_backup_archive()
    return Response(
        content=data,
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{admin_ops.backup_filename()}"',
        },
    )


@router.post("/reset")
def reset_db(confirm: str = Form("")):
    if confirm != "RESET":
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "confirmation manquante (taper RESET)",
        )
    try:
        snapshot = admin_ops.reset_database()
    except admin_ops.AdminError as exc:
        return RedirectResponse(
            f"/admin?err={_quote(str(exc))}", status_code=303,
        )
    return RedirectResponse(
        f"/admin?msg={_quote(f'base réinitialisée — snapshot préalable: {snapshot.name}')}",
        status_code=303,
    )


@router.post("/restore")
async def restore_db(
    confirm: str = Form(""),
    archive: UploadFile = File(...),
):
    if confirm != "RESTORE":
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "confirmation manquante (taper RESTORE)",
        )
    payload = await archive.read()
    if not payload:
        raise HTTPException(400, "archive vide")
    try:
        snapshot = admin_ops.restore_from_archive(payload)
    except admin_ops.AdminError as exc:
        return RedirectResponse(
            f"/admin?err={_quote(str(exc))}", status_code=303,
        )
    return RedirectResponse(
        f"/admin?msg={_quote(f'base restaurée — snapshot préalable: {snapshot.name}')}",
        status_code=303,
    )


def _quote(value: str) -> str:
    from urllib.parse import quote
    return quote(value)
