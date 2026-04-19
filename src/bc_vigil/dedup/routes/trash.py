from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session
from bc_vigil.dedup import quarantine

router = APIRouter(prefix="/dedup/trash", tags=["dedup-trash"])


@router.get("", response_class=HTMLResponse)
def list_trash(
    request: Request,
    status_filter: str | None = None,
    session: Session = Depends(get_session),
):
    stmt = (
        select(models.DedupDeletion)
        .order_by(models.DedupDeletion.deleted_at.desc())
        .limit(500)
    )
    if status_filter in (
        models.DELETION_QUARANTINED,
        models.DELETION_RESTORED,
        models.DELETION_PURGED,
        models.DELETION_FAILED,
    ):
        stmt = stmt.where(models.DedupDeletion.status == status_filter)
    rows = session.scalars(stmt).all()
    return request.app.state.templates.TemplateResponse(
        request, "dedup/trash/list.html",
        {
            "deletions": rows,
            "selected_status": status_filter or "",
            "statuses": (
                models.DELETION_QUARANTINED,
                models.DELETION_RESTORED,
                models.DELETION_PURGED,
                models.DELETION_FAILED,
            ),
        },
    )


@router.post("/{deletion_id}/restore")
def restore(deletion_id: int):
    try:
        quarantine.restore(deletion_id)
    except quarantine.QuarantineError as exc:
        raise HTTPException(400, str(exc))
    return RedirectResponse("/dedup/trash", status_code=303)


@router.post("/{deletion_id}/purge")
def purge(deletion_id: int):
    try:
        quarantine.purge_one(deletion_id)
    except quarantine.QuarantineError as exc:
        raise HTTPException(400, str(exc))
    return RedirectResponse("/dedup/trash", status_code=303)
