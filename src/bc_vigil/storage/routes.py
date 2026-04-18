from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from bc_vigil.storage import disks

router = APIRouter(prefix="/storage", tags=["storage"])


@router.get("", response_class=HTMLResponse)
def list_storage(request: Request):
    return request.app.state.templates.TemplateResponse(
        request, "storage/list.html",
        {"disks": disks.list_disks()},
    )
