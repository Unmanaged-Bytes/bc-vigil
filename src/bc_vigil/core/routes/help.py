from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()

TOPICS = ("overview", "integrity", "dedup", "admin", "faq")
DEFAULT_TOPIC = "overview"


@router.get("/help", response_class=HTMLResponse)
def help_index():
    return RedirectResponse(f"/help/{DEFAULT_TOPIC}", status_code=303)


@router.get("/help/{topic}", response_class=HTMLResponse)
def help_topic(topic: str, request: Request):
    if topic not in TOPICS:
        raise HTTPException(404)
    return request.app.state.templates.TemplateResponse(
        request, "help/page.html", {"current_topic": topic},
    )
