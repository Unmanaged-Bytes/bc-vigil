from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from bc_vigil.i18n import current_lang

router = APIRouter()


@router.get("/help", response_class=HTMLResponse)
def help_page(request: Request):
    lang = current_lang(request)
    template = "help_en.html" if lang == "en" else "help_fr.html"
    return request.app.state.templates.TemplateResponse(request, template, {})
