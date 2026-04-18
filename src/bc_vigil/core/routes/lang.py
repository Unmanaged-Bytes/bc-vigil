from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from bc_vigil.i18n import COOKIE_NAME, SUPPORTED

router = APIRouter()


@router.get("/lang/{lang}")
def set_language(lang: str, next: str = "/"):
    response = RedirectResponse(next, status_code=303)
    if lang in SUPPORTED:
        response.set_cookie(
            COOKIE_NAME, lang, max_age=365 * 24 * 3600,
            samesite="lax", httponly=False,
        )
    return response
