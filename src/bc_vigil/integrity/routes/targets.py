from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session


@dataclass
class PathCheck:
    normalized: str | None
    error: str | None

router = APIRouter(prefix="/targets", tags=["targets"])


@router.get("", response_class=HTMLResponse)
def list_targets(request: Request, session: Session = Depends(get_session)):
    targets = session.scalars(select(models.Target).order_by(models.Target.name)).all()
    return request.app.state.templates.TemplateResponse(
        request, "targets/list.html", {"targets": targets, "algorithms": models.ALGORITHMS},
    )


@router.get("/new", response_class=HTMLResponse)
def new_target_form(request: Request):
    return request.app.state.templates.TemplateResponse(
        request, "targets/form.html",
        {"target": None, "algorithms": models.ALGORITHMS, "error": None},
    )


@router.post("")
def create_target(
    request: Request,
    name: str = Form(...),
    path: str = Form(...),
    algorithm: str = Form("sha256"),
    threads: str = Form("auto"),
    includes: str = Form(""),
    excludes: str = Form(""),
    session: Session = Depends(get_session),
):
    error = _validate_basic(name, algorithm, threads)
    resolved: str | None = None
    if error is None:
        check = _normalize_path(path)
        if check.error is not None:
            error = check.error
        else:
            resolved = check.normalized
    if error is None:
        if session.scalar(select(models.Target).where(models.Target.name == name)):
            error = f"un target nommé {name!r} existe déjà"
    if error is not None:
        return request.app.state.templates.TemplateResponse(
            request, "targets/form.html",
            {
                "target": {
                    "name": name, "path": path,
                    "algorithm": algorithm, "threads": threads,
                    "includes": includes, "excludes": excludes,
                },
                "algorithms": models.ALGORITHMS, "error": error,
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    target = models.Target(
        name=name, path=resolved, algorithm=algorithm, threads=threads,
        includes=_clean_patterns(includes),
        excludes=_clean_patterns(excludes),
    )
    session.add(target)
    session.commit()
    return RedirectResponse("/targets", status_code=303)


def _clean_patterns(raw: str) -> str | None:
    lines = [line.strip() for line in (raw or "").splitlines() if line.strip()]
    return "\n".join(lines) if lines else None


@router.get("/{target_id}", response_class=HTMLResponse)
def show_target(target_id: int, request: Request, session: Session = Depends(get_session)):
    from sqlalchemy import func, select as _select
    from bc_vigil.integrity.scheduler_utils import is_schedule_stuck

    target = session.get(models.Target, target_id)
    if target is None:
        raise HTTPException(404)

    drift_count = session.scalar(
        _select(func.count()).select_from(models.Scan).where(
            models.Scan.target_id == target.id,
            models.Scan.status == models.SCAN_DRIFT,
            models.Scan.acknowledged.is_(False),
        )
    ) or 0

    schedule_stats = []
    for sched in target.schedules:
        last_scheduled = session.scalar(
            _select(models.Scan).where(
                models.Scan.target_id == target.id,
                models.Scan.trigger == "scheduled",
            ).order_by(models.Scan.started_at.desc()).limit(1)
        )
        last_at = last_scheduled.started_at if last_scheduled else None
        schedule_stats.append({
            "schedule": sched,
            "last_run_at": last_at,
            "stuck": is_schedule_stuck(sched.cron, last_at) if sched.enabled else False,
        })

    return request.app.state.templates.TemplateResponse(
        request, "targets/detail.html",
        {
            "target": target,
            "scans": target.scans[:20],
            "drift_count": drift_count,
            "schedule_stats": schedule_stats,
        },
    )


@router.post("/{target_id}/duplicate")
def duplicate_target(target_id: int, session: Session = Depends(get_session)):
    source = session.get(models.Target, target_id)
    if source is None:
        raise HTTPException(404)
    base_name = f"{source.name} (copie)"
    new_name = base_name
    n = 2
    while session.scalar(select(models.Target).where(models.Target.name == new_name)):
        new_name = f"{base_name} {n}"
        n += 1
    clone = models.Target(
        name=new_name, path=source.path, algorithm=source.algorithm,
        threads=source.threads, includes=source.includes, excludes=source.excludes,
    )
    session.add(clone)
    session.commit()
    session.refresh(clone)
    return RedirectResponse(f"/targets/{clone.id}", status_code=303)


@router.post("/{target_id}/delete")
def delete_target(target_id: int, session: Session = Depends(get_session)):
    target = session.get(models.Target, target_id)
    if target is None:
        raise HTTPException(404)
    from bc_vigil.integrity import scheduler
    for schedule in target.schedules:
        scheduler.remove_schedule(schedule.id)
    session.delete(target)
    session.commit()
    return RedirectResponse("/targets", status_code=303)


def _validate_basic(name: str, algorithm: str, threads: str) -> str | None:
    if not name.strip():
        return "nom requis"
    if algorithm not in models.ALGORITHMS:
        return f"algo invalide: {algorithm}"
    if threads not in ("auto", "0") and not threads.isdigit():
        return "threads doit être auto, 0 ou un entier"
    return None


def _normalize_path(raw: str) -> PathCheck:
    raw = raw.strip()
    if not raw:
        return PathCheck(None, "path requis")
    expanded = Path(raw).expanduser()
    if not expanded.is_absolute():
        return PathCheck(None, f"path doit être absolu (reçu: {raw!r})")
    try:
        resolved = expanded.resolve(strict=False)
    except OSError as exc:
        return PathCheck(None, f"path illisible ({raw!r}): {exc}")
    if not resolved.exists():
        return PathCheck(None, f"path inexistant: {resolved}")
    if not resolved.is_dir() and not resolved.is_file():
        return PathCheck(None, f"path n'est ni un fichier ni un répertoire: {resolved}")
    return PathCheck(str(resolved), None)
