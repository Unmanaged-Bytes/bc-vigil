from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from bc_vigil import models
from bc_vigil.db import get_session
from bc_vigil.dedup import disk_detect


@dataclass
class PathCheck:
    normalized: str | None
    error: str | None


router = APIRouter(prefix="/dedup/targets", tags=["dedup-targets"])


@router.get("", response_class=HTMLResponse)
def list_targets(request: Request, session: Session = Depends(get_session)):
    targets = session.scalars(
        select(models.DedupTarget).order_by(models.DedupTarget.name)
    ).all()
    return request.app.state.templates.TemplateResponse(
        request, "dedup/targets/list.html",
        {"targets": targets, "algorithms": models.DEDUP_ALGORITHMS},
    )


@router.get("/new", response_class=HTMLResponse)
def new_target_form(request: Request):
    return request.app.state.templates.TemplateResponse(
        request, "dedup/targets/form.html",
        {
            "target": None,
            "algorithms": models.DEDUP_ALGORITHMS,
            "error": None,
            "disk": None,
            "notices": [],
        },
    )


@router.post("")
def create_target(
    request: Request,
    name: str = Form(...),
    path: str = Form(...),
    algorithm: str = Form("xxh3"),
    threads: str = Form("auto"),
    includes: str = Form(""),
    excludes: str = Form(""),
    minimum_size: str = Form(""),
    include_hidden: bool = Form(False),
    follow_symlinks: bool = Form(False),
    match_hardlinks: bool = Form(False),
    one_file_system: bool = Form(False),
    advanced_threads_override: bool = Form(False),
    accept_network_fs: bool = Form(False),
    session: Session = Depends(get_session),
):
    error = _validate_basic(name, algorithm, threads)
    resolved: str | None = None
    min_size_value: int | None = None
    disk: disk_detect.DiskInfo | None = None
    notices: list[str] = []
    if error is None:
        check = _normalize_path(path)
        if check.error is not None:
            error = check.error
        else:
            resolved = check.normalized
    if error is None and resolved is not None:
        disk = disk_detect.detect_disk_info(Path(resolved))
        if disk.kind == disk_detect.KIND_NETWORK and not accept_network_fs:
            error = (
                f"chemin sur système de fichiers réseau "
                f"({disk.fstype!r}). Cocher la case d'opt-in pour continuer."
            )
        elif disk.kind == disk_detect.KIND_HDD and not advanced_threads_override:
            if threads != "1":
                notices.append(
                    f"disque mécanique détecté ({disk.block_device}) — "
                    f"threads forcé à 1"
                )
            threads = "1"
    if error is None:
        min_size_value, min_err = _parse_optional_int(
            minimum_size, 0, "taille minimale",
        )
        if min_err is not None:
            error = min_err
    if error is None:
        if session.scalar(
            select(models.DedupTarget).where(models.DedupTarget.name == name)
        ):
            error = f"une cible dedup nommée {name!r} existe déjà"
    if error is not None:
        return request.app.state.templates.TemplateResponse(
            request, "dedup/targets/form.html",
            {
                "target": {
                    "name": name, "path": path,
                    "algorithm": algorithm, "threads": threads,
                    "includes": includes, "excludes": excludes,
                    "minimum_size": minimum_size,
                    "include_hidden": include_hidden,
                    "follow_symlinks": follow_symlinks,
                    "match_hardlinks": match_hardlinks,
                    "one_file_system": one_file_system,
                    "advanced_threads_override": advanced_threads_override,
                    "accept_network_fs": accept_network_fs,
                },
                "algorithms": models.DEDUP_ALGORITHMS,
                "error": error,
                "disk": disk,
                "notices": notices,
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    target = models.DedupTarget(
        name=name, path=resolved, algorithm=algorithm, threads=threads,
        includes=_clean_patterns(includes),
        excludes=_clean_patterns(excludes),
        minimum_size=min_size_value,
        include_hidden=include_hidden,
        follow_symlinks=follow_symlinks,
        match_hardlinks=match_hardlinks,
        one_file_system=one_file_system,
    )
    session.add(target)
    session.commit()
    return RedirectResponse("/dedup/targets", status_code=303)


def _clean_patterns(raw: str) -> str | None:
    lines = [line.strip() for line in (raw or "").splitlines() if line.strip()]
    return "\n".join(lines) if lines else None


@router.get("/{target_id}", response_class=HTMLResponse)
def show_target(
    target_id: int, request: Request, session: Session = Depends(get_session),
):
    from sqlalchemy import select as _select
    from bc_vigil.dedup.scheduler_utils import is_schedule_stuck

    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)

    schedule_stats = []
    for sched in target.schedules:
        last_scheduled = session.scalar(
            _select(models.DedupScan).where(
                models.DedupScan.target_id == target.id,
                models.DedupScan.trigger == "scheduled",
            ).order_by(models.DedupScan.started_at.desc()).limit(1)
        )
        last_at = last_scheduled.started_at if last_scheduled else None
        schedule_stats.append({
            "schedule": sched,
            "last_run_at": last_at,
            "stuck": is_schedule_stuck(sched.cron, last_at) if sched.enabled else False,
        })

    disk = disk_detect.detect_disk_info(Path(target.path))
    return request.app.state.templates.TemplateResponse(
        request, "dedup/targets/detail.html",
        {
            "target": target,
            "scans": target.scans[:20],
            "schedule_stats": schedule_stats,
            "disk": disk,
        },
    )


@router.get("/{target_id}/edit", response_class=HTMLResponse)
def edit_target_form(target_id: int, request: Request, session: Session = Depends(get_session)):
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    return request.app.state.templates.TemplateResponse(
        request, "dedup/targets/form.html",
        {
            "target": target,
            "algorithms": models.DEDUP_ALGORITHMS,
            "error": None,
            "disk": disk_detect.detect_disk_info(Path(target.path)),
            "notices": [],
            "edit": True,
        },
    )


@router.post("/{target_id}/update")
def update_target(
    target_id: int,
    request: Request,
    name: str = Form(...),
    algorithm: str = Form("xxh3"),
    threads: str = Form("auto"),
    includes: str = Form(""),
    excludes: str = Form(""),
    minimum_size: str = Form(""),
    include_hidden: bool = Form(False),
    follow_symlinks: bool = Form(False),
    match_hardlinks: bool = Form(False),
    one_file_system: bool = Form(False),
    session: Session = Depends(get_session),
):
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    error = _validate_basic(name, algorithm, threads)
    min_size_value: int | None = None
    if error is None:
        min_size_value, min_err = _parse_optional_int(
            minimum_size, 0, "taille minimale",
        )
        if min_err is not None:
            error = min_err
    if error is None and name != target.name:
        if session.scalar(
            select(models.DedupTarget).where(models.DedupTarget.name == name)
        ):
            error = f"une cible dedup nommée {name!r} existe déjà"
    if error is not None:
        return request.app.state.templates.TemplateResponse(
            request, "dedup/targets/form.html",
            {
                "target": {
                    "id": target_id, "name": name, "path": target.path,
                    "algorithm": algorithm, "threads": threads,
                    "includes": includes, "excludes": excludes,
                    "minimum_size": minimum_size,
                    "include_hidden": include_hidden,
                    "follow_symlinks": follow_symlinks,
                    "match_hardlinks": match_hardlinks,
                    "one_file_system": one_file_system,
                },
                "algorithms": models.DEDUP_ALGORITHMS,
                "error": error,
                "disk": disk_detect.detect_disk_info(Path(target.path)),
                "notices": [],
                "edit": True,
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    target.name = name
    target.algorithm = algorithm
    target.threads = threads
    target.includes = _clean_patterns(includes)
    target.excludes = _clean_patterns(excludes)
    target.minimum_size = min_size_value
    target.include_hidden = include_hidden
    target.follow_symlinks = follow_symlinks
    target.match_hardlinks = match_hardlinks
    target.one_file_system = one_file_system
    session.commit()
    return RedirectResponse(f"/dedup/targets/{target_id}", status_code=303)


@router.post("/{target_id}/duplicate")
def duplicate_target(target_id: int, session: Session = Depends(get_session)):
    source = session.get(models.DedupTarget, target_id)
    if source is None:
        raise HTTPException(404)
    base_name = f"{source.name} (copie)"
    new_name = base_name
    n = 2
    while session.scalar(
        select(models.DedupTarget).where(models.DedupTarget.name == new_name)
    ):
        new_name = f"{base_name} {n}"
        n += 1
    clone = models.DedupTarget(
        name=new_name, path=source.path, algorithm=source.algorithm,
        threads=source.threads, includes=source.includes, excludes=source.excludes,
        minimum_size=source.minimum_size,
        include_hidden=source.include_hidden,
        follow_symlinks=source.follow_symlinks,
        match_hardlinks=source.match_hardlinks,
        one_file_system=source.one_file_system,
    )
    session.add(clone)
    session.commit()
    session.refresh(clone)
    return RedirectResponse(f"/dedup/targets/{clone.id}", status_code=303)


@router.post("/{target_id}/delete")
def delete_target(target_id: int, session: Session = Depends(get_session)):
    target = session.get(models.DedupTarget, target_id)
    if target is None:
        raise HTTPException(404)
    from bc_vigil.dedup import scheduler
    for schedule in target.schedules:
        scheduler.remove_schedule(schedule.id)
    session.delete(target)
    session.commit()
    return RedirectResponse("/dedup/targets", status_code=303)


def _validate_basic(name: str, algorithm: str, threads: str) -> str | None:
    if not name.strip():
        return "nom requis"
    if algorithm not in models.DEDUP_ALGORITHMS:
        return f"algo invalide: {algorithm}"
    if threads not in ("auto", "0") and not threads.isdigit():
        return "threads doit être auto, 0 ou un entier"
    return None


def _parse_optional_int(
    raw: str, lo: int, label: str,
) -> tuple[int | None, str | None]:
    raw = (raw or "").strip()
    if raw == "":
        return (None, None)
    try:
        n = int(raw)
    except ValueError:
        return (None, f"{label} doit être un entier")
    if n < lo:
        return (None, f"{label} doit être ≥ {lo}")
    return (n, None)


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
