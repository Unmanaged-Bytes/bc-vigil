from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from croniter import croniter

MODES = ("every_minutes", "hourly", "daily", "weekly", "monthly", "cron")

_DAY_INDEX = {
    "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6, "sun": 0,
}
_DAY_LABELS_FR = {
    "mon": "lun", "tue": "mar", "wed": "mer", "thu": "jeu",
    "fri": "ven", "sat": "sam", "sun": "dim",
}


@dataclass
class BuildResult:
    cron: str | None
    error: str | None
    description: str | None = None


def build_cron(
    mode: str,
    *,
    interval_minutes: str | None = None,
    minute_of_hour: str | None = None,
    time: str | None = None,
    days: list[str] | None = None,
    day_of_month: str | None = None,
    cron_expr: str | None = None,
) -> BuildResult:
    if mode not in MODES:
        return BuildResult(None, f"mode inconnu: {mode!r}")

    if mode == "every_minutes":
        n = _parse_int(interval_minutes, 1, 59, "intervalle minutes")
        if isinstance(n, str):
            return BuildResult(None, n)
        return BuildResult(f"*/{n} * * * *", None, f"Toutes les {n} minutes")

    if mode == "hourly":
        m = _parse_int(minute_of_hour, 0, 59, "minute de l'heure")
        if isinstance(m, str):
            return BuildResult(None, m)
        label = f"Toutes les heures à la minute {m:02d}"
        return BuildResult(f"{m} * * * *", None, label)

    if mode == "daily":
        hh, mm = _parse_hhmm(time)
        if isinstance(hh, str):
            return BuildResult(None, hh)
        return BuildResult(
            f"{mm} {hh} * * *", None, f"Tous les jours à {hh:02d}:{mm:02d} UTC",
        )

    if mode == "weekly":
        hh, mm = _parse_hhmm(time)
        if isinstance(hh, str):
            return BuildResult(None, hh)
        if not days:
            return BuildResult(None, "au moins un jour de la semaine requis")
        indices = []
        seen = set()
        for d in days:
            if d not in _DAY_INDEX:
                return BuildResult(None, f"jour invalide: {d!r}")
            idx = _DAY_INDEX[d]
            if idx in seen:
                continue
            seen.add(idx)
            indices.append(idx)
        indices.sort()
        day_field = ",".join(str(i) for i in indices)
        labels = ", ".join(_DAY_LABELS_FR[d] for d in days if d in _DAY_LABELS_FR)
        return BuildResult(
            f"{mm} {hh} * * {day_field}", None,
            f"Chaque semaine ({labels}) à {hh:02d}:{mm:02d} UTC",
        )

    if mode == "monthly":
        hh, mm = _parse_hhmm(time)
        if isinstance(hh, str):
            return BuildResult(None, hh)
        dom = _parse_int(day_of_month, 1, 31, "jour du mois")
        if isinstance(dom, str):
            return BuildResult(None, dom)
        return BuildResult(
            f"{mm} {hh} {dom} * *", None,
            f"Chaque mois le {dom} à {hh:02d}:{mm:02d} UTC",
        )

    raw = (cron_expr or "").strip()
    if not raw:
        return BuildResult(None, "expression cron requise")
    if not croniter.is_valid(raw):
        return BuildResult(None, f"expression cron invalide: {raw!r}")
    return BuildResult(raw, None, f"Cron: {raw}")


def next_occurrences(cron: str, count: int = 5) -> list[datetime]:
    now = datetime.now(timezone.utc)
    it = croniter(cron, now)
    return [it.get_next(datetime) for _ in range(count)]


def _parse_int(value: str | None, lo: int, hi: int, label: str) -> int | str:
    if value is None or str(value).strip() == "":
        return f"{label} requis"
    try:
        n = int(str(value).strip())
    except ValueError:
        return f"{label} doit être un entier"
    if n < lo or n > hi:
        return f"{label} doit être entre {lo} et {hi}"
    return n


def _parse_hhmm(value: str | None) -> tuple[int, int] | tuple[str, None]:
    if not value or not value.strip():
        return ("heure requise (HH:MM)", None)
    raw = value.strip()
    if ":" not in raw:
        return (f"format attendu HH:MM (reçu {raw!r})", None)
    try:
        hh_s, mm_s = raw.split(":", 1)
        hh, mm = int(hh_s), int(mm_s)
    except ValueError:
        return (f"format attendu HH:MM (reçu {raw!r})", None)
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return (f"heure hors plage (reçu {raw!r})", None)
    return (hh, mm)
