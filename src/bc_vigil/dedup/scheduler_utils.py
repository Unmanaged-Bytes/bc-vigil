from __future__ import annotations

from datetime import datetime, timedelta, timezone

from croniter import croniter


def cron_period(expr: str, now: datetime | None = None) -> timedelta:
    now = now or datetime.now(timezone.utc)
    it = croniter(expr, now)
    next1 = it.get_next(datetime)
    next2 = it.get_next(datetime)
    return next2 - next1


def is_schedule_stuck(
    cron_expr: str,
    last_run_at: datetime | None,
    now: datetime | None = None,
    multiplier: float = 2.0,
) -> bool:
    now = now or datetime.now(timezone.utc)
    period = cron_period(cron_expr, now)
    if period <= timedelta(0):
        return False

    if last_run_at is None:
        return False

    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)
    return (now - last_run_at) > period * multiplier
