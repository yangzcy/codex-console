from __future__ import annotations

from datetime import datetime


def should_run_deferred_task(task, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    next_retry_at = getattr(task, "next_retry_at", None)
    status = str(getattr(task, "status", "") or "").strip().lower()

    if status != "deferred":
        return False
    if next_retry_at is None:
        return True
    return next_retry_at <= now


def choose_task_bucket(task) -> str:
    return str(getattr(task, "defer_bucket", "") or "fresh").strip() or "fresh"
