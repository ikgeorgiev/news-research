from __future__ import annotations

from app.scheduler import IngestionScheduler

_scheduler: IngestionScheduler | None = None


def get_scheduler() -> IngestionScheduler | None:
    return _scheduler


def set_scheduler(s: IngestionScheduler | None) -> None:
    global _scheduler
    _scheduler = s
