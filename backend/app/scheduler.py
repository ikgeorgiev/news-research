from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.ingestion import IngestionCycleResult, run_ingestion_cycle

logger = logging.getLogger(__name__)


class IngestionScheduler:
    def __init__(self, settings: Settings, session_factory: Callable[[], Session]):
        self.settings = settings
        self.session_factory = session_factory
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._lock = threading.Lock()

    def _acquire_global_lock(self, db: Session) -> bool:
        bind = db.get_bind()
        if bind is None or bind.dialect.name != "postgresql":
            return True
        return bool(
            db.scalar(select(func.pg_try_advisory_lock(self.settings.ingestion_advisory_lock_key)))
        )

    def _release_global_lock(self, db: Session) -> None:
        bind = db.get_bind()
        if bind is None or bind.dialect.name != "postgresql":
            return
        db.scalar(select(func.pg_advisory_unlock(self.settings.ingestion_advisory_lock_key)))

    def start(self) -> None:
        if not self.settings.scheduler_enabled:
            logger.info("Scheduler disabled by configuration")
            return

        if self._scheduler.running:
            return

        self._scheduler.add_job(
            self._run_job,
            trigger=IntervalTrigger(seconds=self.settings.ingest_interval_seconds),
            id="ingestion_cycle",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            jitter=10,
        )
        self._scheduler.start()
        logger.info("Ingestion scheduler started")

    def shutdown(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Ingestion scheduler stopped")

    def run_once(self) -> IngestionCycleResult | None:
        if not self._lock.acquire(blocking=False):
            return None
        try:
            with self.session_factory() as db:
                acquired_global_lock = self._acquire_global_lock(db)
                if not acquired_global_lock:
                    return None
                try:
                    return run_ingestion_cycle(db, self.settings)
                finally:
                    try:
                        self._release_global_lock(db)
                    except Exception:
                        logger.exception("Failed to release ingestion advisory lock")
        finally:
            self._lock.release()

    def _run_job(self) -> None:
        try:
            result = self.run_once()
            if result is None:
                logger.info("Ingestion job skipped because a run is already in progress")
                return

            logger.info(
                "Ingestion cycle complete",
                extra={
                    "total_feeds": result["total_feeds"],
                    "total_items_seen": result["total_items_seen"],
                    "total_items_inserted": result["total_items_inserted"],
                    "failed_feeds": result["failed_feeds"],
                },
            )
        except Exception:
            logger.exception("Ingestion cycle failed with unhandled exception")
