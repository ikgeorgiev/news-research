from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.database import get_engine
from app.ingestion import IngestionCycleResult, run_ingestion_cycle
from app.monitoring import record_ingestion_failure, record_ingestion_skip, record_ingestion_success

logger = logging.getLogger(__name__)
_scheduler: "IngestionScheduler | None" = None


class IngestionScheduler:
    def __init__(self, settings: Settings, session_factory: Callable[[], Session]):
        self.settings = settings
        self.session_factory = session_factory
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._lock = threading.Lock()

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
        from app.monitoring import INGESTION_SCHEDULER_ENABLED
        INGESTION_SCHEDULER_ENABLED.set(1)
        logger.info("Ingestion scheduler started")

    def shutdown(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Ingestion scheduler stopped")

    def run_once(self) -> IngestionCycleResult | None:
        started_at = time.perf_counter()
        if not self._lock.acquire(blocking=False):
            logger.info("Ingestion job skipped: another run is already in progress (in-process lock)")
            record_ingestion_skip("inprocess_lock")
            return None
        try:
            result = self._run_with_global_lock()
            if result is not None:
                record_ingestion_success(result, time.perf_counter() - started_at)
            return result
        except Exception:
            record_ingestion_failure(time.perf_counter() - started_at)
            raise
        finally:
            self._lock.release()

    def _run_with_global_lock(self) -> IngestionCycleResult | None:
        engine = get_engine()

        # Skip advisory lock for non-PostgreSQL (e.g. SQLite in tests).
        if engine.dialect.name != "postgresql":
            with self.session_factory() as db:
                return run_ingestion_cycle(db, self.settings)

        # Hold the advisory lock on a dedicated connection that stays open for
        # the full duration. Previously the lock was acquired via the Session,
        # but Session.commit() inside run_ingestion_cycle can return the underlying
        # connection back to the pool.
        #
        # pg_advisory_lock is connection/session-level: it stays bound to the
        # PostgreSQL connection, not the SQLAlchemy Session. If the Session later
        # checks out a different pooled connection for pg_advisory_unlock, the
        # unlock is a no-op and the lock leaks until that pooled connection is
        # recycled, causing subsequent cycles to be skipped.
        with engine.connect() as lock_conn:
            lock_key = self.settings.ingestion_advisory_lock_key
            acquired = lock_conn.execute(select(func.pg_try_advisory_lock(lock_key))).scalar()
            # Avoid leaving the lock connection "idle in transaction" for the full run.
            lock_conn.commit()
            if not acquired:
                logger.info("Ingestion job skipped: advisory lock is held by another instance")
                record_ingestion_skip("advisory_lock")
                return None
            try:
                with self.session_factory() as db:
                    return run_ingestion_cycle(db, self.settings)
            finally:
                try:
                    unlocked = lock_conn.execute(select(func.pg_advisory_unlock(lock_key))).scalar()
                    lock_conn.commit()
                    if not unlocked:
                        logger.warning("Ingestion advisory unlock returned false (key=%s)", lock_key)
                except Exception:
                    logger.exception("Failed to release ingestion advisory lock")

    def _run_job(self) -> None:
        try:
            result = self.run_once()
            if result is None:
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


def get_scheduler() -> IngestionScheduler | None:
    return _scheduler


def set_scheduler(scheduler: IngestionScheduler | None) -> None:
    global _scheduler
    _scheduler = scheduler
