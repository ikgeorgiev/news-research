from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

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
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        self._coordinator = None

    def snapshot(self) -> dict:
        if self._coordinator is not None:
            return self._coordinator.snapshot()
        return {"mode": "batch", "status": "running" if self._thread and self._thread.is_alive() else "stopped"}

    def start(self) -> None:
        if not self.settings.scheduler_enabled:
            logger.info("Scheduler disabled by configuration")
            return

        if self._thread is not None and self._thread.is_alive():
            return

        self._shutdown.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="ingestion-scheduler",
        )
        self._thread.start()
        from app.monitoring import INGESTION_SCHEDULER_ENABLED, INGESTION_SCHEDULER_STARTED_AT
        INGESTION_SCHEDULER_ENABLED.set(1)
        INGESTION_SCHEDULER_STARTED_AT.set(time.time())
        logger.info(
            "Ingestion scheduler started (cooldown=%ss)",
            self.settings.ingest_cooldown_seconds,
        )

    def shutdown(self) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=35)
            if self._thread.is_alive():
                logger.error("Scheduler still draining; retain its identity and leader lock")
                return
            self._thread = None
        from app.monitoring import INGESTION_SCHEDULER_ENABLED
        INGESTION_SCHEDULER_ENABLED.set(0)
        logger.info("Ingestion scheduler stopped")

    def _loop(self) -> None:
        if (getattr(self.settings, "ingestion_independent_sources", True)
                and get_engine().dialect.name == "postgresql"):
            self._independent_loop()
            return
        while not self._shutdown.is_set():
            try:
                result = self.run_once()
                if result is not None:
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
                if self._shutdown.wait(timeout=self.settings.ingest_cooldown_seconds):
                    break
                continue
            if self._shutdown.wait(timeout=self.settings.ingest_cooldown_seconds):
                break

    def _independent_loop(self) -> None:
        from app.source_scheduler import SourceCoordinator
        while not self._shutdown.is_set():
            if not self._lock.acquire(blocking=False):
                self._shutdown.wait(max(0.1, self.settings.ingest_cooldown_seconds))
                continue
            try:
                # One owner for all sources; manual run_once and other app
                # instances use the same existing advisory-lock key.
                epoch_stop = threading.Event()
                self._coordinator = SourceCoordinator(self.settings, self.session_factory, epoch_stop)
                def coordinate(lock_conn):
                    backend_pid = lock_conn.exec_driver_sql("SELECT pg_backend_pid()").scalar()
                    lock_conn.commit()
                    def heartbeat():
                        if self._shutdown.is_set():
                            epoch_stop.set()
                            return
                        if lock_conn.invalidated:
                            raise RuntimeError("Ingestion leader connection lost")
                        current_pid = lock_conn.exec_driver_sql("SELECT pg_backend_pid()").scalar()
                        lock_conn.commit()
                        if current_pid != backend_pid:
                            raise RuntimeError("Ingestion leader connection changed")
                    self._coordinator.run(heartbeat)
                self._run_with_global_lock(coordinate)
            except Exception:
                logger.exception("Independent ingestion ownership ended; pending work drained")
            finally:
                self._lock.release()
            self._shutdown.wait(max(0.1, self.settings.ingest_cooldown_seconds))

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

    def _run_with_global_lock(self, operation=None) -> IngestionCycleResult | None:
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
                if operation is not None:
                    return operation(lock_conn)
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
                    # Never return a possibly lock-owning connection to the pool.
                    lock_conn.invalidate()


def get_scheduler() -> IngestionScheduler | None:
    return _scheduler


def set_scheduler(scheduler: IngestionScheduler | None) -> None:
    global _scheduler
    _scheduler = scheduler
