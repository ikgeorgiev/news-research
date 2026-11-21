"""Independent source schedules under the application's existing leader lock."""
from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone

from app.feed_runtime import _load_tickers_from_csv_if_changed
from app.ingestion import (
    _run_feed_ingestion, _run_post_ingestion_remaps, _run_push_alerts,
    _run_raw_feed_pruning, _run_stale_revalidation, sync_runtime_state,
)
from app.monitoring import record_ingestion_failure, record_ingestion_success

logger = logging.getLogger(__name__)
EMPTY_SYNC = {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}


@dataclass(frozen=True)
class ScheduledChunk:
    key: str
    source: object


def source_plan(sources, parallel_yahoo):
    plan = {}
    for source in sources:
        if not source.feeds:
            continue
        if source.code == "yahoo" and parallel_yahoo:
            for feed in source.feeds:
                key = "yahoo/" + hashlib.sha256(feed.url.encode()).hexdigest()[:16]
                chunk = copy.copy(source)
                chunk.feeds = [feed]
                plan[key] = ScheduledChunk(key, chunk)
        else:
            plan[source.code] = source
    return plan


class SourceCoordinator:
    def __init__(self, settings, session_factory, stop: threading.Event):
        self.settings = settings
        self.factory = session_factory
        self.stop = stop
        self._state_lock = threading.Lock()
        self._state = {"mode": "independent_sources", "status": "starting", "sources": {}}

    def snapshot(self):
        with self._state_lock:
            return copy.deepcopy(self._state)

    def _update(self, code=None, **values):
        with self._state_lock:
            target = self._state if code is None else self._state["sources"].setdefault(code, {})
            target.update(values)

    def _plan(self):
        with self.factory() as db:
            return sync_runtime_state(db, self.settings, ticker_loader=_load_tickers_from_csv_if_changed)

    def _poll(self, source):
        key = source.key if isinstance(source, ScheduledChunk) else source.code
        source = source.source if isinstance(source, ScheduledChunk) else source
        # The outer executor owns the worker budget. Feeds on one source remain
        # serial, including issuer feeds sharing its host and durable controls.
        settings = copy.copy(self.settings)
        settings.ingestion_max_workers = 1
        started = time.perf_counter()
        self._update(key, source=source.code, status="polling", started_at=datetime.now(timezone.utc).isoformat())
        try:
            with self.factory() as db:
                feeds, seen, inserted, failed, notify_failed = _run_feed_ingestion(
                    db, settings, source_feeds=[source], should_stop=self.stop.is_set,
                )
                pushes = _run_push_alerts(db, settings, total_inserted=inserted, notify_failed=notify_failed)
            result = dict(total_feeds=len(feeds), total_items_seen=seen,
                          total_items_inserted=inserted, failed_feeds=failed,
                          ticker_sync=dict(EMPTY_SYNC), source_remaps=[], feeds=feeds, push_alerts=pushes)
            record_ingestion_success(result, time.perf_counter()-started)
            statuses = {}
            for feed in feeds:
                status = feed.get("status", "unknown")
                statuses[status] = statuses.get(status, 0) + 1
            self._update(key, status="waiting", finished_at=datetime.now(timezone.utc).isoformat(),
                         last_duration_seconds=round(time.perf_counter()-started, 3),
                         failed_feeds=failed, inserted=inserted, error=None, feed_outcomes=statuses)
        except Exception as exc:
            record_ingestion_failure(time.perf_counter()-started)
            self._update(key, status="error", error=type(exc).__name__,
                         finished_at=datetime.now(timezone.utc).isoformat())
            logger.exception("Source cycle failed: %s", source.code)
        return time.monotonic()

    def _maintenance(self, ticker_sync):
        with self.factory() as db:
            _run_post_ingestion_remaps(db, self.settings, ticker_sync=ticker_sync)
            if not self.stop.is_set():
                _run_stale_revalidation(db, self.settings)
            if not self.stop.is_set():
                _run_raw_feed_pruning(db, self.settings)

    def run(self, heartbeat=lambda: None):
        workers = max(1, self.settings.ingestion_max_workers)
        cooldown = max(0.0, self.settings.ingest_cooldown_seconds)
        refresh_interval = max(1.0, getattr(self.settings, "source_plan_refresh_seconds", 30.0))
        maintenance_interval = max(1.0, getattr(self.settings, "ingestion_maintenance_seconds", 60.0))
        active: dict[str, Future] = {}
        due: dict[str, float] = {}
        plan = {}
        next_refresh = next_heartbeat = next_maintenance = 0.0
        pending_sync = dict(EMPTY_SYNC)
        maintenance_sync: dict[str, int] = {}
        maintenance = None
        self._update(status="running", worker_limit=workers, cooldown_seconds=cooldown)
        try:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="news-source") as pool, \
                 ThreadPoolExecutor(max_workers=1, thread_name_prefix="news-maintenance") as maintenance_pool:
                try:
                    while not self.stop.is_set():
                        now = time.monotonic()
                        if now >= next_heartbeat:
                            heartbeat()  # Loss of the leader connection stops all new work.
                            next_heartbeat = now + 2.0
                        for code, future in list(active.items()):
                            if future.done():
                                due[code] = future.result() + cooldown
                                del active[code]
                        if now >= next_refresh:
                            sync = self._plan()
                            plan = source_plan(sync["source_feeds"], getattr(self.settings,"ingestion_parallel_yahoo",False))
                            for key in pending_sync:
                                pending_sync[key] += sync["ticker_sync"][key]
                            next_refresh = now + refresh_interval
                            for code in set(due)-set(plan)-set(active):
                                due.pop(code)
                                self._update(code, status="inactive")
                        if maintenance is not None and maintenance.done():
                            try:
                                maintenance.result()
                                self._update(maintenance_status="waiting", maintenance_error=None)
                            except Exception as exc:
                                for key, count in maintenance_sync.items():
                                    pending_sync[key] += count
                                self._update(maintenance_status="error", maintenance_error=type(exc).__name__)
                                logger.exception("News maintenance failed")
                            maintenance = None
                        if maintenance is None and now >= next_maintenance and not self.stop.is_set():
                            maintenance_sync = pending_sync
                            pending_sync = dict(EMPTY_SYNC)
                            maintenance = maintenance_pool.submit(self._maintenance, maintenance_sync)
                            next_maintenance = now + maintenance_interval
                            self._update(maintenance_status="running")
                        # Oldest due source gets the next free slot; never fill an
                        # unbounded queue with work waiting behind a slow source.
                        for code in sorted(plan, key=lambda code: (due.get(code, 0), code.startswith("yahoo/"), code)):
                            if self.stop.is_set() or len(active) >= workers:
                                break
                            if code not in active and due.get(code, 0) <= time.monotonic():
                                active[code] = pool.submit(self._poll, plan[code])
                        self.stop.wait(0.05)
                finally:
                    # Wait for bounded in-flight calls before releasing leadership.
                    # The ingestion function checks this flag between feed requests.
                    self.stop.set()
                    self._update(status="draining")
        finally:
            self._update(status="stopped")
