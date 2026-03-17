from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, TypedDict

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, sessionmaker

from app.article_maintenance import (
    SourceRemapStats,
    remap_source_articles,
    revalidate_stale_article_tickers,
)
from app.article_ingest import (
    IngestFeedResult,
    ingest_feed,
)
from app.config import Settings
from app.feed_runtime import (
    _load_tickers_from_csv_if_changed,
    _mark_raw_feed_prune_done,
    _should_run_raw_feed_prune,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
)
from app.ticker_context import load_ticker_context
from app.models import (
    Source,
)
from app.push_alerts import check_and_send_alerts
from app.sources import (
    PAGE_FETCH_CONFIGS,
    SourceFeed,
    build_source_feeds,
    seed_sources,
)
logger = logging.getLogger(__name__)

# Compatibility facade for tests and callers that still import runtime helpers
# from app.ingestion while the implementation lives in smaller modules.


class TickerSyncStats(TypedDict):
    loaded: int
    created: int
    updated: int
    unchanged: int

class IngestionCycleResult(TypedDict):
    total_feeds: int
    total_items_seen: int
    total_items_inserted: int
    failed_feeds: int
    ticker_sync: TickerSyncStats
    source_remaps: list[SourceRemapStats]
    feeds: list[IngestFeedResult]
    push_alerts: dict[str, int]


class RuntimeSyncResult(TypedDict):
    source_feeds: list[SourceFeed]
    ticker_sync: TickerSyncStats
    stale_runs_fixed: int


def _normalize_ticker_sync_stats(raw_stats: dict[str, int]) -> TickerSyncStats:
    return {
        "loaded": int(raw_stats.get("loaded", 0) or 0),
        "created": int(raw_stats.get("created", 0) or 0),
        "updated": int(raw_stats.get("updated", 0) or 0),
        "unchanged": int(raw_stats.get("unchanged", 0) or 0),
    }


def _failed_feed_result(
    source_code: str,
    feed_url: str,
    error: str,
) -> IngestFeedResult:
    return {
        "source": source_code,
        "feed_url": feed_url,
        "status": "failed",
        "items_seen": 0,
        "items_inserted": 0,
        "error": error,
    }


def sync_runtime_state(
    db: Session,
    settings: Settings,
    *,
    ticker_loader: Callable[[Session, str], dict[str, int]],
) -> RuntimeSyncResult:
    ticker_sync = _normalize_ticker_sync_stats(
        ticker_loader(db, settings.tickers_csv_path)
    )
    source_feeds = build_source_feeds(settings, db)
    seed_sources(db, source_feeds)
    stale_runs_fixed = reconcile_stale_ingestion_runs(
        db,
        stale_after_seconds=settings.ingestion_stale_run_timeout_seconds,
    )
    return {
        "source_feeds": source_feeds,
        "ticker_sync": ticker_sync,
        "stale_runs_fixed": stale_runs_fixed,
    }


def _run_feed_ingestion(
    db: Session,
    settings: Settings,
    *,
    source_feeds: list[SourceFeed],
) -> tuple[list[IngestFeedResult], int, int, int]:
    source_map = {
        source.code: source
        for source in db.scalars(
            select(Source).where(
                and_(
                    Source.code.in_([item.code for item in source_feeds]),
                    Source.enabled.is_(True),
                )
            )
        ).all()
    }
    ticker_context = load_ticker_context(db)
    max_workers = settings.ingestion_max_workers
    feed_ingest_kwargs = {
        "known_symbols": ticker_context.known_symbols,
        "symbol_to_id": ticker_context.symbol_to_id,
        "timeout_seconds": settings.request_timeout_seconds,
        "fetch_max_attempts": settings.feed_fetch_max_attempts,
        "fetch_backoff_seconds": settings.feed_fetch_backoff_seconds,
        "fetch_backoff_jitter_seconds": settings.feed_fetch_backoff_jitter_seconds,
        "enable_conditional_get": settings.ingestion_enable_conditional_get,
        "failure_backoff_base_seconds": settings.feed_failure_backoff_base_seconds,
        "failure_backoff_max_seconds": settings.feed_failure_backoff_max_seconds,
        "symbol_keywords": ticker_context.symbol_keywords,
    }
    bind = db.get_bind()
    dialect_name = (
        getattr(getattr(bind, "dialect", None), "name", None)
        if bind is not None
        else None
    )
    if dialect_name != "postgresql":
        # SQLite in-memory tests must remain single-threaded; additional sessions may attach to a different DB.
        max_workers = 1

    tasks: list[tuple[int, str, str]] = []
    for source_item in source_feeds:
        source_row = source_map.get(source_item.code)
        if source_row is None:
            continue
        for feed_url in source_item.feed_urls:
            tasks.append((source_row.id, source_row.code, feed_url))
    source_task_locks = {source_id: threading.Lock() for source_id, _, _ in tasks}

    def _ingest_with_source(
        task_db: Session,
        source_row: Source,
        feed_url: str,
    ) -> IngestFeedResult:
        return ingest_feed(
            task_db,
            source=source_row,
            feed_url=feed_url,
            **feed_ingest_kwargs,
        )

    def _ingest_task(
        source_id: int, source_code: str, feed_url: str
    ) -> IngestFeedResult:
        source_task_lock = source_task_locks[source_id]
        # Keep feeds from the same source serialized even in parallel mode.
        # Raw dedupe keys are source-scoped and pre-fetched per feed, so running
        # same-source feeds concurrently can race and insert duplicate raw rows.
        with source_task_lock:
            if max_workers <= 1:
                source_row = source_map.get(source_code)
                if source_row is None:
                    return _failed_feed_result(
                        source_code,
                        feed_url,
                        "Source row missing (unexpected)",
                    )
                return _ingest_with_source(db, source_row, feed_url)

            # Parallel workers: each task uses its own DB session.
            if bind is None:
                raise RuntimeError(
                    "DB bind is missing; cannot create per-worker sessions"
                )
            worker_factory = sessionmaker(autoflush=False, autocommit=False, bind=bind)
            with worker_factory() as worker_db:
                source_row = worker_db.scalar(
                    select(Source).where(Source.id == source_id)
                )
                if source_row is None:
                    return _failed_feed_result(
                        source_code,
                        feed_url,
                        f"Source id {source_id} not found",
                    )
                return _ingest_with_source(worker_db, source_row, feed_url)

    results: list[IngestFeedResult] = []
    if max_workers <= 1:
        for source_id, source_code, feed_url in tasks:
            results.append(_ingest_task(source_id, source_code, feed_url))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(_ingest_task, source_id, source_code, feed_url): (
                    source_code,
                    feed_url,
                )
                for source_id, source_code, feed_url in tasks
            }
            for future in as_completed(future_to_task):
                source_code, feed_url = future_to_task[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(_failed_feed_result(source_code, feed_url, str(exc)))

    per_feed = sorted(
        results, key=lambda r: (r.get("source", ""), r.get("feed_url", ""))
    )
    total_seen = 0
    total_inserted = 0
    failed = 0
    for result in per_feed:
        total_seen += int(result["items_seen"])
        total_inserted += int(result["items_inserted"])
        if result["status"] == "failed":
            failed += 1

    return per_feed, total_seen, total_inserted, failed


def _run_post_ingestion_remaps(
    db: Session,
    settings: Settings,
    *,
    ticker_sync: TickerSyncStats,
) -> list[SourceRemapStats]:
    source_remaps: list[SourceRemapStats] = []
    ticker_changed = ticker_sync["created"] > 0 or ticker_sync["updated"] > 0
    if ticker_changed:
        for code in PAGE_FETCH_CONFIGS:
            source_remaps.append(
                remap_source_articles(
                    db, settings, source_code=code, limit=500, only_unmapped=True
                )
            )
    return source_remaps


def _run_stale_revalidation(db: Session, settings: Settings) -> None:
    try:
        revalidation_stats = revalidate_stale_article_tickers(
            db,
            limit=100,
            timeout_seconds=settings.request_timeout_seconds,
        )
        if revalidation_stats["scanned"]:
            logger.info(
                "Revalidated %s stale article mappings (%s changed, %s purged, %s unchanged)",
                revalidation_stats["scanned"],
                revalidation_stats["revalidated"],
                revalidation_stats["purged"],
                revalidation_stats["unchanged"],
            )
    except Exception:  # fault-isolation: broad catch intentional
        logger.exception("Revalidation failed, continuing ingestion cycle")
        db.rollback()


def _run_raw_feed_pruning(db: Session, settings: Settings) -> None:
    prune_interval_seconds = settings.raw_feed_prune_interval_seconds
    if _should_run_raw_feed_prune(prune_interval_seconds):
        pruned_raw_items = prune_raw_feed_items(
            db,
            retention_days=settings.raw_feed_retention_days,
            batch_size=settings.raw_feed_prune_batch_size,
            max_batches=settings.raw_feed_prune_max_batches,
        )
        _mark_raw_feed_prune_done()
        if pruned_raw_items:
            logger.info(
                "Pruned %s raw feed items older than %s days",
                pruned_raw_items,
                settings.raw_feed_retention_days,
            )


def _run_push_alerts(
    db: Session,
    settings: Settings,
    *,
    total_inserted: int,
) -> dict[str, int]:
    push_alerts = {
        "scanned": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
    }
    if total_inserted > 0:
        try:
            return check_and_send_alerts(db, settings)
        except Exception:  # Broad catch: push failures must not abort ingestion
            logger.exception("Push alert processing failed after ingestion cycle")
    return push_alerts


def run_ingestion_cycle(db: Session, settings: Settings) -> IngestionCycleResult:
    runtime_sync = sync_runtime_state(
        db,
        settings,
        ticker_loader=_load_tickers_from_csv_if_changed,
    )
    stale_runs_fixed = runtime_sync["stale_runs_fixed"]
    if stale_runs_fixed:
        logger.warning("Marked %s stale ingestion runs as failed", stale_runs_fixed)

    ticker_sync = runtime_sync["ticker_sync"]
    source_feeds = runtime_sync["source_feeds"]
    per_feed, total_seen, total_inserted, failed = _run_feed_ingestion(
        db,
        settings,
        source_feeds=source_feeds,
    )
    source_remaps = _run_post_ingestion_remaps(
        db,
        settings,
        ticker_sync=ticker_sync,
    )
    _run_stale_revalidation(db, settings)
    _run_raw_feed_pruning(db, settings)
    push_alerts = _run_push_alerts(
        db,
        settings,
        total_inserted=total_inserted,
    )

    return {
        "total_feeds": len(per_feed),
        "total_items_seen": total_seen,
        "total_items_inserted": total_inserted,
        "failed_feeds": failed,
        "ticker_sync": ticker_sync,
        "source_remaps": source_remaps,
        "feeds": per_feed,
        "push_alerts": push_alerts,
    }


