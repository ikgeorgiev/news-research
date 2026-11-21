from __future__ import annotations

import logging
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
from app.ingestion_history import compact_ingestion_runs
from app.ticker_context import load_ticker_context
from app.models import (
    Source,
)
from app.push_alerts import check_and_send_alerts_locked, push_dispatcher_is_active
from app.sources import (
    PAGE_FETCH_CONFIGS,
    FeedDef,
    SourceFeed,
    build_source_feeds,
    seed_sources,
)
logger = logging.getLogger(__name__)

# Public orchestration surface for ingestion flows. Callers should import
# scheduler/runtime entrypoints from here rather than reaching into helper modules.


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
        "notify_failed": False,
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
    should_stop: Callable[[], bool] = lambda: False,
) -> tuple[list[IngestFeedResult], int, int, int, bool]:
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
        "reader_archive_directory": getattr(settings, "article_body_archive_dir", None),
        "postcommit_enrichment_enabled": getattr(settings, "postcommit_enrichment_enabled", True),
        "known_symbols": ticker_context.known_symbols,
        "symbol_to_id": ticker_context.symbol_to_id,
        "timeout_seconds": settings.request_timeout_seconds,
        "feed_poll_timeout_seconds": getattr(
            settings,
            "feed_poll_timeout_seconds",
            None,
        ),
        "ingest_source_page_timeout_seconds": getattr(
            settings,
            "ingest_source_page_timeout_seconds",
            None,
        ),
        "globenewswire_source_page_timeout_seconds": getattr(
            settings,
            "globenewswire_source_page_timeout_seconds",
            5,
        ),
        "globenewswire_source_page_max_fetches_per_feed": getattr(
            settings,
            "globenewswire_source_page_max_fetches_per_feed",
            3,
        ),
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

    tasks: list[tuple[int, str, FeedDef]] = []
    for source_item in source_feeds:
        source_row = source_map.get(source_item.code)
        if source_row is None:
            continue
        for feed_def in source_item.feeds:
            tasks.append((source_row.id, source_row.code, feed_def))
    def _serialize_source(source_code: str) -> bool:
        return not (
            source_code == "yahoo"
            and bool(settings.ingestion_parallel_yahoo)
        )

    def _ingest_with_source(
        task_db: Session,
        source_row: Source,
        feed_def: FeedDef,
    ) -> IngestFeedResult:
        return ingest_feed(
            task_db,
            source=source_row,
            feed_url=feed_def.url,
            persistence_policy_override=feed_def.persistence_policy_override,
            article_source_name=feed_def.article_source_name,
            **({"issuer_feed_start": feed_def.published_after, "issuer_baseline_required": True}
               if feed_def.published_after is not None else {}),
            **feed_ingest_kwargs,
        )

    def _ingest_task(
        source_id: int, source_code: str, feed_def: FeedDef
    ) -> IngestFeedResult:
        if max_workers <= 1:
            source_row = source_map.get(source_code)
            if source_row is None:
                return _failed_feed_result(
                    source_code,
                    feed_def.url,
                    "Source row missing (unexpected)",
                )
            return _ingest_with_source(db, source_row, feed_def)

        # Parallel workers: each feed retains its own DB session.
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
                    feed_def.url,
                    f"Source id {source_id} not found",
                )
            return _ingest_with_source(worker_db, source_row, feed_def)

    def _ingest_group(
        group: list[tuple[int, str, FeedDef]],
    ) -> list[IngestFeedResult]:
        group_results: list[IngestFeedResult] = []
        for source_id, source_code, feed_def in group:
            if should_stop():
                break
            try:
                group_results.append(_ingest_task(source_id, source_code, feed_def))
            except Exception as exc:
                group_results.append(_failed_feed_result(source_code, feed_def.url, str(exc)))
        return group_results

    results: list[IngestFeedResult] = []
    if max_workers <= 1:
        for source_id, source_code, feed_def in tasks:
            if should_stop():
                break
            results.append(_ingest_task(source_id, source_code, feed_def))
    else:
        # Serialize a source in one job instead of occupying other worker slots
        # with jobs blocked on its lock. Independent wires can start immediately.
        serial_groups: dict[int, list[tuple[int, str, FeedDef]]] = {}
        parallel_groups: list[list[tuple[int, str, FeedDef]]] = []
        for task in tasks:
            source_id, source_code, _ = task
            if _serialize_source(source_code):
                serial_groups.setdefault(source_id, []).append(task)
            else:
                parallel_groups.append([task])
        # Give each serial source a slot before queuing optional Yahoo fan-out.
        groups = [*serial_groups.values(), *parallel_groups]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_ingest_group, group) for group in groups]
            for future in as_completed(futures):
                results.extend(future.result())

    per_feed = sorted(
        results, key=lambda r: (r.get("source", ""), r.get("feed_url", ""))
    )
    total_seen = 0
    total_inserted = 0
    failed = 0
    notify_failed = False
    for result in per_feed:
        total_seen += int(result["items_seen"])
        total_inserted += int(result["items_inserted"])
        notify_failed = notify_failed or bool(result.get("notify_failed", False))
        if result["status"] == "failed":
            failed += 1

    return per_feed, total_seen, total_inserted, failed, notify_failed


def _run_post_ingestion_remaps(
    db: Session,
    settings: Settings,
    *,
    ticker_sync: TickerSyncStats,
) -> list[SourceRemapStats]:
    source_remaps: list[SourceRemapStats] = []
    ticker_changed = ticker_sync["created"] > 0 or ticker_sync["updated"] > 0
    if ticker_changed:
        gn_timeout = getattr(
            settings,
            "globenewswire_source_page_timeout_seconds",
            None,
        )
        for code in PAGE_FETCH_CONFIGS:
            source_remaps.append(
                remap_source_articles(
                    db,
                    settings,
                    source_code=code,
                    limit=500,
                    only_unmapped=True,
                    timeout_seconds=settings.request_timeout_seconds,
                    globenewswire_source_page_timeout_seconds=gn_timeout,
                )
            )
    return source_remaps


def _run_stale_revalidation(db: Session, settings: Settings) -> None:
    try:
        revalidation_stats = revalidate_stale_article_tickers(
            db,
            limit=100,
            timeout_seconds=settings.request_timeout_seconds,
            globenewswire_source_page_timeout_seconds=getattr(
                settings,
                "globenewswire_source_page_timeout_seconds",
                None,
            ),
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


def _maintain_feed_history(db: Session, settings: Settings) -> None:
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

    if settings.ingestion_run_detail_days:
        count = compact_ingestion_runs(db, detail_days=settings.ingestion_run_detail_days)
        db.commit()
        if count:
            logger.info("Added %s routine RSS polls to the daily counts", count)


def _run_push_alerts(
    db: Session,
    settings: Settings,
    *,
    total_inserted: int,
    notify_failed: bool,
) -> dict[str, int]:
    push_alerts = {
        "scanned": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
    }
    if total_inserted > 0:
        if push_dispatcher_is_active() and not notify_failed:
            return push_alerts
        try:
            locked_stats = check_and_send_alerts_locked(db, settings)
            if locked_stats is not None:
                return locked_stats
        except Exception:  # Broad catch: push failures must not abort ingestion
            db.rollback()
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
    per_feed, total_seen, total_inserted, failed, notify_failed = _run_feed_ingestion(
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
    push_alerts = _run_push_alerts(
        db,
        settings,
        total_inserted=total_inserted,
        notify_failed=notify_failed,
    )
    _maintain_feed_history(db, settings)

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


