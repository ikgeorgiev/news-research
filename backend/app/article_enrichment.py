"""Bounded full-page coverage after the first validated headline is committed."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import threading
import time
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.article_maintenance import _upsert_article_tickers
from app.models import Article, ArticleEnrichment, ArticleTicker, Source
from app.pg_utils import acquire_article_dedupe_locks
from app.sources import PAGE_FETCH_CONFIGS, _is_source_article_url
from app.sse import notify_new_articles
from app.ticker_context import load_ticker_context
from app.ticker_extraction import (
    _extract_source_html_tickers, _fetch_source_page_html, _verified_ticker_hits,
)
from app.utils import to_utc
from app.page_access import reset_page_outcome, get_page_outcome

logger = logging.getLogger(__name__)
LEASE_SECONDS = 120
MAX_ATTEMPTS = 12
MAX_AGE = timedelta(days=1)
BATCH_SIZE = 3
POLL_SECONDS = 5


def utc_now():
    return datetime.now(timezone.utc)


def enqueue_article_enrichment(db, *, article, source, page_url, feed_url):
    """Called inside the article transaction; never fetch a page here."""
    config = PAGE_FETCH_CONFIGS.get(source.code)
    now = utc_now()
    if config is None or not _is_source_article_url(page_url, config.hostname_suffix):
        return
    # Replayed history must not turn into an unbounded source-page backfill.
    if to_utc(article.published_at) < now - timedelta(days=2):
        return
    insert = pg_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
    db.execute(insert(ArticleEnrichment).values(
        article_id=article.id, source_id=source.id, page_url=page_url,
        feed_url=feed_url, status="pending", attempts=0,
        available_at=now, created_at=now,
    ).on_conflict_do_nothing(index_elements=["article_id"]))


@dataclass(frozen=True)
class Claim:
    article_id: int
    source_code: str
    page_url: str
    feed_url: str
    token: str
    notify_only: bool
    queued_at: datetime


def claim_one(factory) -> Claim | None:
    now = utc_now()
    with factory() as db:
        row = db.execute(select(ArticleEnrichment, Source.code).join(
            Source, Source.id == ArticleEnrichment.source_id,
        ).where(
            ArticleEnrichment.status.in_(("pending", "running", "notify")),
            ArticleEnrichment.available_at <= now,
            Source.enabled.is_(True),
        ).order_by(ArticleEnrichment.available_at, ArticleEnrichment.article_id)
          .limit(1).with_for_update(skip_locked=True, of=ArticleEnrichment)).first()
        if row is None:
            return None
        job, code = row
        notify_only = job.status == "notify"
        if not notify_only and (job.attempts >= MAX_ATTEMPTS or to_utc(job.created_at) + MAX_AGE <= now):
            job.status, job.completed_at, job.lease_token = "expired", now, None
            db.commit()
            logger.info("article_enrichment_expired article_id=%s", job.article_id)
            return None
        token = uuid4().hex
        job.lease_token = token
        job.available_at = now + timedelta(seconds=LEASE_SECONDS)
        if not notify_only:
            job.status = "running"
            job.attempts += 1
        claim = Claim(job.article_id, code, job.page_url, job.feed_url, token,
                      notify_only, to_utc(job.created_at))
        db.commit()
        return claim


def _deliver_notification(factory, claim):
    # The mapping transaction has committed. A failed notification retains a
    # durable 'notify' job; recovery sends only a refresh, not another page fetch.
    with factory() as db:
        try:
            notify_new_articles(db, 0)
        except Exception:
            db.rollback()
            logger.warning("Article enrichment refresh notification failed", exc_info=True)
            return False
        db.execute(update(ArticleEnrichment).where(
            ArticleEnrichment.article_id == claim.article_id,
            ArticleEnrichment.lease_token == claim.token,
            ArticleEnrichment.status == "notify",
        ).values(status="done", completed_at=utc_now(), lease_token=None))
        db.commit()
        return True


def _finish(factory, claim, html, timings=None, page_outcome=None, *, archive_directory=None):
    timings = timings if timings is not None else {}
    now = utc_now()
    with factory() as db:
        article = db.get(Article, claim.article_id)
        if article is None:
            return "stale_lease", 0
        # Same lock order as ingestion: article identity first, queue row last.
        # Otherwise a duplicate feed can hold ticker locks while enqueue waits
        # on this worker, and this worker can wait on that feed's ticker insert.
        acquire_article_dedupe_locks(db, article.canonical_url_hash, article.title_normalized_hash)
        db.refresh(article)
        now = utc_now()
        job = db.scalar(select(ArticleEnrichment).where(
            ArticleEnrichment.article_id == claim.article_id,
            ArticleEnrichment.lease_token == claim.token,
            ArticleEnrichment.status == "running",
            ArticleEnrichment.available_at > now,
        ).with_for_update())
        if job is None:
            return "stale_lease", 0
        source = db.get(Source, job.source_id)
        config = PAGE_FETCH_CONFIGS.get(source.code) if source else None
        if not source or not source.enabled or not article or config is None:
            job.status, job.completed_at, job.lease_token = "expired", now, None
            db.commit()
            return "expired", 0
        if page_outcome is not None and not page_outcome.attempted:
            # claim_one reserved one attempt; a control/cache hit made no request.
            job.attempts = max(0, job.attempts - 1)
        if html is None:
            if page_outcome is not None and page_outcome.kind in {"refused", "unknown_in_flight"}:
                job.status, job.lease_token = "held", None
                db.commit()
                return "held", 0
            job.status = "pending"
            job.lease_token = None
            job.available_at = now + timedelta(seconds=min(3600, 60 * 2 ** min(max(job.attempts - 1, 0), 6)))
            if page_outcome is not None and page_outcome.retry_at is not None:
                job.available_at = max(job.available_at, to_utc(page_outcome.retry_at))
            db.commit()
            return "retry", 0
        # Load validation AFTER the HTTP request, so changes to active symbols
        # or fund keyword rules while it was in flight are respected.
        validation_started = time.perf_counter()
        context = load_ticker_context(db)
        hits = _verified_ticker_hits(source.code, _extract_source_html_tickers(
            article.title, article.summary or "", claim.page_url, claim.feed_url,
            context.known_symbols, html, config, symbol_keywords=context.symbol_keywords,
        ))
        existing = {row.ticker_id: row for row in db.scalars(select(ArticleTicker).where(
            ArticleTicker.article_id == article.id,
        ))}
        additions = {symbol: hit for symbol, hit in hits.items()
                     if context.symbol_to_id.get(symbol) not in existing}
        _upsert_article_tickers(db, article.id, additions, context.symbol_to_id,
                               existing_rows=existing, prune_missing=False)
        changed = len(additions)
        job.status = "notify" if changed else "done"
        job.completed_at = None if changed else now
        job.lease_token = claim.token if changed else None
        # Keep the notification leased until this worker delivers it or dies.
        job.available_at = now + timedelta(seconds=LEASE_SECONDS)
        timings["validation_ms"] = round((time.perf_counter() - validation_started) * 1000, 3)
        archive_metadata = dict(source_code=source.code, page_url=claim.page_url,
                                article_url=article.canonical_url, title=article.title,
                                published_at=to_utc(article.published_at).isoformat())
        final_status = job.status
        commit_started = time.perf_counter()
        db.commit()
        timings["commit_ms"] = round((time.perf_counter() - commit_started) * 1000, 3)
        timings["committed_at"] = utc_now().isoformat()
    # No DB connection/row lock or network work is retained for disk formatting.
    # Failure cannot put a completed page/notification back in the request queue.
    if archive_directory is not None:
        from app.reader_archive import save_publisher_archive
        try:
            timings['reader_archive'] = save_publisher_archive(
                archive_directory, claim.article_id, html, **archive_metadata)
        except (ValueError, OSError, TypeError):
            timings['reader_archive'] = 'unavailable'
            logger.warning('Reader archive unavailable article_id=%s', claim.article_id, exc_info=True)
    return final_status, changed


def process_one(factory, *, timeout_seconds=4, archive_directory=None):
    claim = claim_one(factory)
    if claim is None:
        return None
    started = time.perf_counter()
    page_ms = 0.0
    changed = 0
    timings = {}
    if claim.notify_only:
        status = "notify"
    else:
        config = PAGE_FETCH_CONFIGS.get(claim.source_code)
        # No database connection or row lock is retained during HTTP I/O.
        page_started = time.perf_counter()
        reset_page_outcome()
        html = _fetch_source_page_html(claim.page_url, timeout_seconds, config) if config else None
        page_ms = (time.perf_counter() - page_started) * 1000
        status, changed = _finish(factory, claim, html, timings, page_outcome=get_page_outcome(),
                                 archive_directory=archive_directory)
    notify_started = time.perf_counter()
    notified = _deliver_notification(factory, claim) if status == "notify" else None
    logger.info("article_enrichment_latency %s", json.dumps({
        "article_id": claim.article_id, "source": claim.source_code,
        "queued_at": claim.queued_at.isoformat(), "finished_at": utc_now().isoformat(),
        "queue_age_seconds": round((utc_now() - claim.queued_at).total_seconds(), 3),
        "page_lookup_ms": round(page_ms, 3),
        "worker_ms": round((time.perf_counter() - started) * 1000, 3),
        "added_tickers": changed, "status": status, "notification_ok": notified,
        "notification_call_ms": round((time.perf_counter() - notify_started) * 1000, 3) if notified is not None else None,
        **timings,
    }, sort_keys=True))
    return {"status": status, "added_tickers": changed, "notification_ok": notified}


class ArticleEnrichmentWorker:
    def __init__(self, factory, *, timeout_seconds=4, archive_directory=None):
        self.factory = factory
        self.timeout_seconds = min(timeout_seconds, 10)
        self.archive_directory = archive_directory
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="article-enrichment", daemon=True)
        self._thread.start()
        logger.info("Article enrichment worker started (batch=%s, cooldown=%ss)", BATCH_SIZE, POLL_SECONDS)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _loop(self):
        while not self._stop.is_set():
            try:
                for _ in range(BATCH_SIZE):
                    if self._stop.is_set() or process_one(self.factory, timeout_seconds=self.timeout_seconds,
                                                        archive_directory=self.archive_directory) is None:
                        break
            except Exception:
                # Claimed work remains durable and becomes available after its lease.
                logger.exception("Article enrichment worker failed; lease will recover")
            self._stop.wait(POLL_SECONDS)
