from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from sqlalchemy import and_, delete, select, tuple_
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app import http_client
from app.models import FeedPollState, IngestionRun, RawFeedItem, Ticker
from app.ticker_loader import load_tickers_from_csv


logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "cef-news-feed/0.1 (+local)",
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.1",
}

_tickers_csv_mtime_cache: dict[str, float] = {}
_tickers_csv_mtime_cache_lock = threading.Lock()
_last_raw_feed_prune_monotonic: float | None = None
_last_raw_feed_prune_lock = threading.Lock()


def _fetch_feed_with_retries(
    *,
    feed_url: str,
    timeout_seconds: int,
    max_attempts: int,
    backoff_seconds: float,
    backoff_jitter_seconds: float,
    extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
    last_error: httpx.HTTPStatusError | httpx.RequestError | None = None
    headers = dict(REQUEST_HEADERS)
    if extra_headers:
        headers.update({str(k): str(v) for k, v in extra_headers.items() if v})
    for attempt in range(1, max_attempts + 1):
        try:
            response = http_client.get_http_client().get(
                feed_url,
                timeout=timeout_seconds,
                headers=headers,
            )
            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code == 429:
                retry_after = _parse_retry_after_seconds(
                    getattr(getattr(response, "headers", None), "get", lambda _k: None)(
                        "Retry-After"
                    )
                )
                if attempt >= max_attempts:
                    response.raise_for_status()
                sleep_seconds = (
                    retry_after
                    if retry_after is not None
                    else (backoff_seconds * (2 ** (attempt - 1)))
                    + random.uniform(0.0, max(0.0, backoff_jitter_seconds))
                )
                logger.warning(
                    "Feed request attempt %s/%s got 429 for %s; retry_after=%s sleep=%.3fs",
                    attempt,
                    max_attempts,
                    feed_url,
                    retry_after,
                    sleep_seconds,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                continue
            response.raise_for_status()
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise

            sleep_seconds = (backoff_seconds * (2 ** (attempt - 1))) + random.uniform(
                0.0, max(0.0, backoff_jitter_seconds)
            )
            logger.warning(
                "Feed request attempt %s/%s failed for %s: %s",
                attempt,
                max_attempts,
                feed_url,
                exc,
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError("feed fetch retry loop exited unexpectedly")


def _parse_retry_after_seconds(header_value: str | None) -> float | None:
    if header_value is None:
        return None
    value = str(header_value).strip()
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (ValueError, TypeError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(
        0.0,
        (
            retry_at.astimezone(timezone.utc) - datetime.now(timezone.utc)
        ).total_seconds(),
    )


def _get_or_create_feed_poll_state(db: Session, feed_url: str) -> FeedPollState:
    state = db.scalar(
        select(FeedPollState).where(FeedPollState.feed_url == feed_url).limit(1)
    )
    if state is not None:
        return state

    candidate = FeedPollState(feed_url=feed_url)
    try:
        with db.begin_nested():
            db.add(candidate)
            db.flush()
        return candidate
    except IntegrityError:
        state = db.scalar(
            select(FeedPollState).where(FeedPollState.feed_url == feed_url).limit(1)
        )
        if state is None:
            raise
        return state


def _get_feed_conditional_headers(feed_state: FeedPollState | None) -> dict[str, str]:
    if feed_state is None:
        return {}
    headers: dict[str, str] = {}
    if feed_state.etag:
        headers["If-None-Match"] = feed_state.etag
    if feed_state.last_modified:
        headers["If-Modified-Since"] = feed_state.last_modified
    return headers


def _update_feed_http_cache(
    feed_state: FeedPollState, response: httpx.Response
) -> None:
    headers = getattr(response, "headers", None)
    if headers is None or not hasattr(headers, "get"):
        return
    etag = headers.get("ETag")
    last_modified = headers.get("Last-Modified")
    if etag:
        feed_state.etag = str(etag)
    if last_modified:
        feed_state.last_modified = str(last_modified)


def _compute_feed_failure_backoff_seconds(
    failure_count: int,
    *,
    base_seconds: float,
    max_seconds: float,
) -> float:
    base = max(0.0, float(base_seconds))
    cap = max(0.0, float(max_seconds))
    if base <= 0.0 or cap <= 0.0:
        return 0.0
    exponent = max(0, int(failure_count) - 1)
    return min(base * (2**exponent), cap)


def _mark_feed_failure_backoff(
    feed_state: FeedPollState,
    *,
    now_utc: datetime,
    base_seconds: float,
    max_seconds: float,
) -> None:
    next_failure_count = max(0, int(feed_state.failure_count or 0)) + 1
    delay_seconds = _compute_feed_failure_backoff_seconds(
        next_failure_count,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    feed_state.failure_count = next_failure_count
    feed_state.last_failure_at = now_utc
    feed_state.backoff_until = (
        now_utc + timedelta(seconds=delay_seconds) if delay_seconds > 0 else now_utc
    )


def _reset_feed_failure_backoff(feed_state: FeedPollState) -> None:
    feed_state.failure_count = 0
    feed_state.last_failure_at = None
    feed_state.backoff_until = None


def _should_run_raw_feed_prune(interval_seconds: int) -> bool:
    if interval_seconds <= 0:
        return False

    now = time.monotonic()
    with _last_raw_feed_prune_lock:
        last = _last_raw_feed_prune_monotonic
        if last is not None and (now - last) < interval_seconds:
            return False
        return True


def _mark_raw_feed_prune_done() -> None:
    global _last_raw_feed_prune_monotonic
    with _last_raw_feed_prune_lock:
        _last_raw_feed_prune_monotonic = time.monotonic()


def _load_tickers_from_csv_if_changed(db: Session, csv_path: str) -> dict[str, int]:
    try:
        tickers_exist = db.scalar(select(Ticker.id).limit(1))
    except SQLAlchemyError:
        tickers_exist = None

    if tickers_exist is None:
        return load_tickers_from_csv(db, csv_path)

    try:
        mtime = Path(csv_path).stat().st_mtime
    except OSError:
        return load_tickers_from_csv(db, csv_path)

    with _tickers_csv_mtime_cache_lock:
        last = _tickers_csv_mtime_cache.get(csv_path)
        if last is not None and last == mtime:
            return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

    stats = load_tickers_from_csv(db, csv_path)
    with _tickers_csv_mtime_cache_lock:
        _tickers_csv_mtime_cache[csv_path] = mtime
    return stats


@dataclass(slots=True)
class PreparedFeedEntry:
    entry: dict
    raw_title: str
    title: str
    raw_link: str
    article_url: str
    is_exact_source_url: bool
    published_at: datetime
    raw_guid: str | None


def _prefetch_recorded_raw_keys(
    db: Session,
    *,
    source_id: int,
    prepared_entries: list[PreparedFeedEntry],
) -> tuple[set[str], set[tuple[str, datetime]]]:
    if not prepared_entries:
        return set(), set()

    guids = {item.raw_guid for item in prepared_entries if item.raw_guid}
    pairs = {(item.raw_link, item.published_at) for item in prepared_entries}

    existing_guids: set[str] = set()
    if guids:
        existing_guids = {
            guid
            for guid in db.scalars(
                select(RawFeedItem.raw_guid).where(
                    and_(
                        RawFeedItem.source_id == source_id,
                        RawFeedItem.article_id.is_not(None),
                        RawFeedItem.raw_guid.in_(list(guids)),
                    )
                )
            ).all()
            if guid
        }

    existing_pairs: set[tuple[str, datetime]] = set()
    if pairs:
        rows = db.execute(
            select(RawFeedItem.raw_link, RawFeedItem.raw_pub_date).where(
                and_(
                    RawFeedItem.source_id == source_id,
                    RawFeedItem.article_id.is_not(None),
                    tuple_(RawFeedItem.raw_link, RawFeedItem.raw_pub_date).in_(
                        list(pairs)
                    ),
                )
            )
        ).all()
        existing_pairs = {
            (str(link), pub_date) for link, pub_date in rows if link and pub_date
        }

    return existing_guids, existing_pairs


def reconcile_stale_ingestion_runs(
    db: Session,
    *,
    stale_after_seconds: int,
    now: datetime | None = None,
) -> int:
    if stale_after_seconds <= 0:
        return 0

    current_time = now or datetime.now(timezone.utc)
    cutoff = current_time - timedelta(seconds=stale_after_seconds)
    stale_runs = db.scalars(
        select(IngestionRun).where(
            and_(
                IngestionRun.status == "running",
                IngestionRun.finished_at.is_(None),
                IngestionRun.started_at <= cutoff,
            )
        )
    ).all()
    if not stale_runs:
        return 0

    reason = (
        f"Marked failed after exceeding stale run timeout ({stale_after_seconds}s)."
    )
    for run in stale_runs:
        run.status = "failed"
        run.finished_at = current_time
        if not (run.error_text or "").strip():
            run.error_text = reason
    db.commit()
    return len(stale_runs)


def prune_raw_feed_items(
    db: Session,
    *,
    retention_days: int,
    batch_size: int,
    max_batches: int,
    now: datetime | None = None,
) -> int:
    if retention_days <= 0 or batch_size <= 0 or max_batches <= 0:
        return 0

    current_time = now or datetime.now(timezone.utc)
    cutoff = current_time - timedelta(days=retention_days)
    total_deleted = 0

    for _ in range(max_batches):
        stale_ids = db.scalars(
            select(RawFeedItem.id)
            .where(RawFeedItem.fetched_at < cutoff)
            .order_by(RawFeedItem.id.asc())
            .limit(batch_size)
        ).all()
        if not stale_ids:
            break

        db.execute(delete(RawFeedItem).where(RawFeedItem.id.in_(stale_ids)))
        db.commit()
        total_deleted += len(stale_ids)

        if len(stale_ids) < batch_size:
            break

    return total_deleted
