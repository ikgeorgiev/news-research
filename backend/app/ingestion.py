from __future__ import annotations

import logging
import random
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import TypedDict
from urllib.parse import parse_qs, urlparse, urlunparse

import feedparser
import requests
from sqlalchemy import and_, delete, desc, func, select, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.models import Article, ArticleTicker, FeedPollState, IngestionRun, RawFeedItem, Source, Ticker
from app.push_alerts import check_and_send_alerts
from app.sources import build_source_feeds, seed_sources
from app.ticker_loader import load_tickers_from_csv
from app.utils import canonicalize_url, clean_summary_text, normalize_title, parse_datetime, sha256_str, to_json_safe

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "cef-news-feed/0.1 (+local)",
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.1",
}
SOURCE_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; cef-news-feed/0.1; +local)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.1",
}
GENERAL_SOURCE_CODE = "businesswire"
SOURCE_PAGE_CACHE_TTL_SECONDS = 6 * 60 * 60
SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS = 60
SOURCE_PAGE_CACHE_MAX_ITEMS = 1024

EXCHANGE_PATTERN = re.compile(r"\b(?:NYSE|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b")
PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")
TOKEN_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")
HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>", flags=re.IGNORECASE | re.DOTALL
)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
TABLE_CELL_SYMBOL_PATTERN = re.compile(
    r"<td[^>]*>(?:\s|<[^>]+>)*([A-Z][A-Z0-9\.\-]{0,9})(?:\s|<[^>]+>)*</td>",
    flags=re.IGNORECASE,
)
STOPWORDS = {
    "A",
    "AN",
    "AND",
    "ARE",
    "AS",
    "AT",
    "BY",
    "CEO",
    "ETF",
    "FOR",
    "FROM",
    "IN",
    "INC",
    "IS",
    "IT",
    "NAV",
    "NEW",
    "NOT",
    "OF",
    "ON",
    "OR",
    "Q",
    "THE",
    "TO",
    "US",
    "USA",
    "WITH",
}
# Symbols that are also common finance words should not be inferred via generic token scans.
# They remain matchable via stronger signals (context, exchange pattern, or parenthesized symbol).
AMBIGUOUS_TOKEN_SYMBOLS = {
    "FUND",
    "IDE",
}
_source_page_cache: OrderedDict[str, tuple[float, str | None]] = OrderedDict()
_source_page_cache_lock = threading.Lock()
_tickers_csv_mtime_cache: dict[str, float] = {}
_tickers_csv_mtime_cache_lock = threading.Lock()
_last_raw_feed_prune_monotonic: float | None = None
_last_raw_feed_prune_lock = threading.Lock()


@dataclass(slots=True, frozen=True)
class SourcePageConfig:
    source_code: str
    hostname_suffix: str
    table_match_type: str


PAGE_FETCH_CONFIGS: dict[str, SourcePageConfig] = {
    "businesswire": SourcePageConfig("businesswire", "businesswire.com", "bw_table"),
    "prnewswire": SourcePageConfig("prnewswire", "prnewswire.com", "prn_table"),
    "globenewswire": SourcePageConfig("globenewswire", "globenewswire.com", "gnw_table"),
}


class TickerSyncStats(TypedDict):
    loaded: int
    created: int
    updated: int
    unchanged: int


class IngestFeedResult(TypedDict):
    source: str
    feed_url: str
    status: str
    items_seen: int
    items_inserted: int
    error: str | None


class SourceRemapStats(TypedDict):
    source_code: str
    processed: int
    articles_with_hits: int
    remapped_articles: int
    only_unmapped: bool


BusinessWireRemapStats = SourceRemapStats


class IngestionCycleResult(TypedDict):
    total_feeds: int
    total_items_seen: int
    total_items_inserted: int
    failed_feeds: int
    ticker_sync: TickerSyncStats
    businesswire_remap: SourceRemapStats
    source_remaps: list[SourceRemapStats]
    feeds: list[IngestFeedResult]
    push_alerts: dict[str, int]


def _clamp_label(value: str | None, max_len: int = 120) -> str:
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _hash_hex_to_signed_bigint(value: str) -> int:
    raw = int(value[:16], 16)
    if raw >= 2**63:
        raw -= 2**64
    return raw


def _acquire_dedupe_locks(db: Session, url_hash: str, title_hash: str) -> None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return

    # Acquire in sorted order to avoid deadlock when multiple workers lock same keys.
    for key in sorted({_hash_hex_to_signed_bigint(url_hash), _hash_hex_to_signed_bigint(title_hash)}):
        db.execute(select(func.pg_advisory_xact_lock(key)))


def _fetch_feed_with_retries(
    *,
    feed_url: str,
    timeout_seconds: int,
    max_attempts: int,
    backoff_seconds: float,
    backoff_jitter_seconds: float,
    extra_headers: dict[str, str] | None = None,
) -> requests.Response:
    last_error: requests.RequestException | None = None
    headers = dict(REQUEST_HEADERS)
    if extra_headers:
        headers.update({str(k): str(v) for k, v in extra_headers.items() if v})
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(feed_url, timeout=timeout_seconds, headers=headers)
            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code == 429:
                retry_after = _parse_retry_after_seconds(
                    getattr(getattr(response, "headers", None), "get", lambda _k: None)("Retry-After")
                )
                if attempt >= max_attempts:
                    response.raise_for_status()
                sleep_seconds = (
                    retry_after
                    if retry_after is not None
                    else (backoff_seconds * (2 ** (attempt - 1))) + random.uniform(
                        0.0, max(0.0, backoff_jitter_seconds)
                    )
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
        except requests.RequestException as exc:
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
    except Exception:
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _get_or_create_feed_poll_state(db: Session, feed_url: str) -> FeedPollState:
    state = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == feed_url).limit(1))
    if state is not None:
        return state

    candidate = FeedPollState(feed_url=feed_url)
    try:
        # Savepoint keeps concurrent workers from aborting ingest on unique collisions.
        with db.begin_nested():
            db.add(candidate)
            db.flush()
        return candidate
    except IntegrityError:
        state = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == feed_url).limit(1))
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


def _update_feed_http_cache(feed_state: FeedPollState, response: requests.Response) -> None:
    # Keep this defensive: unit tests use fake response objects that may not have headers/status_code.
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
    return min(base * (2 ** exponent), cap)


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
    feed_state.backoff_until = now_utc + timedelta(seconds=delay_seconds) if delay_seconds > 0 else now_utc


def _reset_feed_failure_backoff(feed_state: FeedPollState) -> None:
    feed_state.failure_count = 0
    feed_state.last_failure_at = None
    feed_state.backoff_until = None


def _should_run_raw_feed_prune(interval_seconds: int) -> bool:
    """
    Pruning can be expensive on large tables. Running it every ingestion cycle
    (default scheduler interval: 60s) increases tail latency.

    This gate runs prune at most once per interval *per process*.
    The timestamp is NOT set here — the caller must call
    ``_mark_raw_feed_prune_done()`` after a successful prune so that a
    failed prune is retried on the next cycle.
    """

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
    """
    CSV sync is correctness-critical but shouldn't dominate runtime.

    Strategy:
    - If the ticker table is empty, always load (useful after DB resets).
    - Otherwise, load only when the CSV mtime changes (in-process cache).

    Note: Admin endpoint `/admin/tickers/reload` should still call the loader directly
    to force reload + remap.
    """

    try:
        tickers_exist = db.scalar(select(Ticker.id).limit(1))
    except Exception:
        tickers_exist = None

    # If DB is empty, skip mtime cache and force a load.
    if tickers_exist is None:
        return load_tickers_from_csv(db, csv_path)

    try:
        mtime = Path(csv_path).stat().st_mtime
    except Exception:
        # Missing/unreadable file: preserve the previous behavior (loader returns zeros if file missing).
        return load_tickers_from_csv(db, csv_path)

    with _tickers_csv_mtime_cache_lock:
        last = _tickers_csv_mtime_cache.get(csv_path)
        if last is not None and last == mtime:
            return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}
        _tickers_csv_mtime_cache[csv_path] = mtime

    return load_tickers_from_csv(db, csv_path)


@dataclass(slots=True)
class PreparedFeedEntry:
    """
    Pre-parsed feed entry fields used in the ingest loop.

    This keeps per-entry logic readable while enabling batched dedupe queries
    (instead of one SELECT per entry).
    """

    entry: dict
    raw_title: str
    title: str
    link: str
    published_at: datetime
    raw_guid: str | None


def _prefetch_recorded_raw_keys(
    db: Session,
    *,
    source_id: int,
    prepared_entries: list[PreparedFeedEntry],
) -> tuple[set[str], set[tuple[str, datetime]]]:
    """
    Batch-load existing RawFeedItem keys for dedupe.

    The previous approach did 1-2 DB queries per entry to decide whether a raw feed
    row already existed. On large `raw_feed_items` tables that becomes the dominant
    runtime cost.

    This function does two bounded queries per feed and then relies on in-memory
    membership checks during the ingest loop.
    """

    if not prepared_entries:
        return set(), set()

    guids = {item.raw_guid for item in prepared_entries if item.raw_guid}
    pairs = {(item.link, item.published_at) for item in prepared_entries}

    existing_guids: set[str] = set()
    if guids:
        existing_guids = {
            guid
            for guid in db.scalars(
                select(RawFeedItem.raw_guid).where(
                    and_(RawFeedItem.source_id == source_id, RawFeedItem.raw_guid.in_(list(guids)))
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
                    tuple_(RawFeedItem.raw_link, RawFeedItem.raw_pub_date).in_(list(pairs)),
                )
            )
        ).all()
        existing_pairs = {(str(link), pub_date) for link, pub_date in rows if link and pub_date}

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

    reason = f"Marked failed after exceeding stale run timeout ({stale_after_seconds}s)."
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


def _extract_provider(entry: feedparser.FeedParserDict, fallback: str) -> str:
    source = entry.get("source")
    if isinstance(source, dict):
        title = source.get("title")
        if title:
            return _clamp_label(str(title))
    if isinstance(source, str) and source.strip():
        return _clamp_label(source)
    return _clamp_label(fallback)


def _parse_context_symbols(feed_url: str) -> list[str]:
    parsed = urlparse(feed_url)
    values = parse_qs(parsed.query).get("s", [])
    symbols: list[str] = []
    for value in values:
        symbols.extend([token.strip().upper() for token in value.split(",") if token.strip()])
    return symbols


def _canonical_article_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _is_source_article_url(url: str, hostname_suffix: str) -> bool:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not hostname:
        return False
    return hostname == hostname_suffix or hostname.endswith("." + hostname_suffix)


# Backward-compat wrappers (tests import these directly).
def _canonical_businesswire_article_url(url: str) -> str:
    return _canonical_article_url(url)


def _is_businesswire_article_url(url: str) -> bool:
    return _is_source_article_url(url, "businesswire.com")


def _cache_source_page(url: str, fetched_at: float, html_text: str | None) -> None:
    with _source_page_cache_lock:
        _source_page_cache[url] = (fetched_at, html_text)
        _source_page_cache.move_to_end(url)

        while len(_source_page_cache) > SOURCE_PAGE_CACHE_MAX_ITEMS:
            _source_page_cache.popitem(last=False)


def _fetch_source_page_html(url: str, timeout_seconds: int, config: SourcePageConfig) -> str | None:
    fetch_url = _canonical_article_url(url)
    if not _is_source_article_url(fetch_url, config.hostname_suffix):
        return None

    now = time.time()
    with _source_page_cache_lock:
        cached = _source_page_cache.get(fetch_url)
        if cached is not None:
            fetched_at, cached_html = cached
            ttl_seconds = (
                SOURCE_PAGE_CACHE_TTL_SECONDS
                if cached_html is not None
                else SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS
            )
            if now - fetched_at <= ttl_seconds:
                _source_page_cache.move_to_end(fetch_url)
                return cached_html

    html_text: str | None = None
    try:
        response = requests.get(
            fetch_url,
            timeout=timeout_seconds,
            headers=SOURCE_PAGE_HEADERS,
        )
        if response.ok and response.text:
            html_text = response.text
    except Exception:
        html_text = None

    _cache_source_page(fetch_url, time.time(), html_text)
    return html_text


def _fetch_businesswire_page_html(url: str, timeout_seconds: int) -> str | None:
    return _fetch_source_page_html(url, timeout_seconds, PAGE_FETCH_CONFIGS["businesswire"])


def _extract_table_cell_symbols_from_html(html_text: str, known_symbols: set[str]) -> set[str]:
    hits: set[str] = set()
    for raw_symbol in TABLE_CELL_SYMBOL_PATTERN.findall(html_text):
        symbol = raw_symbol.upper().strip()
        if symbol in STOPWORDS:
            continue
        if symbol in known_symbols:
            hits.add(symbol)
    return hits


def _html_to_plain_text(html_text: str) -> str:
    text = HTML_SCRIPT_STYLE_PATTERN.sub(" ", html_text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_source_fallback_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    timeout_seconds: int,
    config: SourcePageConfig,
) -> dict[str, tuple[str, float]]:
    html_text = _fetch_source_page_html(link, timeout_seconds, config)
    if not html_text:
        return {}

    plain_text = _html_to_plain_text(html_text)
    enriched_summary = " ".join([part for part in [summary, plain_text] if part]).strip()
    hits = _extract_entry_tickers(
        title,
        enriched_summary,
        link,
        feed_url,
        known_symbols,
        include_token=False,
    )

    for symbol in _extract_table_cell_symbols_from_html(html_text, known_symbols):
        existing = hits.get(symbol)
        if existing is None or 0.84 > existing[1]:
            hits[symbol] = (config.table_match_type, 0.84)

    return hits


def _extract_businesswire_fallback_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    timeout_seconds: int,
) -> dict[str, tuple[str, float]]:
    return _extract_source_fallback_tickers(
        title, summary, link, feed_url, known_symbols, timeout_seconds,
        PAGE_FETCH_CONFIGS["businesswire"],
    )


def _extract_entry_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    *,
    include_token: bool = True,
) -> dict[str, tuple[str, float]]:
    hits: dict[str, tuple[str, float]] = {}

    def add(symbol: str, match_type: str, confidence: float) -> None:
        if symbol not in known_symbols:
            return
        existing = hits.get(symbol)
        if existing is None or confidence > existing[1]:
            hits[symbol] = (match_type, confidence)

    context_symbols = _parse_context_symbols(feed_url)
    # Yahoo URLs are often batched (s=SYM1,SYM2,...). Applying all context symbols
    # to each entry creates large false-positive mapping. Only trust context when
    # feed URL is scoped to a single symbol.
    if len(context_symbols) == 1:
        add(context_symbols[0], "context", 0.93)

    text = " ".join([title or "", summary or "", link or ""])

    for symbol in EXCHANGE_PATTERN.findall(text):
        add(symbol.upper(), "exchange", 0.88)

    for symbol in PAREN_SYMBOL_PATTERN.findall(text):
        add(symbol.upper(), "paren", 0.75)

    if include_token:
        for symbol in TOKEN_PATTERN.findall(text):
            upper = symbol.upper()
            if upper in STOPWORDS:
                continue
            if upper in AMBIGUOUS_TOKEN_SYMBOLS:
                continue
            add(upper, "token", 0.62)

    return hits


def _should_persist_entry(source_code: str, ticker_hits: dict[str, tuple[str, float]]) -> bool:
    # Persist all Business Wire items (general stream) and ticker-mapped items from other sources.
    return source_code == GENERAL_SOURCE_CODE or bool(ticker_hits)


def _upsert_article(
    db: Session,
    *,
    source_code: str,
    canonical_url: str,
    title: str,
    summary: str | None,
    published_at: datetime,
    source_name: str,
    provider_name: str,
) -> tuple[Article, bool, bool]:
    url_hash = sha256_str(canonical_url)
    title_hash = sha256_str(normalize_title(title))
    summary_norm = normalize_title(summary or "")
    content_hash = sha256_str(f"{title_hash}|{summary_norm[:300]}")
    cluster_key = title_hash

    source_name = _clamp_label(source_name)
    provider_name = _clamp_label(provider_name)
    _acquire_dedupe_locks(db, url_hash, title_hash)

    window_start = published_at - timedelta(hours=48)
    window_end = published_at + timedelta(hours=48)

    def _find_title_window_match() -> Article | None:
        return db.scalar(
            select(Article)
            .where(
                and_(
                    Article.title_normalized_hash == title_hash,
                    Article.published_at >= window_start,
                    Article.published_at <= window_end,
                )
            )
            .order_by(desc(Article.id))
            .limit(1)
        )

    article = db.scalar(select(Article).where(Article.canonical_url_hash == url_hash))
    matched_by_url = article is not None
    created = False

    # Business Wire can emit multiple distinct stories with the same headline
    # and close timestamps. Keep them separate by URL.
    if article is None and source_code != GENERAL_SOURCE_CODE:
        article = _find_title_window_match()
        matched_by_url = False

    if article is None:
        new_article = Article(
            canonical_url=canonical_url,
            canonical_url_hash=url_hash,
            title=title,
            summary=summary,
            published_at=published_at,
            source_name=source_name,
            provider_name=provider_name,
            content_hash=content_hash,
            title_normalized_hash=title_hash,
            cluster_key=cluster_key,
        )
        try:
            # Savepoint prevents a concurrent uniqueness collision from aborting the feed transaction.
            with db.begin_nested():
                db.add(new_article)
                db.flush()
            article = new_article
            created = True
        except IntegrityError:
            # Another worker inserted the same URL/title-window match first. Re-read and continue.
            article = db.scalar(select(Article).where(Article.canonical_url_hash == url_hash))
            matched_by_url = article is not None
            if article is None and source_code != GENERAL_SOURCE_CODE:
                article = _find_title_window_match()
                matched_by_url = False
            if article is None:
                raise

    if article is None:
        raise RuntimeError("article upsert failed to resolve target row")

    if not created:
        article.title = title
        article.source_name = source_name
        article.provider_name = provider_name
        article.content_hash = content_hash
        article.cluster_key = cluster_key
        if summary:
            if matched_by_url:
                # For exact URL matches, trust the feed payload for this URL.
                article.summary = summary
            elif not article.summary or len(summary) > len(article.summary):
                article.summary = summary
        if article.published_at is None or published_at > article.published_at:
            article.published_at = published_at

    return article, created, matched_by_url


def _upsert_article_tickers(
    db: Session,
    article_id: int,
    ticker_hits: dict[str, tuple[str, float]],
    symbol_to_id: dict[str, int],
    *,
    existing_rows: dict[int, ArticleTicker] | None = None,
    prune_missing: bool = False,
) -> None:
    if not ticker_hits and not prune_missing:
        return

    if existing_rows is None:
        existing = {
            row.ticker_id: row
            for row in db.scalars(select(ArticleTicker).where(ArticleTicker.article_id == article_id)).all()
        }
    else:
        existing = existing_rows

    resolved_ticker_ids: set[int] = set()
    for symbol, (match_type, confidence) in ticker_hits.items():
        ticker_id = symbol_to_id.get(symbol)
        if ticker_id is None:
            continue
        resolved_ticker_ids.add(ticker_id)

        row = existing.get(ticker_id)
        if row is None:
            candidate = ArticleTicker(
                article_id=article_id,
                ticker_id=ticker_id,
                match_type=match_type,
                confidence=confidence,
            )
            try:
                # Savepoint prevents concurrent uq_article_ticker collisions from aborting feed ingest.
                with db.begin_nested():
                    db.add(candidate)
                    db.flush()
                row = candidate
                existing[ticker_id] = row
            except IntegrityError:
                row = db.scalar(
                    select(ArticleTicker).where(
                        and_(
                            ArticleTicker.article_id == article_id,
                            ArticleTicker.ticker_id == ticker_id,
                        )
                    )
                )
                if row is None:
                    raise

        if confidence > row.confidence:
            row.confidence = confidence
            row.match_type = match_type

    if prune_missing:
        # For exact URL matches we can trust current extraction and remove stale mappings.
        for ticker_id, row in list(existing.items()):
            if ticker_id in resolved_ticker_ids:
                continue
            db.delete(row)
            existing.pop(ticker_id, None)


def ingest_feed(
    db: Session,
    *,
    source: Source,
    feed_url: str,
    known_symbols: set[str],
    symbol_to_id: dict[str, int],
    timeout_seconds: int,
    fetch_max_attempts: int = 3,
    fetch_backoff_seconds: float = 1.0,
    fetch_backoff_jitter_seconds: float = 0.3,
    enable_conditional_get: bool = True,
    failure_backoff_base_seconds: float = 30.0,
    failure_backoff_max_seconds: float = 600.0,
) -> IngestFeedResult:
    run = IngestionRun(
        source_id=source.id,
        feed_url=feed_url,
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    items_seen = 0
    items_inserted = 0
    committed_items_inserted = 0
    status = "success"
    error_text: str | None = None
    entry_errors = 0
    feed_state = _get_or_create_feed_poll_state(db, feed_url)

    try:
        now_utc = datetime.now(timezone.utc)
        backoff_until = feed_state.backoff_until
        if backoff_until is not None and _to_utc(backoff_until) > now_utc:
            status = "skipped_backoff"
            error_text = f"Feed is in backoff until {_to_utc(backoff_until).isoformat()}"
            committed_items_inserted = 0
        else:
            conditional_headers = _get_feed_conditional_headers(feed_state) if enable_conditional_get else {}
            response = _fetch_feed_with_retries(
                feed_url=feed_url,
                timeout_seconds=timeout_seconds,
                max_attempts=fetch_max_attempts,
                backoff_seconds=fetch_backoff_seconds,
                backoff_jitter_seconds=fetch_backoff_jitter_seconds,
                extra_headers=conditional_headers,
            )
            _update_feed_http_cache(feed_state, response)
            _reset_feed_failure_backoff(feed_state)

            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code == 304:
                # Feed content has not changed since the last run; skip parse + DB work.
                committed_items_inserted = 0
            else:
                parsed = feedparser.parse(response.content)

                source_name = source.name
                feed_title = parsed.feed.get("title") if isinstance(parsed.feed, dict) else None
                if source.code != "yahoo" and feed_title:
                    source_name = _clamp_label(str(feed_title))

                entries = list(getattr(parsed, "entries", []) or [])
                items_seen = len(entries)

                # Feed-level dedupe prefetch: keeps ingest fast even when raw_feed_items grows large.
                now_utc = datetime.now(timezone.utc)
                prepared_entries: list[PreparedFeedEntry] = []
                for entry in entries:
                    raw_title = str(entry.get("title") or "").strip()
                    # Never fall back to raw feed title so stored titles remain plain text.
                    title = clean_summary_text(raw_title)
                    link = canonicalize_url(str(entry.get("link") or "").strip())
                    if not title or not link:
                        continue

                    published_at = parse_datetime(entry.get("published") or entry.get("updated")) or now_utc
                    raw_guid = str(entry.get("id") or entry.get("guid") or "").strip() or None
                    prepared_entries.append(
                        PreparedFeedEntry(
                            entry=entry,
                            raw_title=raw_title,
                            title=title,
                            link=link,
                            published_at=published_at,
                            raw_guid=raw_guid,
                        )
                    )

                recorded_guids, recorded_pairs = _prefetch_recorded_raw_keys(
                    db,
                    source_id=source.id,
                    prepared_entries=prepared_entries,
                )

                provider_name = _clamp_label(source.name)
                page_config = PAGE_FETCH_CONFIGS.get(source.code)

                for prepared in prepared_entries:
                    try:
                        created_article = False
                        persisted_raw = False
                        persisted_raw_guid: str | None = None
                        persisted_pair: tuple[str, datetime] | None = None

                        # Isolate malformed entries so one bad payload does not roll back the full feed run.
                        with db.begin_nested():
                            if prepared.raw_guid and prepared.raw_guid in recorded_guids:
                                continue
                            if (prepared.link, prepared.published_at) in recorded_pairs:
                                continue

                            raw_summary = (
                                str(prepared.entry.get("summary") or prepared.entry.get("description") or "").strip()
                                or None
                            )
                            summary = clean_summary_text(raw_summary)

                            entry_source_name = _extract_provider(prepared.entry, source_name)
                            ticker_hits = _extract_entry_tickers(
                                prepared.title,
                                summary or "",
                                prepared.link,
                                feed_url,
                                known_symbols,
                            )
                            if not ticker_hits and page_config is not None:
                                ticker_hits = _extract_source_fallback_tickers(
                                    prepared.title,
                                    summary or "",
                                    prepared.link,
                                    feed_url,
                                    known_symbols,
                                    timeout_seconds,
                                    page_config,
                                )

                            allow_existing_unmapped = False
                            if not ticker_hits and source.code != GENERAL_SOURCE_CODE:
                                existing_article_id = db.scalar(
                                    select(Article.id)
                                    .where(Article.canonical_url_hash == sha256_str(prepared.link))
                                    .limit(1)
                                )
                                allow_existing_unmapped = existing_article_id is not None

                            if not _should_persist_entry(source.code, ticker_hits) and not allow_existing_unmapped:
                                continue

                            article, created, matched_by_url = _upsert_article(
                                db,
                                source_code=source.code,
                                canonical_url=prepared.link,
                                title=prepared.title,
                                summary=summary,
                                published_at=prepared.published_at,
                                source_name=source_name,
                                provider_name=provider_name,
                            )
                            created_article = created

                            # New articles never have mappings yet; skip the SELECT in _upsert_article_tickers().
                            existing_rows = {} if created else None
                            _upsert_article_tickers(
                                db,
                                article.id,
                                ticker_hits,
                                symbol_to_id,
                                existing_rows=existing_rows,
                                prune_missing=(source.code != GENERAL_SOURCE_CODE and matched_by_url),
                            )

                            payload = {
                                "title": prepared.raw_title,
                                "link": prepared.link,
                                "published": prepared.entry.get("published") or prepared.entry.get("updated"),
                                "summary": raw_summary,
                                "source": entry_source_name,
                            }
                            db.add(
                                RawFeedItem(
                                    source_id=source.id,
                                    article_id=article.id,
                                    feed_url=feed_url,
                                    raw_guid=prepared.raw_guid,
                                    raw_title=prepared.raw_title,
                                    raw_link=prepared.link,
                                    raw_pub_date=prepared.published_at,
                                    raw_payload_json=to_json_safe(payload),
                                )
                            )
                            persisted_raw = True
                            persisted_raw_guid = prepared.raw_guid
                            persisted_pair = (prepared.link, prepared.published_at)

                        # Only update counters/caches after the savepoint commits successfully.
                        if persisted_raw:
                            if created_article:
                                items_inserted += 1
                            if persisted_raw_guid:
                                recorded_guids.add(persisted_raw_guid)
                            if persisted_pair:
                                recorded_pairs.add(persisted_pair)

                    except Exception as entry_exc:
                        entry_errors += 1
                        error_text = (
                            f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}: {entry_exc}"
                        )
                        continue

                db.commit()
                committed_items_inserted = items_inserted
                if entry_errors > 0 and error_text is None:
                    error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}."
    except Exception as exc:
        db.rollback()
        status = "failed"
        error_text = str(exc)
        committed_items_inserted = 0
        now_utc = datetime.now(timezone.utc)
        feed_state = _get_or_create_feed_poll_state(db, feed_url)
        _mark_feed_failure_backoff(
            feed_state,
            now_utc=now_utc,
            base_seconds=failure_backoff_base_seconds,
            max_seconds=failure_backoff_max_seconds,
        )

    run.status = status
    run.items_seen = items_seen
    run.items_inserted = committed_items_inserted
    run.error_text = error_text
    run.finished_at = datetime.now(timezone.utc)
    db.commit()

    return {
        "source": source.code,
        "feed_url": feed_url,
        "status": status,
        "items_seen": items_seen,
        "items_inserted": committed_items_inserted,
        "error": error_text,
    }


def run_ingestion_cycle(
    db: Session, settings: Settings
) -> IngestionCycleResult:
    stale_runs_fixed = reconcile_stale_ingestion_runs(
        db,
        stale_after_seconds=settings.ingestion_stale_run_timeout_seconds,
    )
    if stale_runs_fixed:
        logger.warning("Marked %s stale ingestion runs as failed", stale_runs_fixed)

    raw_ticker_sync = _load_tickers_from_csv_if_changed(db, settings.tickers_csv_path)
    ticker_sync: TickerSyncStats = {
        "loaded": int(raw_ticker_sync.get("loaded", 0) or 0),
        "created": int(raw_ticker_sync.get("created", 0) or 0),
        "updated": int(raw_ticker_sync.get("updated", 0) or 0),
        "unchanged": int(raw_ticker_sync.get("unchanged", 0) or 0),
    }

    source_feeds = build_source_feeds(settings, db)
    seed_sources(db, source_feeds)

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

    ticker_rows = db.execute(select(Ticker.id, Ticker.symbol).where(Ticker.active.is_(True))).all()
    symbol_to_id = {symbol.upper(): ticker_id for ticker_id, symbol in ticker_rows}
    known_symbols = frozenset(symbol_to_id.keys())

    per_feed: list[IngestFeedResult] = []
    total_seen = 0
    total_inserted = 0
    failed = 0

    max_workers = settings.ingestion_max_workers
    enable_conditional_get = settings.ingestion_enable_conditional_get
    bind = db.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", None) if bind is not None else None
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

    def _ingest_task(source_id: int, source_code: str, feed_url: str) -> IngestFeedResult:
        source_task_lock = source_task_locks[source_id]
        # Keep feeds from the same source serialized even in parallel mode.
        # Raw dedupe keys are source-scoped and pre-fetched per feed, so running
        # same-source feeds concurrently can race and insert duplicate raw rows.
        with source_task_lock:
            if max_workers <= 1:
                source_row = source_map.get(source_code)
                if source_row is None:
                    return {
                        "source": source_code,
                        "feed_url": feed_url,
                        "status": "failed",
                        "items_seen": 0,
                        "items_inserted": 0,
                        "error": "Source row missing (unexpected)",
                    }
                return ingest_feed(
                    db,
                    source=source_row,
                    feed_url=feed_url,
                    known_symbols=known_symbols,
                    symbol_to_id=symbol_to_id,
                    timeout_seconds=settings.request_timeout_seconds,
                    fetch_max_attempts=settings.feed_fetch_max_attempts,
                    fetch_backoff_seconds=settings.feed_fetch_backoff_seconds,
                    fetch_backoff_jitter_seconds=settings.feed_fetch_backoff_jitter_seconds,
                    enable_conditional_get=enable_conditional_get,
                    failure_backoff_base_seconds=settings.feed_failure_backoff_base_seconds,
                    failure_backoff_max_seconds=settings.feed_failure_backoff_max_seconds,
                )

            # Parallel workers: each task uses its own DB session.
            if bind is None:
                raise RuntimeError("DB bind is missing; cannot create per-worker sessions")
            worker_factory = sessionmaker(autoflush=False, autocommit=False, bind=bind)
            with worker_factory() as worker_db:
                source_row = worker_db.scalar(select(Source).where(Source.id == source_id))
                if source_row is None:
                    return {
                        "source": source_code,
                        "feed_url": feed_url,
                        "status": "failed",
                        "items_seen": 0,
                        "items_inserted": 0,
                        "error": f"Source id {source_id} not found",
                    }
                return ingest_feed(
                    worker_db,
                    source=source_row,
                    feed_url=feed_url,
                    known_symbols=known_symbols,
                    symbol_to_id=symbol_to_id,
                    timeout_seconds=settings.request_timeout_seconds,
                    fetch_max_attempts=settings.feed_fetch_max_attempts,
                    fetch_backoff_seconds=settings.feed_fetch_backoff_seconds,
                    fetch_backoff_jitter_seconds=settings.feed_fetch_backoff_jitter_seconds,
                    enable_conditional_get=enable_conditional_get,
                    failure_backoff_base_seconds=settings.feed_failure_backoff_base_seconds,
                    failure_backoff_max_seconds=settings.feed_failure_backoff_max_seconds,
                )

    results: list[IngestFeedResult] = []
    if max_workers <= 1:
        for source_id, source_code, feed_url in tasks:
            results.append(_ingest_task(source_id, source_code, feed_url))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(_ingest_task, source_id, source_code, feed_url): (source_code, feed_url)
                for source_id, source_code, feed_url in tasks
            }
            for future in as_completed(future_to_task):
                source_code, feed_url = future_to_task[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        {
                            "source": source_code,
                            "feed_url": feed_url,
                            "status": "failed",
                            "items_seen": 0,
                            "items_inserted": 0,
                            "error": str(exc),
                        }
                    )

    # Keep results deterministic for logs/reports.
    per_feed = sorted(results, key=lambda r: (r.get("source", ""), r.get("feed_url", "")))
    for result in per_feed:
        total_seen += int(result["items_seen"])
        total_inserted += int(result["items_inserted"])
        if result["status"] == "failed":
            failed += 1

    source_remaps: list[SourceRemapStats] = []
    ticker_changed = ticker_sync["created"] > 0 or ticker_sync["updated"] > 0
    if ticker_changed:
        for code in PAGE_FETCH_CONFIGS:
            source_remaps.append(
                remap_source_articles(db, settings, source_code=code, limit=500, only_unmapped=True)
            )
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

    default_bw_remap: SourceRemapStats = {
        "source_code": "businesswire",
        "processed": 0,
        "articles_with_hits": 0,
        "remapped_articles": 0,
        "only_unmapped": True,
    }
    bw_remap = next(
        (r for r in source_remaps if r["source_code"] == "businesswire"),
        default_bw_remap,
    )
    push_alerts = {
        "scanned": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
    }
    if total_inserted > 0:
        try:
            push_alerts = check_and_send_alerts(db, settings)
        except Exception:
            logger.exception("Push alert processing failed after ingestion cycle")

    return {
        "total_feeds": len(per_feed),
        "total_items_seen": total_seen,
        "total_items_inserted": total_inserted,
        "failed_feeds": failed,
        "ticker_sync": ticker_sync,
        "businesswire_remap": bw_remap,
        "source_remaps": source_remaps,
        "feeds": per_feed,
        "push_alerts": push_alerts,
    }


def remap_source_articles(
    db: Session,
    settings: Settings,
    *,
    source_code: str,
    limit: int = 500,
    only_unmapped: bool = True,
) -> SourceRemapStats:
    config = PAGE_FETCH_CONFIGS.get(source_code)
    if config is None:
        return {
            "source_code": source_code,
            "processed": 0,
            "articles_with_hits": 0,
            "remapped_articles": 0,
            "only_unmapped": only_unmapped,
        }

    ticker_rows = db.execute(select(Ticker.id, Ticker.symbol).where(Ticker.active.is_(True))).all()
    symbol_to_id = {symbol.upper(): ticker_id for ticker_id, symbol in ticker_rows}
    known_symbols = set(symbol_to_id.keys())

    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
    source_exists = (
        select(1)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                Source.code == source_code,
            )
        )
        .correlate(Article)
        .exists()
    )

    query = select(Article).where(source_exists)
    if only_unmapped:
        query = query.where(~mapped_exists)

    rows = db.scalars(
        query.order_by(Article.published_at.desc().nullslast(), Article.id.desc()).limit(limit)
    ).all()
    article_ids = [row.id for row in rows]
    ticker_rows_by_article: dict[int, dict[int, ArticleTicker]] = {article_id: {} for article_id in article_ids}
    if article_ids:
        existing_rows = db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
        ).all()
        for ticker_row in existing_rows:
            ticker_rows_by_article.setdefault(ticker_row.article_id, {})[ticker_row.ticker_id] = ticker_row

    processed = 0
    articles_with_hits = 0
    remapped_articles = 0

    for row in rows:
        processed += 1
        summary = row.summary or ""
        ticker_hits = _extract_entry_tickers(
            row.title,
            summary,
            row.canonical_url,
            "",
            known_symbols,
        )
        if not ticker_hits:
            ticker_hits = _extract_source_fallback_tickers(
                row.title,
                summary,
                row.canonical_url,
                "",
                known_symbols,
                settings.request_timeout_seconds,
                config,
            )
        if not ticker_hits:
            continue

        articles_with_hits += 1
        existing_for_article = ticker_rows_by_article.setdefault(row.id, {})
        new_ids = {
            symbol_to_id[symbol]
            for symbol in ticker_hits.keys()
            if symbol in symbol_to_id and symbol_to_id[symbol] not in existing_for_article
        }
        if new_ids:
            remapped_articles += 1

        _upsert_article_tickers(
            db,
            row.id,
            ticker_hits,
            symbol_to_id,
            existing_rows=existing_for_article,
        )

    db.commit()

    return {
        "source_code": source_code,
        "processed": processed,
        "articles_with_hits": articles_with_hits,
        "remapped_articles": remapped_articles,
        "only_unmapped": only_unmapped,
    }


def remap_businesswire_articles(
    db: Session,
    settings: Settings,
    *,
    limit: int = 500,
    only_unmapped: bool = True,
) -> SourceRemapStats:
    return remap_source_articles(
        db, settings, source_code="businesswire", limit=limit, only_unmapped=only_unmapped,
    )
