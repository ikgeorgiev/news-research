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
from functools import lru_cache
from html import unescape
from pathlib import Path
from typing import TypedDict
from urllib.parse import parse_qs, urlparse, urlunparse

import feedparser
import requests
from sqlalchemy import and_, delete, desc, func, or_, select, tuple_
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.models import (
    Article,
    ArticleTicker,
    FeedPollState,
    IngestionRun,
    RawFeedItem,
    Source,
    Ticker,
)
from app.push_alerts import check_and_send_alerts
from app.sources import build_source_feeds, seed_sources
from app.ticker_loader import load_tickers_from_csv
from app.utils import (
    canonicalize_url,
    clean_summary_text,
    normalize_title,
    parse_datetime,
    sha256_str,
    to_json_safe,
)

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

EXCHANGE_PATTERN = re.compile(
    r"\b(?:NYSE|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b"
)
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
MIN_PERSIST_CONFIDENCE = 0.65  # Above token (0.62), below validated_token (0.68)

# Words too generic to validate a CEF match in article text.
_FUND_KEYWORD_STOPWORDS = frozenset(
    {
        "fund",
        "trust",
        "income",
        "bond",
        "municipal",
        "muni",
        "high",
        "yield",
        "total",
        "return",
        "global",
        "equity",
        "premium",
        "credit",
        "capital",
        "investment",
        "investments",
        "management",
        "advisors",
        "advisers",
        "asset",
        "securities",
        "opportunities",
        "opportunity",
        "strategic",
        "strategy",
        "strategies",
        "select",
        "quality",
        "enhanced",
        "diversified",
        "dynamic",
        "value",
        "growth",
        "limited",
        "duration",
        "term",
        "corp",
        "corporation",
        "company",
        "inc",
        "llc",
        "llp",
        "the",
        "and",
        "of",
        "for",
        "new",
        "rate",
        "floating",
        "short",
    }
)
_SHORT_SPONSOR_KEYWORD_STOPWORDS = frozenset(
    {
        "ag",
        "co",
        "ii",
        "iii",
        "inc",
        "llc",
        "llp",
        "lp",
        "ltd",
        "plc",
        "pte",
        "sa",
    }
)
_ALLOWED_TWO_CHAR_SPONSOR_KEYWORDS = frozenset({"c1"})


def _is_short_sponsor_keyword(
    raw_word: str, cleaned: str, *, is_leading_sponsor_word: bool
) -> bool:
    raw_clean = raw_word.strip(".,;()\"'")
    if len(cleaned) < 2 or len(cleaned) > 3:
        return False
    if cleaned in _FUND_KEYWORD_STOPWORDS:
        return False
    if cleaned in _SHORT_SPONSOR_KEYWORD_STOPWORDS:
        return False
    if not raw_clean.isalnum():
        return False
    if not any(char.isalpha() for char in raw_clean):
        return False
    if raw_clean.upper() != raw_clean:
        return False
    if len(cleaned) == 2:
        return cleaned in _ALLOWED_TWO_CHAR_SPONSOR_KEYWORDS
    # Keep 3-letter sponsor acronyms only when they act as the primary
    # sponsor designator, not trailing geography or parenthetical fragments.
    return is_leading_sponsor_word


@lru_cache(maxsize=4096)
def _validation_keyword_pattern(keyword: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])"
    )


def _text_matches_validation_keywords(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    matched_keywords = [
        keyword
        for keyword in keywords
        if _validation_keyword_pattern(keyword).search(text_lower) is not None
    ]
    if not matched_keywords:
        return False
    if any(" " in keyword for keyword in matched_keywords):
        return True
    # One short sponsor-style acronym can be enough (MFS/OFS/C1), but long-form
    # names need more than one matched term so brand-only press releases do not
    # validate bare ticker tokens.
    if any(len(keyword) <= 3 for keyword in matched_keywords):
        return True
    return len(set(matched_keywords)) >= 2


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, frozenset[str]]:
    """Build per-symbol validation keywords from fund_name plus short sponsor acronyms."""
    result: dict[str, frozenset[str]] = {}
    for row in ticker_rows:
        symbol = row[1]
        symbol_lower = symbol.lower()
        fund_name = row[2] if len(row) > 2 else None
        sponsor = row[3] if len(row) > 3 else None
        keywords: set[str] = set()
        if fund_name:
            cleaned_fund_words: list[str] = []
            word_supports_phrase: list[bool] = []
            for idx, raw_word in enumerate(fund_name.split()):
                cleaned = raw_word.strip(".,;()\"'").lower()
                if cleaned == symbol_lower:
                    continue
                cleaned_fund_words.append(cleaned)
                is_short_keyword = _is_short_sponsor_keyword(
                    raw_word,
                    cleaned,
                    is_leading_sponsor_word=idx == 0,
                )
                is_distinctive_keyword = (
                    len(cleaned) >= 4
                    and cleaned not in _FUND_KEYWORD_STOPWORDS
                )
                if (
                    is_distinctive_keyword
                ):
                    keywords.add(cleaned)
                elif is_short_keyword:
                    keywords.add(cleaned)
                word_supports_phrase.append(is_distinctive_keyword or is_short_keyword)
            for idx in range(len(cleaned_fund_words) - 1):
                if not (word_supports_phrase[idx] or word_supports_phrase[idx + 1]):
                    continue
                first = cleaned_fund_words[idx]
                second = cleaned_fund_words[idx + 1]
                keywords.add(f"{first} {second}")
        if sponsor:
            for idx, raw_word in enumerate(sponsor.split()):
                cleaned = raw_word.strip(".,;()\"'").lower()
                if cleaned == symbol_lower:
                    continue
                if _is_short_sponsor_keyword(
                    raw_word,
                    cleaned,
                    is_leading_sponsor_word=idx == 0,
                ):
                    keywords.add(cleaned)
        result[symbol.upper()] = frozenset(keywords)
    return result


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
    "globenewswire": SourcePageConfig(
        "globenewswire", "globenewswire.com", "gnw_table"
    ),
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


class BusinessWireUrlDedupeStats(TypedDict):
    scanned_articles: int
    duplicate_groups: int
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


class TitleDedupeStats(TypedDict):
    scanned_articles: int
    duplicate_groups: int
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


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
    for key in sorted(
        {_hash_hex_to_signed_bigint(url_hash), _hash_hex_to_signed_bigint(title_hash)}
    ):
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


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _get_or_create_feed_poll_state(db: Session, feed_url: str) -> FeedPollState:
    state = db.scalar(
        select(FeedPollState).where(FeedPollState.feed_url == feed_url).limit(1)
    )
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
    feed_state: FeedPollState, response: requests.Response
) -> None:
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
    except SQLAlchemyError:
        tickers_exist = None

    # If DB is empty, skip mtime cache and force a load.
    if tickers_exist is None:
        return load_tickers_from_csv(db, csv_path)

    try:
        mtime = Path(csv_path).stat().st_mtime
    except OSError:
        # Missing/unreadable file: preserve the previous behavior (loader returns zeros if file missing).
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
    """
    Pre-parsed feed entry fields used in the ingest loop.

    This keeps per-entry logic readable while enabling batched dedupe queries
    (instead of one SELECT per entry).
    """

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
    pairs = {(item.raw_link, item.published_at) for item in prepared_entries}

    existing_guids: set[str] = set()
    if guids:
        existing_guids = {
            guid
            for guid in db.scalars(
                select(RawFeedItem.raw_guid).where(
                    and_(
                        RawFeedItem.source_id == source_id,
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
        symbols.extend(
            [token.strip().upper() for token in value.split(",") if token.strip()]
        )
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


def _fetch_source_page_html(
    url: str, timeout_seconds: int, config: SourcePageConfig
) -> str | None:
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
    except (requests.RequestException, AttributeError):
        html_text = None

    _cache_source_page(fetch_url, time.time(), html_text)
    return html_text


def _extract_table_cell_symbols_from_html(
    html_text: str, known_symbols: set[str]
) -> set[str]:
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
    *,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> dict[str, tuple[str, float]]:
    html_text = _fetch_source_page_html(link, timeout_seconds, config)
    if not html_text:
        return {}

    plain_text = _html_to_plain_text(html_text)
    enriched_summary = " ".join(
        [part for part in [summary, plain_text] if part]
    ).strip()
    hits = _extract_entry_tickers(
        title,
        enriched_summary,
        link,
        feed_url,
        known_symbols,
        include_token=True,
        symbol_keywords=symbol_keywords,
    )
    # Fetched pages should be able to rescue validated plain-token matches from
    # richer body text, but should not add new unvalidated token-only hits.
    hits = {
        symbol: hit
        for symbol, hit in hits.items()
        if hit[0] != "token"
    }

    for symbol in _extract_table_cell_symbols_from_html(html_text, known_symbols):
        existing = hits.get(symbol)
        if existing is None or 0.84 > existing[1]:
            hits[symbol] = (config.table_match_type, 0.84)

    return hits


def _extract_entry_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    *,
    include_token: bool = True,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
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
    text_lower = text.lower() if symbol_keywords is not None else None

    for symbol in EXCHANGE_PATTERN.findall(text):
        add(symbol.upper(), "exchange", 0.88)

    for symbol in PAREN_SYMBOL_PATTERN.findall(text):
        upper = symbol.upper()
        if upper in STOPWORDS:
            # Allow real CEF tickers through if fund name validates.
            if (
                symbol_keywords is not None
                and text_lower is not None
                and upper in known_symbols
            ):
                kws = symbol_keywords.get(upper, frozenset())
                if kws and _text_matches_validation_keywords(text_lower, kws):
                    add(upper, "paren", 0.75)
            continue
        add(upper, "paren", 0.75)

    if include_token:
        for symbol in TOKEN_PATTERN.findall(text):
            upper = symbol.upper()
            if upper in STOPWORDS:
                continue
            if upper in AMBIGUOUS_TOKEN_SYMBOLS:
                continue
            # Validate token against fund name/sponsor keywords when available.
            if symbol_keywords is not None and text_lower is not None:
                kws = symbol_keywords.get(upper, frozenset())
                if kws and _text_matches_validation_keywords(text_lower, kws):
                    add(upper, "validated_token", 0.68)
                    continue
            add(upper, "token", 0.62)

    return hits


def _max_ticker_confidence(hits: dict[str, tuple[str, float]]) -> float:
    if not hits:
        return 0.0
    return max(conf for _, conf in hits.values())


def _merge_ticker_hits(
    target: dict[str, tuple[str, float]],
    extra_hits: dict[str, tuple[str, float]],
) -> None:
    for symbol, (match_type, confidence) in extra_hits.items():
        existing = target.get(symbol)
        if existing is None or confidence > existing[1]:
            target[symbol] = (match_type, confidence)


def _should_persist_entry(
    source_code: str, ticker_hits: dict[str, tuple[str, float]]
) -> bool:
    # Persist all Business Wire items (general stream).
    if source_code == GENERAL_SOURCE_CODE:
        return True
    if not ticker_hits:
        return False
    # Require at least one match above bare-token confidence to filter out
    # false positives from common abbreviations (CGO, PMO, FT, etc.).
    max_confidence = _max_ticker_confidence(ticker_hits)
    return max_confidence >= MIN_PERSIST_CONFIDENCE


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
    allow_url_match_overwrite: bool = True,
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

    def _find_title_window_match(
        *, exclude_source_code: str | None = None
    ) -> Article | None:
        conditions = [
            Article.title_normalized_hash == title_hash,
            Article.published_at >= window_start,
            Article.published_at <= window_end,
        ]
        if exclude_source_code:
            # Exclude any article that already has a raw feed row from this
            # source.  This prevents a new BW story from title-matching an
            # article that BW already owns (even if other sources also
            # mirrored it), which would collapse distinct press releases.
            # Also exclude articles with no raw rows at all — after retention
            # pruning we cannot determine provenance, so conservatively skip
            # them rather than risk merging distinct stories.
            has_excluded_source = (
                select(1)
                .select_from(RawFeedItem)
                .join(Source, Source.id == RawFeedItem.source_id)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        Source.code == exclude_source_code,
                    )
                )
                .correlate(Article)
                .exists()
            )
            has_any_raw = (
                select(1)
                .select_from(RawFeedItem)
                .where(RawFeedItem.article_id == Article.id)
                .correlate(Article)
                .exists()
            )
            # Only match articles that have raw rows AND none from the excluded source.
            conditions.append(has_any_raw)
            conditions.append(~has_excluded_source)
        return db.scalar(
            select(Article).where(and_(*conditions)).order_by(desc(Article.id)).limit(1)
        )

    article = db.scalar(select(Article).where(Article.canonical_url_hash == url_hash))
    matched_by_url = article is not None
    created = False

    if article is None:
        if source_code == GENERAL_SOURCE_CODE:
            # Business Wire can emit multiple distinct stories with the same headline
            # and close timestamps (e.g., one per fund).  Only title-match against
            # articles from *other* sources (Yahoo, PR Newswire, etc.) so cross-feed
            # copies merge, but separate BW press releases stay distinct.
            article = _find_title_window_match(exclude_source_code=source_code)
        else:
            article = _find_title_window_match()
        if article is not None:
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
            article = db.scalar(
                select(Article).where(Article.canonical_url_hash == url_hash)
            )
            matched_by_url = article is not None
            if article is None:
                if source_code == GENERAL_SOURCE_CODE:
                    article = _find_title_window_match(exclude_source_code=source_code)
                else:
                    article = _find_title_window_match()
                if article is not None:
                    matched_by_url = False
            if article is None:
                raise

    if article is None:
        raise RuntimeError("article upsert failed to resolve target row")

    if not created:
        should_overwrite_on_url_match = (
            not matched_by_url
        ) or allow_url_match_overwrite
        if should_overwrite_on_url_match:
            article.title = title
            article.source_name = source_name
            article.provider_name = provider_name
            article.content_hash = content_hash
            article.cluster_key = cluster_key
        if summary:
            if matched_by_url and allow_url_match_overwrite:
                # For exact URL matches, trust the feed payload for this URL.
                article.summary = summary
            elif (not matched_by_url) and (
                not article.summary or len(summary) > len(article.summary)
            ):
                # Mirrors (matched_by_url=True, allow_url_match_overwrite=False) intentionally
                # skip summary updates so they don't clobber the canonical source's richer text.
                article.summary = summary
        if should_overwrite_on_url_match:
            article_published = article.published_at
            if article_published is None or _to_utc(published_at) > _to_utc(
                article_published
            ):
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
            for row in db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id == article_id)
            ).all()
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
    symbol_keywords: dict[str, frozenset[str]] | None = None,
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
            error_text = (
                f"Feed is in backoff until {_to_utc(backoff_until).isoformat()}"
            )
            committed_items_inserted = 0
        else:
            conditional_headers = (
                _get_feed_conditional_headers(feed_state)
                if enable_conditional_get
                else {}
            )
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
                feed_title = (
                    parsed.feed.get("title") if isinstance(parsed.feed, dict) else None
                )
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
                    raw_link_input = str(entry.get("link") or "").strip()
                    raw_link = canonicalize_url(raw_link_input)
                    if not title or not raw_link:
                        continue

                    parsed_input = urlparse(raw_link_input)
                    is_businesswire_link = _is_businesswire_article_url(raw_link)
                    article_url = raw_link
                    if is_businesswire_link:
                        # Yahoo and other feeds may append tracking/query params to BW links.
                        # Normalize BW article URLs so cross-feed copies resolve to one article row.
                        article_url = _canonical_businesswire_article_url(raw_link)
                        # Treat any original query/fragment as mirrored so metadata
                        # overwrite/pruning cannot run on canonical BW rows via mirrors.
                        is_exact_source_url = (
                            not parsed_input.query
                            and not parsed_input.fragment
                            and raw_link == article_url
                        )
                    else:
                        is_exact_source_url = raw_link == article_url

                    published_at = (
                        parse_datetime(entry.get("published") or entry.get("updated"))
                        or now_utc
                    )
                    raw_guid = (
                        str(entry.get("id") or entry.get("guid") or "").strip() or None
                    )
                    prepared_entries.append(
                        PreparedFeedEntry(
                            entry=entry,
                            raw_title=raw_title,
                            title=title,
                            raw_link=raw_link,
                            article_url=article_url,
                            is_exact_source_url=is_exact_source_url,
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
                            if (
                                prepared.raw_guid
                                and prepared.raw_guid in recorded_guids
                            ):
                                continue
                            if (
                                prepared.raw_link,
                                prepared.published_at,
                            ) in recorded_pairs:
                                continue

                            raw_summary = (
                                str(
                                    prepared.entry.get("summary")
                                    or prepared.entry.get("description")
                                    or ""
                                ).strip()
                                or None
                            )
                            summary = clean_summary_text(raw_summary)

                            entry_source_name = _extract_provider(
                                prepared.entry, source_name
                            )
                            ticker_hits = _extract_entry_tickers(
                                prepared.title,
                                summary or "",
                                prepared.raw_link,
                                feed_url,
                                known_symbols,
                                symbol_keywords=symbol_keywords,
                            )
                            # Run page-fetch fallback when no hits or all hits are
                            # below the persist threshold (e.g. token-only at 0.62).
                            _max_hit_conf = _max_ticker_confidence(ticker_hits)
                            if (
                                _max_hit_conf < MIN_PERSIST_CONFIDENCE
                                and page_config is not None
                            ):
                                fallback_hits = _extract_source_fallback_tickers(
                                    prepared.title,
                                    summary or "",
                                    prepared.raw_link,
                                    feed_url,
                                    known_symbols,
                                    timeout_seconds,
                                    page_config,
                                    symbol_keywords=symbol_keywords,
                                )
                                # Merge: fallback wins when it has higher confidence.
                                _merge_ticker_hits(ticker_hits, fallback_hits)

                            should_persist_tickers = _should_persist_entry(
                                source.code, ticker_hits
                            )
                            allow_existing_exact_url = False
                            if (
                                not should_persist_tickers
                                and source.code != GENERAL_SOURCE_CODE
                                and prepared.is_exact_source_url
                            ):
                                existing_article_id = db.scalar(
                                    select(Article.id)
                                    .where(
                                        Article.canonical_url_hash
                                        == sha256_str(prepared.article_url)
                                    )
                                    .limit(1)
                                )
                                allow_existing_exact_url = (
                                    existing_article_id is not None
                                )

                            if not should_persist_tickers and not allow_existing_exact_url:
                                continue

                            article, created, matched_by_url = _upsert_article(
                                db,
                                source_code=source.code,
                                canonical_url=prepared.article_url,
                                title=prepared.title,
                                summary=summary,
                                published_at=prepared.published_at,
                                source_name=source_name,
                                provider_name=provider_name,
                                allow_url_match_overwrite=prepared.is_exact_source_url,
                            )
                            created_article = created

                            effective_ticker_hits = (
                                ticker_hits if should_persist_tickers else {}
                            )
                            # New articles never have mappings yet; skip the SELECT in _upsert_article_tickers().
                            existing_rows = {} if created else None
                            _upsert_article_tickers(
                                db,
                                article.id,
                                effective_ticker_hits,
                                symbol_to_id,
                                existing_rows=existing_rows,
                                # Prune only when the source entry URL exactly matches the article URL.
                                # Mirrored Business Wire copies (e.g., Yahoo with query variants) should not
                                # prune mappings on the canonical BW article.
                                prune_missing=(
                                    source.code != GENERAL_SOURCE_CODE
                                    and matched_by_url
                                    and prepared.is_exact_source_url
                                    and bool(effective_ticker_hits)
                                ),
                            )

                            payload = {
                                "title": prepared.raw_title,
                                "link": prepared.raw_link,
                                "published": prepared.entry.get("published")
                                or prepared.entry.get("updated"),
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
                                    raw_link=prepared.raw_link,
                                    raw_pub_date=prepared.published_at,
                                    raw_payload_json=to_json_safe(payload),
                                )
                            )
                            persisted_raw = True
                            persisted_raw_guid = prepared.raw_guid
                            persisted_pair = (prepared.raw_link, prepared.published_at)

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
                        error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}: {entry_exc}"
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


def run_ingestion_cycle(db: Session, settings: Settings) -> IngestionCycleResult:
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

    ticker_rows = db.execute(
        select(Ticker.id, Ticker.symbol, Ticker.fund_name, Ticker.sponsor).where(
            Ticker.active.is_(True)
        )
    ).all()
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    known_symbols = frozenset(symbol_to_id.keys())
    symbol_keywords = _build_symbol_keywords(ticker_rows)

    per_feed: list[IngestFeedResult] = []
    total_seen = 0
    total_inserted = 0
    failed = 0

    max_workers = settings.ingestion_max_workers
    enable_conditional_get = settings.ingestion_enable_conditional_get
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
                    symbol_keywords=symbol_keywords,
                )

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
                    symbol_keywords=symbol_keywords,
                )

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
    per_feed = sorted(
        results, key=lambda r: (r.get("source", ""), r.get("feed_url", ""))
    )
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
                remap_source_articles(
                    db, settings, source_code=code, limit=500, only_unmapped=True
                )
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
        except Exception:  # Broad catch: push failures must not abort ingestion
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

    ticker_rows = db.execute(
        select(Ticker.id, Ticker.symbol, Ticker.fund_name, Ticker.sponsor).where(
            Ticker.active.is_(True)
        )
    ).all()
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    known_symbols = set(symbol_to_id.keys())
    symbol_keywords = _build_symbol_keywords(ticker_rows)

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
        query.order_by(
            Article.published_at.desc().nullslast(), Article.id.desc()
        ).limit(limit)
    ).all()
    article_ids = [row.id for row in rows]
    ticker_rows_by_article: dict[int, dict[int, ArticleTicker]] = {
        article_id: {} for article_id in article_ids
    }
    if article_ids:
        existing_rows = db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
        ).all()
        for ticker_row in existing_rows:
            ticker_rows_by_article.setdefault(ticker_row.article_id, {})[
                ticker_row.ticker_id
            ] = ticker_row

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
            symbol_keywords=symbol_keywords,
        )
        if _max_ticker_confidence(ticker_hits) < MIN_PERSIST_CONFIDENCE:
            fallback_hits = _extract_source_fallback_tickers(
                row.title,
                summary,
                row.canonical_url,
                "",
                known_symbols,
                settings.request_timeout_seconds,
                config,
                symbol_keywords=symbol_keywords,
            )
            _merge_ticker_hits(ticker_hits, fallback_hits)
        if not ticker_hits:
            continue
        if (
            source_code != GENERAL_SOURCE_CODE
            and _max_ticker_confidence(ticker_hits) < MIN_PERSIST_CONFIDENCE
        ):
            continue

        articles_with_hits += 1
        existing_for_article = ticker_rows_by_article.setdefault(row.id, {})
        new_ids = {
            symbol_to_id[symbol]
            for symbol in ticker_hits.keys()
            if symbol in symbol_to_id
            and symbol_to_id[symbol] not in existing_for_article
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


def dedupe_businesswire_url_variants(db: Session) -> BusinessWireUrlDedupeStats:
    candidate_rows = db.scalars(
        select(Article)
        .where(Article.canonical_url.ilike("%businesswire.com%"))
        .order_by(Article.id.asc())
    ).all()

    grouped: dict[str, list[Article]] = {}
    scanned_articles = 0
    for article in candidate_rows:
        canonical_candidate = canonicalize_url(article.canonical_url)
        if not _is_businesswire_article_url(canonical_candidate):
            continue
        scanned_articles += 1
        normalized = _canonical_businesswire_article_url(canonical_candidate)
        grouped.setdefault(normalized, []).append(article)

    duplicate_groups = 0
    merged_articles = 0
    raw_items_relinked = 0
    ticker_rows_relinked = 0
    ticker_rows_updated = 0
    ticker_rows_deleted = 0

    for normalized_url, articles in grouped.items():
        if len(articles) <= 1:
            continue

        duplicate_groups += 1
        article_ids = [row.id for row in articles]
        ticker_rows = db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
        ).all()
        ticker_rows_by_article: dict[int, list[ArticleTicker]] = {}
        for ticker_row in ticker_rows:
            ticker_rows_by_article.setdefault(ticker_row.article_id, []).append(
                ticker_row
            )

        raw_rows = db.scalars(
            select(RawFeedItem).where(RawFeedItem.article_id.in_(article_ids))
        ).all()

        def _winner_rank(row: Article) -> tuple[int, int, int, float, int]:
            cleaned_url = canonicalize_url(row.canonical_url)
            has_normalized_url = int(cleaned_url == normalized_url)
            ticker_count = len(ticker_rows_by_article.get(row.id, []))
            summary_len = len(row.summary or "")
            published = row.published_at or row.created_at
            published_rank = _to_utc(published).timestamp()
            return has_normalized_url, ticker_count, summary_len, published_rank, row.id

        winner = max(articles, key=_winner_rank)
        duplicates = [row for row in articles if row.id != winner.id]
        if not duplicates:
            continue

        winner_cleaned_url = canonicalize_url(winner.canonical_url)
        if winner_cleaned_url != normalized_url:
            winner.canonical_url = normalized_url
            winner.canonical_url_hash = sha256_str(normalized_url)

        winner_tickers: dict[int, ArticleTicker] = {
            row.ticker_id: row for row in ticker_rows_by_article.get(winner.id, [])
        }

        duplicate_ids = {row.id for row in duplicates}
        for raw_row in raw_rows:
            if raw_row.article_id not in duplicate_ids:
                continue
            raw_row.article_id = winner.id
            raw_items_relinked += 1

        for duplicate in duplicates:
            duplicate_summary = duplicate.summary or ""
            winner_summary = winner.summary or ""
            if duplicate_summary and (
                not winner_summary or len(duplicate_summary) > len(winner_summary)
            ):
                winner.summary = duplicate.summary
            if duplicate.published_at and (
                winner.published_at is None
                or duplicate.published_at > winner.published_at
            ):
                winner.published_at = duplicate.published_at
            duplicate_first_seen = duplicate.first_seen_at or duplicate.created_at
            winner_first_seen = winner.first_seen_at or winner.created_at
            if duplicate_first_seen and (
                winner_first_seen is None
                or _to_utc(duplicate_first_seen) < _to_utc(winner_first_seen)
            ):
                winner.first_seen_at = duplicate_first_seen
            if duplicate.first_alert_sent_at and (
                winner.first_alert_sent_at is None
                or _to_utc(duplicate.first_alert_sent_at)
                < _to_utc(winner.first_alert_sent_at)
            ):
                winner.first_alert_sent_at = duplicate.first_alert_sent_at

            for ticker_row in ticker_rows_by_article.get(duplicate.id, []):
                existing = winner_tickers.get(ticker_row.ticker_id)
                if existing is None:
                    ticker_row.article_id = winner.id
                    winner_tickers[ticker_row.ticker_id] = ticker_row
                    ticker_rows_relinked += 1
                    continue

                if ticker_row.confidence > existing.confidence:
                    existing.confidence = ticker_row.confidence
                    existing.match_type = ticker_row.match_type
                    ticker_rows_updated += 1
                db.delete(ticker_row)
                ticker_rows_deleted += 1

            db.flush()
            db.delete(duplicate)
            merged_articles += 1

    db.commit()
    return {
        "scanned_articles": scanned_articles,
        "duplicate_groups": duplicate_groups,
        "merged_articles": merged_articles,
        "raw_items_relinked": raw_items_relinked,
        "ticker_rows_relinked": ticker_rows_relinked,
        "ticker_rows_updated": ticker_rows_updated,
        "ticker_rows_deleted": ticker_rows_deleted,
    }


def dedupe_articles_by_title(
    db: Session, *, window_hours: int = 48
) -> TitleDedupeStats:
    """Merge articles that share the same normalized title within a time window."""
    all_articles = db.scalars(select(Article).order_by(Article.id.asc())).all()

    grouped: dict[str, list[Article]] = {}
    for article in all_articles:
        key = article.title_normalized_hash
        grouped.setdefault(key, []).append(article)

    scanned_articles = len(all_articles)
    duplicate_groups = 0
    merged_articles = 0
    raw_items_relinked = 0
    ticker_rows_relinked = 0
    ticker_rows_updated = 0
    ticker_rows_deleted = 0

    for _title_hash, articles in grouped.items():
        if len(articles) <= 1:
            continue

        # Within each title group, cluster by publish-time window.
        # Sort by publish time so we can use a sliding window where every
        # member is within window_hours of every other member.
        sorted_articles = sorted(
            articles,
            key=lambda a: _to_utc(a.published_at or a.created_at),
        )
        clusters: list[list[Article]] = []
        for article in sorted_articles:
            pub = _to_utc(article.published_at or article.created_at)
            placed = False
            for cluster in clusters:
                earliest = _to_utc(cluster[0].published_at or cluster[0].created_at)
                if (pub - earliest).total_seconds() <= window_hours * 3600:
                    cluster.append(article)
                    placed = True
                    break
            if not placed:
                clusters.append([article])

        for cluster in clusters:
            if len(cluster) <= 1:
                continue

            article_ids = [row.id for row in cluster]

            # Skip clusters where any article has Business Wire provenance or
            # has no raw rows at all (conservatively assume pruned rows could
            # have been BW).  BW regularly publishes distinct press releases
            # with identical headlines (one per fund), and those must not be
            # merged.
            cluster_raw_rows = db.scalars(
                select(RawFeedItem).where(RawFeedItem.article_id.in_(article_ids))
            ).all()
            raw_by_article: dict[int, set[int]] = {}
            for raw_row in cluster_raw_rows:
                raw_by_article.setdefault(raw_row.article_id, set()).add(
                    raw_row.source_id
                )
            bw_source_id = db.scalar(
                select(Source.id).where(Source.code == GENERAL_SOURCE_CODE)
            )
            has_bw_risk = False
            for aid in article_ids:
                source_ids = raw_by_article.get(aid, set())
                if not source_ids:
                    # No raw rows — after retention pruning we can't tell
                    # the provenance, so conservatively treat as BW.
                    has_bw_risk = True
                    break
                if bw_source_id in source_ids:
                    has_bw_risk = True
                    break
            if has_bw_risk:
                continue

            duplicate_groups += 1
            ticker_rows = db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
            ).all()
            ticker_rows_by_article: dict[int, list[ArticleTicker]] = {}
            for ticker_row in ticker_rows:
                ticker_rows_by_article.setdefault(ticker_row.article_id, []).append(
                    ticker_row
                )

            raw_rows = cluster_raw_rows

            def _rank(row: Article) -> tuple[int, int, float, int]:
                ticker_count = len(ticker_rows_by_article.get(row.id, []))
                summary_len = len(row.summary or "")
                published = row.published_at or row.created_at
                return ticker_count, summary_len, _to_utc(published).timestamp(), row.id

            winner = max(cluster, key=_rank)
            duplicates = [row for row in cluster if row.id != winner.id]
            if not duplicates:
                continue

            winner_tickers: dict[int, ArticleTicker] = {
                row.ticker_id: row for row in ticker_rows_by_article.get(winner.id, [])
            }

            duplicate_ids = {row.id for row in duplicates}
            for raw_row in raw_rows:
                if raw_row.article_id not in duplicate_ids:
                    continue
                raw_row.article_id = winner.id
                raw_items_relinked += 1

            for duplicate in duplicates:
                dup_summary = duplicate.summary or ""
                win_summary = winner.summary or ""
                if dup_summary and (
                    not win_summary or len(dup_summary) > len(win_summary)
                ):
                    winner.summary = duplicate.summary
                if duplicate.published_at and (
                    winner.published_at is None
                    or duplicate.published_at > winner.published_at
                ):
                    winner.published_at = duplicate.published_at
                duplicate_first_seen = duplicate.first_seen_at or duplicate.created_at
                winner_first_seen = winner.first_seen_at or winner.created_at
                if duplicate_first_seen and (
                    winner_first_seen is None
                    or _to_utc(duplicate_first_seen) < _to_utc(winner_first_seen)
                ):
                    winner.first_seen_at = duplicate_first_seen
                if duplicate.first_alert_sent_at and (
                    winner.first_alert_sent_at is None
                    or _to_utc(duplicate.first_alert_sent_at)
                    < _to_utc(winner.first_alert_sent_at)
                ):
                    winner.first_alert_sent_at = duplicate.first_alert_sent_at

                for ticker_row in ticker_rows_by_article.get(duplicate.id, []):
                    existing = winner_tickers.get(ticker_row.ticker_id)
                    if existing is None:
                        ticker_row.article_id = winner.id
                        winner_tickers[ticker_row.ticker_id] = ticker_row
                        ticker_rows_relinked += 1
                        continue

                    if ticker_row.confidence > existing.confidence:
                        existing.confidence = ticker_row.confidence
                        existing.match_type = ticker_row.match_type
                        ticker_rows_updated += 1
                    db.delete(ticker_row)
                    ticker_rows_deleted += 1

                db.flush()
                db.delete(duplicate)
                merged_articles += 1

    db.commit()
    return {
        "scanned_articles": scanned_articles,
        "duplicate_groups": duplicate_groups,
        "merged_articles": merged_articles,
        "raw_items_relinked": raw_items_relinked,
        "ticker_rows_relinked": ticker_rows_relinked,
        "ticker_rows_updated": ticker_rows_updated,
        "ticker_rows_deleted": ticker_rows_deleted,
    }


def remap_businesswire_articles(
    db: Session,
    settings: Settings,
    *,
    limit: int = 500,
    only_unmapped: bool = True,
) -> SourceRemapStats:
    return remap_source_articles(
        db,
        settings,
        source_code="businesswire",
        limit=limit,
        only_unmapped=only_unmapped,
    )


class PurgeFalsePositiveStats(TypedDict):
    scanned_articles: int
    purged_articles: int
    deleted_article_tickers: int
    deleted_raw_feed_items: int


def _reextract_purge_article_tickers(
    article: Article,
    raw_contexts: list[tuple[str, str | None, str | None]],
    known_symbols: set[str],
    timeout_seconds: int,
    *,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> dict[str, tuple[str, float]]:
    primary_link = article.canonical_url
    primary_feed_url = ""
    for _source_code, raw_link, feed_url in raw_contexts:
        if raw_link:
            primary_link = raw_link
        if feed_url:
            primary_feed_url = feed_url
        if raw_link or feed_url:
            break

    hits = _extract_entry_tickers(
        article.title,
        article.summary or "",
        primary_link,
        primary_feed_url,
        known_symbols,
        symbol_keywords=symbol_keywords,
    )
    if _max_ticker_confidence(hits) >= MIN_PERSIST_CONFIDENCE:
        return hits

    for source_code, raw_link, feed_url in raw_contexts:
        config = PAGE_FETCH_CONFIGS.get(source_code)
        if config is None:
            continue
        fallback_hits = _extract_source_fallback_tickers(
            article.title,
            article.summary or "",
            raw_link or article.canonical_url,
            feed_url or "",
            known_symbols,
            timeout_seconds,
            config,
            symbol_keywords=symbol_keywords,
        )
        _merge_ticker_hits(hits, fallback_hits)
        if _max_ticker_confidence(hits) >= MIN_PERSIST_CONFIDENCE:
            break

    return hits


def purge_token_only_articles(
    db: Session,
    *,
    dry_run: bool = True,
    limit: int = 2000,
    timeout_seconds: int = 20,
) -> PurgeFalsePositiveStats:
    """Remove articles that were only matched via unvalidated token patterns.

    Targets non-Business Wire articles whose highest-confidence ticker match
    is below MIN_PERSIST_CONFIDENCE (i.e. bare token matches that would no
    longer be persisted under the current rules).
    """
    # Load symbol keywords for re-evaluation.
    ticker_rows = db.execute(
        select(Ticker.id, Ticker.symbol, Ticker.fund_name, Ticker.sponsor).where(
            Ticker.active.is_(True)
        )
    ).all()
    symbol_keywords = _build_symbol_keywords(ticker_rows)
    known_symbols = frozenset(row[1].upper() for row in ticker_rows)

    # Find non-Business Wire articles that have ticker mappings.
    # Use NOT EXISTS instead of NOT IN to avoid NULL-sensitivity issues
    # (raw_feed_items.article_id is nullable).
    bw_source_id = db.scalar(
        select(Source.id).where(Source.code == GENERAL_SOURCE_CODE)
    )
    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
    low_confidence_exists = (
        select(func.max(ArticleTicker.confidence))
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .scalar_subquery()
    )
    has_any_raw = (
        select(1)
        .select_from(RawFeedItem)
        .where(RawFeedItem.article_id == Article.id)
        .correlate(Article)
        .exists()
    )

    bw_exists = (
        (
            select(1)
            .select_from(RawFeedItem)
            .where(
                and_(
                    RawFeedItem.article_id == Article.id,
                    RawFeedItem.source_id == bw_source_id,
                )
            )
            .correlate(Article)
            .exists()
        )
        if bw_source_id is not None
        else None
    )

    scanned = 0
    purged = 0
    deleted_at = 0
    deleted_rfi = 0
    # In dry_run mode, articles with NULL published_at always pass the cursor
    # filter (NULLS LAST) and would be re-scanned every batch.  Track their
    # IDs to avoid inflating counts.
    seen_ids: set[int] = set()

    batch_size = min(max(limit, 200), 1000)
    cursor_published_at: datetime | None = None
    cursor_id: int | None = None

    while purged < limit:
        query = (
            select(Article)
            .where(mapped_exists)
            .where(has_any_raw)
            .where(low_confidence_exists < MIN_PERSIST_CONFIDENCE)
        )
        if bw_exists is not None:
            query = query.where(~bw_exists)
        if cursor_id is not None:
            if cursor_published_at is None:
                query = query.where(
                    and_(
                        Article.published_at.is_(None),
                        Article.id < cursor_id,
                    )
                )
            else:
                query = query.where(
                    or_(
                        Article.published_at.is_(None),
                        Article.published_at < cursor_published_at,
                        and_(
                            Article.published_at == cursor_published_at,
                            Article.id < cursor_id,
                        ),
                    )
                )
        articles = db.scalars(
            query.order_by(
                Article.published_at.desc().nullslast(), Article.id.desc()
            ).limit(batch_size)
        ).all()
        if not articles:
            break

        article_ids = [article.id for article in articles]
        raw_contexts_by_article: dict[int, list[tuple[str, str | None, str | None]]] = (
            {}
        )
        raw_context_rows = db.execute(
            select(
                RawFeedItem.article_id,
                Source.code,
                RawFeedItem.raw_link,
                RawFeedItem.feed_url,
            )
            .join(Source, Source.id == RawFeedItem.source_id)
            .where(RawFeedItem.article_id.in_(article_ids))
            .order_by(RawFeedItem.article_id.asc(), RawFeedItem.id.desc())
        ).all()
        for article_id, source_code, raw_link, feed_url in raw_context_rows:
            raw_contexts_by_article.setdefault(article_id, []).append(
                (source_code, raw_link, feed_url)
            )

        for article in articles:
            if article.id in seen_ids:
                continue
            seen_ids.add(article.id)
            scanned += 1

            raw_contexts = raw_contexts_by_article.get(article.id, [])
            if not raw_contexts:
                # No raw rows means provenance is unknown after retention pruning.
                # Conservatively skip instead of treating the article as non-BW.
                continue
            if any(
                source_code == GENERAL_SOURCE_CODE
                for source_code, _, _ in raw_contexts
            ):
                continue

            # Re-evaluate tickers with the same fallback extraction rules used
            # during ingestion for sources that support page fetch.
            hits = _reextract_purge_article_tickers(
                article,
                raw_contexts,
                known_symbols,
                timeout_seconds,
                symbol_keywords=symbol_keywords,
            )
            max_conf = _max_ticker_confidence(hits)

            if max_conf >= MIN_PERSIST_CONFIDENCE:
                continue

            # This article would no longer be persisted — purge it.
            if not dry_run:
                at_count = db.execute(
                    delete(ArticleTicker).where(ArticleTicker.article_id == article.id)
                ).rowcount
                rfi_count = db.execute(
                    delete(RawFeedItem).where(RawFeedItem.article_id == article.id)
                ).rowcount
                db.execute(delete(Article).where(Article.id == article.id))
                deleted_at += at_count
                deleted_rfi += rfi_count
            else:
                at_count = (
                    db.scalar(
                        select(func.count())
                        .select_from(ArticleTicker)
                        .where(ArticleTicker.article_id == article.id)
                    )
                    or 0
                )
                rfi_count = (
                    db.scalar(
                        select(func.count())
                        .select_from(RawFeedItem)
                        .where(RawFeedItem.article_id == article.id)
                    )
                    or 0
                )
                deleted_at += at_count
                deleted_rfi += rfi_count
            purged += 1
            if purged >= limit:
                break

        last_article = articles[-1]
        cursor_published_at = last_article.published_at
        cursor_id = last_article.id

    if not dry_run:
        db.commit()

    return {
        "scanned_articles": scanned,
        "purged_articles": purged,
        "deleted_article_tickers": deleted_at,
        "deleted_raw_feed_items": deleted_rfi,
    }
