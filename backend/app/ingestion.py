from __future__ import annotations

import time
import re
import threading
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from html import unescape
from urllib.parse import parse_qs, urlparse, urlunparse

import feedparser
import requests
from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Article, ArticleTicker, IngestionRun, RawFeedItem, Source, Ticker
from app.sources import build_source_feeds, seed_sources
from app.ticker_loader import load_tickers_from_csv
from app.utils import canonicalize_url, clean_summary_text, normalize_title, parse_datetime, sha256_str, to_json_safe

REQUEST_HEADERS = {
    "User-Agent": "cef-news-feed/0.1 (+local)",
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.1",
}
BUSINESSWIRE_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; cef-news-feed/0.1; +local)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.1",
}
GENERAL_SOURCE_CODE = "businesswire"
BUSINESSWIRE_PAGE_CACHE_TTL_SECONDS = 6 * 60 * 60
BUSINESSWIRE_PAGE_FAILURE_CACHE_TTL_SECONDS = 60
BUSINESSWIRE_PAGE_CACHE_MAX_ITEMS = 512

EXCHANGE_PATTERN = re.compile(r"\b(?:NYSE|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b")
PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")
TOKEN_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")
HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>", flags=re.IGNORECASE | re.DOTALL
)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
TABLE_CELL_SYMBOL_PATTERN = re.compile(
    r"<td[^>]*>\s*([A-Z][A-Z0-9\.\-]{0,9})\s*</td>",
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
}
_businesswire_page_cache: OrderedDict[str, tuple[float, str | None]] = OrderedDict()
_businesswire_page_cache_lock = threading.Lock()


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


def _canonical_businesswire_article_url(url: str) -> str:
    parsed = urlparse(url)
    # Keep path-only URL for stable cache keys and fewer duplicate fetches.
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _is_businesswire_article_url(url: str) -> bool:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not hostname:
        return False
    return hostname == "businesswire.com" or hostname.endswith(".businesswire.com")


def _cache_businesswire_page(url: str, fetched_at: float, html_text: str | None) -> None:
    with _businesswire_page_cache_lock:
        _businesswire_page_cache[url] = (fetched_at, html_text)
        _businesswire_page_cache.move_to_end(url)

        while len(_businesswire_page_cache) > BUSINESSWIRE_PAGE_CACHE_MAX_ITEMS:
            _businesswire_page_cache.popitem(last=False)


def _fetch_businesswire_page_html(url: str, timeout_seconds: int) -> str | None:
    fetch_url = _canonical_businesswire_article_url(url)
    if not _is_businesswire_article_url(fetch_url):
        return None

    now = time.time()
    with _businesswire_page_cache_lock:
        cached = _businesswire_page_cache.get(fetch_url)
        if cached is not None:
            fetched_at, cached_html = cached
            ttl_seconds = (
                BUSINESSWIRE_PAGE_CACHE_TTL_SECONDS
                if cached_html is not None
                else BUSINESSWIRE_PAGE_FAILURE_CACHE_TTL_SECONDS
            )
            if now - fetched_at <= ttl_seconds:
                _businesswire_page_cache.move_to_end(fetch_url)
                return cached_html

    html_text: str | None = None
    try:
        response = requests.get(
            fetch_url,
            timeout=timeout_seconds,
            headers=BUSINESSWIRE_PAGE_HEADERS,
        )
        if response.ok and response.text:
            html_text = response.text
    except Exception:
        html_text = None

    _cache_businesswire_page(fetch_url, time.time(), html_text)
    return html_text


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


def _extract_businesswire_fallback_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    timeout_seconds: int,
) -> dict[str, tuple[str, float]]:
    html_text = _fetch_businesswire_page_html(link, timeout_seconds)
    if not html_text:
        return {}

    # Avoid noisy all-token scan on large pages; rely on explicit exchange/paren patterns.
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

    # Extract compact symbols from HTML table cells to catch columns like "Ticker: DSM, LEO".
    for symbol in _extract_table_cell_symbols_from_html(html_text, known_symbols):
        existing = hits.get(symbol)
        if existing is None or 0.84 > existing[1]:
            hits[symbol] = ("bw_table", 0.84)

    return hits


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
        if published_at and article.published_at and published_at > article.published_at:
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
) -> dict[str, int | str | None]:
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
    status = "success"
    error_text: str | None = None

    try:
        response = requests.get(feed_url, timeout=timeout_seconds, headers=REQUEST_HEADERS)
        response.raise_for_status()
        parsed = feedparser.parse(response.content)

        source_name = source.name
        feed_title = parsed.feed.get("title") if isinstance(parsed.feed, dict) else None
        if source.code != "yahoo" and feed_title:
            source_name = _clamp_label(str(feed_title))

        for entry in parsed.entries:
            items_seen += 1

            raw_title = str(entry.get("title") or "").strip()
            # Never fall back to raw feed title so stored titles remain plain text.
            title = clean_summary_text(raw_title)
            link = canonicalize_url(str(entry.get("link") or "").strip())
            if not title or not link:
                continue

            raw_summary = str(entry.get("summary") or entry.get("description") or "").strip() or None
            summary = clean_summary_text(raw_summary)
            published_at = parse_datetime(entry.get("published") or entry.get("updated"))
            if published_at is None:
                published_at = datetime.now(timezone.utc)

            provider_name = _clamp_label(source.name)
            entry_source_name = _extract_provider(entry, source_name)
            ticker_hits = _extract_entry_tickers(title, summary or "", link, feed_url, known_symbols)
            if source.code == GENERAL_SOURCE_CODE and not ticker_hits:
                ticker_hits = _extract_businesswire_fallback_tickers(
                    title,
                    summary or "",
                    link,
                    feed_url,
                    known_symbols,
                    timeout_seconds,
                )

            allow_existing_unmapped = False
            if not ticker_hits and source.code != GENERAL_SOURCE_CODE:
                existing_article_id = db.scalar(
                    select(Article.id)
                    .where(Article.canonical_url_hash == sha256_str(link))
                    .limit(1)
                )
                allow_existing_unmapped = existing_article_id is not None

            if not _should_persist_entry(source.code, ticker_hits) and not allow_existing_unmapped:
                continue

            article, created, matched_by_url = _upsert_article(
                db,
                source_code=source.code,
                canonical_url=link,
                title=title,
                summary=summary,
                published_at=published_at,
                source_name=source_name,
                provider_name=provider_name,
            )
            if created:
                items_inserted += 1

            _upsert_article_tickers(
                db,
                article.id,
                ticker_hits,
                symbol_to_id,
                prune_missing=(source.code != GENERAL_SOURCE_CODE and matched_by_url),
            )

            payload = {
                "title": raw_title,
                "link": link,
                "published": entry.get("published") or entry.get("updated"),
                "summary": raw_summary,
                "source": entry_source_name,
            }
            db.add(
                RawFeedItem(
                    source_id=source.id,
                    article_id=article.id,
                    feed_url=feed_url,
                    raw_guid=str(entry.get("id") or entry.get("guid") or "") or None,
                    raw_title=raw_title,
                    raw_link=link,
                    raw_pub_date=published_at,
                    raw_payload_json=to_json_safe(payload),
                )
            )

        db.commit()

    except Exception as exc:
        db.rollback()
        status = "failed"
        error_text = str(exc)

    run.status = status
    run.items_seen = items_seen
    run.items_inserted = items_inserted
    run.error_text = error_text
    run.finished_at = datetime.now(timezone.utc)
    db.commit()

    return {
        "source": source.code,
        "feed_url": feed_url,
        "status": status,
        "items_seen": items_seen,
        "items_inserted": items_inserted,
        "error": error_text,
    }


def run_ingestion_cycle(
    db: Session, settings: Settings
) -> dict[str, int | dict[str, int | bool] | dict[str, int] | list[dict[str, int | str | None]]]:
    ticker_sync = load_tickers_from_csv(db, settings.tickers_csv_path)

    source_feeds = build_source_feeds(settings, db)
    seed_sources(db, source_feeds)

    source_map = {
        source.code: source
        for source in db.scalars(select(Source).where(Source.code.in_([item.code for item in source_feeds]))).all()
    }

    ticker_rows = db.execute(select(Ticker.id, Ticker.symbol).where(Ticker.active.is_(True))).all()
    symbol_to_id = {symbol.upper(): ticker_id for ticker_id, symbol in ticker_rows}
    known_symbols = set(symbol_to_id.keys())

    per_feed: list[dict[str, int | str | None]] = []
    total_seen = 0
    total_inserted = 0
    failed = 0

    for source_item in source_feeds:
        source_row = source_map.get(source_item.code)
        if source_row is None:
            continue

        for feed_url in source_item.feed_urls:
            result = ingest_feed(
                db,
                source=source_row,
                feed_url=feed_url,
                known_symbols=known_symbols,
                symbol_to_id=symbol_to_id,
                timeout_seconds=settings.request_timeout_seconds,
            )
            per_feed.append(result)
            total_seen += int(result["items_seen"])
            total_inserted += int(result["items_inserted"])
            if result["status"] != "success":
                failed += 1

    remap_stats: dict[str, int | bool] | None = None
    ticker_changed = int(ticker_sync.get("created", 0)) > 0 or int(ticker_sync.get("updated", 0)) > 0
    if ticker_changed:
        remap_stats = remap_businesswire_articles(
            db,
            settings,
            limit=500,
            only_unmapped=True,
        )

    return {
        "total_feeds": len(per_feed),
        "total_items_seen": total_seen,
        "total_items_inserted": total_inserted,
        "failed_feeds": failed,
        "ticker_sync": ticker_sync,
        "businesswire_remap": remap_stats or {
            "processed": 0,
            "articles_with_hits": 0,
            "remapped_articles": 0,
            "only_unmapped": True,
        },
        "feeds": per_feed,
    }


def remap_businesswire_articles(
    db: Session,
    settings: Settings,
    *,
    limit: int = 500,
    only_unmapped: bool = True,
) -> dict[str, int | bool]:
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
    businesswire_exists = (
        select(1)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                Source.code == GENERAL_SOURCE_CODE,
            )
        )
        .correlate(Article)
        .exists()
    )

    query = select(Article).where(businesswire_exists)
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
            ticker_hits = _extract_businesswire_fallback_tickers(
                row.title,
                summary,
                row.canonical_url,
                "",
                known_symbols,
                settings.request_timeout_seconds,
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
        "processed": processed,
        "articles_with_hits": articles_with_hits,
        "remapped_articles": remapped_articles,
        "only_unmapped": only_unmapped,
    }
