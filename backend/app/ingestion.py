from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import feedparser
import requests
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Article, ArticleTicker, IngestionRun, RawFeedItem, Source, Ticker
from app.sources import build_source_feeds, seed_sources
from app.utils import canonicalize_url, normalize_title, parse_datetime, sha256_str, to_json_safe

REQUEST_HEADERS = {
    "User-Agent": "cef-news-feed/0.1 (+local)",
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.1",
}

EXCHANGE_PATTERN = re.compile(r"\b(?:NYSE|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b")
PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")
TOKEN_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")
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


def _clamp_label(value: str | None, max_len: int = 120) -> str:
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len]


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


def _extract_entry_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
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

    for symbol in TOKEN_PATTERN.findall(text):
        upper = symbol.upper()
        if upper in STOPWORDS:
            continue
        add(upper, "token", 0.62)

    return hits


def _upsert_article(
    db: Session,
    *,
    canonical_url: str,
    title: str,
    summary: str | None,
    published_at: datetime,
    source_name: str,
    provider_name: str,
) -> tuple[Article, bool]:
    url_hash = sha256_str(canonical_url)
    title_hash = sha256_str(normalize_title(title))
    summary_norm = normalize_title(summary or "")
    content_hash = sha256_str(f"{title_hash}|{summary_norm[:300]}")
    cluster_key = title_hash

    source_name = _clamp_label(source_name)
    provider_name = _clamp_label(provider_name)

    article = db.scalar(select(Article).where(Article.canonical_url_hash == url_hash))
    created = False

    if article is None:
        window_start = published_at - timedelta(hours=48)
        window_end = published_at + timedelta(hours=48)
        article = db.scalar(
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

    if article is None:
        article = Article(
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
        db.add(article)
        db.flush()
        created = True
    else:
        article.title = title
        article.source_name = source_name
        article.provider_name = provider_name
        article.content_hash = content_hash
        article.cluster_key = cluster_key
        if summary and (not article.summary or len(summary) > len(article.summary)):
            article.summary = summary
        if published_at and article.published_at and published_at > article.published_at:
            article.published_at = published_at

    return article, created


def _upsert_article_tickers(
    db: Session,
    article_id: int,
    ticker_hits: dict[str, tuple[str, float]],
    symbol_to_id: dict[str, int],
) -> None:
    if not ticker_hits:
        return

    existing = {
        row.ticker_id: row
        for row in db.scalars(select(ArticleTicker).where(ArticleTicker.article_id == article_id)).all()
    }

    for symbol, (match_type, confidence) in ticker_hits.items():
        ticker_id = symbol_to_id.get(symbol)
        if ticker_id is None:
            continue

        row = existing.get(ticker_id)
        if row is None:
            db.add(
                ArticleTicker(
                    article_id=article_id,
                    ticker_id=ticker_id,
                    match_type=match_type,
                    confidence=confidence,
                )
            )
            continue

        if confidence > row.confidence:
            row.confidence = confidence
            row.match_type = match_type


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

            title = str(entry.get("title") or "").strip()
            link = canonicalize_url(str(entry.get("link") or "").strip())
            if not title or not link:
                continue

            summary = str(entry.get("summary") or entry.get("description") or "").strip() or None
            published_at = parse_datetime(entry.get("published") or entry.get("updated"))
            if published_at is None:
                published_at = datetime.now(timezone.utc)

            provider_name = _clamp_label(source.name)
            entry_source_name = _extract_provider(entry, source_name)

            article, created = _upsert_article(
                db,
                canonical_url=link,
                title=title,
                summary=summary,
                published_at=published_at,
                source_name=source_name,
                provider_name=provider_name,
            )
            if created:
                items_inserted += 1

            ticker_hits = _extract_entry_tickers(title, summary or "", link, feed_url, known_symbols)
            _upsert_article_tickers(db, article.id, ticker_hits, symbol_to_id)

            payload = {
                "title": title,
                "link": link,
                "published": entry.get("published") or entry.get("updated"),
                "summary": summary,
                "source": entry_source_name,
            }
            db.add(
                RawFeedItem(
                    source_id=source.id,
                    article_id=article.id,
                    feed_url=feed_url,
                    raw_guid=str(entry.get("id") or entry.get("guid") or "") or None,
                    raw_title=title,
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


def run_ingestion_cycle(db: Session, settings: Settings) -> dict[str, int | list[dict[str, int | str | None]]]:
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

    return {
        "total_feeds": len(per_feed),
        "total_items_seen": total_seen,
        "total_items_inserted": total_inserted,
        "failed_feeds": failed,
        "feeds": per_feed,
    }
