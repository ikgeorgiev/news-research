from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.article_ingest import ingest_feed
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.utils import sha256_str


class FakeRssResponse:
    """Minimal stand-in for ``httpx.Response`` — returns valid RSS bytes."""

    content = b"<rss />"
    is_success = True
    text = ""

    def raise_for_status(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def seed_source(
    db: Session,
    *,
    code: str = "businesswire",
    name: str = "Business Wire",
    base_url: str = "https://www.businesswire.com",
) -> Source:
    source = Source(code=code, name=name, base_url=base_url, enabled=True)
    db.add(source)
    db.commit()
    db.refresh(source)
    return source


def seed_article(
    db: Session,
    *,
    slug: str | None = None,
    canonical_url: str | None = None,
    title: str | None = None,
    summary: str = "",
    published_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    source_name: str = "Test Source",
    provider_name: str = "Test Provider",
) -> Article:
    """Create and persist an Article row.

    When *slug* is provided, ``canonical_url`` and ``title`` are derived from
    it (unless explicitly supplied).  ``created_at`` defaults to
    *published_at*; ``updated_at`` defaults to *created_at*.
    """
    if canonical_url is None and slug is not None:
        canonical_url = f"https://example.com/{slug}"
    if title is None and slug is not None:
        title = f"Title {slug}"

    effective_created = created_at if created_at is not None else published_at
    effective_updated = updated_at if updated_at is not None else effective_created

    if slug is not None:
        content_hash = sha256_str(f"content-{slug}")
        title_hash = sha256_str(f"title-{slug}")
        cluster_hash = sha256_str(f"cluster-{slug}")
    else:
        content_hash = sha256_str(f"content:{canonical_url}")
        title_hash = sha256_str(f"title:{title.lower()}")
        cluster_hash = sha256_str(f"cluster:{title.lower()}")

    article = Article(
        canonical_url=canonical_url,
        canonical_url_hash=sha256_str(canonical_url),
        title=title,
        summary=summary,
        published_at=published_at,
        source_name=source_name,
        provider_name=provider_name,
        content_hash=content_hash,
        title_normalized_hash=title_hash,
        cluster_key=cluster_hash,
        created_at=effective_created,
        updated_at=effective_updated,
    )
    db.add(article)
    db.commit()
    db.refresh(article)
    return article


def seed_ticker(db: Session, *, symbol: str, **kwargs: Any) -> Ticker:
    """Create and persist a Ticker row.  ``active`` defaults to ``True``."""
    kwargs.setdefault("active", True)
    ticker = Ticker(symbol=symbol, **kwargs)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    return ticker


# ---------------------------------------------------------------------------
# Ingest wrapper
# ---------------------------------------------------------------------------


def call_ingest(
    db: Session,
    source: Source,
    feed_url: str,
    **overrides: Any,
) -> dict:
    """Call ``ingest_feed`` with sensible test defaults.

    Defaults: ``known_symbols=set()``, ``symbol_to_id={}``,
    ``timeout_seconds=5``, ``fetch_max_attempts=1``,
    ``fetch_backoff_seconds=0.0``, ``fetch_backoff_jitter_seconds=0.0``.
    """
    defaults: dict[str, Any] = dict(
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )
    defaults.update(overrides)
    return ingest_feed(db, source=source, feed_url=feed_url, **defaults)


# ---------------------------------------------------------------------------
# Composite seed helper
# ---------------------------------------------------------------------------


def seed_article_with_raw(
    db: Session,
    source: Source,
    *,
    ticker_kwargs: dict[str, Any],
    article_kwargs: dict[str, Any],
    match_type: str,
    confidence: float,
    raw_guid: str,
    feed_url: str,
    extraction_version: int | None = None,
    raw_title: str | None = None,
    raw_payload_json: dict | None = None,
) -> tuple[Article, Ticker]:
    """Create article + ticker + ArticleTicker + RawFeedItem in one call."""
    ticker = seed_ticker(db, **ticker_kwargs)
    article = seed_article(db, **article_kwargs)

    at_kwargs: dict[str, Any] = dict(
        article_id=article.id,
        ticker_id=ticker.id,
        match_type=match_type,
        confidence=confidence,
    )
    if extraction_version is not None:
        at_kwargs["extraction_version"] = extraction_version

    raw_kwargs: dict[str, Any] = dict(
        source_id=source.id,
        article_id=article.id,
        feed_url=feed_url,
        raw_guid=raw_guid,
        raw_link=article.canonical_url,
        raw_pub_date=article.published_at,
        raw_payload_json=raw_payload_json if raw_payload_json is not None else {},
    )
    if raw_title is not None:
        raw_kwargs["raw_title"] = raw_title

    db.add_all([ArticleTicker(**at_kwargs), RawFeedItem(**raw_kwargs)])
    db.commit()
    db.refresh(article)
    return article, ticker
