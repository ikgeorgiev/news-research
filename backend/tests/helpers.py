from __future__ import annotations

from datetime import datetime
from pathlib import Path
import threading
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from app.article_ingest import ingest_feed
from app.database import Base
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


def build_article_ticker(
    *,
    article: Article | int,
    ticker: Ticker | int,
    match_type: str,
    confidence: float,
    extraction_version: int | None = None,
) -> ArticleTicker:
    row = ArticleTicker(
        article_id=article.id if isinstance(article, Article) else article,
        ticker_id=ticker.id if isinstance(ticker, Ticker) else ticker,
        match_type=match_type,
        confidence=confidence,
    )
    if extraction_version is not None:
        row.extraction_version = extraction_version
    return row


def build_raw_feed_item(
    *,
    source: Source | int,
    article: Article | int | None,
    feed_url: str,
    raw_guid: str | None,
    raw_link: str,
    raw_pub_date: datetime | None,
    raw_payload_json: dict | None = None,
    raw_title: str | None = None,
) -> RawFeedItem:
    row = RawFeedItem(
        source_id=source.id if isinstance(source, Source) else source,
        article_id=(
            article.id
            if isinstance(article, Article)
            else article
        ),
        feed_url=feed_url,
        raw_guid=raw_guid,
        raw_link=raw_link,
        raw_pub_date=raw_pub_date,
        raw_payload_json=raw_payload_json if raw_payload_json is not None else {},
    )
    if raw_title is not None:
        row.raw_title = raw_title
    return row


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

    raw_kwargs: dict[str, Any] = dict(
        source=source,
        article=article,
        feed_url=feed_url,
        raw_guid=raw_guid,
        raw_link=article.canonical_url,
        raw_pub_date=article.published_at,
        raw_payload_json=raw_payload_json if raw_payload_json is not None else {},
    )
    if raw_title is not None:
        raw_kwargs["raw_title"] = raw_title

    db.add_all(
        [
            build_article_ticker(
                article=article,
                ticker=ticker,
                match_type=match_type,
                confidence=confidence,
                extraction_version=extraction_version,
            ),
            build_raw_feed_item(**raw_kwargs),
        ]
    )
    db.commit()
    db.refresh(article)
    return article, ticker


def build_sqlite_session_factory(db_path: Path) -> tuple[Any, sessionmaker[Session]]:
    """Create a file-backed SQLite session factory suitable for multi-session tests."""
    if db_path.exists():
        db_path.unlink()
    engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 5},
    )
    Base.metadata.create_all(bind=engine)
    return engine, sessionmaker(autoflush=False, autocommit=False, bind=engine)


def run_concurrent_ingests(
    session_factory: sessionmaker[Session],
    jobs: list[dict[str, Any]],
) -> list[dict]:
    """Run multiple ``call_ingest`` jobs in parallel using separate sessions."""
    barrier = threading.Barrier(len(jobs))
    results: list[dict | None] = [None] * len(jobs)
    errors: list[BaseException | None] = [None] * len(jobs)

    def worker(index: int, job: dict[str, Any]) -> None:
        db = session_factory()
        try:
            source = db.get(Source, job["source_id"])
            if source is None:
                raise RuntimeError(f"Source id {job['source_id']} not found")
            barrier.wait(timeout=5)
            overrides = dict(job.get("overrides", {}))
            results[index] = call_ingest(db, source, job["feed_url"], **overrides)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors[index] = exc
        finally:
            db.close()

    threads = [
        threading.Thread(target=worker, args=(idx, job), daemon=True)
        for idx, job in enumerate(jobs)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    if any(thread.is_alive() for thread in threads):
        raise RuntimeError("Concurrent ingest worker did not finish in time")
    for error in errors:
        if error is not None:
            raise error
    return [result for result in results if result is not None]
