from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import count_news, health, list_news, list_news_ids, require_admin_api_key, settings
from app.models import Article, ArticleTicker, Ticker
from app.ticker_loader import load_tickers_from_csv
from app.utils import sha256_str


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


def _seed_article(
    db: Session,
    *,
    slug: str,
    published_at: datetime,
    created_at: datetime | None = None,
) -> Article:
    article = Article(
        canonical_url=f"https://example.com/{slug}",
        canonical_url_hash=sha256_str(f"https://example.com/{slug}"),
        title=f"Title {slug}",
        summary=f"Summary {slug}",
        published_at=published_at,
        source_name="Test Source",
        provider_name="Test Provider",
        content_hash=sha256_str(f"content-{slug}"),
        title_normalized_hash=sha256_str(f"title-{slug}"),
        cluster_key=sha256_str(f"cluster-{slug}"),
        created_at=created_at or published_at,
        updated_at=created_at or published_at,
    )
    db.add(article)
    db.commit()
    db.refresh(article)
    return article


def test_require_admin_api_key_rejects_when_not_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", None)
    with pytest.raises(HTTPException) as exc:
        require_admin_api_key("anything")
    assert exc.value.status_code == 503


def test_require_admin_api_key_rejects_invalid_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", "expected-key")
    with pytest.raises(HTTPException) as exc:
        require_admin_api_key("wrong-key")
    assert exc.value.status_code == 401


def test_require_admin_api_key_accepts_valid_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", "expected-key")
    require_admin_api_key("expected-key")


def test_health_returns_degraded_when_db_check_raises(monkeypatch: pytest.MonkeyPatch):
    def _raise() -> bool:
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("app.main.db_health_check", _raise)
    response = health()
    assert response["status"] == "degraded"


def test_count_news_treats_empty_ticker_query_like_default_mapped_only():
    db = _make_db_session()
    ticker = Ticker(symbol="AAA", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)

    mapped_article = _seed_article(
        db,
        slug="mapped",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    unmapped_article = _seed_article(
        db,
        slug="unmapped",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    db.add(ArticleTicker(article_id=mapped_article.id, ticker_id=ticker.id))
    db.commit()

    response = count_news(
        ticker=" , ",
        include_unmapped=False,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        db=db,
    )
    assert response.total == 1
    assert unmapped_article.id != mapped_article.id
    db.close()


def test_list_news_cursor_paginates_consistently():
    db = _make_db_session()
    newest = _seed_article(
        db,
        slug="newest",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    middle = _seed_article(
        db,
        slug="middle",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    oldest = _seed_article(
        db,
        slug="oldest",
        published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    page_one = list_news(
        limit=1,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=None,
        db=db,
    )
    assert [item.id for item in page_one.items] == [newest.id]
    assert page_one.next_cursor is not None

    page_two = list_news(
        limit=2,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=page_one.next_cursor,
        db=db,
    )
    assert [item.id for item in page_two.items] == [middle.id, oldest.id]
    db.close()


def test_list_news_ids_supports_cursor_pagination():
    db = _make_db_session()
    first = _seed_article(
        db,
        slug="first",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    second = _seed_article(
        db,
        slug="second",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    third = _seed_article(
        db,
        slug="third",
        published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    page_one = list_news_ids(
        limit=2,
        cursor=None,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        db=db,
    )
    assert page_one.ids == [first.id, second.id]
    assert page_one.next_cursor is not None

    page_two = list_news_ids(
        limit=2,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=page_one.next_cursor,
        db=db,
    )
    assert page_two.ids == [third.id]
    assert page_two.next_cursor is None
    db.close()


def test_ticker_loader_accepts_case_insensitive_header():
    db = _make_db_session()
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temp_dir / f"tickers-{uuid.uuid4().hex}.csv"
    csv_path.write_text("Ticker,fund_name,sponsor,active\nAAA,Fund A,Sponsor A,true\n", encoding="utf-8")

    try:
        stats = load_tickers_from_csv(db, str(csv_path))
        ticker = db.scalar(select(Ticker).where(Ticker.symbol == "AAA"))

        assert stats["loaded"] == 1
        assert stats["created"] == 1
        assert ticker is not None
        assert ticker.fund_name == "Fund A"
    finally:
        if csv_path.exists():
            csv_path.unlink()
    db.close()
