from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select

from app.models import Article, RawFeedItem
from tests.helpers import (
    build_sqlite_session_factory,
    run_concurrent_ingests,
    seed_source,
    seed_ticker,
)


def _configure_yahoo_race_io(monkeypatch, feed_entries: dict[str, list[dict]]) -> None:
    class RoutedResponse:
        is_success = True
        text = ""

        def __init__(self, url: str):
            self.content = url.encode("utf-8")

        def raise_for_status(self) -> None:
            return None

    class RoutedClient:
        def get(self, url, *_args, **_kwargs):
            return RoutedResponse(str(url))

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: RoutedClient(),
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda content: SimpleNamespace(
            feed={"title": "Yahoo Finance"},
            entries=feed_entries[content.decode("utf-8")],
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: "",
    )


def test_concurrent_yahoo_mirror_ingests_share_one_article_and_two_raw_rows(
    monkeypatch,
):
    db_path = Path(__file__).resolve().parent / ".tmp_yahoo_article_race.db"
    engine, session_factory = build_sqlite_session_factory(db_path)
    setup_db = session_factory()
    try:
        yahoo = seed_source(
            setup_db,
            code="yahoo",
            name="Yahoo Finance",
            base_url="https://feeds.finance.yahoo.com",
        )
        ticker = seed_ticker(setup_db, symbol="UTF")
        canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
        _configure_yahoo_race_io(
            monkeypatch,
            {
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A": [
                    {
                        "id": "y-guid-a",
                        "guid": "y-guid-a",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": canonical_url + "?feedref=a",
                        "summary": "Yahoo mirror A",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B": [
                    {
                        "id": "y-guid-b",
                        "guid": "y-guid-b",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": canonical_url + "?feedref=b",
                        "summary": "Yahoo mirror B",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
            },
        )

        results = run_concurrent_ingests(
            session_factory,
            [
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
            ],
        )

        check_db = session_factory()
        try:
            articles = check_db.scalars(select(Article).order_by(Article.id.asc())).all()
            raw_rows = check_db.scalars(
                select(RawFeedItem).order_by(RawFeedItem.id.asc())
            ).all()
        finally:
            check_db.close()

        assert [result["status"] for result in results] == ["success", "success"]
        assert len(articles) == 1
        assert articles[0].canonical_url == canonical_url
        assert len(raw_rows) == 2
        assert {row.raw_guid for row in raw_rows} == {"y-guid-a", "y-guid-b"}
        assert {row.article_id for row in raw_rows} == {articles[0].id}
    finally:
        setup_db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_concurrent_yahoo_same_raw_identity_keeps_one_raw_row(monkeypatch):
    db_path = Path(__file__).resolve().parent / ".tmp_yahoo_raw_race.db"
    engine, session_factory = build_sqlite_session_factory(db_path)
    setup_db = session_factory()
    try:
        yahoo = seed_source(
            setup_db,
            code="yahoo",
            name="Yahoo Finance",
            base_url="https://feeds.finance.yahoo.com",
        )
        ticker = seed_ticker(setup_db, symbol="UTF")
        shared_feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF"
        _configure_yahoo_race_io(
            monkeypatch,
            {
                shared_feed_url: [
                    {
                        "id": "y-guid-shared",
                        "guid": "y-guid-shared",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": "https://example.com/shared-race-story",
                        "summary": "Shared Yahoo item",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ]
            },
        )

        results = run_concurrent_ingests(
            session_factory,
            [
                {
                    "source_id": yahoo.id,
                    "feed_url": shared_feed_url,
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
                {
                    "source_id": yahoo.id,
                    "feed_url": shared_feed_url,
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
            ],
        )

        check_db = session_factory()
        try:
            articles = check_db.scalars(select(Article).order_by(Article.id.asc())).all()
            raw_rows = check_db.scalars(
                select(RawFeedItem).order_by(RawFeedItem.id.asc())
            ).all()
        finally:
            check_db.close()

        assert [result["status"] for result in results] == ["success", "success"]
        assert len(articles) == 1
        assert len(raw_rows) == 1
        assert raw_rows[0].raw_guid == "y-guid-shared"
        assert raw_rows[0].article_id == articles[0].id
    finally:
        setup_db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_concurrent_yahoo_same_raw_identity_preserves_alt_feed_urls(monkeypatch):
    db_path = Path(__file__).resolve().parent / ".tmp_yahoo_raw_alt_race.db"
    engine, session_factory = build_sqlite_session_factory(db_path)
    setup_db = session_factory()
    try:
        yahoo = seed_source(
            setup_db,
            code="yahoo",
            name="Yahoo Finance",
            base_url="https://feeds.finance.yahoo.com",
        )
        ticker = seed_ticker(setup_db, symbol="UTF")
        _configure_yahoo_race_io(
            monkeypatch,
            {
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A": [
                    {
                        "id": "y-guid-shared",
                        "guid": "y-guid-shared",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": "https://example.com/shared-race-story",
                        "summary": "Shared Yahoo item",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B": [
                    {
                        "id": "y-guid-shared",
                        "guid": "y-guid-shared",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": "https://example.com/shared-race-story",
                        "summary": "Shared Yahoo item",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
            },
        )

        results = run_concurrent_ingests(
            session_factory,
            [
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
            ],
        )

        check_db = session_factory()
        try:
            raw_rows = check_db.scalars(
                select(RawFeedItem).order_by(RawFeedItem.id.asc())
            ).all()
        finally:
            check_db.close()

        assert [result["status"] for result in results] == ["success", "success"]
        assert len(raw_rows) == 1
        alt = (raw_rows[0].raw_payload_json or {}).get("_alt_feed_urls") or []
        all_feed_urls = {raw_rows[0].feed_url} | set(alt)
        assert "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A" in all_feed_urls
        assert "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B" in all_feed_urls
    finally:
        setup_db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_concurrent_yahoo_different_guids_same_link_pubdate_merges(monkeypatch):
    db_path = Path(__file__).resolve().parent / ".tmp_yahoo_guid_pair_race.db"
    engine, session_factory = build_sqlite_session_factory(db_path)
    setup_db = session_factory()
    try:
        yahoo = seed_source(
            setup_db,
            code="yahoo",
            name="Yahoo Finance",
            base_url="https://feeds.finance.yahoo.com",
        )
        ticker = seed_ticker(setup_db, symbol="UTF")
        shared_link = "https://www.businesswire.com/news/home/20260301000001/en"
        _configure_yahoo_race_io(
            monkeypatch,
            {
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A": [
                    {
                        "id": "guid-chunk-a",
                        "guid": "guid-chunk-a",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": shared_link,
                        "summary": "Yahoo mirror A",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
                "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B": [
                    {
                        "id": "guid-chunk-b",
                        "guid": "guid-chunk-b",
                        "title": "ACME distribution update NYSE: UTF",
                        "link": shared_link,
                        "summary": "Yahoo mirror B",
                        "published": "2026-03-01T00:00:00Z",
                    }
                ],
            },
        )

        results = run_concurrent_ingests(
            session_factory,
            [
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
                {
                    "source_id": yahoo.id,
                    "feed_url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B",
                    "overrides": {
                        "known_symbols": {"UTF"},
                        "symbol_to_id": {"UTF": ticker.id},
                    },
                },
            ],
        )

        check_db = session_factory()
        try:
            articles = check_db.scalars(select(Article).order_by(Article.id.asc())).all()
            raw_rows = check_db.scalars(
                select(RawFeedItem).order_by(RawFeedItem.id.asc())
            ).all()
        finally:
            check_db.close()

        assert [result["status"] for result in results] == ["success", "success"]
        assert len(articles) == 1
        assert len(raw_rows) == 1
        assert raw_rows[0].article_id == articles[0].id
        alt = (raw_rows[0].raw_payload_json or {}).get("_alt_feed_urls") or []
        all_feed_urls = {raw_rows[0].feed_url} | set(alt)
        assert "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-A" in all_feed_urls
        assert "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF-B" in all_feed_urls
    finally:
        setup_db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()
