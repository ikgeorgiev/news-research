from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker

from app.models import RawFeedItem, Source
from app.raw_feed_items import _upsert_raw_feed_item


DATABASE_URL = os.getenv("DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL or not DATABASE_URL.startswith("postgresql+psycopg://"),
    reason="Postgres-only integration coverage",
)


@pytest.fixture()
def postgres_engine():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


def test_postgres_migration_schema_is_visible(postgres_engine):
    inspector = inspect(postgres_engine)

    tables = set(inspector.get_table_names())
    assert {
        "alembic_version",
        "article_tickers",
        "articles",
        "feed_poll_state",
        "ingestion_runs",
        "push_subscriptions",
        "raw_feed_items",
        "sources",
        "tickers",
    } <= tables

    article_columns = {column["name"] for column in inspector.get_columns("articles")}
    ticker_columns = {column["name"] for column in inspector.get_columns("tickers")}
    article_ticker_columns = {
        column["name"] for column in inspector.get_columns("article_tickers")
    }

    assert {"first_seen_at", "first_alert_sent_at"} <= article_columns
    assert "validation_keywords" in ticker_columns
    assert "extraction_version" in article_ticker_columns

    index_names = {index["name"] for index in inspector.get_indexes("raw_feed_items")}
    assert {
        "uq_raw_feed_items_source_guid_nn",
        "uq_raw_feed_items_source_link_pub_date_nn",
        "uq_raw_feed_items_source_link_guidless_undated",
    } <= index_names


def test_postgres_raw_feed_item_upsert_uses_psycopg_conflict_handling(postgres_engine):
    session_factory = sessionmaker(
        bind=postgres_engine,
        autoflush=False,
        autocommit=False,
    )
    db = session_factory()
    try:
        source = Source(
            code="ci-postgres",
            name="CI Postgres",
            base_url="https://example.com",
            enabled=True,
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        first_feed_url = "https://example.com/feed-a.xml"
        second_feed_url = "https://example.com/feed-b.xml"
        base_values = {
            "source_id": source.id,
            "article_id": None,
            "raw_guid": "guid-1",
            "raw_title": "Story title",
            "raw_link": "https://example.com/story",
            "raw_pub_date": None,
            "raw_payload_json": {"title": "Story title"},
        }

        assert _upsert_raw_feed_item(
            db,
            {
                **base_values,
                "feed_url": first_feed_url,
            },
        )
        db.commit()

        assert _upsert_raw_feed_item(
            db,
            {
                **base_values,
                "feed_url": second_feed_url,
                "raw_payload_json": {"title": "Story title", "updated": True},
            },
        )
        db.commit()

        rows = db.scalars(
            select(RawFeedItem).where(RawFeedItem.source_id == source.id)
        ).all()
        assert len(rows) == 1

        row = rows[0]
        assert row.feed_url == second_feed_url
        assert row.raw_guid == "guid-1"
        assert row.raw_payload_json["_alt_feed_urls"] == [first_feed_url]
    finally:
        db.close()
