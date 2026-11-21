from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session

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
        "article_enrichment",
        "articles",
        "feed_poll_state",
        "ingestion_poll_daily",
        "ingestion_runs",
        "push_subscriptions",
        "raw_feed_items",
        "source_page_controls",
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

    enrichment_columns = {column["name"] for column in inspector.get_columns("article_enrichment")}
    assert {"article_id", "source_id", "page_url", "feed_url", "status", "attempts",
            "available_at", "lease_token", "created_at", "completed_at"} == enrichment_columns
    assert inspector.get_pk_constraint("article_enrichment")["constrained_columns"] == ["article_id"]
    assert "ix_article_enrichment_due" in {
        index["name"] for index in inspector.get_indexes("article_enrichment")
    }
    run_indexes = {idx["name"]: idx["column_names"] for idx in inspector.get_indexes("ingestion_runs")}
    assert run_indexes["ix_ingestion_runs_started_at"] == ["started_at"]
    assert run_indexes["ix_runs_source_started"] == ["source_id", "started_at"]

    index_names = {index["name"] for index in inspector.get_indexes("raw_feed_items")}
    assert {
        "uq_raw_feed_items_source_guid_nn",
        "uq_raw_feed_items_source_link_pub_date_nn",
        "uq_raw_feed_items_source_link_guidless_undated",
    } <= index_names


def test_postgres_raw_feed_item_upsert_uses_psycopg_conflict_handling(postgres_engine):
    # Let the upsert commit normally while discarding the test's rows on exit.
    with postgres_engine.connect() as connection, Session(
        connection, autoflush=False, join_transaction_mode="create_savepoint"
    ) as db:
        connection.begin()
        source = Source(
            code=f"ci-postgres-{uuid4().hex[:12]}",
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
