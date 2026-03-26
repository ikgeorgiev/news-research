from __future__ import annotations

from pathlib import Path
import uuid

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.article_ingest import _upsert_raw_feed_item
from app.models import RawFeedItem, Source
from migrate import run_migrations


def test_alembic_upgrade_head_creates_expected_schema():
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / f"alembic-schema-{uuid.uuid4().hex}.db"
    database_url = f"sqlite:///{db_path.as_posix()}"

    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.set_main_option("script_location", str(Path(__file__).resolve().parents[1] / "alembic"))
    config.attributes["database_url"] = database_url

    try:
        command.upgrade(config, "head")

        engine = create_engine(database_url)
        try:
            inspector = inspect(engine)

            assert sorted(inspector.get_table_names()) == [
                "alembic_version",
                "article_tickers",
                "articles",
                "feed_poll_state",
                "ingestion_runs",
                "push_subscriptions",
                "raw_feed_items",
                "sources",
                "tickers",
            ]

            article_columns = {column["name"] for column in inspector.get_columns("articles")}
            ticker_columns = {column["name"] for column in inspector.get_columns("tickers")}
            article_ticker_columns = {column["name"] for column in inspector.get_columns("article_tickers")}

            assert {"first_seen_at", "first_alert_sent_at"} <= article_columns
            assert "validation_keywords" in ticker_columns
            assert "extraction_version" in article_ticker_columns
        finally:
            engine.dispose()
    finally:
        if db_path.exists():
            db_path.unlink()


def test_run_migrations_bootstraps_legacy_schema_before_stamping():
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / f"alembic-legacy-{uuid.uuid4().hex}.db"
    database_url = f"sqlite:///{db_path.as_posix()}"

    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE articles (
                        id INTEGER PRIMARY KEY,
                        canonical_url TEXT NOT NULL,
                        canonical_url_hash VARCHAR(64) NOT NULL,
                        title TEXT NOT NULL,
                        summary TEXT,
                        published_at DATETIME NOT NULL,
                        source_name VARCHAR(120) NOT NULL,
                        provider_name VARCHAR(120) NOT NULL,
                        content_hash VARCHAR(64) NOT NULL,
                        title_normalized_hash VARCHAR(64) NOT NULL,
                        cluster_key VARCHAR(64) NOT NULL,
                        created_at DATETIME NOT NULL,
                        updated_at DATETIME NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO articles (
                        id,
                        canonical_url,
                        canonical_url_hash,
                        title,
                        summary,
                        published_at,
                        source_name,
                        provider_name,
                        content_hash,
                        title_normalized_hash,
                        cluster_key,
                        created_at,
                        updated_at
                    ) VALUES (
                        1,
                        'https://example.com/legacy',
                        'legacy-hash',
                        'Legacy row',
                        'Legacy summary',
                        '2026-03-01 12:00:00',
                        'Legacy Source',
                        'Legacy Provider',
                        'content-hash',
                        'title-hash',
                        'cluster-hash',
                        '2026-03-01 11:00:00',
                        '2026-03-01 11:00:00'
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE tickers (
                        id INTEGER PRIMARY KEY,
                        symbol VARCHAR(16) NOT NULL,
                        fund_name VARCHAR(255),
                        sponsor VARCHAR(255),
                        active BOOLEAN NOT NULL,
                        created_at DATETIME NOT NULL,
                        updated_at DATETIME NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE article_tickers (
                        id INTEGER PRIMARY KEY,
                        article_id INTEGER NOT NULL,
                        ticker_id INTEGER NOT NULL,
                        match_type VARCHAR(32),
                        confidence FLOAT,
                        CONSTRAINT uq_article_ticker UNIQUE (article_id, ticker_id)
                    )
                    """
                )
            )

        run_migrations(database_url=database_url)

        inspector = inspect(engine)
        article_columns = {column["name"] for column in inspector.get_columns("articles")}
        ticker_columns = {column["name"] for column in inspector.get_columns("tickers")}
        article_ticker_columns = {column["name"] for column in inspector.get_columns("article_tickers")}

        assert "alembic_version" in inspector.get_table_names()
        assert {"first_seen_at", "first_alert_sent_at"} <= article_columns
        assert "validation_keywords" in ticker_columns
        assert "extraction_version" in article_ticker_columns

        with engine.connect() as connection:
            legacy_row = connection.execute(
                text("SELECT first_seen_at, first_alert_sent_at FROM articles WHERE id = 1")
            ).mappings().one()
        assert legacy_row["first_seen_at"] is not None
        assert legacy_row["first_alert_sent_at"] is None
    finally:
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_alembic_sqlite_schema_supports_raw_feed_item_upsert():
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / f"alembic-upsert-{uuid.uuid4().hex}.db"
    database_url = f"sqlite:///{db_path.as_posix()}"

    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[1] / "alembic"),
    )
    config.attributes["database_url"] = database_url

    try:
        command.upgrade(config, "head")

        engine = create_engine(database_url)
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        try:
            db = SessionLocal()
            try:
                source = Source(
                    code="test",
                    name="Test Source",
                    base_url="https://example.com",
                )
                db.add(source)
                db.commit()
                db.refresh(source)

                _upsert_raw_feed_item(
                    db,
                    {
                        "source_id": source.id,
                        "article_id": None,
                        "feed_url": "https://example.com/feed.xml",
                        "raw_guid": "guid-1",
                        "raw_title": "Title",
                        "raw_link": "https://example.com/story",
                        "raw_pub_date": None,
                        "raw_payload_json": {"title": "Title"},
                    },
                )
                db.commit()

                raw_rows = db.query(RawFeedItem).all()
                assert len(raw_rows) == 1
                assert raw_rows[0].raw_guid == "guid-1"
            finally:
                db.close()
        finally:
            engine.dispose()
    finally:
        if db_path.exists():
            db_path.unlink()
