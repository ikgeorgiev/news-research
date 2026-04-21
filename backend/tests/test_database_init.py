from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
import uuid

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

import pytest

from app import database
import app.main as app_main
from app.raw_feed_items import _upsert_raw_feed_item
from app.models import RawFeedItem, Source
import migrate
from migrate import run_migrations


class _RecordingConnection:
    def __init__(self, dialect_name: str, events: list[object]):
        self.dialect = SimpleNamespace(name=dialect_name)
        self.events = events

    def begin(self):
        return self

    def begin_nested(self):
        return self

    def in_transaction(self):
        return False

    def rollback(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = getattr(statement, "text", str(statement))
        if "pg_advisory_lock" in sql:
            self.events.append("lock")
        elif "pg_advisory_unlock" in sql:
            self.events.append("unlock")
        else:
            self.events.append(sql)
        return SimpleNamespace()


class _RecordingEngine:
    def __init__(self, connection: _RecordingConnection):
        self.connection = connection
        self.disposed = False

    def connect(self):
        return self

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False

    def dispose(self):
        self.disposed = True


class _SchemaCheckResult:
    def __init__(self, revision: str | None):
        self.revision = revision

    def scalar_one_or_none(self):
        return self.revision


class _SchemaCheckConnection:
    def __init__(self, revision: str | None):
        self.revision = revision
        self.disposed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        return _SchemaCheckResult(self.revision)


class _SchemaCheckEngine:
    def __init__(self, connection: _SchemaCheckConnection):
        self.connection = connection
        self.disposed = False

    def connect(self):
        return self.connection

    def dispose(self):
        self.disposed = True


def test_get_engine_kwargs_adds_pool_settings_for_postgresql(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_settings",
        lambda: SimpleNamespace(
            database_pool_size=12,
            database_max_overflow=34,
            database_pool_timeout=56.0,
        ),
    )

    assert database.get_engine_kwargs("postgresql+psycopg://example/db") == {
        "pool_pre_ping": True,
        "pool_size": 12,
        "max_overflow": 34,
        "pool_timeout": 56.0,
    }


def test_get_engine_kwargs_skips_pool_settings_for_sqlite(monkeypatch):
    monkeypatch.setattr(
        database,
        "get_settings",
        lambda: SimpleNamespace(
            database_pool_size=12,
            database_max_overflow=34,
            database_pool_timeout=56.0,
        ),
    )

    assert database.get_engine_kwargs("sqlite:///example.db") == {"pool_pre_ping": True}


def test_assert_schema_at_head_skips_sqlite(monkeypatch):
    def fail_if_called(*_args, **_kwargs):
        pytest.fail("SQLite startup should not hit the schema gate")

    monkeypatch.setattr(migrate, "create_engine", fail_if_called)
    monkeypatch.setattr(migrate, "_get_alembic_head_revision", fail_if_called)

    migrate.assert_schema_at_head("sqlite:///example.db")


def test_assert_schema_at_head_refuses_stale_postgresql_schema(monkeypatch):
    connection = _SchemaCheckConnection(revision="20260319_0001")
    engine = _SchemaCheckEngine(connection)
    inspector = SimpleNamespace(get_table_names=lambda: ["alembic_version"])

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "_get_alembic_head_revision", lambda _url: "20260421_0001")
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)

    with pytest.raises(RuntimeError, match="not at Alembic head"):
        migrate.assert_schema_at_head("postgresql+psycopg://example/db")

    assert engine.disposed is True


def test_startup_warns_when_ingestion_workers_exceed_pool_capacity(caplog):
    settings = SimpleNamespace(
        database_pool_size=2,
        database_max_overflow=1,
        ingestion_max_workers=4,
    )

    with caplog.at_level(logging.WARNING, logger=app_main.__name__):
        app_main._warn_if_ingestion_pool_capacity_low(settings)

    assert "exceeds configured DB pool capacity" in caplog.text


def test_lifespan_stops_before_runtime_bootstrap_on_schema_failure(monkeypatch):
    monkeypatch.setattr(
        app_main,
        "get_settings",
        lambda: SimpleNamespace(
            database_url="postgresql+psycopg://example/db",
            behind_proxy=False,
            trusted_proxy_networks=(),
            cors_origins_list=[],
            api_prefix="/api/v1",
            sse_max_connections_per_ip=5,
            database_pool_size=10,
            database_max_overflow=10,
            ingestion_max_workers=4,
        ),
    )
    def fail_schema_check(*_args, **_kwargs):
        raise RuntimeError("stale schema")

    def fail_runtime_bootstrap(*_args, **_kwargs):
        pytest.fail("runtime bootstrap should not start")

    monkeypatch.setattr(app_main, "assert_schema_at_head", fail_schema_check)
    monkeypatch.setattr(app_main, "get_session_factory", lambda: fail_runtime_bootstrap)
    monkeypatch.setattr(app_main, "sync_runtime_state", fail_runtime_bootstrap)

    async def _run():
        with pytest.raises(RuntimeError, match="stale schema"):
            async with app_main.lifespan(app_main.app):
                pytest.fail("lifespan should not reach startup yield")

    asyncio.run(_run())


def test_run_migrations_uses_postgresql_advisory_lock_around_upgrade(monkeypatch):
    events: list[object] = []
    connection = _RecordingConnection("postgresql", events)
    engine = _RecordingEngine(connection)
    inspector = SimpleNamespace(
        get_table_names=lambda: [],
        get_unique_constraints=lambda _table: [],
    )

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)
    monkeypatch.setattr(
        migrate.command,
        "upgrade",
        lambda config, revision: events.append(
            ("upgrade", revision, config.attributes["connection"] is connection)
        ),
    )
    monkeypatch.setattr(migrate.command, "stamp", lambda *_args, **_kwargs: pytest.fail("stamp should not run"))
    monkeypatch.setattr(
        migrate,
        "get_settings",
        lambda: SimpleNamespace(migration_advisory_lock_key=987654321),
    )

    run_migrations(database_url="postgresql+psycopg://example/db")

    assert events == [
        "lock",
        ("upgrade", "head", True),
        "unlock",
    ]
    assert engine.disposed is True


def test_run_migrations_skips_advisory_lock_for_sqlite(monkeypatch):
    events: list[object] = []
    connection = _RecordingConnection("sqlite", events)
    engine = _RecordingEngine(connection)
    inspector = SimpleNamespace(
        get_table_names=lambda: [],
        get_unique_constraints=lambda _table: [],
    )

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)
    monkeypatch.setattr(
        migrate.command,
        "upgrade",
        lambda config, revision: events.append(
            ("upgrade", revision, config.attributes["connection"] is connection)
        ),
    )
    monkeypatch.setattr(migrate.command, "stamp", lambda *_args, **_kwargs: pytest.fail("stamp should not run"))

    run_migrations(database_url="sqlite:///:memory:")

    assert events == [("upgrade", "head", True)]
    assert engine.disposed is True


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
