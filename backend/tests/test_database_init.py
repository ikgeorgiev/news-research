from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

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


def test_assert_schema_at_head_refuses_missing_app_tables(monkeypatch):
    connection = _SchemaCheckConnection(revision="20260421_0001")
    engine = _SchemaCheckEngine(connection)
    inspector = SimpleNamespace(get_table_names=lambda: ["alembic_version", "articles"])

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "_get_alembic_head_revision", lambda _url: "20260421_0001")
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)

    with pytest.raises(RuntimeError, match="missing required app tables"):
        migrate.assert_schema_at_head("postgresql+psycopg://example/db")

    assert engine.disposed is True


def test_assert_schema_at_head_refuses_missing_app_columns(monkeypatch):
    connection = _SchemaCheckConnection(revision="20260421_0001")
    engine = _SchemaCheckEngine(connection)
    inspector = SimpleNamespace(
        get_table_names=lambda: [
            "alembic_version",
            *database.Base.metadata.tables.keys(),
        ],
        get_columns=lambda table_name: [
            {"name": column_name}
            for column_name in database.Base.metadata.tables[table_name].columns.keys()
            if not (
                table_name == "push_subscriptions"
                and column_name == "key_auth"
            )
        ],
    )

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "_get_alembic_head_revision", lambda _url: "20260421_0001")
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)

    with pytest.raises(
        RuntimeError,
        match=r"missing required app columns: push_subscriptions\.key_auth",
    ):
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
    monkeypatch.setattr(migrate, "_assert_app_schema_present", lambda *_args, **_kwargs: None)
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


def test_run_migrations_rolls_back_implicit_transactions_before_unlock(monkeypatch):
    events: list[object] = []
    connection = _RecordingConnection("postgresql", events)
    connection.in_transaction = lambda: True
    connection.rollback = lambda: events.append("rollback")
    engine = _RecordingEngine(connection)
    inspector = SimpleNamespace(get_table_names=lambda: [])

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)
    monkeypatch.setattr(migrate, "_assert_app_schema_present", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        migrate.command,
        "upgrade",
        lambda _config, revision: events.append(("upgrade", revision)),
    )
    monkeypatch.setattr(
        migrate,
        "get_settings",
        lambda: SimpleNamespace(migration_advisory_lock_key=987654321),
    )

    run_migrations(database_url="postgresql+psycopg://example/db")

    assert events == [
        "lock",
        "rollback",
        ("upgrade", "head"),
        "rollback",
        "unlock",
    ]


def test_run_migrations_validates_legacy_snapshot_before_future_upgrade(monkeypatch):
    events: list[object] = []
    state = {"upgraded": False}
    connection = _RecordingConnection("sqlite", events)
    engine = _RecordingEngine(connection)

    current_required_columns = dict(migrate.BASELINE_REQUIRED_COLUMNS)
    current_required_columns["articles"] = (
        current_required_columns["articles"] | {"future_migration_column"}
    )

    inspector = SimpleNamespace(
        get_table_names=lambda: list(migrate.BASELINE_REQUIRED_COLUMNS),
        get_columns=lambda table_name: [
            {"name": column_name}
            for column_name in (
                migrate.BASELINE_REQUIRED_COLUMNS[table_name]
                | (
                    {"future_migration_column"}
                    if state["upgraded"] and table_name == "articles"
                    else set()
                )
            )
        ],
    )

    monkeypatch.setattr(migrate, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(migrate, "inspect", lambda _connection: inspector)
    monkeypatch.setattr(migrate, "_bootstrap_legacy_schema", lambda _connection: None)
    monkeypatch.setattr(
        migrate,
        "_current_required_columns",
        lambda: current_required_columns,
    )
    monkeypatch.setattr(
        migrate.command,
        "stamp",
        lambda _config, revision: events.append(("stamp", revision)),
    )

    def upgrade(_config, revision):
        events.append(("upgrade", revision))
        state["upgraded"] = True

    monkeypatch.setattr(migrate.command, "upgrade", upgrade)

    run_migrations(database_url="sqlite:///:memory:")

    assert events == [
        ("stamp", migrate.BASELINE_REVISION),
        ("upgrade", "head"),
    ]


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
    monkeypatch.setattr(migrate, "_assert_app_schema_present", lambda *_args, **_kwargs: None)
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


def test_alembic_upgrade_head_creates_expected_schema(scratch_dir: Path):
    db_path = scratch_dir / "alembic-schema.db"
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
                "article_enrichment",
                "article_tickers",
                "articles",
                "feed_poll_state",
                "ingestion_runs",
                "push_subscriptions",
                "raw_feed_items",
                "source_page_controls",
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


def test_legacy_schema_snapshot_matches_alembic_baseline_revision(
    scratch_dir: Path,
):
    db_path = scratch_dir / "alembic-baseline.db"
    database_url = f"sqlite:///{db_path.as_posix()}"
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[1] / "alembic"),
    )
    config.attributes["database_url"] = database_url

    try:
        command.upgrade(config, migrate.BASELINE_REVISION)
        engine = create_engine(database_url)
        try:
            inspector = inspect(engine)
            assert set(migrate.BASELINE_REQUIRED_COLUMNS) == (
                set(inspector.get_table_names()) - {"alembic_version"}
            )
            for table_name, expected_columns in migrate.BASELINE_REQUIRED_COLUMNS.items():
                assert expected_columns == frozenset(
                    str(column["name"])
                    for column in inspector.get_columns(table_name)
                )
        finally:
            engine.dispose()
    finally:
        if db_path.exists():
            db_path.unlink()


def test_run_migrations_bootstraps_legacy_schema_before_stamping(
    scratch_dir: Path,
):
    db_path = scratch_dir / "alembic-legacy.db"
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
            # The real pre-Alembic database contained the complete application
            # topology. Keep unrelated tables current in this focused fixture
            # while the three tables above exercise legacy column patching.
            # Migration 0002 must remain responsible for its unique indexes.
            for table_name in (
                "feed_poll_state",
                "push_subscriptions",
                "source_page_controls",
                "sources",
                "ingestion_runs",
                "raw_feed_items",
            ):
                database.Base.metadata.tables[table_name].create(connection)
            for index_name in (
                "uq_raw_feed_items_source_guid_nn",
                "uq_raw_feed_items_source_link_pub_date_nn",
                "uq_raw_feed_items_source_link_guidless_undated",
            ):
                connection.execute(text(f"DROP INDEX IF EXISTS {index_name}"))

        run_migrations(database_url=database_url)

        inspector = inspect(engine)
        article_columns = {column["name"] for column in inspector.get_columns("articles")}
        ticker_columns = {column["name"] for column in inspector.get_columns("tickers")}
        article_ticker_columns = {column["name"] for column in inspector.get_columns("article_tickers")}

        assert set(inspector.get_table_names()) == {
            "alembic_version",
            "article_enrichment",
            "article_tickers",
            "articles",
            "feed_poll_state",
            "ingestion_runs",
            "push_subscriptions",
            "raw_feed_items",
            "source_page_controls",
            "sources",
            "tickers",
        }
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


def test_run_migrations_refuses_to_stamp_partial_legacy_schema(
    scratch_dir: Path,
):
    db_path = scratch_dir / "alembic-partial.db"
    database_url = f"sqlite:///{db_path.as_posix()}"

    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE articles (id INTEGER PRIMARY KEY)"))

        with pytest.raises(RuntimeError, match="Partial app schema detected"):
            run_migrations(database_url=database_url)

        assert "alembic_version" not in inspect(engine).get_table_names()
    finally:
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_run_migrations_refuses_to_stamp_legacy_schema_with_missing_column(
    scratch_dir: Path,
):
    db_path = scratch_dir / "alembic-missing-column.db"
    database_url = f"sqlite:///{db_path.as_posix()}"

    engine = create_engine(database_url)
    try:
        database.Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE push_subscriptions DROP COLUMN key_auth")
            )

        with pytest.raises(
            RuntimeError,
            match=r"missing required app columns: push_subscriptions\.key_auth",
        ):
            run_migrations(database_url=database_url)

        assert "alembic_version" not in inspect(engine).get_table_names()
    finally:
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


def test_alembic_sqlite_schema_supports_raw_feed_item_upsert(scratch_dir: Path):
    db_path = scratch_dir / "alembic-upsert.db"
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
