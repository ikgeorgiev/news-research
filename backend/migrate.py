from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from app import models  # noqa: F401
from app.config import get_settings
from app.database import Base, get_engine_kwargs
from app.pg_utils import is_postgresql_url

# The revision that matches the schema produced by _bootstrap_legacy_schema().
# Legacy databases are stamped here so that any later migrations still run via upgrade().
BASELINE_REVISION = "20260319_0001"
# Immutable topology snapshot for a valid pre-Alembic database after
# _bootstrap_legacy_schema() has applied the compatibility columns. Keep this
# tied to BASELINE_REVISION: later model/migration additions belong only in the
# current-metadata validation that runs after upgrade.
BASELINE_REQUIRED_COLUMNS: Mapping[str, frozenset[str]] = {
    "tickers": frozenset(
        {
            "id",
            "symbol",
            "fund_name",
            "sponsor",
            "validation_keywords",
            "active",
            "created_at",
            "updated_at",
        }
    ),
    "sources": frozenset({"id", "code", "name", "base_url", "enabled"}),
    "feed_poll_state": frozenset(
        {
            "id",
            "feed_url",
            "etag",
            "last_modified",
            "failure_count",
            "last_failure_at",
            "backoff_until",
            "updated_at",
        }
    ),
    "articles": frozenset(
        {
            "id",
            "canonical_url",
            "canonical_url_hash",
            "title",
            "summary",
            "published_at",
            "source_name",
            "provider_name",
            "content_hash",
            "title_normalized_hash",
            "cluster_key",
            "first_seen_at",
            "first_alert_sent_at",
            "created_at",
            "updated_at",
        }
    ),
    "article_tickers": frozenset(
        {
            "id",
            "article_id",
            "ticker_id",
            "match_type",
            "confidence",
            "extraction_version",
        }
    ),
    "raw_feed_items": frozenset(
        {
            "id",
            "source_id",
            "article_id",
            "feed_url",
            "raw_guid",
            "raw_title",
            "raw_link",
            "raw_pub_date",
            "raw_payload_json",
            "fetched_at",
        }
    ),
    "ingestion_runs": frozenset(
        {
            "id",
            "source_id",
            "feed_url",
            "started_at",
            "finished_at",
            "status",
            "items_seen",
            "items_inserted",
            "error_text",
        }
    ),
    "push_subscriptions": frozenset(
        {
            "id",
            "endpoint",
            "key_p256dh",
            "key_auth",
            "expiration_time",
            "alert_scopes_json",
            "last_notified_json",
            "manage_token_hash",
            "active",
            "failure_count",
            "last_error",
            "last_success_at",
            "created_at",
            "updated_at",
        }
    ),
}


def _build_alembic_config(database_url: str) -> Config:
    backend_dir = Path(__file__).resolve().parent
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    config.attributes["database_url"] = database_url
    return config


def _app_table_names() -> set[str]:
    return set(Base.metadata.tables)


def _current_required_columns() -> Mapping[str, frozenset[str]]:
    return {
        table_name: frozenset(str(column_name) for column_name in table.columns.keys())
        for table_name, table in Base.metadata.tables.items()
    }


def _assert_app_schema_present(
    inspector,
    *,
    context: str,
    required_columns: Mapping[str, frozenset[str]] | None = None,
) -> None:
    if required_columns is None:
        required_columns = _current_required_columns()

    existing_tables = set(inspector.get_table_names())
    missing_tables = sorted(set(required_columns) - existing_tables)
    if missing_tables:
        raise RuntimeError(
            f"{context} is missing required app tables: {', '.join(missing_tables)}."
        )

    missing_columns: list[str] = []
    for table_name, expected_columns in required_columns.items():
        existing_columns = {
            str(column["name"])
            for column in inspector.get_columns(table_name)
        }
        missing_columns.extend(
            f"{table_name}.{column_name}"
            for column_name in sorted(expected_columns - existing_columns)
        )
    if missing_columns:
        raise RuntimeError(
            f"{context} is missing required app columns: {', '.join(missing_columns)}."
        )


def _get_alembic_head_revision(database_url: str) -> str:
    config = _build_alembic_config(database_url)
    script = ScriptDirectory.from_config(config)
    head_revision = script.get_current_head()
    if head_revision is None:
        raise RuntimeError("Unable to determine Alembic head revision.")
    return head_revision


def assert_schema_at_head(database_url: str | None = None) -> None:
    if database_url is None:
        database_url = get_settings().database_url

    if not is_postgresql_url(database_url):
        return

    head_revision = _get_alembic_head_revision(database_url)

    engine = create_engine(database_url, **get_engine_kwargs(database_url))
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            existing_tables = set(inspector.get_table_names())
            if "alembic_version" not in existing_tables:
                raise RuntimeError(
                    "Database schema is not initialized; run backend/migrate.py before starting the app."
                )

            current_revision = connection.execute(
                text("SELECT version_num FROM alembic_version LIMIT 1")
            ).scalar_one_or_none()
            if current_revision != head_revision:
                raise RuntimeError(
                    "Database schema revision is not at Alembic head "
                    f"({current_revision!r} != {head_revision!r}); "
                    "run backend/migrate.py before starting the app."
                )
            _assert_app_schema_present(
                inspector,
                context="Database schema at Alembic head",
            )
    finally:
        engine.dispose()


def _bootstrap_legacy_schema(connection: Connection) -> None:
    """Patch a pre-Alembic database so it matches the baseline revision.

    Only adds missing columns/constraints to existing tables — does NOT call
    create_all(), because that would use the current ORM metadata and
    pre-create objects that later migrations are supposed to add.
    """
    _ensure_article_audit_columns(connection)
    _ensure_ticker_columns(connection)
    _ensure_article_ticker_columns(connection)
    _ensure_article_ticker_constraints(connection)


def _ensure_article_audit_columns(connection: Connection) -> None:
    with connection.begin():
        inspector = inspect(connection)
        existing_cols = {col["name"] for col in inspector.get_columns("articles")}
        dialect = connection.dialect.name

        if dialect == "postgresql":
            first_seen_type = "TIMESTAMPTZ"
            first_alert_type = "TIMESTAMPTZ"
            now_expr = "NOW()"
        else:
            first_seen_type = "DATETIME"
            first_alert_type = "DATETIME"
            now_expr = "CURRENT_TIMESTAMP"

        first_seen_added = _add_column_if_missing(
            connection,
            table_name="articles",
            existing_cols=existing_cols,
            column_name="first_seen_at",
            column_type_sql=first_seen_type,
        )
        _add_column_if_missing(
            connection,
            table_name="articles",
            existing_cols=existing_cols,
            column_name="first_alert_sent_at",
            column_type_sql=first_alert_type,
        )

        if first_seen_added:
            connection.execute(
                text(
                    "UPDATE articles "
                    f"SET first_seen_at = COALESCE(first_seen_at, created_at, published_at, {now_expr}) "
                    "WHERE first_seen_at IS NULL"
                )
            )
        connection.execute(
            text("CREATE INDEX IF NOT EXISTS ix_articles_first_seen_at ON articles (first_seen_at)")
        )
        connection.execute(
            text("CREATE INDEX IF NOT EXISTS ix_articles_first_alert_sent_at ON articles (first_alert_sent_at)")
        )


def _ensure_ticker_columns(connection: Connection) -> None:
    with connection.begin():
        inspector = inspect(connection)
        existing_cols = {col["name"] for col in inspector.get_columns("tickers")}
        _add_column_if_missing(
            connection,
            table_name="tickers",
            existing_cols=existing_cols,
            column_name="validation_keywords",
            column_type_sql="TEXT",
        )


def _ensure_article_ticker_columns(connection: Connection) -> None:
    with connection.begin():
        inspector = inspect(connection)
        existing_cols = {col["name"] for col in inspector.get_columns("article_tickers")}
        _add_column_if_missing(
            connection,
            table_name="article_tickers",
            existing_cols=existing_cols,
            column_name="extraction_version",
            column_type_sql="INTEGER DEFAULT 1 NOT NULL",
        )


def _ensure_article_ticker_constraints(connection: Connection) -> None:
    with connection.begin():
        inspector = inspect(connection)
        existing_uniques = inspector.get_unique_constraints("article_tickers")
        has_uq = any(
            set(uc["column_names"]) == {"article_id", "ticker_id"}
            for uc in existing_uniques
        )
        if not has_uq:
            connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_article_ticker "
                    "ON article_tickers (article_id, ticker_id)"
                )
            )


def _add_column_if_missing(
    connection,
    *,
    table_name: str,
    existing_cols: set[str],
    column_name: str,
    column_type_sql: str,
) -> bool:
    if column_name in existing_cols:
        return False
    try:
        with connection.begin_nested():
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"))
        existing_cols.add(column_name)
        return True
    except DBAPIError as exc:
        if _is_duplicate_column_error(exc):
            existing_cols.add(column_name)
            return True
        raise


def _is_duplicate_column_error(exc: DBAPIError) -> bool:
    orig = getattr(exc, "orig", None)
    sqlstate = (
        getattr(orig, "pgcode", None)
        or getattr(orig, "sqlstate", None)
        or getattr(exc, "code", None)
    )
    if str(sqlstate) == "42701":
        return True

    message = str(orig or exc).lower()
    return "duplicate column" in message or (
        "already exists" in message and "column" in message
    )


def run_migrations(database_url: str | None = None) -> None:
    if database_url is None:
        database_url = get_settings().database_url
    config = _build_alembic_config(database_url)

    engine = create_engine(database_url, **get_engine_kwargs(database_url))
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection

            if is_postgresql_url(database_url):
                lock_key = get_settings().migration_advisory_lock_key
                with connection.begin():
                    connection.execute(
                        text("SELECT pg_advisory_lock(:lock_key)"),
                        {"lock_key": lock_key},
                    )
                locked = True
            else:
                locked = False

            try:
                inspector = inspect(connection)
                existing_tables = set(inspector.get_table_names())
                known_app_tables = _app_table_names() | set(BASELINE_REQUIRED_COLUMNS)
                existing_app_tables = existing_tables & known_app_tables
                has_existing_app_schema = bool(existing_app_tables)
                has_version_table = "alembic_version" in existing_tables

                # Alembic and the bootstrap helpers both need a clean transaction
                # boundary here; schema inspection may have autobegun one already.
                if connection.in_transaction():
                    connection.rollback()

                if has_existing_app_schema and not has_version_table:
                    missing_legacy_tables = sorted(
                        set(BASELINE_REQUIRED_COLUMNS) - existing_app_tables
                    )
                    if missing_legacy_tables:
                        raise RuntimeError(
                            "Partial app schema detected without alembic_version; "
                            "refusing to stamp it as the complete legacy baseline. "
                            "Missing tables: "
                            f"{', '.join(missing_legacy_tables)}."
                        )
                    print("Existing app schema detected without alembic_version; bootstrapping legacy schema.")
                    _bootstrap_legacy_schema(connection)
                    _assert_app_schema_present(
                        inspect(connection),
                        context="Legacy database schema after bootstrap",
                        required_columns=BASELINE_REQUIRED_COLUMNS,
                    )
                    if connection.in_transaction():
                        connection.rollback()
                    command.stamp(config, BASELINE_REVISION)

                command.upgrade(config, "head")
                _assert_app_schema_present(
                    inspect(connection),
                    context="Database schema after migration",
                )
            finally:
                if locked:
                    # Schema inspection and failed migration statements can
                    # leave an implicit transaction open. PostgreSQL advisory
                    # locks are session-scoped, so clear that transaction
                    # before beginning the explicit unlock statement.
                    if connection.in_transaction():
                        connection.rollback()
                    with connection.begin():
                        connection.execute(
                            text("SELECT pg_advisory_unlock(:lock_key)"),
                            {"lock_key": lock_key},
                        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    run_migrations()
