from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from app import models  # noqa: F401
from app.config import get_settings
from app.database import Base

# The revision that matches the schema produced by _bootstrap_legacy_schema().
# Legacy databases are stamped here so that any later migrations still run via upgrade().
BASELINE_REVISION = "20260319_0001"


def _build_alembic_config(database_url: str) -> Config:
    backend_dir = Path(__file__).resolve().parent
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    config.attributes["database_url"] = database_url
    return config


def _app_table_names() -> set[str]:
    return set(Base.metadata.tables)


def _bootstrap_legacy_schema(engine: Engine) -> None:
    """Patch a pre-Alembic database so it matches the baseline revision.

    Only adds missing columns/constraints to existing tables — does NOT call
    create_all(), because that would use the current ORM metadata and
    pre-create objects that later migrations are supposed to add.
    """
    _ensure_article_audit_columns(engine)
    _ensure_ticker_columns(engine)
    _ensure_article_ticker_columns(engine)
    _ensure_article_ticker_constraints(engine)


def _ensure_article_audit_columns(engine: Engine) -> None:
    with engine.begin() as connection:
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


def _ensure_ticker_columns(engine: Engine) -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        existing_cols = {col["name"] for col in inspector.get_columns("tickers")}
        _add_column_if_missing(
            connection,
            table_name="tickers",
            existing_cols=existing_cols,
            column_name="validation_keywords",
            column_type_sql="TEXT",
        )


def _ensure_article_ticker_columns(engine: Engine) -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        existing_cols = {col["name"] for col in inspector.get_columns("article_tickers")}
        _add_column_if_missing(
            connection,
            table_name="article_tickers",
            existing_cols=existing_cols,
            column_name="extraction_version",
            column_type_sql="INTEGER DEFAULT 1 NOT NULL",
        )


def _ensure_article_ticker_constraints(engine: Engine) -> None:
    with engine.begin() as connection:
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

    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names())
        has_existing_app_schema = bool(existing_tables & _app_table_names())
        has_version_table = "alembic_version" in existing_tables

        if has_existing_app_schema and not has_version_table:
            print("Existing app schema detected without alembic_version; bootstrapping legacy schema.")
            _bootstrap_legacy_schema(engine)
            command.stamp(config, BASELINE_REVISION)
    finally:
        engine.dispose()

    command.upgrade(config, "head")


if __name__ == "__main__":
    run_migrations()
