from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(autoflush=False, autocommit=False, bind=get_engine())


def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def init_db() -> None:
    # Import models so metadata is populated.
    from app import models  # noqa: F401

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _ensure_article_audit_columns(engine)
    _ensure_ticker_columns(engine)
    _ensure_article_ticker_columns(engine)


def _ensure_article_audit_columns(engine) -> None:
    """Best-effort schema patch for existing databases without migrations."""
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


def _ensure_ticker_columns(engine) -> None:
    """Best-effort schema patch for ticker metadata added without migrations."""
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


def _ensure_article_ticker_columns(engine) -> None:
    """Best-effort schema patch for article ticker metadata added without migrations."""
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


def _add_column_if_missing(
    connection, *, table_name: str, existing_cols: set[str], column_name: str, column_type_sql: str
) -> bool:
    if column_name in existing_cols:
        return False
    try:
        # Wrap in a savepoint so that a duplicate-column error on PostgreSQL
        # only rolls back this statement, not the entire transaction.
        # The context manager auto-rolls back on exception.
        with connection.begin_nested():
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"))
        existing_cols.add(column_name)
        return True
    except DBAPIError as exc:
        # Concurrent startups can race between schema inspection and ALTER TABLE.
        # If another worker already added the column, treat this as success and
        # still run the one-time backfill in this startup.
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


def db_health_check() -> bool:
    engine = get_engine()
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
