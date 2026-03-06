from __future__ import annotations

from sqlalchemy import create_engine, event, text

from app import models  # noqa: F401
from app.database import Base, _ensure_article_audit_columns


def test_ensure_article_audit_columns_backfills_legacy_rows_when_column_added():
    engine = create_engine("sqlite:///:memory:")

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
                    published_at DATETIME,
                    source_name VARCHAR(120) NOT NULL,
                    provider_name VARCHAR(120) NOT NULL,
                    content_hash VARCHAR(64) NOT NULL,
                    title_normalized_hash VARCHAR(64) NOT NULL,
                    cluster_key VARCHAR(64) NOT NULL,
                    created_at DATETIME
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
                    created_at
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
                    '2026-03-01 11:00:00'
                )
                """
            )
        )

    _ensure_article_audit_columns(engine)

    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT first_seen_at, first_alert_sent_at "
                "FROM articles WHERE id = 1"
            )
        ).mappings().one()

    assert row["first_seen_at"] is not None
    assert row["first_alert_sent_at"] is None


def test_ensure_article_audit_columns_skips_backfill_when_column_already_exists():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    executed_statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture_sql(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        executed_statements.append(statement)

    _ensure_article_audit_columns(engine)

    assert not any(
        "UPDATE articles" in statement and "SET first_seen_at" in statement
        for statement in executed_statements
    )
