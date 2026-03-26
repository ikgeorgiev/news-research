"""raw feed item unique indexes

Revision ID: 20260325_0002
Revises: 20260319_0001
Create Date: 2026-03-25 00:00:00.000000

"""
from __future__ import annotations

import json
from typing import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260325_0002"
down_revision: str | None = "20260319_0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

GUID_INDEX_NAME = "uq_raw_feed_items_source_guid_nn"
PAIR_INDEX_NAME = "uq_raw_feed_items_source_link_pub_date_nn"
GUIDLESS_INDEX_NAME = "uq_raw_feed_items_source_link_guidless_undated"
RAW_FEED_ITEMS = sa.table(
    "raw_feed_items",
    sa.column("id", sa.Integer),
    sa.column("article_id", sa.Integer),
    sa.column("raw_payload_json", sa.JSON),
)


def _dedupe_rows(
    connection,
    *,
    group_query: str,
    row_query: str,
    param_names: Sequence[str],
) -> None:
    groups = connection.execute(sa.text(group_query)).mappings().all()
    for group in groups:
        params = {name: group[name] for name in param_names}
        rows = connection.execute(sa.text(row_query), params).mappings().all()
        if len(rows) < 2:
            continue

        latest = rows[0]
        latest_id = int(latest["id"])
        attached_article_id = next(
            (row["article_id"] for row in rows if row["article_id"] is not None),
            None,
        )

        update_values: dict[str, object] = {}
        if attached_article_id is not None:
            update_values["article_id"] = attached_article_id

        # Collect distinct feed_urls from all rows being merged so that
        # maintenance operations can still recover ticker context from
        # feeds whose raw rows are being collapsed.
        all_feed_urls = {
            row["feed_url"] for row in rows if row.get("feed_url")
        }
        survivor_feed_url = latest.get("feed_url")
        alt_feed_urls = sorted(all_feed_urls - {survivor_feed_url})
        if alt_feed_urls:
            raw_payload = latest.get("raw_payload_json")
            if isinstance(raw_payload, str):
                payload = json.loads(raw_payload) if raw_payload else {}
            else:
                payload = dict(raw_payload) if raw_payload else {}
            existing_alt: list[str] = list(payload.get("_alt_feed_urls") or [])
            payload["_alt_feed_urls"] = sorted(set(existing_alt + alt_feed_urls))
            update_values["raw_payload_json"] = payload

        if update_values:
            connection.execute(
                sa.update(RAW_FEED_ITEMS)
                .where(RAW_FEED_ITEMS.c.id == latest_id)
                .values(**update_values)
            )

        loser_ids = [int(row["id"]) for row in rows[1:]]
        if loser_ids:
            connection.execute(
                sa.delete(RAW_FEED_ITEMS).where(RAW_FEED_ITEMS.c.id.in_(loser_ids))
            )


def _cleanup_duplicate_raw_feed_items(connection) -> None:
    _dedupe_rows(
        connection,
        group_query="""
            SELECT source_id, raw_guid
            FROM raw_feed_items
            WHERE raw_guid IS NOT NULL
            GROUP BY source_id, raw_guid
            HAVING COUNT(*) > 1
        """,
        row_query="""
            SELECT id, article_id, fetched_at, feed_url, raw_payload_json
            FROM raw_feed_items
            WHERE source_id = :source_id
              AND raw_guid = :raw_guid
            ORDER BY fetched_at IS NULL, fetched_at DESC, id DESC
        """,
        param_names=("source_id", "raw_guid"),
    )
    _dedupe_rows(
        connection,
        group_query="""
            SELECT source_id, raw_link, raw_pub_date
            FROM raw_feed_items
            WHERE raw_link IS NOT NULL
              AND raw_pub_date IS NOT NULL
            GROUP BY source_id, raw_link, raw_pub_date
            HAVING COUNT(*) > 1
        """,
        row_query="""
            SELECT id, article_id, fetched_at, feed_url, raw_payload_json
            FROM raw_feed_items
            WHERE source_id = :source_id
              AND raw_link = :raw_link
              AND raw_pub_date = :raw_pub_date
            ORDER BY fetched_at IS NULL, fetched_at DESC, id DESC
        """,
        param_names=("source_id", "raw_link", "raw_pub_date"),
    )
    _dedupe_rows(
        connection,
        group_query="""
            SELECT source_id, raw_link
            FROM raw_feed_items
            WHERE raw_link IS NOT NULL
              AND raw_guid IS NULL
              AND raw_pub_date IS NULL
            GROUP BY source_id, raw_link
            HAVING COUNT(*) > 1
        """,
        row_query="""
            SELECT id, article_id, fetched_at, feed_url, raw_payload_json
            FROM raw_feed_items
            WHERE source_id = :source_id
              AND raw_link = :raw_link
              AND raw_guid IS NULL
              AND raw_pub_date IS NULL
            ORDER BY fetched_at IS NULL, fetched_at DESC, id DESC
        """,
        param_names=("source_id", "raw_link"),
    )


def _index_names(connection) -> set[str]:
    inspector = sa.inspect(connection)
    return {
        str(index["name"])
        for index in inspector.get_indexes("raw_feed_items")
        if index.get("name")
    }


def _has_raw_feed_items_table(connection) -> bool:
    inspector = sa.inspect(connection)
    return "raw_feed_items" in set(inspector.get_table_names())


def upgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name not in {"postgresql", "sqlite"}:
        return
    if not _has_raw_feed_items_table(connection):
        return

    _cleanup_duplicate_raw_feed_items(connection)

    existing_indexes = _index_names(connection)
    if GUID_INDEX_NAME not in existing_indexes:
        create_kwargs: dict[str, object] = {"unique": True}
        if connection.dialect.name == "postgresql":
            create_kwargs["postgresql_where"] = sa.text("raw_guid IS NOT NULL")
        elif connection.dialect.name == "sqlite":
            create_kwargs["sqlite_where"] = sa.text("raw_guid IS NOT NULL")
        op.create_index(
            GUID_INDEX_NAME,
            "raw_feed_items",
            ["source_id", "raw_guid"],
            **create_kwargs,
        )
    if PAIR_INDEX_NAME not in existing_indexes:
        create_kwargs = {"unique": True}
        where_clause = sa.text("raw_link IS NOT NULL AND raw_pub_date IS NOT NULL")
        if connection.dialect.name == "postgresql":
            create_kwargs["postgresql_where"] = where_clause
        elif connection.dialect.name == "sqlite":
            create_kwargs["sqlite_where"] = where_clause
        op.create_index(
            PAIR_INDEX_NAME,
            "raw_feed_items",
            ["source_id", "raw_link", "raw_pub_date"],
            **create_kwargs,
        )
    if GUIDLESS_INDEX_NAME not in existing_indexes:
        create_kwargs = {"unique": True}
        where_clause = sa.text(
            "raw_link IS NOT NULL AND raw_guid IS NULL AND raw_pub_date IS NULL"
        )
        if connection.dialect.name == "postgresql":
            create_kwargs["postgresql_where"] = where_clause
        elif connection.dialect.name == "sqlite":
            create_kwargs["sqlite_where"] = where_clause
        op.create_index(
            GUIDLESS_INDEX_NAME,
            "raw_feed_items",
            ["source_id", "raw_link"],
            **create_kwargs,
        )


def downgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name not in {"postgresql", "sqlite"}:
        return
    if not _has_raw_feed_items_table(connection):
        return

    existing_indexes = _index_names(connection)
    if GUIDLESS_INDEX_NAME in existing_indexes:
        op.drop_index(GUIDLESS_INDEX_NAME, table_name="raw_feed_items")
    if PAIR_INDEX_NAME in existing_indexes:
        op.drop_index(PAIR_INDEX_NAME, table_name="raw_feed_items")
    if GUID_INDEX_NAME in existing_indexes:
        op.drop_index(GUID_INDEX_NAME, table_name="raw_feed_items")
