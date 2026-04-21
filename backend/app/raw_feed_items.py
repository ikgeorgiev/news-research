from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import and_, func, select, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql.dml import Insert

from app.models import Article, RawFeedItem
from app.pg_utils import hash_hex_to_signed_bigint
from app.utils import sha256_str, to_json_safe

if TYPE_CHECKING:
    from app.article_ingest import PreparedFeedEntry

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except ImportError:  # pragma: no cover - unavailable in some stripped environments
    pg_insert = None

try:
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
except ImportError:  # pragma: no cover - unavailable in some stripped environments
    sqlite_insert = None


# ---------------------------------------------------------------------------
# Advisory locks for raw feed item deduplication (PostgreSQL only)
# ---------------------------------------------------------------------------


def _acquire_raw_item_locks(
    db: Session,
    *,
    source_id: int,
    raw_guid: str | None,
    raw_link: str,
    published_at: datetime | None,
) -> None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return

    lock_hashes = {
        sha256_str(
            f"raw-pair:{source_id}:{raw_link}:{published_at.isoformat() if published_at is not None else '<none>'}"
        )
    }
    if raw_guid:
        lock_hashes.add(sha256_str(f"raw-guid:{source_id}:{raw_guid}"))

    for key in sorted(hash_hex_to_signed_bigint(value) for value in lock_hashes):
        db.execute(select(func.pg_advisory_xact_lock(key)))


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


def _find_existing_raw_feed_item(
    db: Session,
    *,
    source_id: int,
    raw_guid: str | None,
    raw_link: str,
    published_at: datetime | None,
    require_attached: bool | None,
) -> RawFeedItem | None:
    base_filters = [RawFeedItem.source_id == source_id]
    if require_attached is True:
        base_filters.append(RawFeedItem.article_id.is_not(None))
    elif require_attached is False:
        base_filters.append(RawFeedItem.article_id.is_(None))

    order_by = [RawFeedItem.id.desc()]
    if require_attached is None:
        order_by.insert(0, RawFeedItem.article_id.is_not(None).desc())

    if raw_guid:
        row = db.scalar(
            select(RawFeedItem)
            .where(and_(*base_filters, RawFeedItem.raw_guid == raw_guid))
            .order_by(*order_by)
            .limit(1)
        )
        if row is not None:
            return row

    pair_filters = [
        *base_filters,
        RawFeedItem.raw_link == raw_link,
    ]
    if published_at is None:
        row = db.scalar(
            select(RawFeedItem)
            .where(and_(*pair_filters, RawFeedItem.raw_pub_date.is_(None)))
            .order_by(*order_by)
            .limit(1)
        )
        if row is not None:
            return row

        if raw_guid is None:
            return db.scalar(
                select(RawFeedItem)
                .where(
                    and_(
                        *pair_filters,
                        RawFeedItem.raw_guid.is_(None),
                    )
                )
                .order_by(*order_by)
                .limit(1)
            )

    exact_dated_row = db.scalar(
        select(RawFeedItem)
        .where(and_(*pair_filters, RawFeedItem.raw_pub_date == published_at))
        .order_by(*order_by)
        .limit(1)
    )
    if exact_dated_row is not None:
        return exact_dated_row

    if require_attached is not True:
        legacy_guidless_row = db.scalar(
            select(RawFeedItem)
            .where(
                and_(
                    *base_filters,
                    RawFeedItem.raw_link == raw_link,
                    RawFeedItem.raw_guid.is_(None),
                    RawFeedItem.raw_pub_date.is_(None),
                )
            )
            .order_by(*order_by)
            .limit(1)
        )
        if legacy_guidless_row is not None:
            return legacy_guidless_row

    return None


def _prefetch_recorded_raw_keys(
    db: Session,
    *,
    source_id: int,
    prepared_entries: list[PreparedFeedEntry],
) -> tuple[set[str], set[tuple[str, datetime | None]]]:
    if not prepared_entries:
        return set(), set()

    guids = {item.raw_guid for item in prepared_entries if item.raw_guid}
    pairs_with_pub = {
        (item.raw_link, item.raw_pub_date)
        for item in prepared_entries
        if item.raw_pub_date is not None
    }
    links_without_pub = {
        item.raw_link
        for item in prepared_entries
        if item.raw_pub_date is None and item.raw_guid is None
    }

    existing_guids: set[str] = set()
    if guids:
        existing_guids = {
            guid
            for guid in db.scalars(
                select(RawFeedItem.raw_guid).where(
                    and_(
                        RawFeedItem.source_id == source_id,
                        RawFeedItem.article_id.is_not(None),
                        RawFeedItem.raw_guid.in_(list(guids)),
                    )
                )
            ).all()
            if guid
        }

    existing_pairs: set[tuple[str, datetime | None]] = set()
    if pairs_with_pub:
        rows = db.execute(
            select(RawFeedItem.raw_link, RawFeedItem.raw_pub_date).where(
                and_(
                    RawFeedItem.source_id == source_id,
                    RawFeedItem.article_id.is_not(None),
                    tuple_(RawFeedItem.raw_link, RawFeedItem.raw_pub_date).in_(
                        list(pairs_with_pub)
                    ),
                )
            )
        ).all()
        existing_pairs = {
            (str(link), pub_date) for link, pub_date in rows if link and pub_date
        }
    if links_without_pub:
        rows = db.execute(
            select(RawFeedItem.raw_link).where(
                and_(
                    RawFeedItem.source_id == source_id,
                    RawFeedItem.article_id.is_not(None),
                    RawFeedItem.raw_guid.is_(None),
                    RawFeedItem.raw_link.in_(list(links_without_pub)),
                )
            )
        ).all()
        existing_pairs.update((str(link), None) for (link,) in rows if link)

    return existing_guids, existing_pairs


# ---------------------------------------------------------------------------
# Insert / upsert
# ---------------------------------------------------------------------------


def _build_raw_feed_item_insert(
    db: Session,
    values: dict[str, object],
) -> Insert | None:
    bind = db.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
    table = RawFeedItem.__table__
    if dialect_name == "postgresql" and pg_insert is not None:
        return pg_insert(table).values(**values)
    if dialect_name == "sqlite" and sqlite_insert is not None:
        return sqlite_insert(table).values(**values)
    return None


def _raw_feed_item_conflict_target(
    values: dict[str, object],
) -> tuple[list[sa.ColumnElement[object]], sa.ColumnElement[bool]]:
    table = RawFeedItem.__table__
    if values.get("raw_guid"):
        return (
            [table.c.source_id, table.c.raw_guid],
            table.c.raw_guid.is_not(None),
        )
    if values.get("raw_pub_date") is not None:
        return (
            [table.c.source_id, table.c.raw_link, table.c.raw_pub_date],
            sa.and_(
                table.c.raw_link.is_not(None),
                table.c.raw_pub_date.is_not(None),
            ),
        )
    return (
        [table.c.source_id, table.c.raw_link],
        sa.and_(
            table.c.raw_link.is_not(None),
            table.c.raw_guid.is_(None),
            table.c.raw_pub_date.is_(None),
        ),
    )


def _upsert_raw_feed_item(
    db: Session,
    values: dict[str, object],
) -> bool:
    statement = _build_raw_feed_item_insert(db, values)
    if statement is None:
        candidate = RawFeedItem(**values)
        try:
            with db.begin_nested():
                db.add(candidate)
                db.flush()
            return True
        except IntegrityError:
            return False

    bind = db.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", None)

    table = RawFeedItem.__table__
    index_elements, index_where = _raw_feed_item_conflict_target(values)
    excluded = statement.excluded

    if dialect_name == "postgresql":
        payload_expr: sa.ColumnElement[object] = sa.case(
            (
                table.c.feed_url.is_distinct_from(excluded.feed_url),
                sa.literal_column(
                    "(EXCLUDED.raw_payload_json::jsonb"
                    " || jsonb_build_object("
                    "     '_alt_feed_urls',"
                    "     COALESCE(raw_feed_items.raw_payload_json::jsonb->'_alt_feed_urls', '[]'::jsonb)"
                    "     || COALESCE(EXCLUDED.raw_payload_json::jsonb->'_alt_feed_urls', '[]'::jsonb)"
                    "     || jsonb_build_array(raw_feed_items.feed_url)"
                    " ))::json"
                ),
            ),
            else_=excluded.raw_payload_json,
        )
    elif dialect_name == "sqlite":
        payload_expr = sa.case(
            (
                table.c.feed_url.is_distinct_from(excluded.feed_url),
                sa.literal_column(
                    "json_patch("
                    "  COALESCE(excluded.raw_payload_json, '{}'),"
                    "  json_object("
                    "    '_alt_feed_urls',"
                    "    json(COALESCE(("
                    "      SELECT json_group_array(value)"
                    "      FROM ("
                    "        SELECT DISTINCT value"
                    "        FROM ("
                    "          SELECT raw_feed_items.feed_url AS value"
                    "          UNION ALL"
                    "          SELECT value"
                    "          FROM json_each(COALESCE(raw_feed_items.raw_payload_json, '{}'), '$._alt_feed_urls')"
                    "          UNION ALL"
                    "          SELECT value"
                    "          FROM json_each(COALESCE(excluded.raw_payload_json, '{}'), '$._alt_feed_urls')"
                    "        )"
                    "        WHERE value IS NOT NULL"
                    "      )"
                    "    ), '[]'))"
                    "  )"
                    ")"
                ),
            ),
            else_=excluded.raw_payload_json,
        )
    else:
        payload_expr = excluded.raw_payload_json

    set_values = {
        "article_id": sa.func.coalesce(excluded.article_id, table.c.article_id),
        "feed_url": excluded.feed_url,
        "raw_guid": sa.func.coalesce(excluded.raw_guid, table.c.raw_guid),
        "raw_title": excluded.raw_title,
        "raw_link": excluded.raw_link,
        "raw_pub_date": sa.func.coalesce(excluded.raw_pub_date, table.c.raw_pub_date),
        "raw_payload_json": payload_expr,
        "fetched_at": sa.func.now(),
    }
    try:
        with db.begin_nested():
            db.execute(
                statement.on_conflict_do_update(
                    index_elements=index_elements,
                    index_where=index_where,
                    set_=set_values,
                )
            )
        return True
    except IntegrityError:
        return False


# ---------------------------------------------------------------------------
# Persist / update helpers
# ---------------------------------------------------------------------------


def _persist_raw_feed_item(
    db: Session,
    *,
    source_id: int,
    article: Article | None,
    prepared: PreparedFeedEntry,
    feed_url: str,
    raw_summary: str | None,
    entry_source_name: str,
    existing_row: RawFeedItem | None = None,
) -> None:
    payload = {
        "title": prepared.raw_title,
        "link": prepared.raw_link,
        "published": prepared.entry.get("published")
        or prepared.entry.get("updated"),
        "summary": raw_summary,
        "source": entry_source_name,
    }
    row = existing_row or _find_existing_raw_feed_item(
        db,
        source_id=source_id,
        raw_guid=prepared.raw_guid,
        raw_link=prepared.raw_link,
        published_at=prepared.raw_pub_date,
        require_attached=None,
    )
    if row is None:
        candidate_values = {
            "source_id": source_id,
            "article_id": article.id if article is not None else None,
            "feed_url": feed_url,
            "raw_guid": prepared.raw_guid,
            "raw_title": prepared.raw_title,
            "raw_link": prepared.raw_link,
            "raw_pub_date": prepared.raw_pub_date,
            "raw_payload_json": to_json_safe(payload),
        }
        if _upsert_raw_feed_item(db, candidate_values):
            return
        row = _find_existing_raw_feed_item(
            db,
            source_id=source_id,
            raw_guid=prepared.raw_guid,
            raw_link=prepared.raw_link,
            published_at=prepared.raw_pub_date,
            require_attached=None,
        )
        if row is None:
            raise RuntimeError("raw feed item upsert failed to resolve target row")

    existing_alt: list[str] = list(
        (row.raw_payload_json or {}).get("_alt_feed_urls") or []
    )
    if row.feed_url and row.feed_url != feed_url:
        if row.feed_url not in existing_alt:
            existing_alt.append(row.feed_url)
    if existing_alt:
        payload["_alt_feed_urls"] = sorted(set(existing_alt))

    if article is not None or row.article_id is None:
        row.article_id = article.id if article is not None else None
    row.feed_url = feed_url
    row.raw_guid = prepared.raw_guid
    row.raw_title = prepared.raw_title
    row.raw_link = prepared.raw_link
    row.raw_pub_date = prepared.raw_pub_date
    row.raw_payload_json = to_json_safe(payload)
    row.fetched_at = datetime.now(timezone.utc)


def _preserve_alt_feed_url(
    db: Session,
    *,
    source_id: int,
    prepared: PreparedFeedEntry,
    feed_url: str,
    existing_row: RawFeedItem | None = None,
) -> None:
    row = existing_row or _find_existing_raw_feed_item(
        db,
        source_id=source_id,
        raw_guid=prepared.raw_guid,
        raw_link=prepared.raw_link,
        published_at=prepared.raw_pub_date,
        require_attached=None,
    )
    if row is None or not row.feed_url or row.feed_url == feed_url:
        return

    payload = dict(row.raw_payload_json or {})
    alt: list[str] = list(payload.get("_alt_feed_urls") or [])
    if feed_url not in alt:
        alt.append(feed_url)
        payload["_alt_feed_urls"] = sorted(set(alt))
        row.raw_payload_json = payload
    row.fetched_at = datetime.now(timezone.utc)
