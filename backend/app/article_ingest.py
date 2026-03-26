from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TypedDict
from urllib.parse import urlparse

import feedparser
import sqlalchemy as sa
from sqlalchemy import and_, desc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql.dml import Insert

logger = logging.getLogger(__name__)

from app.article_maintenance import _upsert_article_tickers
from app.constants import TITLE_DEDUP_WINDOW_HOURS
from app.feed_runtime import (
    PreparedFeedEntry,
    _fetch_feed_with_retries,
    _get_feed_conditional_headers,
    _get_or_create_feed_poll_state,
    _mark_feed_failure_backoff,
    _prefetch_recorded_raw_keys,
    _reset_feed_failure_backoff,
    _update_feed_http_cache,
)
from app.models import Article, IngestionRun, RawFeedItem, Source
from app.sse import notify_new_articles
from app.sources import PAGE_FETCH_CONFIGS
from app.constants import MIN_PERSIST_CONFIDENCE
from app.ticker_extraction import (
    _canonical_businesswire_article_url,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _is_businesswire_article_url,
    _max_ticker_confidence,
    _merge_ticker_hits,
    _should_persist_entry,
    _verified_ticker_hits,
)
from app.utils import GENERAL_SOURCE_CODE, canonicalize_url, clean_summary_text, normalize_title, parse_datetime, sha256_str, to_json_safe, to_utc

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except ImportError:  # pragma: no cover - unavailable in some stripped environments
    pg_insert = None

try:
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
except ImportError:  # pragma: no cover - unavailable in some stripped environments
    sqlite_insert = None


class IngestFeedResult(TypedDict):
    source: str
    feed_url: str
    status: str
    items_seen: int
    items_inserted: int
    notify_failed: bool
    error: str | None


def _clamp_label(value: str | None, max_len: int = 120) -> str:
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _hash_hex_to_signed_bigint(value: str) -> int:
    raw = int(value[:16], 16)
    if raw >= 2**63:
        raw -= 2**64
    return raw


def _acquire_dedupe_locks(db: Session, url_hash: str, title_hash: str) -> None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return

    for key in sorted(
        {_hash_hex_to_signed_bigint(url_hash), _hash_hex_to_signed_bigint(title_hash)}
    ):
        db.execute(select(func.pg_advisory_xact_lock(key)))


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

    for key in sorted(_hash_hex_to_signed_bigint(value) for value in lock_hashes):
        db.execute(select(func.pg_advisory_xact_lock(key)))


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
        # Prefer rows already attached to an article when historical duplicates
        # exist. The long-term schema hardening collapses these to one row per
        # natural key, but the reader still needs deterministic behavior while
        # old data may contain both attached and detached copies.
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

    return db.scalar(
        select(RawFeedItem)
        .where(and_(*pair_filters, RawFeedItem.raw_pub_date == published_at))
        .order_by(*order_by)
        .limit(1)
    )


def _extract_provider(entry: feedparser.FeedParserDict, fallback: str) -> str:
    source = entry.get("source")
    if isinstance(source, dict):
        title = source.get("title")
        if title:
            return _clamp_label(str(title))
    if isinstance(source, str) and source.strip():
        return _clamp_label(source)
    return _clamp_label(fallback)


@dataclass(slots=True)
class ArticleMatch:
    article: Article | None = None
    matched_by_url: bool = False


def _find_matching_article(
    db: Session,
    *,
    url_hash: str,
    title_hash: str,
    window_start: datetime,
    window_end: datetime,
    source_code: str,
) -> ArticleMatch:
    article = db.scalar(select(Article).where(Article.canonical_url_hash == url_hash))
    if article is not None:
        return ArticleMatch(article=article, matched_by_url=True)

    if source_code == GENERAL_SOURCE_CODE:
        article = _find_title_window_match(
            db,
            title_hash=title_hash,
            window_start=window_start,
            window_end=window_end,
            exclude_source_code=source_code,
        )
    else:
        article = _find_title_window_match(
            db,
            title_hash=title_hash,
            window_start=window_start,
            window_end=window_end,
        )
    if article is not None:
        return ArticleMatch(article=article, matched_by_url=False)

    return ArticleMatch()


def _find_title_window_match(
    db: Session,
    *,
    title_hash: str,
    window_start: datetime,
    window_end: datetime,
    exclude_source_code: str | None = None,
) -> Article | None:
    conditions = [
        Article.title_normalized_hash == title_hash,
        Article.published_at >= window_start,
        Article.published_at <= window_end,
    ]
    if exclude_source_code:
        has_excluded_source = (
            select(1)
            .select_from(RawFeedItem)
            .join(Source, Source.id == RawFeedItem.source_id)
            .where(
                and_(
                    RawFeedItem.article_id == Article.id,
                    Source.code == exclude_source_code,
                )
            )
            .correlate(Article)
            .exists()
        )
        has_any_raw = (
            select(1)
            .select_from(RawFeedItem)
            .where(RawFeedItem.article_id == Article.id)
            .correlate(Article)
            .exists()
        )
        conditions.append(has_any_raw)
        conditions.append(~has_excluded_source)
    return db.scalar(
        select(Article).where(and_(*conditions)).order_by(desc(Article.id)).limit(1)
    )


def _upsert_article(
    db: Session,
    *,
    source_code: str,
    canonical_url: str,
    title: str,
    summary: str | None,
    published_at: datetime,
    source_name: str,
    provider_name: str,
    allow_url_match_overwrite: bool = True,
) -> tuple[Article, bool, bool]:
    url_hash = sha256_str(canonical_url)
    title_hash = sha256_str(normalize_title(title))
    summary_norm = normalize_title(summary or "")
    content_hash = sha256_str(f"{title_hash}|{summary_norm[:300]}")
    cluster_key = title_hash

    source_name = _clamp_label(source_name)
    provider_name = _clamp_label(provider_name)
    _acquire_dedupe_locks(db, url_hash, title_hash)

    window_start = published_at - timedelta(hours=TITLE_DEDUP_WINDOW_HOURS)
    window_end = published_at + timedelta(hours=TITLE_DEDUP_WINDOW_HOURS)

    match = _find_matching_article(
        db,
        url_hash=url_hash,
        title_hash=title_hash,
        window_start=window_start,
        window_end=window_end,
        source_code=source_code,
    )
    article = match.article
    matched_by_url = match.matched_by_url
    created = False

    if article is None:
        new_article = Article(
            canonical_url=canonical_url,
            canonical_url_hash=url_hash,
            title=title,
            summary=summary,
            published_at=published_at,
            source_name=source_name,
            provider_name=provider_name,
            content_hash=content_hash,
            title_normalized_hash=title_hash,
            cluster_key=cluster_key,
        )
        try:
            with db.begin_nested():
                db.add(new_article)
                db.flush()
            article = new_article
            created = True
        except IntegrityError:
            match = _find_matching_article(
                db,
                url_hash=url_hash,
                title_hash=title_hash,
                window_start=window_start,
                window_end=window_end,
                source_code=source_code,
            )
            article = match.article
            matched_by_url = match.matched_by_url
            if article is None:
                raise

    if article is None:
        raise RuntimeError("article upsert failed to resolve target row")

    if not created:
        if matched_by_url and allow_url_match_overwrite:
            # Same URL, exact source — safe to overwrite all fields.
            article.title = title
            article.source_name = source_name
            article.provider_name = provider_name
            article.content_hash = content_hash
            article.cluster_key = cluster_key
            if summary:
                article.summary = summary
            article_published = article.published_at
            if article_published is None or to_utc(published_at) > to_utc(
                article_published
            ):
                article.published_at = published_at
        elif not matched_by_url:
            # Title-window match from a different feed entry.  Only enrich
            # — do NOT overwrite source_name/provider_name/title to avoid
            # a cross-source attribution swap (e.g. PRN → BW).
            if summary and (
                not article.summary or len(summary) > len(article.summary)
            ):
                article.summary = summary
            article_published = article.published_at
            if article_published is None or to_utc(published_at) > to_utc(
                article_published
            ):
                article.published_at = published_at

    return article, created, matched_by_url


@dataclass(slots=True)
class EntryResult:
    created_article: bool = False
    persisted_raw: bool = False
    has_article: bool = False
    raw_guid: str | None = None
    raw_pair: tuple[str, datetime | None] | None = None


def _prepare_feed_entries(
    entries: list,
    now_utc: datetime,
) -> list[PreparedFeedEntry]:
    prepared: list[PreparedFeedEntry] = []
    for entry in entries:
        raw_title = str(entry.get("title") or "").strip()
        title = clean_summary_text(raw_title)
        raw_link_input = str(entry.get("link") or "").strip()
        raw_link = canonicalize_url(raw_link_input)
        if not title or not raw_link:
            continue

        parsed_input = urlparse(raw_link_input)
        is_businesswire_link = _is_businesswire_article_url(raw_link)
        article_url = raw_link
        if is_businesswire_link:
            article_url = _canonical_businesswire_article_url(raw_link)
            is_exact_source_url = (
                not parsed_input.query
                and not parsed_input.fragment
                and raw_link == article_url
            )
        else:
            is_exact_source_url = raw_link == article_url

        raw_pub_date = parse_datetime(entry.get("published") or entry.get("updated"))
        published_at = raw_pub_date or now_utc
        raw_guid = (
            str(entry.get("id") or entry.get("guid") or "").strip() or None
        )
        prepared.append(
            PreparedFeedEntry(
                entry=entry,
                raw_title=raw_title,
                title=title,
                raw_link=raw_link,
                article_url=article_url,
                is_exact_source_url=is_exact_source_url,
                raw_pub_date=raw_pub_date,
                published_at=published_at,
                raw_guid=raw_guid,
            )
        )
    return prepared


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
        # Non-upsert dialects still fall back to the old refetch-and-update
        # path if a uniqueness race happens after the pre-check.
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

    # Preserve alternate feed URLs so maintenance can recover ticker context
    # when the same raw item was ingested from multiple Yahoo ticker feeds.
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
) -> None:
    row = _find_existing_raw_feed_item(
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
    if feed_url in alt:
        return
    alt.append(feed_url)
    payload["_alt_feed_urls"] = sorted(set(alt))
    row.raw_payload_json = payload
    row.fetched_at = datetime.now(timezone.utc)


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

    # When the feed_url changes (same article seen from a different Yahoo
    # ticker feed), merge the old feed_url into _alt_feed_urls so that
    # maintenance can recover ticker context from all original feeds.
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
        # The conflict target (e.g. GUID index) did not match, but a
        # *different* unique index was violated (e.g. the link+pub_date
        # index).  Roll back the savepoint and let the caller fall through
        # to the refetch-and-update path.
        return False


def _process_single_entry(
    db: Session,
    *,
    prepared: PreparedFeedEntry,
    source: Source,
    source_name: str,
    provider_name: str,
    feed_url: str,
    known_symbols: set[str],
    symbol_to_id: dict[str, int],
    timeout_seconds: int,
    page_config,
    recorded_guids: set[str],
    recorded_pairs: set[tuple[str, datetime | None]],
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> EntryResult:
    result = EntryResult()
    article: Article | None = None
    allow_exact_url_refresh = (
        prepared.raw_guid is None
        and prepared.raw_pub_date is None
        and prepared.is_exact_source_url
    )

    if prepared.raw_guid and prepared.raw_guid in recorded_guids:
        _preserve_alt_feed_url(
            db,
            source_id=source.id,
            prepared=prepared,
            feed_url=feed_url,
        )
        return result
    if (
        (prepared.raw_link, prepared.raw_pub_date) in recorded_pairs
        and not allow_exact_url_refresh
    ):
        _preserve_alt_feed_url(
            db,
            source_id=source.id,
            prepared=prepared,
            feed_url=feed_url,
        )
        return result

    with db.begin_nested():
        _acquire_raw_item_locks(
            db,
            source_id=source.id,
            raw_guid=prepared.raw_guid,
            raw_link=prepared.raw_link,
            published_at=prepared.raw_pub_date,
        )
        existing_raw = _find_existing_raw_feed_item(
            db,
            source_id=source.id,
            raw_guid=prepared.raw_guid,
            raw_link=prepared.raw_link,
            published_at=prepared.raw_pub_date,
            require_attached=True,
        )
        if existing_raw is not None and not allow_exact_url_refresh:
            # Record this feed_url as an alternate so maintenance can
            # recover ticker context from all Yahoo ticker feeds.
            if (
                existing_raw.feed_url
                and existing_raw.feed_url != feed_url
            ):
                payload = dict(existing_raw.raw_payload_json or {})
                alt: list[str] = list(payload.get("_alt_feed_urls") or [])
                if feed_url not in alt:
                    alt.append(feed_url)
                    payload["_alt_feed_urls"] = sorted(set(alt))
                    existing_raw.raw_payload_json = payload
                existing_raw.fetched_at = datetime.now(timezone.utc)
            return result

        raw_summary = (
            str(
                prepared.entry.get("summary")
                or prepared.entry.get("description")
                or ""
            ).strip()
            or None
        )
        summary = clean_summary_text(raw_summary)

        entry_source_name = _extract_provider(
            prepared.entry, source_name
        )
        ticker_hits = _extract_entry_tickers(
            prepared.title,
            summary or "",
            prepared.raw_link,
            feed_url,
            known_symbols,
            symbol_keywords=symbol_keywords,
        )
        max_hit_conf = _max_ticker_confidence(ticker_hits)
        if (
            max_hit_conf < MIN_PERSIST_CONFIDENCE
            and page_config is not None
        ):
            fallback_hits = _extract_source_fallback_tickers(
                prepared.title,
                summary or "",
                prepared.raw_link,
                feed_url,
                known_symbols,
                timeout_seconds,
                page_config,
                symbol_keywords=symbol_keywords,
            )
            if fallback_hits:
                _merge_ticker_hits(ticker_hits, fallback_hits)

        should_persist_tickers = _should_persist_entry(
            source.code, ticker_hits
        )
        allow_existing_exact_url = False
        if (
            not should_persist_tickers
            and source.code != GENERAL_SOURCE_CODE
            and prepared.is_exact_source_url
        ):
            existing_article_id = db.scalar(
                select(Article.id)
                .where(
                    Article.canonical_url_hash
                    == sha256_str(prepared.article_url)
                )
                .limit(1)
            )
            allow_existing_exact_url = (
                existing_article_id is not None
            )
        effective_ticker_hits = _verified_ticker_hits(
            source.code,
            ticker_hits,
        )
        if should_persist_tickers or allow_existing_exact_url:
            article, created, matched_by_url = _upsert_article(
                db,
                source_code=source.code,
                canonical_url=prepared.article_url,
                title=prepared.title,
                summary=summary,
                published_at=prepared.published_at,
                source_name=source_name,
                provider_name=provider_name,
                allow_url_match_overwrite=prepared.is_exact_source_url,
            )
            result.created_article = created

            existing_rows = {} if created else None
            _upsert_article_tickers(
                db,
                article.id,
                effective_ticker_hits,
                symbol_to_id,
                existing_rows=existing_rows,
                prune_missing=(
                    source.code != GENERAL_SOURCE_CODE
                    and matched_by_url
                    and prepared.is_exact_source_url
                    and bool(effective_ticker_hits)
                ),
            )

        _persist_raw_feed_item(
            db,
            source_id=source.id,
            article=article,
            prepared=prepared,
            feed_url=feed_url,
            raw_summary=raw_summary,
            entry_source_name=entry_source_name,
            existing_row=existing_raw if allow_exact_url_refresh else None,
        )
        result.persisted_raw = True
        result.has_article = article is not None
        result.raw_guid = prepared.raw_guid
        result.raw_pair = (prepared.raw_link, prepared.raw_pub_date)

    return result


def ingest_feed(
    db: Session,
    *,
    source: Source,
    feed_url: str,
    known_symbols: set[str],
    symbol_to_id: dict[str, int],
    timeout_seconds: int,
    fetch_max_attempts: int = 3,
    fetch_backoff_seconds: float = 1.0,
    fetch_backoff_jitter_seconds: float = 0.3,
    enable_conditional_get: bool = True,
    failure_backoff_base_seconds: float = 30.0,
    failure_backoff_max_seconds: float = 600.0,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> IngestFeedResult:
    run = IngestionRun(
        source_id=source.id,
        feed_url=feed_url,
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    items_seen = 0
    items_inserted = 0
    committed_items_inserted = 0
    notify_failed = False
    status = "success"
    error_text: str | None = None
    entry_errors = 0
    feed_state = _get_or_create_feed_poll_state(db, feed_url)

    try:
        now_utc = datetime.now(timezone.utc)
        backoff_until = feed_state.backoff_until
        if backoff_until is not None and to_utc(backoff_until) > now_utc:
            status = "skipped_backoff"
            error_text = (
                f"Feed is in backoff until {to_utc(backoff_until).isoformat()}"
            )
            committed_items_inserted = 0
        else:
            conditional_headers = (
                _get_feed_conditional_headers(feed_state)
                if enable_conditional_get
                else {}
            )
            response = _fetch_feed_with_retries(
                feed_url=feed_url,
                timeout_seconds=timeout_seconds,
                max_attempts=fetch_max_attempts,
                backoff_seconds=fetch_backoff_seconds,
                backoff_jitter_seconds=fetch_backoff_jitter_seconds,
                extra_headers=conditional_headers,
            )
            _update_feed_http_cache(feed_state, response)
            _reset_feed_failure_backoff(feed_state)

            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code == 304:
                committed_items_inserted = 0
            else:
                parsed = feedparser.parse(response.content)

                source_name = source.name
                feed_title = (
                    parsed.feed.get("title") if isinstance(parsed.feed, dict) else None
                )
                if source.code != "yahoo" and feed_title:
                    source_name = _clamp_label(str(feed_title))

                entries = list(getattr(parsed, "entries", []) or [])
                items_seen = len(entries)

                now_utc = datetime.now(timezone.utc)
                prepared_entries = _prepare_feed_entries(entries, now_utc)

                recorded_guids, recorded_pairs = _prefetch_recorded_raw_keys(
                    db,
                    source_id=source.id,
                    prepared_entries=prepared_entries,
                )

                provider_name = _clamp_label(source.name)
                page_config = PAGE_FETCH_CONFIGS.get(source.code)

                for prepared in prepared_entries:
                    try:
                        entry_result = _process_single_entry(
                            db,
                            prepared=prepared,
                            source=source,
                            source_name=source_name,
                            provider_name=provider_name,
                            feed_url=feed_url,
                            known_symbols=known_symbols,
                            symbol_to_id=symbol_to_id,
                            timeout_seconds=timeout_seconds,
                            page_config=page_config,
                            recorded_guids=recorded_guids,
                            recorded_pairs=recorded_pairs,
                            symbol_keywords=symbol_keywords,
                        )

                        if entry_result.persisted_raw:
                            if entry_result.created_article:
                                items_inserted += 1
                            if entry_result.has_article and entry_result.raw_guid:
                                recorded_guids.add(entry_result.raw_guid)
                            if entry_result.has_article and entry_result.raw_pair:
                                recorded_pairs.add(entry_result.raw_pair)

                    except Exception as entry_exc:  # fault-isolation: broad catch intentional
                        entry_errors += 1
                        error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}: {entry_exc}"
                        continue

                db.commit()
                committed_items_inserted = items_inserted
                if committed_items_inserted > 0:
                    try:
                        notify_new_articles(db, committed_items_inserted)
                    except Exception:
                        notify_failed = True
                        logger.warning("pg_notify for new articles failed", exc_info=True)
                if entry_errors > 0 and error_text is None:
                    error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}."
    except Exception as exc:
        db.rollback()
        status = "failed"
        error_text = str(exc)
        committed_items_inserted = 0
        now_utc = datetime.now(timezone.utc)
        feed_state = _get_or_create_feed_poll_state(db, feed_url)
        _mark_feed_failure_backoff(
            feed_state,
            now_utc=now_utc,
            base_seconds=failure_backoff_base_seconds,
            max_seconds=failure_backoff_max_seconds,
        )

    run.status = status
    run.items_seen = items_seen
    run.items_inserted = committed_items_inserted
    run.error_text = error_text
    run.finished_at = datetime.now(timezone.utc)
    db.commit()

    return {
        "source": source.code,
        "feed_url": feed_url,
        "status": status,
        "items_seen": items_seen,
        "items_inserted": committed_items_inserted,
        "notify_failed": notify_failed,
        "error": error_text,
    }
