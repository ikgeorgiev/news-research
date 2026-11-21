from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import time
from typing import TypedDict
from urllib.parse import urlparse

import feedparser
import httpx
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from app.article_maintenance import _upsert_article_tickers
from app.article_enrichment import enqueue_article_enrichment
from app.constants import MIN_PERSIST_CONFIDENCE, TITLE_DEDUP_WINDOW_HOURS
from app.enrichment_retry import RETRY_KEY, due_enrichment_entries, pending_retry, retry_is_due
from app.page_access import reset_page_outcome, get_page_outcome
from app.page_body_capture import capture_page_body, captured_page_body
from app.feed_runtime import (
    _fetch_feed_with_retries,
    _fetch_issuer_feed,
    _get_feed_conditional_headers,
    _get_or_create_feed_poll_state,
    _mark_feed_failure_backoff,
    _parse_retry_after_seconds,
    _reset_feed_failure_backoff,
    _update_feed_http_cache,
)
from app.models import Article, ArticleEnrichment, IngestionRun, RawFeedItem, Source
from app.monitoring import observe_article_commit
from app.pg_utils import acquire_article_dedupe_locks as _acquire_dedupe_locks
from app.release_matching import release_identity, select_release_candidate
from app.raw_feed_items import (
    _acquire_raw_item_locks,
    _find_existing_raw_feed_item,
    _prefetch_recorded_raw_keys,
    _persist_raw_feed_item,
    _preserve_alt_feed_url,
)
from app.sse import notify_new_articles
from app.sources import (
    PAGE_FETCH_CONFIGS,
    POLICY_GENERAL_ALLOWED,
    get_effective_source_policy,
    _is_source_article_url,
)
from app.research_timing import record_feed_receipt
from app.ticker_context import SymbolKeywordProfile
from app.ticker_extraction import (
    _canonical_businesswire_article_url,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _is_businesswire_article_url,
    _max_ticker_confidence,
    _merge_ticker_hits,
    _should_persist_entry,
    _source_page_fetch_requires_network,
    _verified_ticker_hits,
)
from app.utils import GENERAL_SOURCE_CODE, article_content_hash, canonicalize_url, clean_summary_text, normalize_title, parse_datetime, sha256_str, to_utc


def _extract_retry_after_seconds(exc: BaseException) -> float | None:
    # Preserve server-provided Retry-After (RFC 7231 §7.1.3) from 429 responses so
    # per-feed backoff respects upstream rate-limit hints. Without this, max_attempts=1
    # would drop the header parsed inside _fetch_feed_with_retries on the final attempt.
    response = getattr(exc, "response", None)
    if not isinstance(exc, httpx.HTTPStatusError) or response is None:
        return None
    if int(getattr(response, "status_code", 0) or 0) != 429:
        return None
    headers = getattr(response, "headers", None)
    header_value = headers.get("Retry-After") if headers is not None else None
    return _parse_retry_after_seconds(header_value)


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
    canonical_url: str | None = None,
    raw_guid: str | None = None,
    summary: str | None = None,
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
            identity=release_identity(canonical_url, raw_guid, source_code),
            summary=summary,
        )
    else:
        article = _find_title_window_match(
            db,
            title_hash=title_hash,
            window_start=window_start,
            window_end=window_end,
            identity=release_identity(canonical_url, raw_guid, source_code),
            summary=summary,
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
    identity: tuple[str, str, str] | None = None,
    summary: str | None = None,
) -> Article | None:
    conditions = [
        or_(Article.title_normalized_hash == title_hash, Article.cluster_key == title_hash),
        Article.published_at >= window_start,
        Article.published_at <= window_end,
    ]
    if exclude_source_code and identity is None:
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
    # Older exact-URL corrections updated cluster_key but left the former title
    # hash. Check the stored headline before using either index as a title match.
    rows = db.scalars(select(Article).where(and_(*conditions)).order_by(desc(Article.id)))
    candidates = [article for article in rows if sha256_str(normalize_title(article.title)) == title_hash]
    return select_release_candidate(db, candidates, identity=identity, summary=summary)


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
    raw_guid: str | None = None,
) -> tuple[Article, bool, bool]:
    url_hash = sha256_str(canonical_url)
    title_hash = sha256_str(normalize_title(title))

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
        canonical_url=canonical_url,
        raw_guid=raw_guid,
        summary=summary,
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
            content_hash=article_content_hash(title_hash, summary),
            title_normalized_hash=title_hash,
            cluster_key=title_hash,
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
                canonical_url=canonical_url,
                raw_guid=raw_guid,
                summary=summary,
            )
            article = match.article
            matched_by_url = match.matched_by_url
            if article is None:
                raise

    if article is None:
        raise RuntimeError("article upsert failed to resolve target row")

    if not created and (not matched_by_url or allow_url_match_overwrite):
        if matched_by_url:
            # Same URL, exact source — safe to overwrite all fields.
            article.title = title
            article.source_name = source_name
            article.provider_name = provider_name
            if summary:
                article.summary = summary
        elif summary and len(summary) > len(article.summary or ""):
            # Syndicated copies can enrich the summary while retaining attribution.
            article.summary = summary
        article.title_normalized_hash = title_hash
        article.cluster_key = title_hash
        article.content_hash = article_content_hash(title_hash, article.summary)
        if article.published_at is None or to_utc(published_at) > to_utc(article.published_at):
            article.published_at = published_at

    return article, created, matched_by_url


@dataclass(slots=True)
class EntryResult:
    created_article: bool = False
    persisted_raw: bool = False
    has_article: bool = False
    used_page_fetch: bool = False
    page_lookup_seconds: float = 0.0
    raw_guid: str | None = None
    raw_pair: tuple[str, datetime | None] | None = None
    reader_body: dict | None = None


@dataclass(slots=True)
class PreparedFeedEntry:
    entry: dict
    raw_title: str
    title: str
    raw_link: str
    article_url: str
    is_exact_source_url: bool
    raw_pub_date: datetime | None
    published_at: datetime
    raw_guid: str | None
    language: str | None = None


LANGUAGE_SEGMENT_PATTERN = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$", re.IGNORECASE)


def _primary_language(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    return re.split(r"[-_]", text, maxsplit=1)[0] or None


def _extract_entry_language(entry: dict, raw_link: str) -> str | None:
    language = _primary_language(entry.get("language") or entry.get("dc_language"))
    if language:
        return language

    title_detail = entry.get("title_detail")
    if isinstance(title_detail, dict):
        language = _primary_language(title_detail.get("language"))
        if language:
            return language

    parsed = urlparse(raw_link)
    path_parts = [part for part in parsed.path.split("/") if part]
    if path_parts and LANGUAGE_SEGMENT_PATTERN.match(path_parts[0]):
        return _primary_language(path_parts[0])

    for index, part in enumerate(path_parts):
        if part.lower() != "news-release":
            continue
        for candidate in path_parts[index + 1 :]:
            if LANGUAGE_SEGMENT_PATTERN.match(candidate):
                return _primary_language(candidate)
        break

    return None


def _should_skip_entry_language(source_code: str | None, language: str | None) -> bool:
    # GlobeNewswire often publishes the same release in multiple languages with
    # different URLs/titles.  Keep English when the feed gives us a language
    # signal; keep unknown-language entries so older/stubbed feeds are not lost.
    return source_code == "globenewswire" and language is not None and language != "en"


def _matches_detached_raw_row(
    row: RawFeedItem | None,
    *,
    prepared: PreparedFeedEntry,
    raw_summary: str | None,
    entry_source_name: str,
) -> bool:
    if row is None or row.article_id is not None:
        return False
    payload = row.raw_payload_json or {}
    row_pub_date = to_utc(row.raw_pub_date) if row.raw_pub_date is not None else None
    prepared_pub_date = (
        to_utc(prepared.raw_pub_date)
        if prepared.raw_pub_date is not None
        else None
    )
    return (
        row.raw_guid == prepared.raw_guid
        and row.raw_title == prepared.raw_title
        and row.raw_link == prepared.raw_link
        and row_pub_date == prepared_pub_date
        and payload.get("summary") == raw_summary
        and payload.get("source") == entry_source_name
    )


def _prepare_feed_entries(
    entries: list,
    now_utc: datetime,
    *,
    source_code: str | None = None,
) -> list[PreparedFeedEntry]:
    prepared: list[PreparedFeedEntry] = []
    for entry in entries:
        raw_title = str(entry.get("title") or "").strip()
        title = clean_summary_text(raw_title)
        raw_link_input = str(entry.get("link") or "").strip()
        raw_link = canonicalize_url(raw_link_input)
        if not title or not raw_link:
            continue
        language = _extract_entry_language(entry, raw_link)
        if _should_skip_entry_language(source_code, language):
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
                language=language,
            )
        )
    return prepared


def _prioritize_feed_matches(
    entries: list[PreparedFeedEntry],
    feed_url: str,
    known_symbols: set[str],
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None,
) -> list[PreparedFeedEntry]:
    """Keep page lookups from delaying entries already matched by the feed.

    This only schedules work; the normal persistence checks still validate every
    entry. Preserve feed order within each group, including page-fetch budgets.
    """
    ready: list[PreparedFeedEntry] = []
    remaining: list[PreparedFeedEntry] = []
    for prepared in entries:
        try:
            summary = clean_summary_text(str(
                prepared.entry.get("summary") or prepared.entry.get("description") or ""
            ).strip())
            hits = _extract_entry_tickers(
                prepared.title, summary or "", prepared.raw_link, feed_url,
                known_symbols, symbol_keywords=symbol_keywords,
            )
            matched = _max_ticker_confidence(hits) >= MIN_PERSIST_CONFIDENCE
        except Exception:
            # Classification errors must retain the existing per-entry fault
            # isolation instead of failing the whole response during scheduling.
            matched = False
        (ready if matched else remaining).append(prepared)
    return ready + remaining


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
    source_page_timeout_seconds: int | None = None,
    persistence_policy_override: str | None = None,
    recorded_guids: set[str],
    recorded_pairs: set[tuple[str, datetime | None]],
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None = None,
    page_fetch_deferred: bool = False,
    postcommit_enrichment_enabled: bool = True,
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
            _preserve_alt_feed_url(
                db,
                source_id=source.id,
                prepared=prepared,
                feed_url=feed_url,
                existing_row=existing_raw,
            )
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
        existing_detached_raw = None
        if persistence_policy_override is not None or page_config is not None or page_fetch_deferred:
            existing_detached_raw = _find_existing_raw_feed_item(
                db,
                source_id=source.id,
                raw_guid=prepared.raw_guid,
                raw_link=prepared.raw_link,
                published_at=prepared.raw_pub_date,
                require_attached=False,
            )
            # Strict feeds can legitimately see an item before page fallback
            # extraction is able to validate it. Keep the detached raw row for
            # dedupe, but still re-run extraction so later polls can recover
            # and attach the row once validation succeeds.
            if existing_detached_raw is not None and not _matches_detached_raw_row(
                existing_detached_raw,
                prepared=prepared,
                raw_summary=raw_summary,
                entry_source_name=entry_source_name,
            ):
                existing_detached_raw = None

        ticker_hits = _extract_entry_tickers(
            prepared.title,
            summary or "",
            prepared.raw_link,
            feed_url,
            known_symbols,
            symbol_keywords=symbol_keywords,
        )
        max_hit_conf = _max_ticker_confidence(ticker_hits)
        previous_retry = ((existing_detached_raw.raw_payload_json or {}).get(RETRY_KEY)
                          if existing_detached_raw is not None else None)
        enrichment_retry = None
        now = datetime.now(timezone.utc)
        config_for_retry = page_config or PAGE_FETCH_CONFIGS.get(source.code)
        can_retry = config_for_retry is not None and _is_source_article_url(
            prepared.raw_link, config_for_retry.hostname_suffix,
        )
        if max_hit_conf < MIN_PERSIST_CONFIDENCE and can_retry:
            if previous_retry and not retry_is_due(previous_retry, now):
                enrichment_retry = previous_retry
            elif page_fetch_deferred:
                enrichment_retry = pending_retry(previous_retry, attempted=False, now=now)
        if (
            max_hit_conf < MIN_PERSIST_CONFIDENCE
            and page_config is not None
            and enrichment_retry is None
        ):
            page_timeout = (
                source_page_timeout_seconds
                if source_page_timeout_seconds is not None
                else timeout_seconds
            )
            page_lookup_started = time.perf_counter()
            reset_page_outcome()
            fallback_hits = _extract_source_fallback_tickers(
                prepared.title,
                summary or "",
                prepared.raw_link,
                feed_url,
                known_symbols,
                page_timeout,
                page_config,
                symbol_keywords=symbol_keywords,
            )
            result.page_lookup_seconds = time.perf_counter() - page_lookup_started
            page_outcome = get_page_outcome()
            result.used_page_fetch = page_outcome.attempted if page_outcome is not None else True
            # None means no usable page; an empty dict means extraction ran
            # successfully but found no validated fund. Only the former queues.
            if fallback_hits is None and can_retry:
                enrichment_retry = pending_retry(previous_retry, attempted=True, now=datetime.now(timezone.utc), outcome=page_outcome)
            if fallback_hits:
                _merge_ticker_hits(ticker_hits, fallback_hits)

        should_persist_tickers = _should_persist_entry(
            source.code,
            ticker_hits,
            persistence_policy_override=persistence_policy_override,
        )
        is_general_allowed_feed = (
            get_effective_source_policy(
                source.code,
                feed_url=feed_url,
                persistence_policy_override=persistence_policy_override,
            )
            == POLICY_GENERAL_ALLOWED
        )
        allow_existing_exact_url = False
        if (
            not should_persist_tickers
            and not is_general_allowed_feed
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
                raw_guid=prepared.raw_guid,
            )
            result.created_article = created

            existing_rows = {} if created else None
            # A truncated RSS replay must not erase later full-page coverage,
            # including after the raw-feed retention window has elapsed.
            preserve_page_coverage = (
                not created and not result.used_page_fetch
                and db.get(ArticleEnrichment, article.id) is not None
            )
            _upsert_article_tickers(
                db,
                article.id,
                effective_ticker_hits,
                symbol_to_id,
                existing_rows=existing_rows,
                prune_missing=(
                    not is_general_allowed_feed
                    and not preserve_page_coverage
                    and matched_by_url
                    and prepared.is_exact_source_url
                    and bool(effective_ticker_hits)
                ),
            )

        if (postcommit_enrichment_enabled and article is not None
                and effective_ticker_hits and not result.used_page_fetch):
            enqueue_article_enrichment(
                db, article=article, source=source,
                page_url=prepared.raw_link, feed_url=feed_url,
            )

        captured = captured_page_body()
        if article is not None and captured and captured['page_url'] == prepared.raw_link:
            result.reader_body = {**captured, 'article_id': article.id,
                'article_url': article.canonical_url, 'title': article.title,
                'published_at': to_utc(article.published_at).isoformat()}

        _persist_raw_feed_item(
            db,
            source_id=source.id,
            article=article,
            prepared=prepared,
            feed_url=feed_url,
            raw_summary=raw_summary,
            entry_source_name=entry_source_name,
            existing_row=existing_raw if allow_exact_url_refresh else None,
            enrichment_retry=enrichment_retry,
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
    persistence_policy_override: str | None = None,
    article_source_name: str | None = None,
    known_symbols: set[str],
    symbol_to_id: dict[str, int],
    timeout_seconds: int,
    feed_poll_timeout_seconds: int | None = None,
    ingest_source_page_timeout_seconds: int | None = None,
    globenewswire_source_page_timeout_seconds: int = 5,
    globenewswire_source_page_max_fetches_per_feed: int = 2,
    businesswire_source_page_max_fetches_per_feed: int = 3,
    postcommit_enrichment_enabled: bool = True,
    issuer_feed_start: datetime | None = None,
    issuer_baseline_required: bool = False,
    reader_archive_directory: str | None = None,
    fetch_max_attempts: int = 3,
    fetch_backoff_seconds: float = 1.0,
    fetch_backoff_jitter_seconds: float = 0.3,
    enable_conditional_get: bool = True,
    failure_backoff_base_seconds: float = 30.0,
    failure_backoff_max_seconds: float = 600.0,
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None = None,
) -> IngestFeedResult:
    issuer_baseline_cutoff = None
    if issuer_baseline_required:
        from app.issuer_window import issuer_session
        if issuer_feed_start is None:
            raise ValueError('Issuer baseline requires explicit enrollment')
        active, session_start, _ = issuer_session(issuer_feed_start)
        if not active:
            return dict(source=source.code, feed_url=feed_url, status='skipped_issuer_window',
                        items_seen=0, items_inserted=0, notify_failed=False, error=None)
        # Persist baseline completion in the existing run ledger. Repeated app
        # starts in the same session reuse it; a new weekday needs a fresh 200.
        issuer_baseline_cutoff = db.scalar(select(IngestionRun.finished_at).where(
            IngestionRun.source_id == source.id, IngestionRun.feed_url == feed_url,
            IngestionRun.status == 'issuer_baseline', IngestionRun.started_at >= session_start,
            IngestionRun.finished_at.is_not(None),
        ).order_by(IngestionRun.finished_at).limit(1))
        if issuer_baseline_cutoff is not None:
            issuer_baseline_cutoff = to_utc(issuer_baseline_cutoff)
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
    committed_items_inserted = 0
    notify_failed = False
    status = "success"
    error_text: str | None = None
    entry_errors = 0
    receipt_recorded = False
    feed_request_started_at = None
    feed_received_at = None
    response = None
    parsed = None
    entries = []
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
            if issuer_baseline_required and issuer_baseline_cutoff is None:
                conditional_headers = {}
            feed_fetch_timeout = (
                feed_poll_timeout_seconds
                if feed_poll_timeout_seconds is not None
                else timeout_seconds
            )
            trace_source_code = source.code
            fetch_started = time.perf_counter()
            feed_request_started_at = datetime.now(timezone.utc)
            if issuer_feed_start is not None:
                response = _fetch_issuer_feed(feed_url, feed_fetch_timeout, conditional_headers)
            else:
                response = _fetch_feed_with_retries(
                    feed_url=feed_url,
                    timeout_seconds=feed_fetch_timeout,
                    max_attempts=fetch_max_attempts,
                    backoff_seconds=fetch_backoff_seconds,
                    backoff_jitter_seconds=fetch_backoff_jitter_seconds,
                    extra_headers=conditional_headers,
                )
            feed_received = time.perf_counter()
            feed_received_at = datetime.now(timezone.utc)
            status_code = int(getattr(response, "status_code", 200) or 200)
            if issuer_baseline_required and issuer_baseline_cutoff is None and status_code != 200:
                raise ValueError('A fresh full issuer response is required for the daily baseline')
            source_name = _clamp_label(article_source_name or source.name)
            entries = []
            if status_code != 304:
                parsed = feedparser.parse(response.content)
                entries = list(getattr(parsed, "entries", []) or [])
                if issuer_feed_start is not None and (parsed.bozo or not parsed.feed.get("title")):
                    raise ValueError("Unusable publisher feed")
                feed_title = parsed.feed.get("title") if isinstance(parsed.feed, dict) else None
                if source.code not in {"businesswire", "yahoo"} and feed_title:
                    source_name = _clamp_label(str(feed_title))
                items_seen = len(entries)

            record_feed_receipt(
                source_code=trace_source_code, feed_url=feed_url,
                request_started_at=feed_request_started_at, received_at=feed_received_at,
                status_code=status_code, entries=entries,
                parsed_version=getattr(parsed, "version", ""),
                parse_bozo=getattr(parsed, "bozo", True),
                feed_title_present=bool(getattr(parsed, "feed", {}).get("title")),
            )
            receipt_recorded = True
            now_utc = datetime.now(timezone.utc)
            prepared_entries = _prepare_feed_entries(
                entries,
                now_utc,
                source_code=source.code,
            )

            if issuer_feed_start is not None:
                enrollment_cutoff = max(issuer_feed_start, issuer_baseline_cutoff or issuer_feed_start)
                prepared_entries = [
                    item for item in prepared_entries
                    if item.raw_pub_date is not None and item.raw_pub_date > enrollment_cutoff
                ]

            if issuer_baseline_required and issuer_baseline_cutoff is None:
                prepared_entries = []
                status = 'issuer_baseline'

            if source.code in PAGE_FETCH_CONFIGS and status != 'issuer_baseline':
                pending = _prepare_feed_entries(
                    due_enrichment_entries(db, source.id, feed_url), now_utc,
                    source_code=source.code,
                )
                if issuer_baseline_required:
                    pending = [item for item in pending if item.raw_pub_date is not None
                               and item.raw_pub_date > enrollment_cutoff]
                def entry_key(item):
                    return ("guid", item.raw_guid) if item.raw_guid else ("url", item.raw_link, item.raw_pub_date)
                current = {entry_key(item): item for item in prepared_entries}
                pending_keys = {entry_key(item) for item in pending}
                ordered = []
                for item in pending:
                    selected = current.get(entry_key(item), item)
                    selected.entry = dict(selected.entry, _enrichment_queued_at=item.entry.get("_enrichment_queued_at"))
                    ordered.append(selected)
                # Due retries get a fair share before fresh page-dependent work;
                # feed-validated headlines are still prioritized below.
                prepared_entries = ordered + [item for item in prepared_entries if entry_key(item) not in pending_keys]

            recorded_guids, recorded_pairs = _prefetch_recorded_raw_keys(
                db,
                source_id=source.id,
                prepared_entries=prepared_entries,
            )

            provider_name = _clamp_label(source.name)
            page_config = PAGE_FETCH_CONFIGS.get(source.code)
            source_page_timeout_seconds = (
                globenewswire_source_page_timeout_seconds
                if source.code == "globenewswire"
                else ingest_source_page_timeout_seconds
            )
            page_fetches_remaining = (
                globenewswire_source_page_max_fetches_per_feed
                if source.code == "globenewswire"
                else businesswire_source_page_max_fetches_per_feed
                if source.code == "businesswire" and get_effective_source_policy(
                    source.code, feed_url=feed_url,
                    persistence_policy_override=persistence_policy_override,
                ) != POLICY_GENERAL_ALLOWED
                else None
            )
            retry_fetches_remaining = page_fetches_remaining
            if page_fetches_remaining is not None and page_fetches_remaining >= 2:
                # Due retries run first, but must not consume every slot while
                # current-feed work is waiting. Keep one lookup available for
                # current items; retries retain the rest of the existing cap.
                # Already attached entries need no reservation. On a 304 the
                # full allowance remains available to durable pending work.
                has_current_work = any(
                    "_enrichment_queued_at" not in item.entry
                    and (not item.raw_guid or item.raw_guid not in recorded_guids)
                    and (item.raw_link, item.raw_pub_date) not in recorded_pairs
                    for item in prepared_entries
                )
                if has_current_work:
                    retry_fetches_remaining -= 1

            if page_config is not None:
                prepared_entries = _prioritize_feed_matches(
                    prepared_entries, feed_url, known_symbols, symbol_keywords,
                )

            for prepared in prepared_entries:
                is_pending_work = "_enrichment_queued_at" in prepared.entry
                page_fetch_requires_network = False
                effective_page_config = page_config
                if (
                    page_config is not None
                    and page_fetches_remaining is not None
                ):
                    page_fetch_requires_network = _source_page_fetch_requires_network(
                        prepared.raw_link,
                        page_config,
                    )
                    if (
                        page_fetch_requires_network
                        and (page_fetches_remaining <= 0 or (
                            is_pending_work and retry_fetches_remaining <= 0
                        ))
                    ):
                        effective_page_config = None
                try:
                    processing_started = time.perf_counter()
                    with capture_page_body(enabled=reader_archive_directory is not None):
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
                            page_config=effective_page_config,
                            page_fetch_deferred=page_config is not None and effective_page_config is None,
                            source_page_timeout_seconds=source_page_timeout_seconds,
                            persistence_policy_override=persistence_policy_override,
                            recorded_guids=recorded_guids,
                            recorded_pairs=recorded_pairs,
                            symbol_keywords=symbol_keywords,
                            postcommit_enrichment_enabled=postcommit_enrichment_enabled,
                        )
                    if (
                        page_fetch_requires_network
                        and page_fetches_remaining is not None
                        and entry_result.used_page_fetch
                    ):
                        page_fetches_remaining -= 1
                        if is_pending_work:
                            retry_fetches_remaining -= 1

                except SQLAlchemyError:
                    # A database failure is retryable feed work, not a
                    # malformed entry that can be skipped and cached away.
                    raise
                except Exception as entry_exc:  # fault-isolation: broad catch intentional
                    db.rollback()
                    if (
                        page_fetch_requires_network
                        and effective_page_config is not None
                        and page_fetches_remaining is not None
                    ):
                        page_fetches_remaining -= 1
                        if is_pending_work:
                            retry_fetches_remaining -= 1
                    entry_errors += 1
                    error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}: {entry_exc}"
                    continue

                # Release this entry's raw/article locks before processing
                # another one. Overlapping feeds can list articles in the
                # opposite order; holding locks for a whole feed deadlocks.
                # This also makes validated news visible before later slow
                # enrichment work in the same response.
                commit_started = time.perf_counter()
                db.commit()
                commit_finished = time.perf_counter()
                committed_at = datetime.now(timezone.utc)
                if entry_result.persisted_raw:
                    if entry_result.created_article:
                        committed_items_inserted += 1
                        notification_started = time.perf_counter()
                        notification_ok = True
                        try:
                            notify_new_articles(db, 1)
                        except Exception:
                            notification_ok = False
                            notify_failed = True
                            logger.warning("pg_notify for new articles failed", exc_info=True)
                        notification_finished = time.perf_counter()
                        # Receipt is when this HTTP response became available,
                        # not when the publisher first released the story.
                        # Notification timing is the server call, not UI delivery.
                        latency_trace = {
                            "source": trace_source_code,
                            "discovery_kind": "pending_enrichment" if "_enrichment_queued_at" in prepared.entry else "feed_response",
                            "enrichment_queued_at": prepared.entry.get("_enrichment_queued_at"),
                            "feed_url": feed_url,
                            "article_url": prepared.article_url,
                            "feed_received_at": feed_received_at.isoformat(),
                            "committed_at": committed_at.isoformat(),
                            "feed_request_ms": round((feed_received - fetch_started) * 1000, 3),
                            "wait_before_processing_ms": round((processing_started - feed_received) * 1000, 3),
                            "processing_ms": round((commit_started - processing_started) * 1000, 3),
                            "page_lookup_ms": round(entry_result.page_lookup_seconds * 1000, 3),
                            "commit_ms": round((commit_finished - commit_started) * 1000, 3),
                            "receipt_to_commit_ms": round((commit_finished - feed_received) * 1000, 3),
                            "notification_call_ms": round((notification_finished - notification_started) * 1000, 3),
                            "notification_ok": notification_ok,
                            "used_page_fetch": entry_result.used_page_fetch,
                        }
                        observe_article_commit(latency_trace)
                        logger.info("article_latency %s", json.dumps(latency_trace, sort_keys=True))
                    if entry_result.has_article and entry_result.raw_guid:
                        recorded_guids.add(entry_result.raw_guid)
                    if entry_result.has_article and entry_result.raw_pair:
                        recorded_pairs.add(entry_result.raw_pair)

                # Save only after the article transaction and its ordinary
                # notification have completed. Archive failures never replay it.
                if reader_archive_directory is not None and entry_result.reader_body:
                    from app.reader_archive import save_publisher_archive
                    try:
                        save_publisher_archive(reader_archive_directory, **entry_result.reader_body)
                    except (ValueError, OSError, TypeError):
                        logger.warning('Reader archive unavailable article_id=%s',
                            entry_result.reader_body['article_id'], exc_info=True)

            if entry_errors > 0 and error_text is None:
                error_text = f"Skipped {entry_errors} malformed entr{'y' if entry_errors == 1 else 'ies'}."

            # Checkpoint HTTP validators only after the response is processed.
            # A later failure must leave earlier commits intact while allowing
            # the unprocessed entries to be fetched again instead of a 304.
            feed_state = _get_or_create_feed_poll_state(db, feed_url)
            _update_feed_http_cache(feed_state, response)
            _reset_feed_failure_backoff(feed_state)
            if issuer_feed_start is not None:
                feed_state.backoff_until = datetime.now(timezone.utc) + timedelta(seconds=60)
    except Exception as exc:
        if feed_request_started_at is not None and not receipt_recorded:
            error_response = getattr(exc, "response", None)
            record_feed_receipt(
                source_code=source.code, feed_url=feed_url,
                request_started_at=feed_request_started_at,
                received_at=feed_received_at or datetime.now(timezone.utc),
                status_code=getattr(error_response, "status_code", getattr(response, "status_code", None)),
                entries=entries, error_kind=type(exc).__name__,
            )
        db.rollback()
        status = "failed"
        error_text = str(exc)
        now_utc = datetime.now(timezone.utc)
        feed_state = _get_or_create_feed_poll_state(db, feed_url)
        server_hint_seconds = _extract_retry_after_seconds(exc)
        _mark_feed_failure_backoff(
            feed_state,
            now_utc=now_utc,
            base_seconds=failure_backoff_base_seconds,
            max_seconds=failure_backoff_max_seconds,
            server_hint_seconds=server_hint_seconds,
        )
        if issuer_feed_start is not None:
            # For the opt-in issuer feed, a refusal is terminal until operator review.
            response = getattr(exc, "response", None)
            if getattr(response, "status_code", None) in {401, 403, 404}:
                feed_state.backoff_until = datetime.max.replace(tzinfo=timezone.utc)
            else:
                issuer_delay = max(60.0, server_hint_seconds or 0.0)
                feed_state.backoff_until = max(feed_state.backoff_until, now_utc + timedelta(seconds=issuer_delay))

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
