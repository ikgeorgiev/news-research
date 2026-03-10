from __future__ import annotations

import logging
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from app.article_filters import build_article_query
from app.config import get_settings
from app.database import db_health_check, get_db, get_session_factory, init_db
from app.ingestion import (
    PAGE_FETCH_CONFIGS,
    dedupe_articles_by_title,
    dedupe_businesswire_url_variants,
    purge_token_only_articles,
    remap_source_articles,
    sync_runtime_state,
)
from app.monitoring import observe_http_request, render_metrics
from app.models import Article, ArticleTicker, IngestionRun, PushSubscription, RawFeedItem, Source, Ticker
from app.push_alerts import hash_manage_token, normalize_scopes, push_runtime_enabled, seed_last_notified_watermarks
from app.schemas import (
    DedupeResponse,
    IngestionRunItem,
    IngestionRunResponse,
    MarkAlertsSentRequest,
    MarkAlertsSentResponse,
    NewsGlobalSummary,
    NewsIdsResponse,
    NewsItem,
    NewsListResponse,
    PurgeFalsePositiveResponse,
    ReloadTickersResponse,
    RunIngestionResponse,
    SourceRemapResponse,
    PushDeleteRequest,
    PushDeleteResponse,
    PushUpsertRequest,
    PushUpsertResponse,
    PushVapidKeyResponse,
    TickerItem,
    TickerListResponse,
)
from app.scheduler import IngestionScheduler
from app.ticker_loader import load_tickers_from_csv
from app.query_utils import contains_literal_pattern, iter_chunks, prefix_literal_pattern
from app.utils import clean_summary_text, decode_cursor, encode_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler: IngestionScheduler | None = None
MAX_NEWS_IDS_PAGE_SIZE = 5000
GLOBAL_NEWS_TRACKED_LIMIT = 100
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _article_sort_key_expr():
    return func.coalesce(Article.published_at, Article.created_at, Article.first_seen_at)


def _coalesce_row_timestamp(*values: datetime | None) -> datetime:
    for value in values:
        if value is not None:
            return value
    return EPOCH_UTC


def _published_at_for_response(row: object) -> datetime:
    """Protect API serialization/cursoring from legacy rows with missing timestamps."""
    return _coalesce_row_timestamp(
        getattr(row, "published_at", None),
        getattr(row, "created_at", None),
        getattr(row, "first_seen_at", None),
    )


def _first_seen_at_for_response(row: object) -> datetime:
    """Protect serialization for legacy rows created before first_seen_at existed."""
    return _coalesce_row_timestamp(
        getattr(row, "first_seen_at", None),
        getattr(row, "created_at", None),
        getattr(row, "published_at", None),
    )


def _parse_aggregated_symbols(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = [item for item in value.split(",") if item]
    elif isinstance(value, (list, tuple)):
        raw_values = [str(item) for item in value if item]
    else:
        raw_values = [str(value)]
    unique = sorted({item.strip() for item in raw_values if item and item.strip()})
    return unique


def _build_news_page_subquery(
    db: Session,
    *,
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = False,
    include_unmapped_from_provider: str | None = None,
    from_: datetime | None = None,
    to: datetime | None = None,
    cursor: str | None = None,
    limit: int | None = None,
    article_id: int | None = None,
):
    query = build_article_query(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
    )
    sort_key = _article_sort_key_expr()
    cursor_payload = decode_cursor(cursor) if cursor else None
    if cursor_payload:
        cursor_published, cursor_id = cursor_payload
        query = query.where(
            or_(
                sort_key < cursor_published,
                and_(sort_key == cursor_published, Article.id < cursor_id),
            )
        )
    if article_id is not None:
        query = query.where(Article.id == article_id)
    query = query.with_only_columns(
        Article.id.label("id"),
        Article.title.label("title"),
        Article.canonical_url.label("url"),
        Article.source_name.label("source"),
        Article.provider_name.label("provider_name"),
        Article.summary.label("summary"),
        Article.published_at.label("published_at"),
        Article.created_at.label("created_at"),
        Article.first_seen_at.label("first_seen_at"),
        Article.first_alert_sent_at.label("alert_sent_at"),
        Article.cluster_key.label("dedupe_group"),
        sort_key.label("sort_ts"),
    ).order_by(sort_key.desc(), Article.id.desc())
    if limit is not None:
        query = query.limit(limit)
    return query.subquery("news_page")


def _serialize_news_item(row: object) -> NewsItem:
    return NewsItem(
        id=row.id,
        title=row.title,
        url=row.url,
        source=row.source,
        provider=row.provider,
        summary=clean_summary_text(row.summary),
        published_at=_published_at_for_response(row),
        first_seen_at=_first_seen_at_for_response(row),
        alert_sent_at=row.alert_sent_at,
        tickers=_parse_aggregated_symbols(row.tickers),
        dedupe_group=row.dedupe_group,
    )


def _build_ticker_agg_subquery(db: Session, news_page):
    ticker_rows = (
        select(
            news_page.c.id.label("article_id"),
            Ticker.symbol.label("symbol"),
        )
        .select_from(news_page)
        .join(ArticleTicker, ArticleTicker.article_id == news_page.c.id)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(Ticker.active.is_(True))
        .distinct()
        .subquery("ticker_rows")
    )
    dialect_name = db.get_bind().dialect.name if db.get_bind() is not None else ""
    if dialect_name == "postgresql":
        ticker_agg = func.array_agg(ticker_rows.c.symbol).label("tickers")
    else:
        ticker_agg = func.group_concat(ticker_rows.c.symbol, ",").label("tickers")
    return (
        select(
            ticker_rows.c.article_id,
            ticker_agg,
        )
        .group_by(ticker_rows.c.article_id)
        .subquery("ticker_agg")
    )


def _build_enriched_news_select(db: Session, news_page):
    ticker_agg = _build_ticker_agg_subquery(db, news_page)
    canonical_provider = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == news_page.c.id,
                RawFeedItem.raw_link == news_page.c.url,
            )
        )
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    latest_provider = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(RawFeedItem.article_id == news_page.c.id)
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return (
        select(
            news_page.c.id,
            news_page.c.title,
            news_page.c.url,
            news_page.c.source,
            news_page.c.provider_name,
            news_page.c.summary,
            news_page.c.published_at,
            news_page.c.created_at,
            news_page.c.first_seen_at,
            news_page.c.alert_sent_at,
            news_page.c.dedupe_group,
            news_page.c.sort_ts,
            func.coalesce(canonical_provider, latest_provider, news_page.c.provider_name).label("provider"),
            ticker_agg.c.tickers.label("tickers"),
        )
        .select_from(news_page.outerjoin(ticker_agg, ticker_agg.c.article_id == news_page.c.id))
        .order_by(news_page.c.sort_ts.desc(), news_page.c.id.desc())
    )


def _build_global_summary(db: Session) -> NewsGlobalSummary | None:
    sort_key = _article_sort_key_expr()
    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(
            and_(
                ArticleTicker.article_id == Article.id,
                Ticker.active.is_(True),
            )
        )
        .correlate(Article)
        .exists()
    )
    include_provider_exists = (
        select(1)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                func.lower(Source.name) == "business wire",
            )
        )
        .correlate(Article)
        .exists()
    )
    base_query = select(Article).where(
        or_(
            mapped_exists,
            and_(~mapped_exists, include_provider_exists),
        )
    )
    summary_page = (
        base_query.with_only_columns(
            Article.id.label("id"),
            func.count().over().label("total"),
        )
        .order_by(sort_key.desc(), Article.id.desc())
        .limit(GLOBAL_NEWS_TRACKED_LIMIT)
        .subquery("global_summary_page")
    )
    dialect_name = db.get_bind().dialect.name if db.get_bind() is not None else ""
    if dialect_name == "postgresql":
        tracked_ids_agg = func.array_agg(summary_page.c.id).label("tracked_ids")
    else:
        tracked_ids_agg = func.group_concat(summary_page.c.id, ",").label("tracked_ids")
    row = db.execute(
        select(
            func.max(summary_page.c.total).label("total"),
            tracked_ids_agg,
        ).select_from(summary_page)
    ).one()
    tracked_ids_value = row.tracked_ids
    if tracked_ids_value is None:
        tracked_ids: list[int] = []
    elif isinstance(tracked_ids_value, str):
        tracked_ids = [int(value) for value in tracked_ids_value.split(",") if value]
    else:
        tracked_ids = [int(value) for value in tracked_ids_value if value is not None]
    total = int(row.total or 0)
    return NewsGlobalSummary(
        total=total,
        tracked_ids=tracked_ids,
        tracked_limit=GLOBAL_NEWS_TRACKED_LIMIT,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler

    settings = get_settings()
    init_db()

    with get_session_factory()() as db:
        runtime_sync = sync_runtime_state(
            db,
            settings,
            ticker_loader=load_tickers_from_csv,
        )
        logger.info("Ticker load stats: %s", runtime_sync["ticker_sync"])
        stale_runs_fixed = runtime_sync["stale_runs_fixed"]
        if stale_runs_fixed:
            logger.warning("Marked %s stale ingestion runs as failed at startup", stale_runs_fixed)

    scheduler = IngestionScheduler(settings, get_session_factory())
    scheduler.start()

    yield

    if scheduler is not None:
        scheduler.shutdown()


app = FastAPI(title="CEF News Feed API", lifespan=lifespan)
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration_seconds = time.perf_counter() - started_at
        route = request.scope.get("route")
        path_template = getattr(route, "path", request.url.path)
        observe_http_request(request.method, path_template, status_code, duration_seconds)


def require_admin_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    expected_key = (settings.admin_api_key or "").strip()
    if not expected_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin API key is not configured",
        )
    if x_api_key is None or not secrets.compare_digest(x_api_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin API key",
        )


@app.get("/health")
def health():
    try:
        ok = db_health_check()
    except Exception:
        logger.exception("Database health check failed")
        ok = False
    return {
        "status": "ok" if ok else "degraded",
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/metrics", include_in_schema=False)
def metrics():
    payload, content_type = render_metrics()
    return Response(content=payload, media_type=content_type)


def _require_push_runtime_enabled() -> str:
    if not push_runtime_enabled(settings):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Push notifications are not configured",
        )
    return (settings.vapid_public_key or "").strip()


@app.get(f"{settings.api_prefix}/push/vapid-key", response_model=PushVapidKeyResponse)
def get_push_vapid_key():
    if not push_runtime_enabled(settings):
        return PushVapidKeyResponse(enabled=False, public_key=None)
    return PushVapidKeyResponse(
        enabled=True,
        public_key=(settings.vapid_public_key or "").strip() or None,
    )


@app.put(f"{settings.api_prefix}/push/subscription", response_model=PushUpsertResponse)
def upsert_push_subscription(payload: PushUpsertRequest, db: Session = Depends(get_db)):
    _require_push_runtime_enabled()

    endpoint = payload.subscription.endpoint.strip()
    if not endpoint:
        raise HTTPException(status_code=422, detail="Subscription endpoint is required")

    p256dh = payload.subscription.keys.p256dh.strip()
    auth = payload.subscription.keys.auth.strip()
    if not p256dh or not auth:
        raise HTTPException(status_code=422, detail="Subscription keys are required")

    submitted_manage_token = (payload.manage_token or "").strip() or None
    scopes = normalize_scopes(payload.scopes.model_dump())
    existing = db.scalar(select(PushSubscription).where(PushSubscription.endpoint == endpoint))

    created = existing is None
    manage_token = submitted_manage_token
    subscription = existing

    if subscription is None:
        manage_token = secrets.token_urlsafe(32)
        subscription = PushSubscription(
            endpoint=endpoint,
            key_p256dh=p256dh,
            key_auth=auth,
            expiration_time=payload.subscription.expiration_time,
            manage_token_hash=hash_manage_token(manage_token),
            active=True,
        )
        db.add(subscription)
    else:
        if not submitted_manage_token:
            raise HTTPException(status_code=401, detail="Manage token is required for this subscription")
        provided_hash = hash_manage_token(submitted_manage_token)
        if not secrets.compare_digest(provided_hash, subscription.manage_token_hash):
            raise HTTPException(status_code=401, detail="Invalid manage token")
        manage_token = submitted_manage_token

    next_watermarks, seeded = seed_last_notified_watermarks(
        db,
        scopes=scopes,
        existing=subscription.last_notified_json if not created else None,
    )

    subscription.endpoint = endpoint
    subscription.key_p256dh = p256dh
    subscription.key_auth = auth
    subscription.expiration_time = payload.subscription.expiration_time
    subscription.alert_scopes_json = scopes
    subscription.last_notified_json = next_watermarks
    subscription.active = True
    subscription.last_error = None

    db.commit()
    db.refresh(subscription)

    return PushUpsertResponse(
        id=subscription.id,
        active=subscription.active,
        created=created,
        manage_token=manage_token,
        seeded_last_notified=seeded,
    )


@app.delete(f"{settings.api_prefix}/push/subscription", response_model=PushDeleteResponse)
def delete_push_subscription(payload: PushDeleteRequest, db: Session = Depends(get_db)):
    _require_push_runtime_enabled()

    endpoint = payload.endpoint.strip()
    manage_token = payload.manage_token.strip()
    if not endpoint:
        return PushDeleteResponse(deleted=False)

    subscription = db.scalar(select(PushSubscription).where(PushSubscription.endpoint == endpoint))
    if subscription is None:
        return PushDeleteResponse(deleted=False)

    provided_hash = hash_manage_token(manage_token)
    if not secrets.compare_digest(provided_hash, subscription.manage_token_hash):
        raise HTTPException(status_code=401, detail="Invalid manage token")

    db.delete(subscription)
    db.commit()
    return PushDeleteResponse(deleted=True)


@app.get(f"{settings.api_prefix}/tickers", response_model=TickerListResponse)
def list_tickers(
    q: str | None = Query(default=None, description="Prefix match on ticker symbol"),
    limit: int = Query(default=5000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = select(Ticker).where(Ticker.active.is_(True)).order_by(Ticker.symbol.asc())
    if q:
        query = query.where(
            Ticker.symbol.ilike(
                prefix_literal_pattern(q.strip().upper()),
                escape="\\",
            )
        )

    total = db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0
    rows = db.scalars(query.limit(limit).offset(offset)).all()
    return TickerListResponse(
        items=[
            TickerItem(
                symbol=row.symbol,
                fund_name=row.fund_name,
                sponsor=row.sponsor,
                active=row.active,
            )
            for row in rows
        ],
        total=total,
    )


@app.get(f"{settings.api_prefix}/news/ids", response_model=NewsIdsResponse)
def list_news_ids(
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = Query(
        default=False,
        description="Include articles not mapped to any active ticker",
    ),
    include_unmapped_from_provider: str | None = Query(
        default=None,
        description="Include articles not mapped to any active ticker only from this provider while keeping active-ticker-mapped articles from all providers",
    ),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=MAX_NEWS_IDS_PAGE_SIZE, ge=1, le=MAX_NEWS_IDS_PAGE_SIZE),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    query = build_article_query(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
    )
    sort_key = _article_sort_key_expr()
    cursor_payload = decode_cursor(cursor) if cursor else None
    if cursor_payload:
        cursor_published, cursor_id = cursor_payload
        query = query.where(
            or_(
                sort_key < cursor_published,
                and_(sort_key == cursor_published, Article.id < cursor_id),
            )
        )

    id_query = (
        query.with_only_columns(Article.id, sort_key.label("sort_ts"))
        .order_by(sort_key.desc(), Article.id.desc())
        .limit(limit + 1)
    )
    rows = db.execute(id_query).all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    ids = [article_id for article_id, _sort_ts in rows]
    next_cursor = None
    if has_more and rows:
        last_id, last_sort_ts = rows[-1]
        next_cursor = encode_cursor(last_sort_ts or EPOCH_UTC, last_id)
    return NewsIdsResponse(ids=ids, next_cursor=next_cursor)


@app.get(f"{settings.api_prefix}/news", response_model=NewsListResponse)
def list_news(
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = Query(
        default=False,
        description="Include articles not mapped to any active ticker",
    ),
    include_unmapped_from_provider: str | None = Query(
        default=None,
        description="Include articles not mapped to any active ticker only from this provider while keeping active-ticker-mapped articles from all providers",
    ),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    include_global_summary: bool = Query(default=False),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    news_page = _build_news_page_subquery(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
        cursor=cursor,
        limit=limit + 1,
    )
    rows = db.execute(_build_enriched_news_select(db, news_page)).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    items = [_serialize_news_item(row) for row in rows]

    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = encode_cursor(_published_at_for_response(last), last.id)

    global_summary = _build_global_summary(db) if include_global_summary else None

    return NewsListResponse(
        items=items,
        next_cursor=next_cursor,
        meta={
            "count": len(items),
            "limit": limit,
            "sort": "latest",
        },
        global_summary=global_summary,
    )


@app.get(f"{settings.api_prefix}/news/{{article_id}}", response_model=NewsItem)
def get_news_item(article_id: int, db: Session = Depends(get_db)):
    news_page = _build_news_page_subquery(db, article_id=article_id)
    row = db.execute(_build_enriched_news_select(db, news_page)).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Article not found")
    return _serialize_news_item(row)


@app.post(
    f"{settings.api_prefix}/news/alerts/sent",
    response_model=MarkAlertsSentResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def mark_news_alerts_sent(payload: MarkAlertsSentRequest, db: Session = Depends(get_db)):
    unique_ids = sorted({int(article_id) for article_id in payload.article_ids if int(article_id) > 0})
    if not unique_ids:
        return MarkAlertsSentResponse(requested=0, marked=0, first_alert_sent_at=None)

    sent_at = datetime.now(timezone.utc)
    updated = 0
    for chunk in iter_chunks(unique_ids):
        updated += db.execute(
            update(Article)
            .where(
                Article.id.in_(chunk),
                Article.first_alert_sent_at.is_(None),
            )
            .values(first_alert_sent_at=sent_at)
        ).rowcount or 0
    db.commit()
    return MarkAlertsSentResponse(
        requested=len(unique_ids),
        marked=int(updated),
        first_alert_sent_at=sent_at if updated else None,
    )


@app.post(
    f"{settings.api_prefix}/admin/ingest/run-once",
    response_model=RunIngestionResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_run_ingestion():
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Ingestion scheduler is not available")

    result = scheduler.run_once()
    if result is None:
        raise HTTPException(status_code=409, detail="Ingestion is already running")

    return RunIngestionResponse(
        total_feeds=int(result["total_feeds"]),
        total_items_seen=int(result["total_items_seen"]),
        total_items_inserted=int(result["total_items_inserted"]),
        failed_feeds=int(result["failed_feeds"]),
    )


@app.get(
    f"{settings.api_prefix}/admin/ingest/status",
    response_model=IngestionRunResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_ingestion_status(
    limit: int = Query(default=100, ge=1, le=300), db: Session = Depends(get_db)
):
    rows = db.execute(
        select(IngestionRun, Source.code)
        .join(Source, Source.id == IngestionRun.source_id)
        .order_by(IngestionRun.started_at.desc())
        .limit(limit)
    ).all()

    items = [
        IngestionRunItem(
            id=run.id,
            source=source_code,
            feed_url=run.feed_url,
            started_at=run.started_at,
            finished_at=run.finished_at,
            status=run.status,
            items_seen=run.items_seen,
            items_inserted=run.items_inserted,
            error_text=run.error_text,
        )
        for run, source_code in rows
    ]
    return IngestionRunResponse(items=items)


@app.post(
    f"{settings.api_prefix}/admin/dedupe/businesswire-url-variants",
    response_model=DedupeResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_dedupe_businesswire_url_variants(db: Session = Depends(get_db)):
    result = dedupe_businesswire_url_variants(db)
    return DedupeResponse(
        scanned_articles=int(result["scanned_articles"]),
        duplicate_groups=int(result["duplicate_groups"]),
        merged_articles=int(result["merged_articles"]),
        raw_items_relinked=int(result["raw_items_relinked"]),
        ticker_rows_relinked=int(result["ticker_rows_relinked"]),
        ticker_rows_updated=int(result["ticker_rows_updated"]),
        ticker_rows_deleted=int(result["ticker_rows_deleted"]),
    )


@app.post(
    f"{settings.api_prefix}/admin/dedupe/title",
    response_model=DedupeResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_dedupe_by_title(db: Session = Depends(get_db)):
    result = dedupe_articles_by_title(db)
    return DedupeResponse(
        scanned_articles=int(result["scanned_articles"]),
        duplicate_groups=int(result["duplicate_groups"]),
        merged_articles=int(result["merged_articles"]),
        raw_items_relinked=int(result["raw_items_relinked"]),
        ticker_rows_relinked=int(result["ticker_rows_relinked"]),
        ticker_rows_updated=int(result["ticker_rows_updated"]),
        ticker_rows_deleted=int(result["ticker_rows_deleted"]),
    )


@app.post(
    f"{settings.api_prefix}/admin/purge/false-positives",
    response_model=PurgeFalsePositiveResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_purge_false_positives(
    dry_run: bool = Query(
        default=True,
        description="Preview what would be deleted without actually deleting",
    ),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    result = purge_token_only_articles(
        db,
        dry_run=dry_run,
        limit=limit,
        timeout_seconds=settings.request_timeout_seconds,
    )
    return PurgeFalsePositiveResponse(
        dry_run=dry_run,
        scanned_articles=result["scanned_articles"],
        purged_articles=result["purged_articles"],
        deleted_article_tickers=result["deleted_article_tickers"],
        deleted_raw_feed_items=result["deleted_raw_feed_items"],
    )


@app.post(
    f"{settings.api_prefix}/admin/remap/{{source_code}}",
    response_model=SourceRemapResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_remap_source(
    source_code: str,
    limit: int = Query(default=500, ge=1, le=5000),
    only_unmapped: bool = Query(
        default=True,
        description="Remap only articles that currently have no ticker mapping",
    ),
    db: Session = Depends(get_db),
):
    if source_code not in PAGE_FETCH_CONFIGS:
        raise HTTPException(
            status_code=400,
            detail=f"Source '{source_code}' does not support page-fetch remap. "
            f"Supported: {', '.join(sorted(PAGE_FETCH_CONFIGS.keys()))}",
        )
    result = remap_source_articles(
        db, settings, source_code=source_code, limit=limit, only_unmapped=only_unmapped,
    )
    return SourceRemapResponse(
        source_code=result["source_code"],
        processed=int(result["processed"]),
        articles_with_hits=int(result["articles_with_hits"]),
        remapped_articles=int(result["remapped_articles"]),
        only_unmapped=bool(result["only_unmapped"]),
    )


@app.post(
    f"{settings.api_prefix}/admin/tickers/reload",
    response_model=ReloadTickersResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def admin_reload_tickers(
    remap_unmapped: bool = Query(
        default=True,
        description="Run source remaps after loading ticker CSV",
    ),
    remap_limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    ticker_stats = load_tickers_from_csv(db, settings.tickers_csv_path)

    source_remap_payloads: list[SourceRemapResponse] = []
    if remap_unmapped:
        for code in sorted(PAGE_FETCH_CONFIGS.keys()):
            result = remap_source_articles(
                db, settings, source_code=code, limit=remap_limit, only_unmapped=True,
            )
            source_remap_payloads.append(SourceRemapResponse(
                source_code=result["source_code"],
                processed=int(result["processed"]),
                articles_with_hits=int(result["articles_with_hits"]),
                remapped_articles=int(result["remapped_articles"]),
                only_unmapped=bool(result["only_unmapped"]),
            ))

    return ReloadTickersResponse(
        loaded=int(ticker_stats["loaded"]),
        created=int(ticker_stats["created"]),
        updated=int(ticker_stats["updated"]),
        unchanged=int(ticker_stats["unchanged"]),
        source_remaps=source_remap_payloads if source_remap_payloads else None,
    )
