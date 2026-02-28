from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.database import db_health_check, get_db, get_session_factory, init_db
from app.ingestion import remap_businesswire_articles
from app.models import Article, ArticleTicker, IngestionRun, RawFeedItem, Source, Ticker
from app.schemas import (
    BusinessWireRemapResponse,
    IngestionRunItem,
    IngestionRunResponse,
    NewsItem,
    NewsListResponse,
    ReloadTickersResponse,
    RunIngestionResponse,
    TickerItem,
    TickerListResponse,
)
from app.scheduler import IngestionScheduler
from app.sources import build_source_feeds, seed_sources
from app.ticker_loader import load_tickers_from_csv
from app.utils import clean_summary_text, decode_cursor, encode_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler: IngestionScheduler | None = None


def _article_tickers_map(db: Session, article_ids: list[int]) -> dict[int, list[str]]:
    if not article_ids:
        return {}
    rows = db.execute(
        select(ArticleTicker.article_id, Ticker.symbol)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(ArticleTicker.article_id.in_(article_ids))
        .order_by(ArticleTicker.article_id.asc(), Ticker.symbol.asc())
    ).all()
    mapped: dict[int, list[str]] = {}
    for article_id, symbol in rows:
        mapped.setdefault(article_id, []).append(symbol)
    return mapped


def _article_providers_map(db: Session, article_ids: list[int]) -> dict[int, str]:
    if not article_ids:
        return {}
    rows = db.execute(
        select(
            RawFeedItem.article_id,
            Source.name,
            RawFeedItem.raw_link,
            Article.canonical_url,
            RawFeedItem.id,
        )
        .join(Source, Source.id == RawFeedItem.source_id)
        .join(Article, Article.id == RawFeedItem.article_id)
        .where(RawFeedItem.article_id.in_(article_ids))
        .order_by(RawFeedItem.article_id.asc(), RawFeedItem.id.desc())
    ).all()
    matched: dict[int, str] = {}
    fallback: dict[int, str] = {}
    for article_id, source_name, raw_link, canonical_url, _raw_id in rows:
        fallback.setdefault(article_id, source_name)
        if raw_link and canonical_url and raw_link == canonical_url:
            matched[article_id] = source_name
    return {article_id: matched.get(article_id) or fallback.get(article_id, "") for article_id in article_ids}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler

    settings = get_settings()
    init_db()

    with get_session_factory()() as db:
        source_feeds = build_source_feeds(settings, db)
        seed_sources(db, source_feeds)

        ticker_stats = load_tickers_from_csv(db, settings.tickers_csv_path)
        logger.info("Ticker load stats: %s", ticker_stats)

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


@app.get("/health")
def health():
    ok = db_health_check()
    return {
        "status": "ok" if ok else "degraded",
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get(f"{settings.api_prefix}/tickers", response_model=TickerListResponse)
def list_tickers(
    q: str | None = Query(default=None, description="Prefix match on ticker symbol"),
    db: Session = Depends(get_db),
):
    query = select(Ticker).where(Ticker.active.is_(True)).order_by(Ticker.symbol.asc())
    if q:
        query = query.where(Ticker.symbol.ilike(f"%{q.strip().upper()}%"))

    rows = db.scalars(query).all()
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
        total=len(rows),
    )


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
        description="Include unmapped articles only from this provider while keeping mapped articles from all providers",
    ),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = None,
    sort: str = Query(default="latest", pattern="^(latest)$"),
    db: Session = Depends(get_db),
):
    query = select(Article)

    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )

    if ticker:
        ticker_symbols = [t.strip().upper() for t in ticker.split(",") if t.strip()]
        if ticker_symbols:
            # Match articles that have AT LEAST ONE of the requested tickers
            ticker_match_exists = (
                select(1)
                .select_from(ArticleTicker)
                .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
                .where(
                    and_(
                        ArticleTicker.article_id == Article.id,
                        Ticker.symbol.in_(ticker_symbols),
                    )
                )
                .correlate(Article)
                .exists()
            )
            query = query.where(ticker_match_exists)
    elif include_unmapped:
        pass
    elif include_unmapped_from_provider:
        include_name = include_unmapped_from_provider.strip()
        include_source_row = db.scalar(
            select(Source).where(func.lower(Source.name) == include_name.lower())
        )
        if include_source_row is None:
            query = query.where(mapped_exists)
        else:
            include_provider_exists = (
                select(1)
                .select_from(RawFeedItem)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        RawFeedItem.source_id == include_source_row.id,
                    )
                )
                .correlate(Article)
                .exists()
            )
            query = query.where(
                or_(mapped_exists, and_(~mapped_exists, include_provider_exists))
            )
    else:
        query = query.where(mapped_exists)

    if source:
        query = query.where(Article.source_name.ilike(f"%{source.strip()}%"))

    if provider:
        provider_text = provider.strip()
        source_row = db.scalar(
            select(Source).where(func.lower(Source.name) == provider_text.lower())
        )
        if source_row is not None:
            provider_exists = (
                select(1)
                .select_from(RawFeedItem)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        RawFeedItem.source_id == source_row.id,
                    )
                )
                .correlate(Article)
                .exists()
            )
            query = query.where(provider_exists)
        else:
            query = query.where(Article.provider_name.ilike(f"%{provider_text}%"))

    if q:
        query = query.where(Article.title.ilike(f"%{q.strip()}%"))

    if from_:
        query = query.where(Article.published_at >= from_)

    if to:
        query = query.where(Article.published_at <= to)

    cursor_payload = decode_cursor(cursor) if cursor else None
    if cursor_payload:
        cursor_published, cursor_id = cursor_payload
        query = query.where(
            or_(
                Article.published_at < cursor_published,
                and_(Article.published_at == cursor_published, Article.id < cursor_id),
            )
        )

    query = query.order_by(
        Article.published_at.desc().nullslast(), Article.id.desc()
    ).limit(limit + 1)

    rows = db.scalars(query).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    tickers_by_article = _article_tickers_map(db, [row.id for row in rows])
    providers_by_article = _article_providers_map(db, [row.id for row in rows])

    items = [
        NewsItem(
            id=row.id,
            title=row.title,
            url=row.canonical_url,
            source=row.source_name,
            provider=providers_by_article.get(row.id) or row.provider_name,
            summary=clean_summary_text(row.summary),
            published_at=row.published_at,
            tickers=tickers_by_article.get(row.id, []),
            dedupe_group=row.cluster_key,
        )
        for row in rows
    ]

    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = encode_cursor(last.published_at, last.id)

    return NewsListResponse(
        items=items,
        next_cursor=next_cursor,
        meta={
            "count": len(items),
            "limit": limit,
            "sort": sort,
        },
    )


@app.get(f"{settings.api_prefix}/news/{{article_id}}", response_model=NewsItem)
def get_news_item(article_id: int, db: Session = Depends(get_db)):
    row = db.scalar(select(Article).where(Article.id == article_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Article not found")

    tickers_by_article = _article_tickers_map(db, [row.id])
    providers_by_article = _article_providers_map(db, [row.id])
    return NewsItem(
        id=row.id,
        title=row.title,
        url=row.canonical_url,
        source=row.source_name,
        provider=providers_by_article.get(row.id) or row.provider_name,
        summary=clean_summary_text(row.summary),
        published_at=row.published_at,
        tickers=tickers_by_article.get(row.id, []),
        dedupe_group=row.cluster_key,
    )


@app.post(
    f"{settings.api_prefix}/admin/ingest/run-once", response_model=RunIngestionResponse
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
    f"{settings.api_prefix}/admin/ingest/status", response_model=IngestionRunResponse
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
    f"{settings.api_prefix}/admin/remap/businesswire",
    response_model=BusinessWireRemapResponse,
)
def admin_remap_businesswire(
    limit: int = Query(default=500, ge=1, le=5000),
    only_unmapped: bool = Query(
        default=True,
        description="Remap only Business Wire articles that currently have no ticker mapping",
    ),
    db: Session = Depends(get_db),
):
    result = remap_businesswire_articles(
        db,
        settings,
        limit=limit,
        only_unmapped=only_unmapped,
    )
    return BusinessWireRemapResponse(
        processed=int(result["processed"]),
        articles_with_hits=int(result["articles_with_hits"]),
        remapped_articles=int(result["remapped_articles"]),
        only_unmapped=bool(result["only_unmapped"]),
    )


@app.post(
    f"{settings.api_prefix}/admin/tickers/reload",
    response_model=ReloadTickersResponse,
)
def admin_reload_tickers(
    remap_unmapped_businesswire: bool = Query(
        default=True,
        description="Run Business Wire remap after loading ticker CSV",
    ),
    remap_limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    ticker_stats = load_tickers_from_csv(db, settings.tickers_csv_path)

    remap_payload: BusinessWireRemapResponse | None = None
    if remap_unmapped_businesswire:
        result = remap_businesswire_articles(
            db,
            settings,
            limit=remap_limit,
            only_unmapped=True,
        )
        remap_payload = BusinessWireRemapResponse(
            processed=int(result["processed"]),
            articles_with_hits=int(result["articles_with_hits"]),
            remapped_articles=int(result["remapped_articles"]),
            only_unmapped=bool(result["only_unmapped"]),
        )

    return ReloadTickersResponse(
        loaded=int(ticker_stats["loaded"]),
        created=int(ticker_stats["created"]),
        updated=int(ticker_stats["updated"]),
        unchanged=int(ticker_stats["unchanged"]),
        remap=remap_payload,
    )
