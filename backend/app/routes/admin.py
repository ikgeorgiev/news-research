from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.article_maintenance import (
    dedupe_articles_by_title,
    dedupe_businesswire_url_variants,
    purge_token_only_articles,
    remap_source_articles,
    revalidate_stale_article_tickers,
)
from app.config import get_settings
from app.database import get_db
from app.deps import require_admin_api_key
from app.models import IngestionRun, Source
from app.scheduler import get_scheduler
from app.schemas import (
    DedupeResponse,
    IngestionRunItem,
    IngestionRunResponse,
    PurgeFalsePositiveResponse,
    ReloadTickersResponse,
    RevalidationResponse,
    RunIngestionResponse,
    SourceRemapResponse,
)
from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_loader import load_tickers_from_csv

admin_router = APIRouter(dependencies=[Depends(require_admin_api_key)])


def _dedupe_response_payload(result: dict[str, int]) -> DedupeResponse:
    return DedupeResponse(
        scanned_articles=int(result["scanned_articles"]),
        duplicate_groups=int(result["duplicate_groups"]),
        merged_articles=int(result["merged_articles"]),
        raw_items_relinked=int(result["raw_items_relinked"]),
        ticker_rows_relinked=int(result["ticker_rows_relinked"]),
        ticker_rows_updated=int(result["ticker_rows_updated"]),
        ticker_rows_deleted=int(result["ticker_rows_deleted"]),
    )


def _source_remap_response_payload(result: dict[str, int | bool | str]) -> SourceRemapResponse:
    return SourceRemapResponse(
        source_code=str(result["source_code"]),
        processed=int(result["processed"]),
        articles_with_hits=int(result["articles_with_hits"]),
        remapped_articles=int(result["remapped_articles"]),
        only_unmapped=bool(result["only_unmapped"]),
    )


@admin_router.post("/admin/ingest/run-once", response_model=RunIngestionResponse)
def admin_run_ingestion():
    scheduler = get_scheduler()
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


@admin_router.get("/admin/ingest/status", response_model=IngestionRunResponse)
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


@admin_router.post("/admin/dedupe/businesswire-url-variants", response_model=DedupeResponse)
def admin_dedupe_businesswire_url_variants(db: Session = Depends(get_db)):
    return _dedupe_response_payload(dedupe_businesswire_url_variants(db))


@admin_router.post("/admin/dedupe/title", response_model=DedupeResponse)
def admin_dedupe_by_title(db: Session = Depends(get_db)):
    return _dedupe_response_payload(dedupe_articles_by_title(db))


@admin_router.post("/admin/purge/false-positives", response_model=PurgeFalsePositiveResponse)
def admin_purge_false_positives(
    dry_run: bool = Query(
        default=True,
        description="Preview what would be deleted without actually deleting",
    ),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    result = purge_token_only_articles(
        db,
        dry_run=dry_run,
        limit=limit,
        timeout_seconds=settings.request_timeout_seconds,
        globenewswire_source_page_timeout_seconds=getattr(
            settings, "globenewswire_source_page_timeout_seconds", None
        ),
    )
    return PurgeFalsePositiveResponse(
        dry_run=dry_run,
        scanned_articles=result["scanned_articles"],
        purged_articles=result["purged_articles"],
        deleted_article_tickers=result["deleted_article_tickers"],
        deleted_raw_feed_items=result["deleted_raw_feed_items"],
    )


@admin_router.post("/admin/revalidate", response_model=RevalidationResponse)
def admin_revalidate_articles(
    limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    result = revalidate_stale_article_tickers(
        db,
        limit=limit,
        timeout_seconds=settings.request_timeout_seconds,
        globenewswire_source_page_timeout_seconds=getattr(
            settings, "globenewswire_source_page_timeout_seconds", None
        ),
    )
    return RevalidationResponse(
        scanned=int(result["scanned"]),
        revalidated=int(result["revalidated"]),
        purged=int(result["purged"]),
        unchanged=int(result["unchanged"]),
    )


@admin_router.post("/admin/remap/{source_code}", response_model=SourceRemapResponse)
def admin_remap_source(
    source_code: str,
    limit: int = Query(default=500, ge=1, le=5000),
    only_unmapped: bool = Query(
        default=True,
        description="Remap only articles that currently have no ticker mapping",
    ),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    if source_code not in PAGE_FETCH_CONFIGS:
        raise HTTPException(
            status_code=400,
            detail=f"Source '{source_code}' does not support page-fetch remap. "
            f"Supported: {', '.join(sorted(PAGE_FETCH_CONFIGS.keys()))}",
        )
    result = remap_source_articles(
        db,
        settings,
        source_code=source_code,
        limit=limit,
        only_unmapped=only_unmapped,
        globenewswire_source_page_timeout_seconds=getattr(
            settings, "globenewswire_source_page_timeout_seconds", None
        ),
    )
    return _source_remap_response_payload(result)


@admin_router.post("/admin/tickers/reload", response_model=ReloadTickersResponse)
def admin_reload_tickers(
    remap_unmapped: bool = Query(
        default=True,
        description="Run source remaps after loading ticker CSV",
    ),
    remap_only_unmapped: bool = Query(
        default=True,
        description="When remapping, restrict the pass to unmapped articles only",
    ),
    remap_limit: int = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    settings = get_settings()
    ticker_stats = load_tickers_from_csv(db, settings.tickers_csv_path)

    source_remap_payloads: list[SourceRemapResponse] = []
    if remap_unmapped:
        gn_timeout = getattr(
            settings, "globenewswire_source_page_timeout_seconds", None
        )
        for code in sorted(PAGE_FETCH_CONFIGS.keys()):
            result = remap_source_articles(
                db,
                settings,
                source_code=code,
                limit=remap_limit,
                only_unmapped=remap_only_unmapped,
                globenewswire_source_page_timeout_seconds=gn_timeout,
            )
            source_remap_payloads.append(_source_remap_response_payload(result))

    return ReloadTickersResponse(
        loaded=int(ticker_stats["loaded"]),
        created=int(ticker_stats["created"]),
        updated=int(ticker_stats["updated"]),
        unchanged=int(ticker_stats["unchanged"]),
        source_remaps=source_remap_payloads if source_remap_payloads else None,
    )
