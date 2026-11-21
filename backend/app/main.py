from __future__ import annotations

import logging
import time
from contextlib import ExitStack, asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.article_enrichment import ArticleEnrichmentWorker
from app.financialcontent.runtime import FinancialContentWorker
from app.free_news import FreeDiscoveryWorker
from app.research_timing import configure_research_timing, research_timing_status
from app.database import db_health_check, get_session_factory
from app.ingestion import sync_runtime_state
from app.monitoring import observe_http_request, render_metrics
from app.page_access import PageAccessStore, configure_page_access
from app.push_alerts import PushAlertDispatcher
from app.routes.admin import admin_router
from app.routes.news import news_router
from app.routes.push import push_router
from app.routes.sse import sse_router
from app.scheduler import IngestionScheduler, get_scheduler, set_scheduler
from app.sse import SSEBroadcaster, SSEConnectionLimiter
from app.ticker_loader import load_tickers_from_csv
from migrate import assert_schema_at_head

logging.basicConfig(level=logging.INFO)
# Keep third-party transport chatter out of normal app logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _warn_if_ingestion_pool_capacity_low(settings) -> None:  # noqa: ANN001
    pool_capacity = settings.database_pool_size + settings.database_max_overflow
    if settings.ingestion_max_workers <= pool_capacity:
        return

    logger.warning(
        "INGESTION_MAX_WORKERS (%s) exceeds configured DB pool capacity "
        "(DATABASE_POOL_SIZE + DATABASE_MAX_OVERFLOW = %s); ingestion workers "
        "may wait for database connections.",
        settings.ingestion_max_workers,
        pool_capacity,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.behind_proxy and not settings.trusted_proxy_networks:
        logger.warning(
            "BEHIND_PROXY is enabled but TRUSTED_PROXY_IPS is empty; "
            "forwarded client IP headers will be ignored."
        )

    _warn_if_ingestion_pool_capacity_low(settings)

    assert_schema_at_head(settings.database_url)
    with ExitStack() as cleanup:
        research_handler = configure_research_timing(settings)
        if research_handler is not None:
            cleanup.callback(research_handler.close)
            cleanup.callback(logging.getLogger().removeHandler, research_handler)

        session_factory = get_session_factory()
        with session_factory() as db:
            runtime_sync = sync_runtime_state(db, settings, ticker_loader=load_tickers_from_csv)
            logger.info("Ticker load stats: %s", runtime_sync["ticker_sync"])
            stale_runs_fixed = runtime_sync["stale_runs_fixed"]
            if stale_runs_fixed:
                logger.warning("Marked %s stale ingestion runs as failed at startup", stale_runs_fixed)

        page_access_store = PageAccessStore.for_database(settings.database_url)
        cleanup.callback(page_access_store.close)
        configure_page_access(page_access_store)
        cleanup.callback(configure_page_access, None)

        # Start consumers first so they remain available while producers drain.
        # Register each stop before start, which may launch a thread and then fail.
        broadcaster = SSEBroadcaster(settings.database_url)
        cleanup.callback(broadcaster.stop)
        broadcaster.start()
        push_dispatcher = PushAlertDispatcher(settings.database_url, settings, session_factory)
        cleanup.callback(push_dispatcher.stop)
        push_dispatcher.start()

        if settings.scheduler_enabled and settings.postcommit_enrichment_enabled:
            enrichment_worker = ArticleEnrichmentWorker(
                session_factory, timeout_seconds=settings.ingest_source_page_timeout_seconds,
                archive_directory=settings.article_body_archive_dir,
            )
            cleanup.callback(enrichment_worker.stop)
            enrichment_worker.start()

        scheduler = IngestionScheduler(settings, session_factory)
        set_scheduler(scheduler)
        cleanup.callback(set_scheduler, None)
        cleanup.callback(scheduler.shutdown)
        scheduler.start()
        financialcontent_worker = FinancialContentWorker(settings, session_factory)
        cleanup.callback(financialcontent_worker.stop)
        financialcontent_worker.start()
        free_discovery = FreeDiscoveryWorker(settings)
        cleanup.callback(free_discovery.stop)
        free_discovery.start()

        for name, resource in (
            ("financialcontent_worker", financialcontent_worker),
            ("free_discovery_worker", free_discovery),
            ("sse_broadcaster", broadcaster),
            ("sse_connection_limiter", SSEConnectionLimiter(settings.sse_max_connections_per_ip)),
            ("push_alert_dispatcher", push_dispatcher),
        ):
            setattr(app.state, name, resource)
            cleanup.callback(delattr, app.state, name)

        yield


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
        # A request that does not match a route has no stable path template.
        # Never use the raw URL here: arbitrary 404 paths would otherwise create
        # an unbounded number of persistent Prometheus label combinations.
        fastapi_scope = request.scope.get("fastapi")
        effective_route = (
            fastapi_scope.get("effective_route_context")
            if isinstance(fastapi_scope, dict)
            else None
        )
        path_template = (
            getattr(effective_route, "path", None)
            or getattr(route, "path", None)
            or "__unmatched__"
        )
        observe_http_request(request.method, path_template, status_code, duration_seconds)


@app.get("/health")
def health():
    try:
        ok = db_health_check()
    except Exception:
        logger.exception("Database health check failed")
        ok = False
    payload = {
        "status": "ok" if ok else "degraded",
        "time": datetime.now(timezone.utc).isoformat(),
        "timing": research_timing_status(),
    }
    fc = getattr(app.state, "financialcontent_worker", None)
    if fc is not None:
        payload["financialcontent"] = fc.snapshot()
    scheduler = get_scheduler()
    if scheduler is not None:
        payload["ingestion"] = scheduler.snapshot()
    discovery = getattr(app.state, "free_discovery_worker", None)
    if discovery is not None:
        payload["free_discovery"] = discovery.snapshot()
    return JSONResponse(
        content=payload,
        status_code=status.HTTP_200_OK if ok else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@app.get("/metrics", include_in_schema=False)
def metrics():
    payload, content_type = render_metrics()
    return Response(content=payload, media_type=content_type)


app.include_router(news_router, prefix=settings.api_prefix)
app.include_router(admin_router, prefix=settings.api_prefix)
app.include_router(push_router, prefix=settings.api_prefix)
app.include_router(sse_router, prefix=settings.api_prefix)
