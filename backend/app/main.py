from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import db_health_check, get_session_factory, init_db
from app.ingestion import sync_runtime_state
from app.monitoring import observe_http_request, render_metrics
from app.routes.admin import admin_router
from app.routes.news import news_router
from app.routes.push import push_router
from app.scheduler import IngestionScheduler, set_scheduler
from app.ticker_loader import load_tickers_from_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
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
    set_scheduler(scheduler)
    scheduler.start()

    yield

    scheduler.shutdown()
    set_scheduler(None)


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


app.include_router(news_router, prefix=settings.api_prefix)
app.include_router(admin_router, prefix=settings.api_prefix)
app.include_router(push_router, prefix=settings.api_prefix)
