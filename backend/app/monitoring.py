from __future__ import annotations

import math
import time
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests handled by the API",
    ["method", "path", "status"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

INGESTION_CYCLE_TOTAL = Counter(
    "ingestion_cycle_total",
    "Number of ingestion cycle outcomes",
    ["status"],
)
INGESTION_CYCLE_DURATION_SECONDS = Histogram(
    "ingestion_cycle_duration_seconds",
    "Ingestion cycle duration in seconds",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600),
)
INGESTION_LAST_CYCLE_UNIX_SECONDS = Gauge(
    "ingestion_last_cycle_unix_seconds",
    "UNIX timestamp of the most recent finished ingestion cycle (including skips)",
)
INGESTION_LAST_COMPLETED_CYCLE_UNIX_SECONDS = Gauge(
    "ingestion_last_completed_cycle_unix_seconds",
    "UNIX timestamp of the most recent ingestion cycle that actually ran (success or failure, not skips)",
)
INGESTION_LAST_CYCLE_DURATION_SECONDS = Gauge(
    "ingestion_last_cycle_duration_seconds",
    "Duration of the most recent successful ingestion cycle in seconds",
)
INGESTION_LAST_CYCLE_ITEMS_SEEN = Gauge(
    "ingestion_last_cycle_items_seen",
    "Items seen in the most recent successful ingestion cycle",
)
INGESTION_LAST_CYCLE_ITEMS_INSERTED = Gauge(
    "ingestion_last_cycle_items_inserted",
    "Items inserted in the most recent successful ingestion cycle",
)
INGESTION_LAST_CYCLE_FAILED_FEEDS = Gauge(
    "ingestion_last_cycle_failed_feeds",
    "Failed feeds in the most recent successful ingestion cycle",
)
INGESTION_SCHEDULER_ENABLED = Gauge(
    "ingestion_scheduler_enabled",
    "1 if the ingestion scheduler is running, 0 otherwise",
)
INGESTION_SCHEDULER_STARTED_AT = Gauge(
    "ingestion_scheduler_started_at_unix_seconds",
    "UNIX timestamp when the ingestion scheduler was started",
)

# Keep labels bounded; article URLs, IDs and arbitrary provider names belong in
# the structured traces, never in Prometheus time-series labels.
NEWS_SOURCES = ("businesswire", "prnewswire", "globenewswire", "yahoo", "other")
NEWS_DISCOVERY_KINDS = ("feed_response", "pending_enrichment")
ARTICLE_STAGES = (
    "wait_before_processing", "processing", "page_lookup", "commit",
    "receipt_to_commit", "notification_call",
)
NEWS_ARTICLES_COMMITTED = Counter(
    "news_articles_committed_total", "New article commits, including commits in a later failed feed run",
    ["source", "discovery_kind"],
)
NEWS_ARTICLE_STAGE_SECONDS = Histogram(
    "news_article_stage_seconds", "Article stage duration; page lookup is part of processing; notification is server-side only",
    ["source", "discovery_kind", "stage"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)
SOURCE_PAGE_REQUESTS = Counter(
    "source_page_requests_total", "Actual source-page HTTP attempts; cached lookups excluded",
    ["source", "status"],
)
SOURCE_PAGE_REQUEST_SECONDS = Histogram(
    "source_page_request_seconds", "Actual source-page HTTP request duration, including failed requests",
    ["source"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
for _source in NEWS_SOURCES:
    SOURCE_PAGE_REQUEST_SECONDS.labels(source=_source)
    for _kind in NEWS_DISCOVERY_KINDS:
        # Export zeroes before the first article so its first increment is visible.
        NEWS_ARTICLES_COMMITTED.labels(source=_source, discovery_kind=_kind)
        for _stage in ARTICLE_STAGES:
            NEWS_ARTICLE_STAGE_SECONDS.labels(source=_source, discovery_kind=_kind, stage=_stage)


def _news_source(source: str) -> str:
    return source if source in NEWS_SOURCES else "other"


def observe_article_commit(trace: dict[str, Any]) -> None:
    source = _news_source(trace["source"])
    kind = trace["discovery_kind"]
    if kind not in NEWS_DISCOVERY_KINDS:
        return
    NEWS_ARTICLES_COMMITTED.labels(source=source, discovery_kind=kind).inc()
    for stage in ARTICLE_STAGES:
        value = trace.get(f"{stage}_ms")
        if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
            NEWS_ARTICLE_STAGE_SECONDS.labels(source=source, discovery_kind=kind, stage=stage).observe(value / 1000)


def observe_source_page_request(source: str, status_code: int | None, duration_seconds: float) -> None:
    source = _news_source(source)
    status = str(status_code) if isinstance(status_code, int) and 100 <= status_code <= 599 else "error"
    SOURCE_PAGE_REQUESTS.labels(source=source, status=status).inc()
    SOURCE_PAGE_REQUEST_SECONDS.labels(source=source).observe(max(0, duration_seconds))

PUSH_NOTIFICATIONS_TOTAL = Counter(
    "push_notifications_sent_total",
    "Number of push notification delivery attempts by status",
    ["status"],
)
PUSH_NOTIFICATION_DURATION_SECONDS = Histogram(
    "push_notification_duration_seconds",
    "Push notification delivery latency in seconds",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
PUSH_ACTIVE_SUBSCRIPTIONS = Gauge(
    "push_active_subscriptions",
    "Number of active push subscriptions",
)


def observe_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    method_label = method.upper()
    status_label = str(status_code)
    HTTP_REQUESTS_TOTAL.labels(method=method_label, path=path, status=status_label).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method_label, path=path).observe(duration_seconds)


def record_ingestion_skip(reason: str) -> None:
    INGESTION_CYCLE_TOTAL.labels(status=f"skipped_{reason}").inc()
    INGESTION_LAST_CYCLE_UNIX_SECONDS.set(time.time())


def record_ingestion_success(result: dict[str, Any], duration_seconds: float) -> None:
    INGESTION_CYCLE_TOTAL.labels(status="success").inc()
    INGESTION_CYCLE_DURATION_SECONDS.observe(duration_seconds)
    INGESTION_LAST_CYCLE_UNIX_SECONDS.set(time.time())
    INGESTION_LAST_COMPLETED_CYCLE_UNIX_SECONDS.set(time.time())
    INGESTION_LAST_CYCLE_DURATION_SECONDS.set(duration_seconds)
    INGESTION_LAST_CYCLE_ITEMS_SEEN.set(float(int(result.get("total_items_seen", 0) or 0)))
    INGESTION_LAST_CYCLE_ITEMS_INSERTED.set(float(int(result.get("total_items_inserted", 0) or 0)))
    INGESTION_LAST_CYCLE_FAILED_FEEDS.set(float(int(result.get("failed_feeds", 0) or 0)))


def record_ingestion_failure(duration_seconds: float) -> None:
    INGESTION_CYCLE_TOTAL.labels(status="failed").inc()
    INGESTION_CYCLE_DURATION_SECONDS.observe(duration_seconds)
    INGESTION_LAST_CYCLE_UNIX_SECONDS.set(time.time())
    INGESTION_LAST_COMPLETED_CYCLE_UNIX_SECONDS.set(time.time())


def record_push_delivery(status: str) -> None:
    PUSH_NOTIFICATIONS_TOTAL.labels(status=str(status or "unknown")).inc()


def record_push_delivery_duration(duration_seconds: float) -> None:
    PUSH_NOTIFICATION_DURATION_SECONDS.observe(max(0.0, float(duration_seconds)))


def set_push_active_subscriptions(value: int) -> None:
    PUSH_ACTIVE_SUBSCRIPTIONS.set(max(0.0, float(value)))


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
