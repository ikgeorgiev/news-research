from __future__ import annotations

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
