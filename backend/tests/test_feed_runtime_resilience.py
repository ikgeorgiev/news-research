from __future__ import annotations

from datetime import timezone
from types import SimpleNamespace

import httpx
from sqlalchemy import select

from app.feed_runtime import (
    _fetch_feed_with_retries,
    _get_feed_conditional_headers,
    _update_feed_http_cache,
)
from app.models import FeedPollState
from tests.helpers import FakeRssResponse, call_ingest, seed_source


def test_fetch_feed_with_retries_succeeds_after_transient_failures(monkeypatch):
    attempts = {"count": 0}

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise httpx.TimeoutException("temporary timeout")
        return FakeRssResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr("app.feed_runtime.time.sleep", lambda _seconds: None)

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=3,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert attempts["count"] == 3
    assert response.content == b"<rss />"


def test_fetch_feed_with_retries_honors_retry_after_for_429(monkeypatch):
    attempts = {"count": 0}
    slept: list[float] = []

    class RateLimitedResponse:
        status_code = 429
        headers = httpx.Headers({"Retry-After": "2"})
        content = b""
        request = httpx.Request("GET", "https://example.com/feed.xml")

        def raise_for_status(self) -> None:
            raise httpx.HTTPStatusError(
                "429",
                request=self.request,
                response=httpx.Response(429, headers=self.headers, request=self.request),
            )

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return RateLimitedResponse()
        return FakeRssResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.feed_runtime.time.sleep", lambda seconds: slept.append(float(seconds))
    )

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=2,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert attempts["count"] == 2
    assert slept == [2.0]
    assert response.content == b"<rss />"


def test_fetch_feed_with_retries_returns_real_httpx_304_response(monkeypatch):
    request = httpx.Request("GET", "https://example.com/feed.xml")

    def fake_get(*_args, **_kwargs):
        return httpx.Response(304, request=request, headers=httpx.Headers({}))

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=1,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert response.status_code == 304


def test_update_feed_http_cache_reads_requests_case_insensitive_headers(db_session):
    db = db_session
    feed_url = "https://example.com/feed-with-conditional-cache.xml"
    state = FeedPollState(feed_url=feed_url)
    db.add(state)
    db.commit()

    class FakeResponse:
        headers = httpx.Headers(
            {
                "etag": '"abc123"',
                "last-modified": "Tue, 03 Mar 2026 10:00:00 GMT",
            }
        )

    _update_feed_http_cache(state, FakeResponse())  # type: ignore[arg-type]
    db.commit()
    headers = _get_feed_conditional_headers(state)

    assert headers["If-None-Match"] == '"abc123"'
    assert headers["If-Modified-Since"] == "Tue, 03 Mar 2026 10:00:00 GMT"


def test_ingest_feed_persists_conditional_headers_across_runs(db_session, monkeypatch):
    db = db_session
    source = seed_source(db)
    sent_headers: list[dict[str, str]] = []
    attempts = {"count": 0}
    request = httpx.Request("GET", "https://example.com/conditional.xml")

    def fake_get(*_args, **kwargs):
        attempts["count"] += 1
        sent_headers.append(dict(kwargs.get("headers") or {}))
        if attempts["count"] == 1:
            return httpx.Response(
                200,
                request=request,
                headers=httpx.Headers(
                    {
                        "etag": '"persisted-etag"',
                        "last-modified": "Tue, 03 Mar 2026 10:00:00 GMT",
                    }
                ),
                content=b"<rss />",
            )
        return httpx.Response(304, request=request, headers=httpx.Headers({}))

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[]),
    )

    first = call_ingest(db, source, "https://example.com/conditional.xml")
    second = call_ingest(db, source, "https://example.com/conditional.xml")

    state = db.scalar(
        select(FeedPollState).where(
            FeedPollState.feed_url == "https://example.com/conditional.xml"
        )
    )

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(sent_headers) == 2
    assert sent_headers[1].get("If-None-Match") == '"persisted-etag"'
    assert sent_headers[1].get("If-Modified-Since") == "Tue, 03 Mar 2026 10:00:00 GMT"
    assert state is not None and state.etag == '"persisted-etag"'


def test_ingest_feed_skips_when_feed_is_in_failure_backoff(db_session, monkeypatch):
    db = db_session
    source = seed_source(db)
    calls = {"count": 0}

    def failing_get(*_args, **_kwargs):
        calls["count"] += 1
        raise httpx.TimeoutException("upstream unavailable")

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=failing_get),
    )

    first = call_ingest(
        db, source, "https://example.com/backoff.xml",
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )
    second = call_ingest(
        db, source, "https://example.com/backoff.xml",
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )

    state = db.scalar(
        select(FeedPollState).where(
            FeedPollState.feed_url == "https://example.com/backoff.xml"
        )
    )

    assert first["status"] == "failed"
    assert second["status"] == "skipped_backoff"
    assert calls["count"] == 1
    assert state is not None
    assert state.failure_count == 1
    assert state.backoff_until is not None
