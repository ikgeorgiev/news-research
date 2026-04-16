from __future__ import annotations

import threading
import time
from datetime import timezone
from types import SimpleNamespace

import httpx
from feedparser import FeedParserDict
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
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
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
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
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
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
    )

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=1,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert response.status_code == 304


def test_fetch_feed_with_retries_enforces_total_deadline(monkeypatch):
    attempts = {"count": 0}
    retired_clients: list[object] = []
    close_calls = {"count": 0}
    release_get = threading.Event()

    def close_client() -> None:
        close_calls["count"] += 1

    fake_client = SimpleNamespace(get=None, close=close_client)

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        release_get.wait(timeout=1.0)
        return FakeRssResponse()

    fake_client.get = fake_get

    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: fake_client,
    )
    monkeypatch.setattr(
        "app.http_client.retire_feed_poll_client",
        lambda client=None: retired_clients.append(client) or client,
    )

    try:
        _fetch_feed_with_retries(
            feed_url="https://example.com/feed.xml",
            timeout_seconds=0.01,
            max_attempts=1,
            backoff_seconds=0.01,
            backoff_jitter_seconds=0.0,
        )
    except httpx.ReadTimeout:
        pass
    else:  # pragma: no cover - explicit regression guard
        raise AssertionError("expected the total request deadline to be enforced")

    assert attempts["count"] == 1
    assert retired_clients == [fake_client]
    assert close_calls["count"] == 0

    release_get.set()
    time.sleep(0.02)

    assert close_calls["count"] == 1


def test_get_feed_poll_client_retires_dead_worker_clients(monkeypatch):
    import app.http_client as http_client

    created_clients: list[SimpleNamespace] = []

    def make_client():
        client = SimpleNamespace(closed=False)

        def close() -> None:
            client.closed = True

        client.close = close
        created_clients.append(client)
        return client

    monkeypatch.setattr("app.http_client.create_feed_poll_client", make_client)
    http_client.close_http_client()

    def worker() -> None:
        http_client.get_feed_poll_client()

    thread = threading.Thread(target=worker, name="feed-poll-test-worker")
    thread.start()
    thread.join()

    current_client = http_client.get_feed_poll_client()

    assert len(created_clients) == 2
    assert created_clients[0].closed is True
    assert created_clients[1] is current_client
    assert created_clients[1].closed is False

    http_client.close_http_client()


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
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
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
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=failing_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
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


def test_ingest_feed_uses_feed_poll_timeout_for_feed_fetch(db_session, monkeypatch):
    # Regression: request_timeout_seconds (body-fetch budget) must not be applied
    # to feed polling — a slow article page shouldn't force the short feed-poll
    # value onto body extraction, and vice versa.
    db = db_session
    source = seed_source(db)
    observed_timeouts: list[int | float] = []
    request = httpx.Request("GET", "https://example.com/split-timeout.xml")

    def fake_get(*_args, **kwargs):
        observed_timeouts.append(kwargs.get("timeout"))
        return httpx.Response(
            200,
            request=request,
            headers=httpx.Headers({}),
            content=b"<rss />",
        )

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[]),
    )

    result = call_ingest(
        db,
        source,
        "https://example.com/split-timeout.xml",
        timeout_seconds=15,
        feed_poll_timeout_seconds=4,
    )

    assert result["status"] == "success"
    assert observed_timeouts == [4]


def test_ingest_feed_uses_short_ingestion_timeout_for_page_fallback(
    db_session, monkeypatch
):
    db = db_session
    source = seed_source(db)
    request = httpx.Request("GET", "https://example.com/fallback-timeout.xml")
    observed_fallback_timeouts: list[int | float] = []

    def fake_get(*_args, **_kwargs):
        return httpx.Response(
            200,
            request=request,
            headers=httpx.Headers({}),
            content=b"<rss />",
        )

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=fake_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Business Wire"},
            entries=[
                FeedParserDict(
                    {
                        "title": "Fallback timeout article",
                        "link": "https://www.businesswire.com/news/home/20260415000001/en",
                    }
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "app.article_ingest._extract_entry_tickers",
        lambda *_args, **_kwargs: {},
    )

    def fake_fallback(*_args, **_kwargs):
        observed_fallback_timeouts.append(_args[5])
        return {}

    monkeypatch.setattr(
        "app.article_ingest._extract_source_fallback_tickers",
        fake_fallback,
    )

    result = call_ingest(
        db,
        source,
        "https://example.com/fallback-timeout.xml",
        timeout_seconds=15,
        ingest_source_page_timeout_seconds=4,
    )

    assert result["status"] == "success"
    assert observed_fallback_timeouts == [4]


def test_ingest_feed_honors_retry_after_on_429_with_single_attempt(db_session, monkeypatch):
    db = db_session
    source = seed_source(db)
    request = httpx.Request("GET", "https://example.com/rate-limited.xml")

    def rate_limited_get(*_args, **_kwargs):
        return httpx.Response(
            429,
            request=request,
            headers=httpx.Headers({"Retry-After": "120"}),
            content=b"",
        )

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=rate_limited_get),
    )
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client",
        lambda: SimpleNamespace(get=rate_limited_get, close=lambda: None),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client",
        lambda _client=None: None,
    )

    result = call_ingest(
        db,
        source,
        "https://example.com/rate-limited.xml",
        failure_backoff_base_seconds=10.0,
        failure_backoff_max_seconds=600.0,
    )

    state = db.scalar(
        select(FeedPollState).where(
            FeedPollState.feed_url == "https://example.com/rate-limited.xml"
        )
    )

    assert result["status"] == "failed"
    assert state is not None and state.backoff_until is not None
    last_failure = state.last_failure_at
    assert last_failure is not None
    last_failure_utc = (
        last_failure if last_failure.tzinfo else last_failure.replace(tzinfo=timezone.utc)
    )
    backoff_utc = (
        state.backoff_until
        if state.backoff_until.tzinfo
        else state.backoff_until.replace(tzinfo=timezone.utc)
    )
    honored_delay = (backoff_utc - last_failure_utc).total_seconds()
    # Server hinted Retry-After: 120 must dominate the 10s exponential base.
    assert honored_delay >= 120.0
    assert honored_delay <= 600.0
