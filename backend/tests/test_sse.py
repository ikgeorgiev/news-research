from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes.sse import _client_ip, _stream_news_events, sse_router
from app.sse import SSEBroadcaster, SSEConnectionLimiter


def test_sse_broadcaster_fans_out_to_async_subscribers():
    async def run_check():
        broadcaster = SSEBroadcaster("postgresql://cef:cef@localhost/test")

        with broadcaster.subscribe() as listener_queue:
            broadcaster._fan_out({"count": 3})
            message = await asyncio.wait_for(listener_queue.get(), timeout=1.0)

        assert message == {"count": 3}

    asyncio.run(run_check())


def test_sse_stream_recovers_when_broadcaster_becomes_available():
    class FakeRequest:
        def __init__(self):
            self.disconnected = False

        async def is_disconnected(self) -> bool:
            return self.disconnected

    class FakeBroadcaster:
        def __init__(self):
            self.is_available = False
            self._listeners: list[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, int]]]] = []

        @contextmanager
        def subscribe(self):
            listener_queue: asyncio.Queue[dict[str, int]] = asyncio.Queue(maxsize=8)
            event_loop = asyncio.get_running_loop()
            self._listeners.append((event_loop, listener_queue))
            try:
                yield listener_queue
            finally:
                self._listeners.remove((event_loop, listener_queue))

        def publish(self, payload: dict[str, int]) -> None:
            for event_loop, listener_queue in list(self._listeners):
                event_loop.call_soon_threadsafe(listener_queue.put_nowait, payload)

    async def run_check():
        request = FakeRequest()
        broadcaster = FakeBroadcaster()
        stream = _stream_news_events(request, broadcaster)

        assert await asyncio.wait_for(anext(stream), timeout=1.0) == ": connected\n\n"

        async def activate_and_publish():
            await asyncio.sleep(0.2)
            broadcaster.is_available = True
            await asyncio.sleep(1.1)
            broadcaster.publish({"count": 1})

        task = asyncio.create_task(activate_and_publish())
        ready_message = await asyncio.wait_for(anext(stream), timeout=2.0)
        message = await asyncio.wait_for(anext(stream), timeout=2.0)
        request.disconnected = True
        await task
        await stream.aclose()

        assert ready_message == 'event: ready\ndata: {}\n\n'
        assert message == 'event: new-articles\ndata: {"count": 1}\n\n'

    asyncio.run(run_check())


def test_sse_stream_sends_unavailable_when_broadcaster_goes_down():
    class FakeRequest:
        def __init__(self):
            self.disconnected = False

        async def is_disconnected(self) -> bool:
            return self.disconnected

    class FakeBroadcaster:
        def __init__(self):
            self.is_available = True
            self._listeners: list[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, int]]]] = []

        @contextmanager
        def subscribe(self):
            listener_queue: asyncio.Queue[dict[str, int]] = asyncio.Queue(maxsize=8)
            event_loop = asyncio.get_running_loop()
            self._listeners.append((event_loop, listener_queue))
            try:
                yield listener_queue
            finally:
                self._listeners.remove((event_loop, listener_queue))

    async def run_check():
        request = FakeRequest()
        broadcaster = FakeBroadcaster()
        stream = _stream_news_events(request, broadcaster)

        assert await asyncio.wait_for(anext(stream), timeout=1.0) == ": connected\n\n"
        ready_message = await asyncio.wait_for(anext(stream), timeout=2.0)
        assert ready_message == "event: ready\ndata: {}\n\n"

        # Simulate broadcaster going down
        broadcaster.is_available = False

        unavailable_message = await asyncio.wait_for(anext(stream), timeout=2.0)
        request.disconnected = True
        await stream.aclose()

        assert unavailable_message == "event: unavailable\ndata: {}\n\n"

    asyncio.run(run_check())


def test_sse_connection_limiter_enforces_per_ip_cap():
    limiter = SSEConnectionLimiter(max_connections_per_ip=2)

    assert limiter.try_acquire("127.0.0.1") is True
    assert limiter.try_acquire("127.0.0.1") is True
    assert limiter.try_acquire("127.0.0.1") is False
    assert limiter.active_connections_for("127.0.0.1") == 2


def test_sse_connection_limiter_release_allows_new_connection():
    limiter = SSEConnectionLimiter(max_connections_per_ip=1)

    assert limiter.try_acquire("127.0.0.1") is True
    assert limiter.try_acquire("127.0.0.1") is False

    limiter.release("127.0.0.1")

    assert limiter.try_acquire("127.0.0.1") is True


def test_client_ip_ignores_forwarded_headers_when_trust_proxy_is_false():
    request = SimpleNamespace(
        headers={
            "x-forwarded-for": "203.0.113.10, 198.51.100.1",
            "x-real-ip": "203.0.113.20",
        },
        client=SimpleNamespace(host="127.0.0.1"),
    )

    assert _client_ip(request, trust_proxy=False) == "127.0.0.1"


def test_client_ip_uses_forwarded_for_when_trust_proxy_is_true():
    request = SimpleNamespace(
        headers={
            "x-forwarded-for": "203.0.113.10, 198.51.100.1",
            "x-real-ip": "203.0.113.20",
        },
        client=SimpleNamespace(host="127.0.0.1"),
    )

    assert _client_ip(request, trust_proxy=True) == "203.0.113.10"


def test_client_ip_falls_back_to_real_ip_when_no_forwarded_for():
    request = SimpleNamespace(
        headers={
            "x-real-ip": "203.0.113.20",
        },
        client=SimpleNamespace(host="127.0.0.1"),
    )

    assert _client_ip(request, trust_proxy=True) == "203.0.113.20"


def test_sse_route_rejects_connections_over_the_per_ip_cap():
    class FakeBroadcaster:
        is_available = False

    app = FastAPI()
    app.include_router(sse_router, prefix="/api/v1")
    app.state.sse_broadcaster = FakeBroadcaster()
    app.state.sse_connection_limiter = SSEConnectionLimiter(max_connections_per_ip=1)
    app.state.sse_connection_limiter.try_acquire("testclient")

    with TestClient(app) as client:
        response = client.get("/api/v1/events/news")

    assert response.status_code == 429
    assert response.json() == {
        "detail": "Too many concurrent event streams for this IP. Limit: 1."
    }
