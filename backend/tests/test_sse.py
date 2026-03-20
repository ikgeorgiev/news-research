from __future__ import annotations

import asyncio
from contextlib import contextmanager

from app.routes.sse import _stream_news_events
from app.sse import SSEBroadcaster


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
