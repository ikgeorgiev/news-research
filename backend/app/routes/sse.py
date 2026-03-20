from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.sse import SSEBroadcaster

sse_router = APIRouter(tags=["sse"])


async def _stream_news_events(request: Request, broadcaster: SSEBroadcaster):
    yield ": connected\n\n"
    next_keepalive_at = time.monotonic() + 30.0

    while True:
        if await request.is_disconnected():
            return

        if not broadcaster.is_available:
            now = time.monotonic()
            if now >= next_keepalive_at:
                yield ": keepalive\n\n"
                next_keepalive_at = now + 30.0
            await asyncio.sleep(1)
            continue

        with broadcaster.subscribe() as listener_queue:
            yield "event: ready\ndata: {}\n\n"
            next_keepalive_at = time.monotonic() + 30.0
            while True:
                if await request.is_disconnected():
                    return
                try:
                    message = await asyncio.wait_for(listener_queue.get(), timeout=1.0)
                    yield f"event: new-articles\ndata: {json.dumps(message)}\n\n"
                    next_keepalive_at = time.monotonic() + 30.0
                except asyncio.TimeoutError:
                    if not broadcaster.is_available:
                        yield "event: unavailable\ndata: {}\n\n"
                        break
                    now = time.monotonic()
                    if now >= next_keepalive_at:
                        yield ": keepalive\n\n"
                        next_keepalive_at = now + 30.0


@sse_router.get("/events/news")
async def news_event_stream(request: Request):
    broadcaster: SSEBroadcaster = request.app.state.sse_broadcaster

    return StreamingResponse(
        _stream_news_events(request, broadcaster),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
