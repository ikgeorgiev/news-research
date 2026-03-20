from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from app.config import get_settings
from app.sse import SSEBroadcaster, SSEConnectionLimiter

sse_router = APIRouter(tags=["sse"])


def _client_ip(request: Request, *, trust_proxy: bool = False) -> str:
    if trust_proxy:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
    client = request.client
    if client is None or not client.host:
        return "unknown"
    return client.host


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
    connection_limiter: SSEConnectionLimiter = request.app.state.sse_connection_limiter
    client_ip = _client_ip(request, trust_proxy=get_settings().behind_proxy)

    if not connection_limiter.try_acquire(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                "Too many concurrent event streams for this IP. "
                f"Limit: {connection_limiter.max_connections_per_ip}."
            ),
        )

    async def _guarded_stream():
        try:
            async for chunk in _stream_news_events(request, broadcaster):
                yield chunk
        finally:
            connection_limiter.release(client_ip)

    return StreamingResponse(
        _guarded_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
