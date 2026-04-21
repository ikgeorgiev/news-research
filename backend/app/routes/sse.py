from __future__ import annotations

import asyncio
import json
from ipaddress import IPv4Network, IPv6Network, ip_address
import time
from typing import Sequence

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from app.config import get_settings
from app.sse import SSEBroadcaster, SSEConnectionLimiter

sse_router = APIRouter(tags=["sse"])


def _client_ip(
    request: Request,
    *,
    trust_proxy: bool = False,
    trusted_proxy_networks: Sequence[IPv4Network | IPv6Network] = (),
) -> str:
    client = request.client
    if client is None or not client.host:
        return "unknown"
    client_host = client.host.strip()

    try:
        ip_address(client_host)
    except ValueError:
        return client_host

    if not trust_proxy or not trusted_proxy_networks:
        return client_host

    if not _is_trusted_proxy(client_host, trusted_proxy_networks):
        return client_host

    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return _client_ip_from_forwarded_chain(
            client_host,
            forwarded_for,
            trusted_proxy_networks=trusted_proxy_networks,
        )

    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        real_ip = real_ip.strip()
        try:
            ip_address(real_ip)
        except ValueError:
            return client_host
        return real_ip

    return client_host


def _is_trusted_proxy(
    host: str, trusted_proxy_networks: Sequence[IPv4Network | IPv6Network]
) -> bool:
    try:
        address = ip_address(host)
    except ValueError:
        return False

    return any(address in network for network in trusted_proxy_networks)


def _client_ip_from_forwarded_chain(
    remote_host: str,
    forwarded_for: str,
    *,
    trusted_proxy_networks: Sequence[IPv4Network | IPv6Network],
) -> str:
    candidate = remote_host

    for forwarded_host in reversed(
        [value.strip() for value in forwarded_for.split(",") if value.strip()]
    ):
        if not _is_trusted_proxy(candidate, trusted_proxy_networks):
            return candidate
        try:
            ip_address(forwarded_host)
        except ValueError:
            return candidate
        candidate = forwarded_host

    return candidate


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
    settings = get_settings()
    client_ip = _client_ip(
        request,
        trust_proxy=settings.behind_proxy,
        trusted_proxy_networks=settings.trusted_proxy_networks,
    )

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
