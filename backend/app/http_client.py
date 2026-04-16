from __future__ import annotations

import threading

import httpx

_client_lock = threading.Lock()
_http_client: httpx.Client | None = None
_feed_poll_client_lock = threading.Lock()
_feed_poll_clients: dict[threading.Thread, httpx.Client] = {}


def _create_client(*, http2: bool) -> httpx.Client:
    return httpx.Client(
        follow_redirects=True,
        http2=http2,
    )


def create_http_client() -> httpx.Client:
    # Source-page fallback fetches are mostly single-request HTML loads rather
    # than multiplexed feed polling. Business Wire frequently resets HTTP/2
    # streams for these article-page requests, which leaves otherwise mappable
    # stories stuck as generic/unmapped. Use HTTP/1.1 for the shared page-fetch
    # client while keeping feed polling on HTTP/2.
    return _create_client(http2=False)


def create_feed_poll_client() -> httpx.Client:
    return _create_client(http2=True)


def get_http_client() -> httpx.Client:
    global _http_client
    if _http_client is None:
        with _client_lock:
            if _http_client is None:
                _http_client = create_http_client()
    return _http_client


def get_feed_poll_client() -> httpx.Client:
    thread = threading.current_thread()
    with _feed_poll_client_lock:
        _cleanup_dead_feed_poll_clients_locked(exclude_thread=thread)
        client = _feed_poll_clients.get(thread)
        if client is None:
            client = create_feed_poll_client()
            _feed_poll_clients[thread] = client
        return client


def retire_feed_poll_client(
    expected_client: httpx.Client | None = None,
) -> httpx.Client | None:
    with _feed_poll_client_lock:
        thread = threading.current_thread()
        client = _feed_poll_clients.get(thread)
        if client is None:
            return None
        if expected_client is not None and client is not expected_client:
            return None
        _feed_poll_clients.pop(thread, None)
        return client


def reset_feed_poll_client(expected_client: httpx.Client | None = None) -> None:
    client = retire_feed_poll_client(expected_client)
    if client is not None:
        client.close()


def _cleanup_dead_feed_poll_clients_locked(
    *, exclude_thread: threading.Thread | None = None
) -> None:
    stale_threads = [
        thread
        for thread in list(_feed_poll_clients.keys())
        if thread is not exclude_thread and not thread.is_alive()
    ]
    for thread in stale_threads:
        client = _feed_poll_clients.pop(thread, None)
        if client is not None:
            client.close()


def close_http_client() -> None:
    global _http_client
    with _client_lock:
        if _http_client is None:
            pass
        else:
            _http_client.close()
            _http_client = None
    with _feed_poll_client_lock:
        clients = list(_feed_poll_clients.values())
        _feed_poll_clients.clear()
    for client in clients:
        client.close()
