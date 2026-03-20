from __future__ import annotations

import threading

import httpx

_client_lock = threading.Lock()
_http_client: httpx.Client | None = None


def get_http_client() -> httpx.Client:
    global _http_client
    if _http_client is None:
        with _client_lock:
            if _http_client is None:
                _http_client = httpx.Client(
                    follow_redirects=True,
                    http2=True,
                )
    return _http_client


def close_http_client() -> None:
    global _http_client
    with _client_lock:
        if _http_client is None:
            return
        _http_client.close()
        _http_client = None
