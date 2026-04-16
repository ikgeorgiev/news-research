from __future__ import annotations

import app.http_client as http_client


def test_create_http_client_disables_http2_for_source_page_fetches(monkeypatch):
    captured: dict[str, object] = {}

    def fake_client(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(http_client.httpx, "Client", fake_client)

    client = http_client.create_http_client()

    assert client is not None
    assert captured == {
        "follow_redirects": True,
        "http2": False,
    }


def test_create_feed_poll_client_keeps_http2_enabled(monkeypatch):
    captured: dict[str, object] = {}

    def fake_client(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(http_client.httpx, "Client", fake_client)

    client = http_client.create_feed_poll_client()

    assert client is not None
    assert captured == {
        "follow_redirects": True,
        "http2": True,
    }
