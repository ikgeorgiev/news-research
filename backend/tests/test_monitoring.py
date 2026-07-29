from fastapi.testclient import TestClient

import app.main as app_main


def test_unmatched_requests_use_one_bounded_metrics_path(monkeypatch):
    observed: list[tuple[str, str, int]] = []

    monkeypatch.setattr(
        app_main,
        "observe_http_request",
        lambda method, path, status_code, _duration: observed.append(
            (method, path, status_code)
        ),
    )

    client = TestClient(app_main.app)
    assert client.get("/missing/first").status_code == 404
    assert client.get("/missing/second").status_code == 404

    assert observed == [
        ("GET", "__unmatched__", 404),
        ("GET", "__unmatched__", 404),
    ]


def test_included_routes_keep_their_full_metrics_path(monkeypatch):
    observed: list[tuple[str, str, int]] = []

    monkeypatch.setattr(
        app_main,
        "observe_http_request",
        lambda method, path, status_code, _duration: observed.append(
            (method, path, status_code)
        ),
    )

    response = TestClient(app_main.app).get("/api/v1/push/vapid-key")

    assert response.status_code == 200
    assert observed == [("GET", "/api/v1/push/vapid-key", 200)]
