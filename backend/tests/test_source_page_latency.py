from collections import OrderedDict
import json
import logging
import threading
from types import SimpleNamespace

import httpx
import pytest
from prometheus_client import REGISTRY

from app import sources


@pytest.mark.parametrize("outcome", [200, 403, "timeout"])
def test_page_request_timing_distinguishes_response_error_and_cache(monkeypatch, caplog, outcome):
    url = "https://www.businesswire.com/news/home/latency-test/en/example"
    attempts = []

    def get(*args, **kwargs):
        attempts.append((args, kwargs))
        if outcome == "timeout":
            raise httpx.ReadTimeout("test timeout")
        return httpx.Response(outcome, text="<html>example</html>")

    monkeypatch.setattr(sources.http_client, "get_http_client", lambda: SimpleNamespace(get=get))
    ticks = iter([10.0, 10.125])
    monkeypatch.setattr(sources.time, "perf_counter", lambda: next(ticks))
    # Migration tests reconfigure logging and can disable existing loggers.
    monkeypatch.setattr(sources.logger, "disabled", False)
    caplog.set_level(logging.INFO, logger="app.sources")
    kwargs = {
        "cache": OrderedDict(), "cache_lock": threading.Lock(), "headers": {},
        "cache_ttl_seconds": 600, "failure_cache_ttl_seconds": 60, "cache_max_items": 10,
        "now_fn": lambda: 100.0,
    }
    labels = {"source": "businesswire", "status": "error" if outcome == "timeout" else str(outcome)}
    before_requests = REGISTRY.get_sample_value("source_page_requests_total", labels) or 0
    before_seconds = REGISTRY.get_sample_value("source_page_request_seconds_sum", {"source": "businesswire"})
    first = sources._fetch_source_page_html(url, 2, sources.PAGE_FETCH_CONFIGS["businesswire"], **kwargs)
    second = sources._fetch_source_page_html(url, 2, sources.PAGE_FETCH_CONFIGS["businesswire"], **kwargs)
    assert second == first
    assert first == ("<html>example</html>" if outcome == 200 else None)
    assert len(attempts) == 1
    assert REGISTRY.get_sample_value("source_page_requests_total", labels) == before_requests + 1
    assert REGISTRY.get_sample_value("source_page_request_seconds_sum", {"source": "businesswire"}) == pytest.approx(before_seconds + 0.125)
    records = [r.getMessage() for r in caplog.records if r.getMessage().startswith("source_page_latency ")]
    assert len(records) == 1, "Cache hits must not appear as new network requests"
    sample = json.loads(records[0].split(" ", 1)[1])
    assert sample["request_ms"] == 125.0
    assert sample["status_code"] == (None if outcome == "timeout" else outcome)
    assert sample["request_error"] == ("ReadTimeout" if outcome == "timeout" else None)
    assert sample["received_nonempty_html"] is (outcome == 200)
    assert sample["source"] == "businesswire"
    assert sample["page_url"] == url
