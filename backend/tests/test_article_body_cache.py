import hashlib
import sys

from app.body_cache import ArticleBodyCache
from app import ticker_extraction
from app.sources import PAGE_FETCH_CONFIGS


def test_same_html_reuses_body_but_changed_html_is_reparsed():
    cache = ArticleBodyCache()
    calls = []
    def extract(html):
        calls.append(html)
        return html + " body"
    assert cache.extract("first", extract) == "first body"
    assert cache.extract("first", extract) == "first body"
    assert cache.extract("edited", extract) == "edited body"
    assert calls == ["first", "edited"]


def test_failed_body_extraction_is_retried():
    cache = ArticleBodyCache()
    attempts = iter([None, "recovered body"])
    calls = []
    def extract(html):
        calls.append(html)
        return next(attempts)
    assert cache.extract("page", extract) is None
    assert cache.extract("page", extract) == "recovered body"
    assert cache.extract("page", extract) == "recovered body"
    assert len(calls) == 2


def test_cache_evicts_least_recent_result_and_honors_byte_budget():
    body = "result"
    one_entry = sys.getsizeof(hashlib.sha256(b"a").digest()) + sys.getsizeof(body)
    cache = ArticleBodyCache(max_items=10, max_bytes=one_entry * 2)
    calls = []
    def extract(html):
        calls.append(html)
        return body
    for html in ("a", "b", "a", "c", "a", "b"):
        cache.extract(html, extract)
    assert calls == ["a", "b", "c", "b"]
    cache.clear()
    cache.extract("a", extract)
    assert calls[-1] == "a" and len(calls) == 5


def test_oversized_result_is_returned_without_caching():
    cache = ArticleBodyCache(max_bytes=1)
    calls = []
    def extract(html):
        calls.append(html)
        return "body"
    assert cache.extract("page", extract) == "body"
    assert cache.extract("page", extract) == "body"
    assert len(calls) == 2


def test_cached_body_revalidates_current_ticker_universe_and_keywords(monkeypatch):
    html = "<article>stable content for changing validation context</article>"
    calls = []
    def extract(_html):
        calls.append(_html)
        return "Cohen Steers Infrastructure Fund (UTF) declares a distribution to shareholders."
    monkeypatch.setattr(ticker_extraction, "_article_body_cache", ArticleBodyCache(), raising=False)
    monkeypatch.setattr(ticker_extraction, "extract_article_body", extract)
    monkeypatch.setattr(ticker_extraction, "_fetch_source_page_html", lambda *_: html)
    args = ("Distribution update", "", "https://www.prnewswire.com/news-releases/example.html", "")
    def run(known, keywords):
        return ticker_extraction._extract_source_fallback_tickers(
            *args, known, 4, PAGE_FETCH_CONFIGS["prnewswire"], symbol_keywords=keywords,
        )
    assert run(set(), {}) == {}
    assert "UTF" in run({"UTF"}, {"UTF": frozenset({"cohen", "steers"})})
    assert "UTF" not in run({"UTF"}, {"UTF": frozenset({"unrelated", "sponsor"})})
    assert len(calls) == 1
