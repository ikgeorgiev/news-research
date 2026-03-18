import sys
from types import SimpleNamespace

import pytest

from app.article_ingest import _clamp_label
from app.sources import PAGE_FETCH_CONFIGS
from app.constants import MIN_PERSIST_CONFIDENCE, NO_KEYWORDS_CONFIDENCE
from app.ticker_extraction import (
    _extract_article_body,
    _build_symbol_keywords,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _extract_table_cell_symbols_from_html,
    _fetch_source_page_html,
    _is_businesswire_article_url,
    _is_source_article_url,
    _should_persist_entry,
    _source_page_cache,
)
@pytest.fixture()
def clean_page_cache():
    _source_page_cache.clear()
    yield
    _source_page_cache.clear()

def test_extract_tickers_from_context_and_text():
    known = {"GOF", "UTF", "AAPL"}
    title = "Fund update for (GOF) and NYSE: UTF"
    summary = "The manager also mentioned AAPL in a comparison section."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF,UTF"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert "GOF" in hits
    assert "UTF" in hits
    assert "AAPL" in hits
    assert hits["GOF"][1] >= 0.75

def test_multi_symbol_context_does_not_mass_assign():
    known = {"GOF", "UTF", "PDI"}
    title = "GOF monthly update"
    summary = "Distribution policy unchanged."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF,UTF,PDI"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert "GOF" in hits
    assert "UTF" not in hits
    assert "PDI" not in hits

def test_single_symbol_context_is_allowed():
    known = {"GOF", "UTF"}
    title = "Monthly portfolio commentary"
    summary = "No explicit symbol in text."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert hits.get("GOF") == ("context", 0.93)

def test_clamp_label_caps_to_column_width():
    long_text = "X" * 240
    clipped = _clamp_label(long_text)
    assert len(clipped) == 120

def test_should_persist_entry_keeps_businesswire_without_tickers():
    assert _should_persist_entry("businesswire", {}) is True

def test_should_persist_entry_drops_non_bw_without_tickers():
    assert _should_persist_entry("prnewswire", {}) is False
    assert _should_persist_entry("globenewswire", {}) is False
    assert _should_persist_entry("yahoo", {}) is False

def test_should_persist_entry_rejects_token_only_hits():
    hits = {"UTF": ("token", 0.62)}
    assert _should_persist_entry("prnewswire", hits) is False

def test_should_persist_entry_keeps_validated_token_hits():
    hits = {"CGO": ("validated_token", 0.68)}
    assert _should_persist_entry("prnewswire", hits) is True

def test_should_persist_entry_keeps_paren_hits():
    hits = {"CGO": ("paren", 0.75)}
    assert _should_persist_entry("prnewswire", hits) is True

def test_should_persist_entry_rejects_subthreshold_paren_hits():
    hits = {"CGO": ("paren", 0.62)}
    assert _should_persist_entry("prnewswire", hits) is False

def test_should_persist_entry_keeps_exchange_hits():
    hits = {"CGO": ("exchange", 0.88)}
    assert _should_persist_entry("globenewswire", hits) is True

def test_extract_entry_tickers_can_disable_token_scan():
    known = {"DSM"}
    hits = _extract_entry_tickers(
        "BNY update",
        "DSM distribution declared",
        "https://example.com/story",
        "",
        known,
        include_token=False,
    )
    assert "DSM" not in hits

def test_extract_entry_tickers_ignores_ambiguous_fund_token():
    known = {"FUND", "PDT"}
    hits = _extract_entry_tickers(
        "JOHN HANCOCK PREMIUM DIVIDEND FUND NOTICE TO SHAREHOLDERS",
        "",
        "https://finance.yahoo.com/news/john-hancock-premium-dividend-fund-214400046.html?.tsrc=rss",
        "",
        known,
    )
    assert "FUND" not in hits

def test_extract_entry_tickers_keeps_explicit_fund_symbol():
    known = {"FUND"}
    hits = _extract_entry_tickers(
        "Acquirer announces NYSE: FUND merger update",
        "",
        "https://example.com/story",
        "",
        known,
    )
    assert hits.get("FUND") == ("exchange", 0.88)

def test_extract_table_cell_symbols_from_html():
    known = {"DSM", "LEO", "GOF", "FUND"}
    html = """
    <table>
      <tr><th>Fund</th><th>Ticker</th></tr>
      <tr><td>BNY Mellon Strategic Municipal Bond Fund</td><td>DSM</td></tr>
      <tr><td>BNY Mellon Strategic Municipals</td><td>LEO</td></tr>
    </table>
    """

    hits = _extract_table_cell_symbols_from_html(html, known)
    assert hits == {"DSM", "LEO"}
    assert "FUND" not in hits

def test_is_businesswire_article_url_allows_expected_hosts():
    assert _is_businesswire_article_url("https://www.businesswire.com/news/home/abc")
    assert _is_businesswire_article_url("http://feed.businesswire.com/rss/home")
    assert _is_businesswire_article_url("https://businesswire.com/news/home/abc?x=1")
    assert not _is_businesswire_article_url("https://example.com/news/home/abc")
    assert not _is_businesswire_article_url("file:///tmp/businesswire.html")

def test_is_source_article_url_prnewswire():
    assert _is_source_article_url(
        "https://www.prnewswire.com/news-releases/abc-123.html", "prnewswire.com"
    )
    assert _is_source_article_url(
        "https://prnewswire.com/news-releases/abc-123.html", "prnewswire.com"
    )
    assert not _is_source_article_url(
        "https://evil.com/prnewswire.com", "prnewswire.com"
    )
    assert not _is_source_article_url("file:///tmp/prnewswire.html", "prnewswire.com")

def test_is_source_article_url_globenewswire():
    assert _is_source_article_url(
        "https://www.globenewswire.com/news-release/2026/01/abc", "globenewswire.com"
    )
    assert _is_source_article_url(
        "https://globenewswire.com/news-release/abc", "globenewswire.com"
    )
    assert not _is_source_article_url(
        "https://example.com/globenewswire", "globenewswire.com"
    )
