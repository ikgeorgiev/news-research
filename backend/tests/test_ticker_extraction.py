from app.ingestion import (
    _businesswire_page_cache,
    _fetch_businesswire_page_html,
    _clamp_label,
    _extract_businesswire_fallback_tickers,
    _extract_entry_tickers,
    _extract_table_cell_symbols_from_html,
    _is_businesswire_article_url,
    _should_persist_entry,
)


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


def test_should_persist_entry_keeps_non_bw_with_tickers():
    hits = {"UTF": ("token", 0.62)}
    assert _should_persist_entry("prnewswire", hits) is True


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


def test_businesswire_fallback_extracts_table_symbols(monkeypatch):
    def fake_fetch(_url: str, _timeout: int) -> str:
        return """
        <html><body>
          <h1>BNY Mellon Municipal Bond Closed-End Funds Declare Distributions</h1>
          <table>
            <tr><th>Fund</th><th>Ticker</th></tr>
            <tr><td>BNY Mellon Strategic Municipal Bond Fund</td><td>DSM</td></tr>
            <tr><td>BNY Mellon Strategic Municipals</td><td>LEO</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_businesswire_page_html", fake_fetch)

    hits = _extract_businesswire_fallback_tickers(
        "BNY Mellon Municipal Bond Closed-End Funds Declare Distributions",
        "",
        "https://www.businesswire.com/news/home/20260227228090/en",
        "",
        {"DSM", "LEO", "GOF"},
        timeout_seconds=5,
    )

    assert "DSM" in hits
    assert "LEO" in hits
    assert hits["DSM"][0] == "bw_table"
    assert hits["DSM"][1] == 0.84


def test_businesswire_page_fetch_retries_after_failed_cache_ttl(monkeypatch):
    class FakeResponse:
        def __init__(self, ok: bool, text: str):
            self.ok = ok
            self.text = text

    calls = {"count": 0}

    def fake_get(_url: str, timeout: int, headers: dict[str, str]) -> FakeResponse:  # noqa: ARG001
        calls["count"] += 1
        if calls["count"] == 1:
            return FakeResponse(ok=False, text="")
        return FakeResponse(ok=True, text="<html>ok</html>")

    timestamps = [1000.0, 1000.0, 1000.5, 1000.5]

    def fake_time() -> float:
        if timestamps:
            return timestamps.pop(0)
        return 1000.5

    _businesswire_page_cache.clear()
    monkeypatch.setattr("app.ingestion.requests.get", fake_get)
    monkeypatch.setattr("app.ingestion.time.time", fake_time)
    monkeypatch.setattr("app.ingestion.BUSINESSWIRE_PAGE_FAILURE_CACHE_TTL_SECONDS", 0)

    first = _fetch_businesswire_page_html("https://www.businesswire.com/news/home/abc", 5)
    second = _fetch_businesswire_page_html("https://www.businesswire.com/news/home/abc", 5)

    assert first is None
    assert second == "<html>ok</html>"
    assert calls["count"] == 2

    _businesswire_page_cache.clear()


def test_is_businesswire_article_url_allows_expected_hosts():
    assert _is_businesswire_article_url("https://www.businesswire.com/news/home/abc")
    assert _is_businesswire_article_url("http://feed.businesswire.com/rss/home")
    assert _is_businesswire_article_url("https://businesswire.com/news/home/abc?x=1")
    assert not _is_businesswire_article_url("https://example.com/news/home/abc")
    assert not _is_businesswire_article_url("file:///tmp/businesswire.html")


def test_businesswire_fetch_skips_non_businesswire_hosts(monkeypatch):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["count"] += 1
        raise AssertionError("non-businesswire URL should not be fetched")

    _businesswire_page_cache.clear()
    monkeypatch.setattr("app.ingestion.requests.get", fake_get)

    result = _fetch_businesswire_page_html("https://evil.example.com/news/home/abc", 5)

    assert result is None
    assert calls["count"] == 0
