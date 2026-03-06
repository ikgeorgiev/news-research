import pytest

from app.ingestion import (
    MIN_PERSIST_CONFIDENCE,
    PAGE_FETCH_CONFIGS,
    _build_symbol_keywords,
    _extract_businesswire_fallback_tickers,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _extract_table_cell_symbols_from_html,
    _fetch_businesswire_page_html,
    _fetch_source_page_html,
    _is_businesswire_article_url,
    _is_source_article_url,
    _source_page_cache,
    _clamp_label,
    _should_persist_entry,
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


def test_businesswire_fallback_extracts_table_symbols(monkeypatch):
    def fake_fetch(_url, _timeout, _config):
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

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

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


def test_businesswire_page_fetch_retries_after_failed_cache_ttl(monkeypatch, clean_page_cache):
    class FakeResponse:
        def __init__(self, ok: bool, text: str):
            self.ok = ok
            self.text = text

    calls = {"count": 0}

    def fake_get(
        _url: str, timeout: int, headers: dict[str, str]
    ) -> FakeResponse:  # noqa: ARG001
        calls["count"] += 1
        if calls["count"] == 1:
            return FakeResponse(ok=False, text="")
        return FakeResponse(ok=True, text="<html>ok</html>")

    timestamps = [1000.0, 1000.0, 1000.5, 1000.5]

    def fake_time() -> float:
        if timestamps:
            return timestamps.pop(0)
        return 1000.5

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)
    monkeypatch.setattr("app.ingestion.time.time", fake_time)
    monkeypatch.setattr("app.ingestion.SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS", 0)

    first = _fetch_businesswire_page_html(
        "https://www.businesswire.com/news/home/abc", 5
    )
    second = _fetch_businesswire_page_html(
        "https://www.businesswire.com/news/home/abc", 5
    )

    assert first is None
    assert second == "<html>ok</html>"
    assert calls["count"] == 2


def test_is_businesswire_article_url_allows_expected_hosts():
    assert _is_businesswire_article_url("https://www.businesswire.com/news/home/abc")
    assert _is_businesswire_article_url("http://feed.businesswire.com/rss/home")
    assert _is_businesswire_article_url("https://businesswire.com/news/home/abc?x=1")
    assert not _is_businesswire_article_url("https://example.com/news/home/abc")
    assert not _is_businesswire_article_url("file:///tmp/businesswire.html")


def test_businesswire_fetch_skips_non_businesswire_hosts(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["count"] += 1
        raise AssertionError("non-businesswire URL should not be fetched")

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)

    result = _fetch_businesswire_page_html("https://evil.example.com/news/home/abc", 5)

    assert result is None
    assert calls["count"] == 0


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


def test_prnewswire_fallback_extracts_table_symbols(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Invesco Closed-End Funds Declare Dividends</h1>
          <table>
            <tr><th>Fund Name</th><th>Ticker</th></tr>
            <tr><td>Invesco Municipal Trust</td><td>VKQ</td></tr>
            <tr><td>Invesco Municipal Opportunity Trust</td><td>VMO</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Invesco Closed-End Funds Declare Dividends",
        "",
        "https://www.prnewswire.com/news-releases/invesco-302701172.html",
        "",
        {"VKQ", "VMO", "OIA"},
        timeout_seconds=5,
        config=config,
    )

    assert "VKQ" in hits
    assert "VMO" in hits
    assert hits["VKQ"][0] == "prn_table"
    assert hits["VKQ"][1] == 0.84
    assert "OIA" not in hits


def test_prnewswire_fallback_validates_plain_token_from_fetched_body(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Calamos Global Total Return Fund CGO declares distribution.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Distribution update",
        "",
        "https://www.prnewswire.com/news-releases/calamos-302701173.html",
        "",
        {"CGO"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    assert "CGO" in hits
    assert hits["CGO"][0] == "validated_token"
    assert hits["CGO"][1] >= MIN_PERSIST_CONFIDENCE


def test_globenewswire_fallback_extracts_exchange_and_table(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>BNY Mellon Funds Declare Distributions</h1>
          <p>The Board of Directors of NYSE: DSM announced distributions.</p>
          <table>
            <tr><td>BNY Mellon Strategic Municipals</td><td>LEO</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BNY Mellon Funds Declare Distributions",
        "",
        "https://www.globenewswire.com/news-release/2026/03/abc",
        "",
        {"DSM", "LEO"},
        timeout_seconds=5,
        config=config,
    )

    assert "DSM" in hits
    assert hits["DSM"][0] == "exchange"
    assert "LEO" in hits
    assert hits["LEO"][0] == "gnw_table"


def test_source_page_fetch_skips_wrong_host(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("wrong host should not be fetched")

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)

    config = PAGE_FETCH_CONFIGS["prnewswire"]
    result = _fetch_source_page_html("https://evil.example.com/news/abc", 5, config)

    assert result is None
    assert calls["count"] == 0


# --- Fund name/sponsor validation tests ---


def test_build_symbol_keywords_extracts_distinctive_words():
    rows = [
        (1, "CGO", "Calamos Global Total Return", "Calamos Advisors LLC"),
        (2, "PMO", "Putnam Muni Opportunities", "Franklin Advisers, Inc."),
        (3, "CMU", "MFS High Yield Municipal", "MFS"),
        (4, "CFND", "C1 Fund Inc.", "C1 Advisors LLC"),
        (5, "DNP", "DNP Select Income", "Duff & Phelps Inv Mgmt Co (IL)"),
        (
            6,
            "EDF",
            "Virtus Stone Harbor Emerging Markets Inc",
            "Virtus Investment Advisors (VIA)",
        ),
    ]
    kws = _build_symbol_keywords(rows)
    assert "calamos" in kws["CGO"]
    assert "calamos global" in kws["CGO"]
    assert "putnam" in kws["PMO"]
    assert "putnam muni" in kws["PMO"]
    assert "franklin" not in kws["PMO"]
    assert "mfs" in kws["CMU"]
    assert "mfs high" in kws["CMU"]
    assert "high" not in kws["CMU"]
    assert "yield" not in kws["CMU"]
    assert "municipal" not in kws["CMU"]
    assert "c1" in kws["CFND"]
    assert "il" not in kws["DNP"]
    assert "via" not in kws["EDF"]
    # Generic words should be excluded
    assert "fund" not in kws.get("CGO", frozenset())
    assert "trust" not in kws.get("CGO", frozenset())
    assert "llc" not in kws.get("CGO", frozenset())
    assert "inc" not in kws.get("PMO", frozenset())


def test_token_match_validated_when_fund_name_present():
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}
    hits = _extract_entry_tickers(
        "Calamos Global Total Return CGO declares distribution",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][0] == "validated_token"
    assert hits["CGO"][1] == 0.68


def test_token_match_not_validated_without_fund_name():
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}
    hits = _extract_entry_tickers(
        "Envestnet appoints new CGO to lead growth strategy",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][0] == "token"
    assert hits["CGO"][1] == 0.62


def test_token_match_not_validated_with_sponsor_brand_only_context():
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}
    hits = _extract_entry_tickers(
        "Calamos appoints new CGO",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][0] == "token"
    assert hits["CGO"][1] == 0.62


def test_paren_match_bypasses_fund_name_validation():
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}
    hits = _extract_entry_tickers(
        "Fund update for (CGO) distribution schedule",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][0] == "paren"
    assert hits["CGO"][1] == 0.75


def test_exchange_match_bypasses_fund_name_validation():
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}
    hits = _extract_entry_tickers(
        "NYSE: CGO distribution declared",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][0] == "exchange"
    assert hits["CGO"][1] == 0.88


def test_generic_fund_words_do_not_validate_plain_token_match():
    known = {"CMU"}
    sym_kws = _build_symbol_keywords(
        [(1, "CMU", "MFS High Yield Municipal", "MFS")]
    )
    hits = _extract_entry_tickers(
        "Company expands high yield municipal CMU platform",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CMU" in hits
    assert hits["CMU"][0] == "token"
    assert hits["CMU"][1] < MIN_PERSIST_CONFIDENCE


def test_token_match_validated_when_short_sponsor_acronym_present():
    known = {"CMU"}
    sym_kws = _build_symbol_keywords(
        [(1, "CMU", "MFS High Yield Municipal", "MFS")]
    )
    hits = _extract_entry_tickers(
        "MFS High Yield Municipal CMU declares distribution",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CMU" in hits
    assert hits["CMU"][0] == "validated_token"
    assert hits["CMU"][1] >= MIN_PERSIST_CONFIDENCE


def test_short_keyword_validation_respects_word_boundaries():
    known = {"DNP"}
    sym_kws = {"DNP": frozenset({"il"})}
    hits = _extract_entry_tickers(
        "Company will appoint new DNP leader",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "DNP" in hits
    assert hits["DNP"][0] == "token"
    assert hits["DNP"][1] == 0.62


def test_two_letter_sponsor_keyword_does_not_validate_location_suffix():
    known = {"DNP"}
    sym_kws = _build_symbol_keywords(
        [(1, "DNP", "DNP Select Income", "Duff & Phelps Inv Mgmt Co (IL)")]
    )
    hits = _extract_entry_tickers(
        "Chicago, IL office names new DNP executive",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "DNP" in hits
    assert hits["DNP"][0] == "token"
    assert hits["DNP"][1] < MIN_PERSIST_CONFIDENCE


def test_trailing_three_letter_sponsor_acronym_does_not_validate_token_match():
    known = {"EDF"}
    sym_kws = _build_symbol_keywords(
        [
            (
                1,
                "EDF",
                "Virtus Stone Harbor Emerging Markets Inc",
                "Virtus Investment Advisors (VIA)",
            )
        ]
    )
    hits = _extract_entry_tickers(
        "Company comments via EDF filing update",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "EDF" in hits
    assert hits["EDF"][0] == "token"
    assert hits["EDF"][1] < MIN_PERSIST_CONFIDENCE


def test_false_positive_articles_filtered_end_to_end():
    """Simulate the user's reported false positives -- token-only matches should not persist."""
    known = {"CGO", "PMO", "FT", "FRA", "SPE", "CEE"}
    sym_kws = {
        "CGO": frozenset({"calamos", "calamos global"}),
        "PMO": frozenset({"putnam", "putnam muni"}),
        "FT": frozenset({"franklin", "franklin universal"}),
        "FRA": frozenset({"blackrock", "blackrock floating"}),
        "SPE": frozenset({"bulldog", "special"}),
        "CEE": frozenset({"central", "eastern", "europe"}),
    }

    cases = [
        (
            "Envestnet Accelerates Adaptive WealthTech Innovation",
            "New CGO appointment announced",
        ),
        ("Mace Consult Launches as Standalone PMO Company", ""),
        ("Sokin Appoints Former FT Partners VP Tom Steer as CFO", ""),
        (
            "Vior Gold Corporation Announces District Scale Projects Acquisition",
            "Listed on FRA exchange",
        ),
    ]

    for title, summary in cases:
        hits = _extract_entry_tickers(
            title, summary, "https://example.com", "", known, symbol_keywords=sym_kws
        )
        # All matches should be unvalidated tokens (0.62) — below persist threshold
        for sym, (match_type, confidence) in hits.items():
            assert (
                confidence < MIN_PERSIST_CONFIDENCE
            ), f"False positive: {sym} in '{title}' has confidence {confidence} ({match_type})"


def test_real_cef_article_persists_end_to_end():
    """A real CEF article with fund name context should persist."""
    known = {"CGO"}
    sym_kws = {"CGO": frozenset({"calamos", "calamos global"})}

    hits = _extract_entry_tickers(
        "Calamos Global Total Return Fund CGO Declares Monthly Distribution",
        "",
        "https://example.com",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "CGO" in hits
    assert hits["CGO"][1] >= MIN_PERSIST_CONFIDENCE
    assert _should_persist_entry("prnewswire", hits) is True


def test_paren_stopword_ticker_blocked_without_fund_name():
    """(USA) in an Occidental press release should NOT match."""
    known = {"USA"}
    sym_kws = {"USA": frozenset({"liberty", "all-star", "equity"})}
    hits = _extract_entry_tickers(
        "Occidental Announces Tender Offers (USA)",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "USA" not in hits


def test_paren_stopword_ticker_allowed_with_fund_name():
    """(USA) in a Liberty All-Star article SHOULD match."""
    known = {"USA"}
    sym_kws = {"USA": frozenset({"liberty", "all-star", "equity"})}
    hits = _extract_entry_tickers(
        "Liberty All-Star Equity Fund (USA) Declares Distribution",
        "",
        "https://example.com/story",
        "",
        known,
        symbol_keywords=sym_kws,
    )
    assert "USA" in hits
    assert hits["USA"][0] == "paren"
    assert hits["USA"][1] == 0.75


def test_businesswire_fallback_preserves_stopword_ticker_with_keywords(monkeypatch):
    """BW fallback should validate stopword tickers via symbol_keywords, not drop them."""

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Liberty All-Star Equity Fund Declares Distribution</h1>
          <p>The Board of Directors of Liberty All-Star Equity Fund (USA) announced
          a monthly distribution.</p>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    sym_kws = {"USA": frozenset({"liberty", "all-star", "equity"})}
    hits = _extract_businesswire_fallback_tickers(
        "Liberty All-Star Equity Fund Declares Distribution",
        "",
        "https://www.businesswire.com/news/home/20260301123456/en",
        "",
        {"USA"},
        timeout_seconds=5,
        symbol_keywords=sym_kws,
    )

    assert "USA" in hits
    assert hits["USA"][0] == "paren"
    assert hits["USA"][1] == 0.75
