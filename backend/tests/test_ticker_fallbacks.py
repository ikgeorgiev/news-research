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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BNY Mellon Municipal Bond Closed-End Funds Declare Distributions",
        "",
        "https://www.businesswire.com/news/home/20260227228090/en",
        "",
        {"DSM", "LEO", "GOF"},
        timeout_seconds=5,
        config=PAGE_FETCH_CONFIGS["businesswire"],
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

    monkeypatch.setattr("app.ticker_extraction.requests.get", fake_get)
    monkeypatch.setattr("app.ticker_extraction.time.time", fake_time)
    monkeypatch.setattr("app.ticker_extraction.SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS", 0)

    bw_config = PAGE_FETCH_CONFIGS["businesswire"]
    first = _fetch_source_page_html(
        "https://www.businesswire.com/news/home/abc", 5, bw_config
    )
    second = _fetch_source_page_html(
        "https://www.businesswire.com/news/home/abc", 5, bw_config
    )

    assert first is None
    assert second == "<html>ok</html>"
    assert calls["count"] == 2

def test_businesswire_fetch_skips_non_businesswire_hosts(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["count"] += 1
        raise AssertionError("non-businesswire URL should not be fetched")

    monkeypatch.setattr("app.ticker_extraction.requests.get", fake_get)

    result = _fetch_source_page_html("https://evil.example.com/news/home/abc", 5, PAGE_FETCH_CONFIGS["businesswire"])

    assert result is None
    assert calls["count"] == 0

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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Invesco Closed-End Funds Declare Dividends",
        "",
        "https://www.prnewswire.com/news-releases/invesco-302701172.html",
        "",
        {"VKQ", "VMO", "OIA"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "VKQ": frozenset({"invesco", "municipal trust"}),
            "VMO": frozenset({"invesco", "municipal opportunity"}),
        },
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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BNY Mellon Funds Declare Distributions",
        "",
        "https://www.globenewswire.com/news-release/2026/03/abc",
        "",
        {"DSM", "LEO"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "LEO": frozenset({"bny", "mellon", "strategic municipals"}),
        },
    )

    assert "DSM" in hits
    assert hits["DSM"][0] == "exchange"
    assert "LEO" in hits
    assert hits["LEO"][0] == "gnw_table"

def test_prnewswire_fallback_drops_table_hits_without_keyword_support(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Generic corporate update</h1>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>CGO</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Generic corporate update",
        "",
        "https://www.prnewswire.com/news-releases/generic-302701199.html",
        "",
        {"CGO"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    assert hits["CGO"] == ("prn_table", 0.62)

def test_source_page_fetch_skips_wrong_host(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("wrong host should not be fetched")

    monkeypatch.setattr("app.ticker_extraction.requests.get", fake_get)

    config = PAGE_FETCH_CONFIGS["prnewswire"]
    result = _fetch_source_page_html("https://evil.example.com/news/abc", 5, config)

    assert result is None
    assert calls["count"] == 0

def test_real_world_asa_page_false_positive_stays_subthreshold(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Nscale announced support from Aker ASA and other investors.</p>
          </article>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>ASA</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "ASA", "ASA Gold and Precious Metals Limited", "ASA")]
    )
    hits = _extract_source_fallback_tickers(
        "Nscale Raises $2 Billion in Series C",
        "",
        "https://www.prnewswire.com/news-releases/nscale-302708359.html",
        "",
        {"ASA"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["ASA"] == ("prn_table", 0.62)

def test_real_world_ra_page_false_positive_stays_subthreshold(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Real-world data suggest deep responses in refractory rheumatoid arthritis (RA).</p>
            <table>
              <tr><th>Line Item</th><th>Value</th></tr>
              <tr><td>Assets</td><td>$108,008</td></tr>
            </table>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "RA", "Brookfield Real Assets Income Fund Inc.", "Brookfield Public Securities Group LLC")]
    )
    hits = _extract_source_fallback_tickers(
        "Artiva Biotherapeutics Reports Full Year 2025 Financial Results and Recent Business Highlights",
        "Initial clinical response data for AlloNK in refractory rheumatoid arthritis (RA) expected in first half of 2026",
        "https://www.globenewswire.com/en/news-release/2026/03/10/example",
        "https://rss.globenewswire.com/en/RssFeed/subjectcode/13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/Earnings%20Releases%20And%20Operating%20Results",
        {"RA"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "RA" not in hits

def test_real_world_pcf_page_false_positive_stays_subthreshold(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>EcoVadis and Watershed partner to close the Scope 3 data gap.</p>
            <p>Combined with the launch of EcoVadis PCF Calculator, the partnership
            with Watershed is a cornerstone of EcoVadis' mission.</p>
            <p>Operational detail (PCF) appears in the body without any fund context.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "PCF", "High Income Securities", "Bulldog Investors LLP")]
    )
    hits = _extract_source_fallback_tickers(
        "EcoVadis and Watershed partner to close the Scope 3 data gap",
        (
            "Combined with the launch of EcoVadis PCF Calculator, the partnership "
            "with Watershed is a cornerstone of EcoVadis' mission."
        ),
        "https://www.prnewswire.com/news-releases/ecovadis-and-watershed-302712475.html",
        "",
        {"PCF"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "PCF" not in hits

def test_keywordless_cef_table_hit_reaches_persist_threshold(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Distribution announcement for shareholders.</p>
          </article>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>DNP</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "DNP", "DNP Select Income", "Duff & Phelps Inv Mgmt Co (IL)")]
    )
    assert sym_kws["DNP"] == frozenset()

    hits = _extract_source_fallback_tickers(
        "Monthly distribution notice",
        "",
        "https://www.prnewswire.com/news-releases/dnp-302708359.html",
        "",
        {"DNP"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["DNP"] == ("prn_table", NO_KEYWORDS_CONFIDENCE)
    assert hits["DNP"][1] >= MIN_PERSIST_CONFIDENCE

def test_noise_wrapped_table_does_not_create_keywordless_table_hit(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Distribution announcement for shareholders.</p>
          </article>
          <aside class="related-articles">
            <table>
              <tr><th>Ticker</th></tr>
              <tr><td>DNP</td></tr>
            </table>
          </aside>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "DNP", "DNP Select Income", "Duff & Phelps Inv Mgmt Co (IL)")]
    )
    assert sym_kws["DNP"] == frozenset()

    hits = _extract_source_fallback_tickers(
        "Monthly distribution notice",
        "",
        "https://www.prnewswire.com/news-releases/dnp-302708359.html",
        "",
        {"DNP"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "DNP" not in hits

def test_sidebar_keyword_noise_does_not_validate_token(monkeypatch):
    """Keywords in sidebar/nav/aside should not validate a token match."""
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Nscale announced a $2 billion raise led by Aker ASA.</p>
          </article>
          <aside class="related-articles">
            <a href="/gold-etf">Gold ETF sees inflows amid precious metals rally</a>
          </aside>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "ASA", "ASA Gold and Precious Metals Limited", "ASA")]
    )
    hits = _extract_source_fallback_tickers(
        "Nscale Raises $2 Billion in Series C",
        "",
        "https://www.prnewswire.com/news-releases/nscale-302708359.html",
        "",
        {"ASA"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    # "gold" and "precious" appear in <aside> sidebar only, not article body.
    # Noise stripping removes them, so ASA should NOT validate.
    assert "ASA" not in hits

def test_article_header_keywords_still_validate_table_hit(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <header>
              <h1>Calamos Global Total Return Fund monthly distribution update</h1>
            </header>
            <table>
              <tr><th>Ticker</th></tr>
              <tr><td>CGO</td></tr>
            </table>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Distribution update",
        "",
        "https://www.prnewswire.com/news-releases/calamos-302701174.html",
        "",
        {"CGO"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    assert hits["CGO"] == ("prn_table", 0.84)

def test_trafilatura_extracts_clean_body(monkeypatch):
    def fake_extract(html_text, **kwargs):
        assert html_text.startswith("<html>")
        assert kwargs == {
            "include_tables": True,
            "include_comments": False,
            "include_links": False,
            "no_fallback": False,
        }
        return "Main article body with enough length to pass the extraction threshold and include tables."

    monkeypatch.setitem(sys.modules, "trafilatura", SimpleNamespace(extract=fake_extract))

    body = _extract_article_body("<html><body><article><p>content</p></article></body></html>")

    assert body is not None
    assert "Main article body" in body

def test_regex_fallback_when_trafilatura_fails(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Distribution announcement for shareholders.</p>
          </article>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>DNP</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)
    monkeypatch.setattr("app.ticker_extraction._extract_article_body", lambda _html: None)

    hits = _extract_source_fallback_tickers(
        "Monthly distribution notice",
        "",
        "https://www.prnewswire.com/news-releases/dnp-302708359.html",
        "",
        {"DNP"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"DNP": frozenset()},
    )

    assert hits["DNP"] == ("prn_table", NO_KEYWORDS_CONFIDENCE)

def test_sidebar_related_article_does_not_tag_main_article(monkeypatch):
    """BW sidebar 'More News' with NYSE: FFA must not tag the unrelated main article."""
    config = PAGE_FETCH_CONFIGS["businesswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <h1>First Trust Advisors L.P. Announces Distribution for
                First Trust Income Opportunities ETF</h1>
            <table>
              <tr><th>Ticker</th><th>Exchange</th><th>Fund Name</th></tr>
              <tr><td>FCEF</td><td>Nasdaq</td>
                  <td>First Trust Income Opportunities ETF</td></tr>
            </table>
          </article>
          <div class="ui-kit-press-release__sidebar bg-blue100">
            <div class="ui-kit-press-release-sidebar">
              <h2>More News From First Trust Advisors L.P.</h2>
              <a href="/other"><h2>First Trust Enhanced Equity Income Fund
                  Declares its Quarterly Distribution</h2></a>
              <div class="rich-text">First Trust Enhanced Equity Income Fund
                  (the "Fund") (NYSE: FFA) has declared the Fund's
                  regularly scheduled quarterly distribution.</div>
            </div>
          </div>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "First Trust Advisors L.P. Announces Distribution for First Trust Income Opportunities ETF",
        "",
        "https://www.businesswire.com/news/home/20260310285136/en",
        "",
        {"FFA", "FCEF"},
        timeout_seconds=5,
        config=config,
    )

    assert "FFA" not in hits

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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = {"USA": frozenset({"liberty", "all-star", "equity"})}
    hits = _extract_source_fallback_tickers(
        "Liberty All-Star Equity Fund Declares Distribution",
        "",
        "https://www.businesswire.com/news/home/20260301123456/en",
        "",
        {"USA"},
        timeout_seconds=5,
        config=PAGE_FETCH_CONFIGS["businesswire"],
        symbol_keywords=sym_kws,
    )

    assert "USA" in hits
    assert hits["USA"][0] == "paren"
    assert hits["USA"][1] == 0.75
