import sys
from types import SimpleNamespace

import httpx
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
        def __init__(self, is_success: bool, text: str):
            self.is_success = is_success
            self.text = text

    calls = {"count": 0}

    def fake_get(
        _url: str,
        timeout: int,
        headers: dict[str, str],
        follow_redirects: bool = True,
    ) -> FakeResponse:  # noqa: ARG001
        calls["count"] += 1
        assert follow_redirects is False
        if calls["count"] == 1:
            return FakeResponse(is_success=False, text="")
        return FakeResponse(is_success=True, text="<html>ok</html>")

    timestamps = [1000.0, 1000.0, 1000.5, 1000.5]

    def fake_time() -> float:
        if timestamps:
            return timestamps.pop(0)
        return 1000.5

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
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

def test_businesswire_fetch_normalizes_registered_mark_slug_for_request(monkeypatch, clean_page_cache):
    observed_urls: list[str] = []

    class FakeResponse:
        is_success = True
        text = "<html>ok</html>"

    def fake_get(
        url: str,
        timeout: int,
        headers: dict[str, str],
        follow_redirects: bool = True,
    ):  # noqa: ARG001
        observed_urls.append(url)
        assert follow_redirects is False
        return FakeResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    result = _fetch_source_page_html(
        "http://www.businesswire.com/news/home/20260414872914/en/Liberty-All-Star%C2%AE-Equity-Fund-March-2026-Monthly-Update?feedref=abc",
        5,
        PAGE_FETCH_CONFIGS["businesswire"],
    )

    assert result == "<html>ok</html>"
    assert observed_urls == [
        "https://www.businesswire.com/news/home/20260414872914/en/Liberty-All-Star-Equity-Fund-March-2026-Monthly-Update"
    ]

def test_businesswire_fetch_upgrades_http_feed_link_to_https(monkeypatch, clean_page_cache):
    observed_urls: list[str] = []

    class FakeResponse:
        is_success = True
        text = "<html>ok</html>"

    def fake_get(
        url: str,
        timeout: int,
        headers: dict[str, str],
        follow_redirects: bool = True,
    ):  # noqa: ARG001
        observed_urls.append(url)
        assert follow_redirects is False
        return FakeResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    result = _fetch_source_page_html(
        "http://www.businesswire.com/news/home/20260422567496/en/Western-Asset-Global-High-Income-Fund-Inc.-Announces-Financial-Position-as-of-February-28-2026?feedref=JjAwJuNHiystnCo",
        5,
        PAGE_FETCH_CONFIGS["businesswire"],
    )

    assert result == "<html>ok</html>"
    assert observed_urls == [
        "https://www.businesswire.com/news/home/20260422567496/en/Western-Asset-Global-High-Income-Fund-Inc.-Announces-Financial-Position-as-of-February-28-2026"
    ]

def test_businesswire_fetch_preserves_other_percent_escapes(monkeypatch, clean_page_cache):
    observed_urls: list[str] = []

    class FakeResponse:
        is_success = True
        text = "<html>ok</html>"

    def fake_get(
        url: str,
        timeout: int,
        headers: dict[str, str],
        follow_redirects: bool = True,
    ):  # noqa: ARG001
        observed_urls.append(url)
        assert follow_redirects is False
        return FakeResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    result = _fetch_source_page_html(
        "http://www.businesswire.com/news/home/20260414872914/en/Fund%C2%AE-Update%2FSeries%231?feedref=abc",
        5,
        PAGE_FETCH_CONFIGS["businesswire"],
    )

    assert result == "<html>ok</html>"
    assert observed_urls == [
        "https://www.businesswire.com/news/home/20260414872914/en/Fund-Update%2FSeries%231"
    ]

def test_businesswire_fetch_skips_non_businesswire_hosts(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["count"] += 1
        raise AssertionError("non-businesswire URL should not be fetched")

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    result = _fetch_source_page_html("https://evil.example.com/news/home/abc", 5, PAGE_FETCH_CONFIGS["businesswire"])

    assert result is None
    assert calls["count"] == 0


def test_businesswire_fetch_does_not_follow_redirects_to_untrusted_hosts(
    monkeypatch, clean_page_cache
):
    requested_urls: list[str] = []

    def fake_get(
        url: str,
        timeout: int,  # noqa: ARG001
        headers: dict[str, str],  # noqa: ARG001
        follow_redirects: bool = True,
    ):
        requested_urls.append(url)
        assert follow_redirects is False
        assert url == "https://www.businesswire.com/news/home/abc"
        return httpx.Response(
            302,
            headers={"Location": "https://evil.example.com/redirected"},
            content=b"<html>redirect body</html>",
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

    result = _fetch_source_page_html(
        "https://www.businesswire.com/news/home/abc",
        5,
        PAGE_FETCH_CONFIGS["businesswire"],
    )

    assert result is None
    assert requested_urls == ["https://www.businesswire.com/news/home/abc"]
    assert _source_page_cache["https://www.businesswire.com/news/home/abc"][1] is None

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

def test_prnewswire_fallback_keeps_body_token_with_phrase_keyword_match(monkeypatch):
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

    assert not hits or "CGO" not in hits


def test_prnewswire_fallback_drops_body_only_validated_token_with_generic_keywords(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>
              With Emerging Market Debt continuing to be a focus for clients,
              we are delighted to be working alongside Aktia's outstanding EMD team.
            </p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Aktia Bank Plc expands its distribution network in Europe through three new sales partnerships",
        "",
        "https://www.prnewswire.com/news-releases/aktia-bank-plc-expands-its-distribution-network-in-europe-through-three-new-sales-partnerships-302737985.html",
        "",
        {"EMD"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=_build_symbol_keywords(
            [
                (
                    1,
                    "EMD",
                    "Western Asset Emerging Markets Debt",
                    "Franklin Templeton Fund Adviser, LLC",
                )
            ]
        ),
    )

    assert not hits or "EMD" not in hits


def test_prnewswire_fallback_ignores_timezone_token_false_positive(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>BlackRock retains first place in a new brand study.</p>
            <p>A webinar is scheduled for Tuesday, 14 April 2026 at 2:00pm BST | 9:00am EST | 9:00pm CST.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BlackRock retains top spot",
        "",
        "https://www.prnewswire.com/news-releases/blackrock-302729339.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset(
                {"blackrock", "technology", "science", "blackrock science"}
            )
        },
    )

    assert "BST" not in hits


def test_prnewswire_fallback_keeps_title_token_when_body_supplies_keywords(monkeypatch):
    """Fetched-body keywords should still validate a title token."""
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>In 26 BST returned over 15% for the year. BlackRock science and technology trust.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BST annual performance review",
        "",
        "https://www.prnewswire.com/news-releases/bst-performance-12345.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset(
                {"blackrock", "technology", "science", "blackrock science"}
            )
        },
    )

    assert not hits or "BST" not in hits


def test_prnewswire_fallback_keeps_two_ordinary_keywords_for_larger_keyword_sets(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>In 26 BST returned over 15% for the year. BlackRock technology trust.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BST annual performance review",
        "",
        "https://www.prnewswire.com/news-releases/bst-performance-12345.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset(
                {"blackrock", "science", "technology", "blackrock science"}
            )
        },
    )

    assert not hits or "BST" not in hits


def test_prnewswire_fallback_keeps_parenthesized_ticker_from_fetched_body(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>BlackRock science and technology trust remains a leader in the segment.</p>
            <p>BlackRock Science and Technology Trust (BST) completed the quarter strongly.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BlackRock retains top spot",
        "",
        "https://www.prnewswire.com/news-releases/blackrock-302729339.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset({"blackrock", "technology", "science"})
        },
    )

    assert hits["BST"][0] == "paren"
    assert hits["BST"][1] >= MIN_PERSIST_CONFIDENCE


def test_prnewswire_fallback_aktia_emd_false_positive_stays_subthreshold(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>With Emerging Market Debt continuing to be a focus for clients, we are delighted
            to be working alongside Aktia's outstanding EMD team.</p>
            <p>The compelling opportunities that local-currency emerging and frontier market debt
            offer portfolios make this an especially timely partnership.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [
            (
                1,
                "EMD",
                "Western Asset Emerging Markets Debt",
                "Franklin Templeton Fund Adviser, LLC",
            )
        ]
    )
    hits = _extract_source_fallback_tickers(
        "Aktia expands its distribution network in Europe",
        "",
        "https://www.prnewswire.com/news-releases/aktia-302737985.html",
        "",
        {"EMD"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "EMD" not in hits


def test_prnewswire_fallback_does_not_upgrade_title_token_from_generic_body_keywords(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>With Emerging Market Debt continuing to be a focus for clients.</p>
            <p>The compelling opportunities that local-currency emerging and frontier market debt offer portfolios make this timely.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Aktia expands its EMD team in Europe",
        "",
        "https://www.prnewswire.com/news-releases/aktia-302737985.html",
        "",
        {"EMD"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=_build_symbol_keywords(
            [
                (
                    1,
                    "EMD",
                    "Western Asset Emerging Markets Debt",
                    "Franklin Templeton Fund Adviser, LLC",
                )
            ]
        ),
    )

    assert "EMD" not in hits


def test_prnewswire_fallback_keeps_body_token_when_title_supplies_phrase(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>BST completed the quarter strongly.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "BlackRock science and technology trust quarterly update",
        "",
        "https://www.prnewswire.com/news-releases/blackrock-bst-12345.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset(
                {"blackrock", "technology", "science", "blackrock science"}
            )
        },
    )

    assert hits["BST"][0] == "validated_token"
    assert hits["BST"][1] >= MIN_PERSIST_CONFIDENCE


def test_prnewswire_fallback_keeps_two_keyword_override_validation(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Duff &amp; Phelps discussed the quarter and portfolio positioning.</p>
            <p>DNP completed the quarter strongly.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Fund quarterly update DNP",
        "",
        "https://www.prnewswire.com/news-releases/dnp-quarterly-update-12345.html",
        "",
        {"DNP"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"DNP": frozenset({"duff", "phelps"})},
    )

    assert not hits or "DNP" not in hits


def test_prnewswire_fallback_keeps_title_token_when_summary_and_body_split_keywords(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Phelps discussed portfolio positioning for the quarter.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "DNP monthly update",
        "Duff announces the monthly commentary",
        "https://www.prnewswire.com/news-releases/dnp-monthly-update-12345.html",
        "",
        {"DNP"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"DNP": frozenset({"duff", "phelps"})},
    )

    assert hits["DNP"][0] == "validated_token"
    assert hits["DNP"][1] >= MIN_PERSIST_CONFIDENCE


def test_extract_entry_tickers_keeps_title_token_when_summary_starts_with_time():
    hits = _extract_entry_tickers(
        "BlackRock Science and Technology Trust BST",
        "9:00am webcast tomorrow",
        "https://example.com",
        "",
        {"BST"},
        symbol_keywords={"BST": frozenset({"blackrock", "technology", "science"})},
    )

    assert hits["BST"][0] == "validated_token"
    assert hits["BST"][1] >= MIN_PERSIST_CONFIDENCE


def test_extract_entry_tickers_keeps_title_paren_when_summary_starts_with_time():
    hits = _extract_entry_tickers(
        "BlackRock Science and Technology Trust (BST)",
        "9:00am webcast tomorrow",
        "https://example.com",
        "",
        {"BST"},
        symbol_keywords={"BST": frozenset({"blackrock", "technology", "science"})},
    )

    assert hits["BST"][0] == "paren"
    assert hits["BST"][1] >= MIN_PERSIST_CONFIDENCE


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


def test_globenewswire_fallback_keeps_body_token_match(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Brookfield Real Assets Income Fund RA declares monthly distribution.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Monthly distribution update",
        "",
        "https://www.globenewswire.com/news-release/2026/03/abc",
        "https://rss.globenewswire.com/en/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        {"RA"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"RA": frozenset({"brookfield", "real assets"})},
    )

    assert not hits or "RA" not in hits


def test_prnewswire_fallback_does_not_validate_table_hit_from_url_slug_only(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Generic corporate update.</p>
            <table>
              <tr><th>Ticker</th></tr>
              <tr><td>BST</td></tr>
            </table>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Generic corporate update",
        "",
        "https://www.prnewswire.com/news-releases/blackrock-science-technology-302701199.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={"BST": frozenset({"blackrock", "technology", "science"})},
    )

    assert hits["BST"] == ("prn_table", 0.62)


def test_prnewswire_fallback_keeps_parenthesized_body_hit_with_slug_keywords(
    monkeypatch,
):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Quarter completed strongly (BST).</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Quarterly update",
        "",
        "https://www.prnewswire.com/news-releases/blackrock-science-technology-quarterly-update-12345.html",
        "",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset(
                {"blackrock", "science", "technology", "blackrock science"}
            )
        },
    )

    assert not hits or "BST" not in hits


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

def test_prnewswire_fallback_validates_fund_table_hit_with_keyword_support(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Sprott Focus Trust FUND Notice to Shareholders</h1>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>FUND</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "FUND", "Sprott Focus Trust", "Sprott Asset Management USA Inc.")]
    )
    hits = _extract_source_fallback_tickers(
        "Sprott Focus Trust FUND Notice to Shareholders",
        "",
        "https://www.prnewswire.com/news-releases/sprott-302701199.html",
        "",
        {"FUND"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["FUND"] == ("prn_table", 0.84)
    assert hits["FUND"][1] >= MIN_PERSIST_CONFIDENCE

def test_prnewswire_fallback_drops_fund_table_hit_without_keyword_support(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Generic corporate update</h1>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>FUND</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "FUND", "Sprott Focus Trust", "Sprott Asset Management USA Inc.")]
    )
    hits = _extract_source_fallback_tickers(
        "Generic corporate update",
        "",
        "https://www.prnewswire.com/news-releases/generic-302701199.html",
        "",
        {"FUND"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["FUND"] == ("prn_table", 0.62)
    assert hits["FUND"][1] < MIN_PERSIST_CONFIDENCE

def test_prnewswire_fallback_validates_ide_table_hit_with_table_fund_context(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Voya Infrastructure Industrials&Matls Fd announces annual meeting results</h1>
          <table>
            <tr><th>Fund Name</th><th>Ticker</th></tr>
            <tr><td>Voya Infrastructure Industrials&Matls Fd</td><td>IDE</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "IDE", "Voya Infrastructure Industrials&Matls Fd", "Voya Investments, LLC.")]
    )
    hits = _extract_source_fallback_tickers(
        "Voya Infrastructure Industrials&Matls Fd announces annual meeting results",
        "",
        "https://www.prnewswire.com/news-releases/voya-302701199.html",
        "",
        {"IDE"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["IDE"] == ("prn_table", 0.84)
    assert hits["IDE"][1] >= MIN_PERSIST_CONFIDENCE

def test_prnewswire_fallback_drops_ide_table_hit_without_table_fund_context(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Voya Infrastructure Industrials&Matls Fd announces annual meeting results</h1>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>IDE</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "IDE", "Voya Infrastructure Industrials&Matls Fd", "Voya Investments, LLC.")]
    )
    hits = _extract_source_fallback_tickers(
        "Voya Infrastructure Industrials&Matls Fd announces annual meeting results",
        "",
        "https://www.prnewswire.com/news-releases/voya-302701199.html",
        "",
        {"IDE"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["IDE"] == ("prn_table", 0.62)
    assert hits["IDE"][1] < MIN_PERSIST_CONFIDENCE


def test_prnewswire_fallback_drops_ide_table_hit_without_keyword_support(monkeypatch):
    config = PAGE_FETCH_CONFIGS["prnewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <h1>Generic corporate update</h1>
          <table>
            <tr><th>Ticker</th></tr>
            <tr><td>IDE</td></tr>
          </table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "IDE", "Voya Infrastructure Industrials&Matls Fd", "Voya Investments, LLC.")]
    )
    hits = _extract_source_fallback_tickers(
        "Generic corporate update",
        "",
        "https://www.prnewswire.com/news-releases/generic-302701199.html",
        "",
        {"IDE"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["IDE"] == ("prn_table", 0.62)
    assert hits["IDE"][1] < MIN_PERSIST_CONFIDENCE

def test_source_page_fetch_skips_wrong_host(monkeypatch, clean_page_cache):
    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("wrong host should not be fetched")

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )

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

def test_globenewswire_timezone_token_does_not_validate_from_body_keywords(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>NANO Nuclear Energy Inc. (NASDAQ: NNE) is a leading advanced nuclear micro modular reactor (MMR) and technology company.</p>
            <p>James Walker will speak at 9:50am BST/4:50am ET and again at 2:30pm BST/9:30am ET.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "BST", "BlackRock Science and Technology Trust", "BlackRock")]
    )
    hits = _extract_source_fallback_tickers(
        "NANO Nuclear to Participate in Upcoming Investor and Industry Events",
        (
            "New York, N.Y., April 13, 2026 (GLOBE NEWSWIRE) -- "
            "NANO Nuclear Energy Inc. (NASDAQ: NNE), a leading advanced nuclear "
            "micro modular reactor (MMR) and technology company."
        ),
        "https://www.globenewswire.com/news-release/2026/04/13/3272493/0/en/example.html",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "BST" not in hits

def test_globenewswire_timezone_token_can_validate_from_entry_keywords(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Join the webcast at 2:00pm BST tomorrow.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "BST", "BlackRock Science and Technology Trust", "BlackRock")]
    )

    hits = _extract_source_fallback_tickers(
        "BlackRock Science and Technology Trust webcast details",
        "Investor webcast information for the fund",
        "https://www.globenewswire.com/news-release/2026/04/13/example.html",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert hits["BST"][0] == "validated_token"
    assert hits["BST"][1] >= MIN_PERSIST_CONFIDENCE

def test_globenewswire_timezone_token_not_validated_from_slug_only(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>Join the webcast at 2:00pm BST tomorrow.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    hits = _extract_source_fallback_tickers(
        "Generic corporate update",
        "Join the webcast at 2:00pm BST tomorrow",
        "https://www.globenewswire.com/news-release/2026/04/13/blackrock-science-technology-webcast.html",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        {"BST"},
        timeout_seconds=5,
        config=config,
        symbol_keywords={
            "BST": frozenset({"blackrock", "technology", "science"})
        },
    )

    assert "BST" not in hits


def test_globenewswire_cenergy_cet_notice_does_not_match_central_securities(monkeypatch):
    config = PAGE_FETCH_CONFIGS["globenewswire"]

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <article>
            <p>CENERGY HOLDINGS SA</p>
            <p>The Meeting is to be held on Tuesday, 26 May 2026 at 10.00 a.m. CET
            at its registered offices.</p>
            <p>The Company must receive the confirmation by Wednesday, 20 May 2026
            at 5.00 p.m. (CET) at the latest.</p>
            <p>Shareholders must record the shares by Tuesday, 12 May 2026,
            at midnight (CET) (the Record Date).</p>
            <p>Owners of dematerialised shares must request their financial institution
            or central securities depositary to issue a certificate.</p>
          </article>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    sym_kws = _build_symbol_keywords(
        [(1, "CET", "Central Securities Corporation", "Central Securities Corp")]
    )
    hits = _extract_source_fallback_tickers(
        "CONVENING NOTICE TO ATTEND THE ANNUAL ORDINARY SHAREHOLDERS' MEETING TO BE HELD ON 26 MAY 2026",
        "The board of directors of Cenergy Holdings SA invites shareholders to participate in its AGM.",
        "https://www.globenewswire.com/news-release/2026/04/21/3278232/0/en/example.html",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        {"CET"},
        timeout_seconds=5,
        config=config,
        symbol_keywords=sym_kws,
    )

    assert "CET" not in hits


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
