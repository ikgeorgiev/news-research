"""Fund identity regressions; these tests never request a publisher page."""
import json
from html import escape
from pathlib import Path

import pytest

from app.constants import MIN_PERSIST_CONFIDENCE
from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_extraction import (
    _build_symbol_keywords,
    _extract_entry_tickers,
    _extract_source_html_tickers,
)


def edf_profile():
    return _build_symbol_keywords([
        (1, "EDF", "Virtus Stone Harbor Emerging Markets Inc",
         "Virtus Investment Advisors (VIA)")
    ])


def test_saved_alaska_release_does_not_assign_edf():
    case = json.loads((Path(__file__).parent / "fixtures/ticker_validation/"
                       "prn_302933435_edf_false_match.json").read_text(encoding="utf8"))
    # Rebuild paragraph markup from the preserved reader text, not raw HTML.
    html = "<html><body><article>" + "".join(
        "<p>" + escape(block["text"]) + "</p>"
        for block in case["body_blocks"] if "text" in block
    ) + "</article></body></html>"
    hits = _extract_source_html_tickers(
        case["title"], case["summary"], case["url"], "", {"EDF"}, html,
        PAGE_FETCH_CONFIGS["prnewswire"], symbol_keywords=edf_profile(),
    )
    assert "EDF" not in hits


@pytest.mark.parametrize("text", [
    "EDF Renewables invests in emerging technology and capital markets",
    "EDF wins contracts in emerging markets",
    "Emerging markets outlook includes EDF Renewables",
])
def test_generic_investment_category_cannot_validate_edf_company(text):
    hits = _extract_entry_tickers(text, "", "https://example.com/news", "",
                                  {"EDF"}, symbol_keywords=edf_profile())
    assert hits.get("EDF", ("", 0))[1] < MIN_PERSIST_CONFIDENCE


@pytest.mark.parametrize("text", [
    "Virtus Stone Harbor Emerging Markets Income Fund (NYSE: EDF) declares distribution",
    "Stone Harbor Emerging Markets Income Fund EDF declares distribution",
    "Virtus Stone Harbor Emerging Markets Inc announces distribution",
])
def test_genuine_edf_identity_still_qualifies(text):
    hits = _extract_entry_tickers(text, "", "https://example.com/news", "",
                                  {"EDF"}, symbol_keywords=edf_profile())
    assert hits["EDF"][1] >= MIN_PERSIST_CONFIDENCE


def test_edf_body_ticker_still_qualifies_with_distinctive_fund_heading():
    hits = _extract_source_html_tickers(
        "Stone Harbor fund declares monthly distribution", "", "https://example.com/news",
        "", {"EDF"}, "<article><p>EDF declared a distribution to shareholders.</p></article>",
        PAGE_FETCH_CONFIGS["prnewswire"], symbol_keywords=edf_profile(),
    )
    assert hits["EDF"][1] >= MIN_PERSIST_CONFIDENCE


def test_reviewed_keywords_remain_explicit_overrides():
    profiles = _build_symbol_keywords([
        (1, "EDF", "Virtus Stone Harbor Emerging Markets Inc", "Virtus", "emerging markets")
    ])
    assert profiles["EDF"].all_keywords == frozenset({"emerging markets"})


@pytest.mark.parametrize("case", json.loads(
    (Path(__file__).parent / "fixtures/ticker_validation/edf_existing_articles.json")
    .read_text(encoding="utf8")
), ids=lambda case: str(case["id"]))
def test_existing_edf_articles_keep_their_verified_exchange_match(case):
    hits = _extract_entry_tickers(case["title"], case["summary"], case["canonical_url"],
                                 "", {"EDF"}, symbol_keywords=edf_profile())
    assert hits["EDF"] == (case["match_type"], case["confidence"])
