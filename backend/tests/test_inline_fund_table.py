import pytest

from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_extraction import (
    _build_symbol_keywords,
    _extract_source_fallback_tickers,
    _verified_ticker_hits,
)


def extract(monkeypatch, *, title, summary, cell, symbols, profiles):
    html = f'<article><table><tr><th>Fund and distribution</th></tr><tr><td>{cell}</td><td>$0.067</td></tr></table></article>'
    monkeypatch.setattr('app.ticker_extraction._fetch_source_page_html', lambda *_: html)
    # Readability extraction can omit the distribution table altogether.
    monkeypatch.setattr('app.ticker_extraction._extract_article_body', lambda _: 'Distribution dates are listed in the table.')
    return _verified_ticker_hits('prnewswire', _extract_source_fallback_tickers(
        title, summary, 'https://www.prnewswire.com/news-releases/funds.html', '',
        symbols, 4, PAGE_FETCH_CONFIGS['prnewswire'], symbol_keywords=profiles,
    ))


def test_recovers_second_fund_from_inline_exchange_table_when_body_omits_it(monkeypatch):
    profiles = _build_symbol_keywords([
        (1, 'AWF', 'AllianceBernstein Global High Income', 'AllianceBernstein'),
        (2, 'AFB', 'AllianceBernstein National Mun Inc', 'AllianceBernstein'),
    ])
    hits = extract(monkeypatch,
        title='AllianceBernstein Closed-End Funds Announce Distribution Rates',
        summary='AllianceBernstein Global High Income Fund (NYSE: AWF) declares a distribution...',
        cell='AllianceBernstein National Municipal Income Fund, Inc. (NYSE: <a>AFB</a>)',
        symbols={'AWF', 'AFB'}, profiles=profiles)
    assert hits == {'AWF': ('exchange', .88), 'AFB': ('prn_table', .84)}


def test_table_fund_name_supplies_validation_when_readable_body_has_no_fund_names(monkeypatch):
    profiles = _build_symbol_keywords([(1, 'AFB', 'AllianceBernstein National Mun Inc', 'AllianceBernstein')])
    hits = extract(monkeypatch, title='Monthly distributions', summary='',
        cell='AllianceBernstein National Municipal Income Fund (NYSE: <a>AFB</a>)',
        symbols={'AFB'}, profiles=profiles)
    assert hits == {'AFB': ('prn_table', .84)}


@pytest.mark.parametrize('cell', [
    'Acme Factory Brands (NYSE: <a>AFB</a>)',
    'AllianceBernstein distribution notes: AFB is an internal reference',
    'AllianceBernstein National Municipal Income Fund (NYSE: <a>UNKNOWN</a>)',
])
def test_inline_table_does_not_accept_unvalidated_or_unlisted_symbols(monkeypatch, cell):
    profiles = _build_symbol_keywords([(1, 'AFB', 'AllianceBernstein National Mun Inc', 'AllianceBernstein')])
    assert extract(monkeypatch, title='AllianceBernstein update', summary='', cell=cell,
        symbols={'AFB'}, profiles=profiles) == {}


def test_common_word_table_symbol_still_requires_headline_support(monkeypatch):
    profiles = _build_symbol_keywords([(1, 'FUND', 'Sprott Focus Trust', 'Sprott Asset Management USA Inc.')])
    assert extract(monkeypatch, title='Corporate results', summary='',
        cell='Sprott Focus Trust (NASDAQ: <a>FUND</a>)', symbols={'FUND'}, profiles=profiles) == {}
