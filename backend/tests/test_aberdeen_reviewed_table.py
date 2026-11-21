"""Offline regression from the retained November 10 structured publisher body.

The HTML envelope is reconstructed, not an original saved network response.
"""
import copy
import csv
from datetime import datetime, timezone
from html import escape
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from lxml import html

from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_context import _build_symbol_keywords
from app.ticker_extraction import _extract_source_html_tickers, _verified_ticker_hits

FIXTURES = Path(__file__).parent / 'fixtures/correctness'
SAVED = json.loads((FIXTURES / 'aberdeen-1726-reader.json').read_text(encoding='utf-8'))
TITLE = SAVED['title']
URL = SAVED['original_url']
FEED = 'https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss'
EXPECTED = set('ACP AGD AOD ASGI AWP FAX HQH HQL IAF IFN MFM MGF MIN MMT THQ THW'.split())
RENAMED = {'MFM', 'MMT'}


def document():
    blocks = []
    for block in SAVED['blocks']:
        if block['type'] == 'table':
            rows = []
            for row in block['rows']:
                cells = ''.join(f'<td>{escape(cell["text"])}</td>' for cell in row)
                rows.append(f'<tr>{cells}</tr>')
            blocks.append('<table>' + ''.join(rows) + '</table>')
        elif 'text' in block:
            blocks.append('<p>' + escape(block['text']) + '</p>')
        elif 'items' in block:
            blocks.append('<ul>' + ''.join('<li>' + escape(x) + '</li>' for x in block['items']) + '</ul>')
    return (f'<html><head><meta charset="utf-8"><link rel="canonical" href="{URL}">'
            f'<meta name="date" content="{SAVED["published_at"]}"></head><body><article>'
            f'<h1>{escape(TITLE)}</h1><div class="release-body"><div class="release-content">'
            + ''.join(blocks) + '</div></div></article></body></html>')


@pytest.fixture
def registry():
    with (FIXTURES / 'registry-at-start.csv').open(encoding='utf-8-sig', newline='') as handle:
        return [dict(row) for row in csv.DictReader(handle) if row['active'].upper() == 'TRUE']


def extract(page, registry, *, url=URL, title=TITLE, source='prnewswire', summary=''):
    known = {r['ticker'] for r in registry}
    profiles = _build_symbol_keywords([(i, r['ticker'], r['fund_name'], r['sponsor'],
                                       r.get('validation_keywords') or None) for i, r in enumerate(registry)])
    return _verified_ticker_hits(source, _extract_source_html_tickers(
        title, summary, url, FEED, known, page, PAGE_FETCH_CONFIGS[source], symbol_keywords=profiles))


def test_saved_aberdeen_table_assigns_all_sixteen_without_extra_funds(registry):
    assert set(extract(document(), registry)) == EXPECTED


@pytest.mark.parametrize('symbol', sorted(RENAMED))
@pytest.mark.parametrize('change', ['wrong_name', 'other_symbol', 'wrong_exchange', 'wrong_amount',
    'wrong_column', 'conflicting_row', 'disabled', 'changed_registry_name', 'changed_sponsor', 'override'])
def test_unreviewed_row_or_registry_cannot_gain_rename_support(registry, symbol, change):
    doc = html.fromstring(document())
    row = doc.xpath('//td[normalize-space(.)=$symbol]/parent::tr', symbol=symbol)[0]
    reg = next(r for r in registry if r['ticker'] == symbol)
    if change == 'wrong_name': row[2].text = 'Unrelated Investment Company'
    elif change == 'other_symbol': row[0].text = 'CMU' if symbol == 'MFM' else 'CXE'
    elif change == 'wrong_exchange': row[1].text = 'Reference'
    elif change == 'wrong_amount': row[3].text = 'Reference'
    elif change == 'wrong_column': row[0].text, row[3].text = row[3].text, row[0].text
    elif change == 'conflicting_row':
        other = copy.deepcopy(row); other[2].text = 'Unrelated Fund'; row.addnext(other)
    elif change == 'disabled': registry.remove(reg)
    elif change == 'changed_registry_name': reg['fund_name'] = 'Different Fund'
    elif change == 'changed_sponsor': reg['sponsor'] = 'Different Sponsor'
    elif change == 'override': reg['validation_keywords'] = 'manual override'
    hits = set(extract(html.tostring(doc, encoding='unicode'), registry))
    assert symbol not in hits
    assert RENAMED - {symbol} <= hits
    assert not hits - EXPECTED


@pytest.mark.parametrize('change', ['header', 'outside_article', 'outside_release', 'outside_release_body', 'sidebar',
    'missing_canonical', 'wrong_canonical', 'duplicate_canonical', 'wrong_title', 'wrong_h1', 'duplicate_h1',
    'other_host', 'other_source', 'http', 'query', 'fragment', 'missing_date', 'old_date', 'conflicting_dates', 'heading_outside_article'])
def test_source_identity_and_distribution_table_scope_are_required(registry, change):
    doc = html.fromstring(document()); title = TITLE; url = URL; source = 'prnewswire'
    table = doc.xpath('//td[normalize-space(.)="Ticker"]/ancestor::table[1]')[0]
    canonical = doc.xpath('//link[@rel="canonical"]')[0]
    if change == 'header': table.xpath('.//tr')[0][2].text = 'Reference'
    elif change == 'outside_article': doc.xpath('//body')[0].append(table)
    elif change == 'outside_release': doc.xpath('//article')[0].append(table)
    elif change == 'outside_release_body':
        body = doc.xpath('//div[@class="release-content"]')[0]
        doc.xpath('//article')[0].append(body)
    elif change == 'sidebar':
        aside = html.Element('aside'); table.getparent().append(aside); aside.append(table)
    elif change == 'missing_canonical': canonical.getparent().remove(canonical)
    elif change == 'wrong_canonical': canonical.set('href', URL.replace('302935591', '999999999'))
    elif change == 'duplicate_canonical': canonical.addnext(copy.deepcopy(canonical))
    elif change == 'wrong_title': title = 'Other release'
    elif change == 'wrong_h1': doc.xpath('//h1')[0].text = 'Other release'
    elif change == 'duplicate_h1': doc.xpath('//body')[0].append(html.fragment_fromstring('<h1>Other release</h1>'))
    elif change == 'heading_outside_article': doc.xpath('//body')[0].append(doc.xpath('//h1')[0])
    elif change == 'other_host': url = URL.replace('www.prnewswire.com', 'example.com')
    elif change == 'other_source': source = 'businesswire'
    elif change == 'http': url = URL.replace('https:', 'http:')
    elif change == 'query': url += '?other=1'
    elif change == 'fragment': url += '#other'
    elif change == 'missing_date': doc.xpath('//meta[@name="date"]')[0].getparent().remove(doc.xpath('//meta[@name="date"]')[0])
    elif change == 'old_date': doc.xpath('//meta[@name="date"]')[0].set('content', '2026-06-07T12:00:00Z')
    elif change == 'conflicting_dates':
        doc.xpath('//head')[0].append(html.fragment_fromstring('<meta property="article:published_time" content="2025-01-01T12:00:00Z">'))
    hits = set(extract(html.tostring(doc, encoding='unicode'), registry, url=url, title=title, source=source))
    assert not hits & RENAMED
    assert not hits - EXPECTED


def test_readability_can_omit_table_without_losing_verified_rows(registry, monkeypatch):
    monkeypatch.setattr('app.ticker_extraction._extract_article_body', lambda _: 'Distribution details follow.')
    assert set(extract(document(), registry)) == EXPECTED


def test_title_hint_and_name_in_other_row_cannot_assign_a_fund(registry):
    doc = html.fromstring(document())
    for symbol in RENAMED:
        row = doc.xpath('//td[normalize-space(.)=$symbol]/parent::tr', symbol=symbol)[0]
        row.getparent().remove(row)
    assert not set(extract(html.tostring(doc, encoding='unicode'), registry,
                           summary='MFM MMT Aberdeen Municipal Income Fund Aberdeen Multi-Market Income Fund')) & RENAMED


def test_each_rename_has_its_own_reviewed_effective_date(registry):
    doc = html.fromstring(document())
    doc.xpath('//meta[@name="date"]')[0].set('content', '2026-06-10T12:00:00Z')
    hits = set(extract(html.tostring(doc, encoding='unicode'), registry))
    assert 'MFM' in hits and 'MMT' not in hits


@pytest.fixture
def pipeline(registry, scratch_dir, monkeypatch):
    import httpx
    from app import article_ingest, article_enrichment, ticker_extraction
    from app.models import Source, Ticker
    from app.ticker_context import load_ticker_context
    from tests.helpers import build_sqlite_session_factory, call_ingest, seed_source
    monkeypatch.setattr(article_enrichment, 'utc_now', lambda: datetime(2026, 11, 10, 21, 18, tzinfo=timezone.utc))
    engine, factory = build_sqlite_session_factory(scratch_dir / 'aberdeen.db')
    with factory() as db:
        source = seed_source(db, code='prnewswire', name='PR Newswire', base_url='https://www.prnewswire.com')
        for row in registry:
            db.add(Ticker(symbol=row['ticker'], fund_name=row['fund_name'], sponsor=row['sponsor'], active=True,
                          validation_keywords=row.get('validation_keywords') or None))
        db.commit(); source_id = source.id
    entries = [dict(id=URL, link=URL, title=TITLE, summary='Aberdeen fund distributions are detailed in the release.',
                    published=SAVED['published_at'])]
    requests, alerts = [], []
    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', lambda **_: httpx.Response(
        200, content=b'feed', request=httpx.Request('GET', FEED)))
    monkeypatch.setattr(article_ingest.feedparser, 'parse', lambda _: SimpleNamespace(feed={}, entries=entries))
    def fetch(url, *_):
        requests.append(url); assert url == URL; return document()
    monkeypatch.setattr(ticker_extraction, '_fetch_source_page_html', fetch)
    monkeypatch.setattr(article_enrichment, '_fetch_source_page_html', fetch)
    monkeypatch.setattr(article_ingest, 'notify_new_articles', lambda db, count: alerts.append(count))
    monkeypatch.setattr(article_enrichment, 'notify_new_articles', lambda db, count: alerts.append(count))
    def ingest():
        with factory() as db:
            ctx = load_ticker_context(db)
            return call_ingest(db, db.get(Source, source_id), FEED, known_symbols=ctx.known_symbols,
                               symbol_to_id=ctx.symbol_to_id, symbol_keywords=ctx.symbol_keywords)
    yield SimpleNamespace(factory=factory, ingest=ingest, entries=entries, requests=requests, alerts=alerts)
    engine.dispose()


def persisted(case):
    from sqlalchemy import select
    from app.models import ArticleTicker, Ticker
    with case.factory() as db:
        return set(db.scalars(select(Ticker.symbol).join(ArticleTicker)))


def test_initial_ingestion_and_duplicate_keep_sixteen_without_duplicate_alert(pipeline):
    assert pipeline.ingest()['items_inserted'] == 1
    assert persisted(pipeline) == EXPECTED and pipeline.alerts == [1]
    assert pipeline.ingest()['items_inserted'] == 0
    assert persisted(pipeline) == EXPECTED and pipeline.alerts == [1]
    assert len(pipeline.requests) == 1


def test_enrichment_adds_missing_names_without_replacing_article_or_realerting(pipeline):
    from sqlalchemy import select
    from app import article_enrichment
    from app.models import Article
    pipeline.entries[0]['summary'] = 'abrdn Income Credit Strategies Fund (NYSE: ACP) declares a distribution.'
    assert pipeline.ingest()['items_inserted'] == 1 and persisted(pipeline) == {'ACP'}
    with pipeline.factory() as db:
        row = db.scalars(select(Article)).one()
        original = row.id, row.first_seen_at, row.created_at, row.first_alert_sent_at
    assert article_enrichment.process_one(pipeline.factory)['added_tickers'] == 15
    assert persisted(pipeline) == EXPECTED and pipeline.alerts == [1, 0]
    with pipeline.factory() as db:
        row = db.scalars(select(Article)).one()
        assert (row.id, row.first_seen_at, row.created_at, row.first_alert_sent_at) == original
    assert article_enrichment.process_one(pipeline.factory) is None
    assert pipeline.ingest()['items_inserted'] == 0 and pipeline.alerts == [1, 0]
