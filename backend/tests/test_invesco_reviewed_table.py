"""Saved issuer documents exercised through the actual application extractor."""
import csv
import copy
from datetime import datetime, timezone
from pathlib import Path

import pytest
from lxml import html

from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_context import _build_symbol_keywords
from app.ticker_extraction import _extract_source_html_tickers, _verified_ticker_hits

FIXTURES = Path(__file__).parent / 'fixtures'
TITLE = 'Invesco Closed-End Funds Declare Dividends'
FEED = 'https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss'
EXPECTED = set('IIM IQI OIA VBF VCV VGM VKI VKQ VLT VMO VPV VTN VVR'.split())
MISSED = set('VCV VGM VKI VMO VPV VTN'.split())
CASES = [('302926053', FIXTURES/'publisher_reader/invesco.html'),
         ('302896150', FIXTURES/'publisher_reader/invesco-october.html')]
SUMMARY = ('ATLANTA, Nov. 2, 2026 /PRNewswire/ -- The Board of Trustees of each of the Invesco '
           'closed-end funds listed below declared dividends. EX-DATE RECORD DATE REINVEST DATE '
           'PAYABLE DATE 11/16/2026 11/16/2026 11/30/2026 11/30/2026 Name of Closed-End '
           'Management Investment Company Ticker Monthly...')


def article_url(wire):
    return f'https://www.prnewswire.com/news-releases/invesco-closed-end-funds-declare-dividends-{wire}.html'


@pytest.fixture
def registry():
    with (FIXTURES/'correctness/registry-at-start.csv').open(encoding='utf-8-sig', newline='') as f:
        return [dict(row) for row in csv.DictReader(f) if row['active'].upper() == 'TRUE']


def context(rows):
    return {r['ticker'] for r in rows}, _build_symbol_keywords([
        (i, r['ticker'], r['fund_name'], r['sponsor'], r.get('validation_keywords') or None)
        for i, r in enumerate(rows)])


def extract(page, link, rows, title=TITLE, summary=SUMMARY, source='prnewswire'):
    known, profiles = context(rows)
    return _verified_ticker_hits(source, _extract_source_html_tickers(
        title, summary, link, FEED, known, page, PAGE_FETCH_CONFIGS[source], symbol_keywords=profiles))


@pytest.mark.parametrize('wire,path', CASES)
def test_both_saved_releases_keep_all_thirteen_funds(wire, path, registry):
    hits = extract(path.read_text(encoding='utf-8'), article_url(wire), registry)
    assert set(hits) == EXPECTED, {'missing': sorted(EXPECTED-set(hits)), 'extra': sorted(set(hits)-EXPECTED)}


@pytest.mark.parametrize('symbol', sorted(MISSED))
@pytest.mark.parametrize('mutation', ['different_row_name', 'wrong_column', 'disabled', 'registry_name_changed',
                                      'sponsor_changed', 'explicit_override', 'conflicting_duplicate'])
def test_invalid_relationship_cannot_gain_a_verified_ticker(symbol, mutation, registry):
    wire, path = CASES[0]
    doc = html.fromstring(path.read_bytes())
    cell = doc.xpath('//td[normalize-space(.)=$symbol]', symbol=symbol)[0]
    row = cell.getparent()
    reg = next(r for r in registry if r['ticker'] == symbol)
    if mutation == 'different_row_name':
        row[0].clear(); row[0].text = 'Invesco Unrelated Municipal Fund'
    elif mutation == 'wrong_column':
        cell.clear(); cell.text = 'Reference'
        row[3].clear(); row[3].text = symbol
    elif mutation == 'disabled':
        registry.remove(reg)
    elif mutation == 'registry_name_changed':
        reg['fund_name'] = 'Different Fund'
    elif mutation == 'sponsor_changed':
        reg['sponsor'] = 'Different Sponsor'
    elif mutation == 'explicit_override':
        reg['validation_keywords'] = 'invesco'
    else:
        conflicting = copy.deepcopy(row)
        conflicting[0].clear(); conflicting[0].text = 'Invesco Unrelated Municipal Fund'
        row.addnext(conflicting)
    hits = extract(html.tostring(doc, encoding='unicode'), article_url(wire), registry)
    assert symbol not in hits
    assert MISSED - {symbol} <= set(hits)
    assert not set(hits) - EXPECTED


@pytest.mark.parametrize('mutation', ['ticker_header', 'table_purpose', 'outside_article', 'outside_release',
                                      'wrong_canonical', 'missing_canonical', 'duplicate_canonical', 'wrong_title',
                                      'wrong_h1', 'duplicate_h1', 'other_host', 'wrong_wire_id', 'query', 'fragment',
                                      'http', 'different_source', 'dividend_column'])
def test_source_or_table_context_mismatch_cannot_add_reviewed_names(mutation, registry):
    wire, path = CASES[0]
    doc = html.fromstring(path.read_bytes())
    table = doc.xpath("//td[normalize-space(.)='Ticker']/ancestor::table[1]")[0]
    link, title, source = article_url(wire), TITLE, 'prnewswire'
    if mutation in ('ticker_header', 'table_purpose', 'dividend_column'):
        target = {'ticker_header': 'Ticker', 'table_purpose': 'Name of Closed-End Management Investment Company',
                  'dividend_column': 'Monthly Dividend Per Share'}[mutation]
        cell = next(c for c in table.xpath('.//td') if ' '.join(' '.join(c.itertext()).split()) == target)
        cell.clear(); cell.text = 'Reference'
    elif mutation == 'outside_article':
        doc.xpath('//body')[0].append(table)
    elif mutation == 'outside_release':
        doc.xpath('//article')[0].append(table)
    elif mutation in ('wrong_canonical', 'missing_canonical', 'duplicate_canonical'):
        canonical = doc.xpath('//link[@rel="canonical"]')[0]
        if mutation == 'wrong_canonical': canonical.set('href', article_url('999999999'))
        elif mutation == 'missing_canonical': canonical.getparent().remove(canonical)
        else: canonical.addnext(copy.deepcopy(canonical))
    elif mutation == 'wrong_title': title = 'Other release'
    elif mutation == 'wrong_h1': doc.xpath('//h1')[0].text = 'Other release'
    elif mutation == 'duplicate_h1': doc.xpath('//body')[0].append(html.fragment_fromstring('<h1>Other release</h1>'))
    elif mutation == 'other_host': link = link.replace('www.prnewswire.com', 'example.org')
    elif mutation == 'wrong_wire_id': link = article_url('999999999')
    elif mutation == 'query': link += '?other=1'
    elif mutation == 'fragment': link += '#other'
    elif mutation == 'http': link = link.replace('https://', 'http://')
    elif mutation == 'different_source': source = 'businesswire'
    hits = extract(html.tostring(doc, encoding='unicode'), link, registry, title=title, source=source)
    assert not MISSED & set(hits)
    assert not set(hits) - EXPECTED


def test_readability_omitting_the_table_does_not_hide_the_validated_rows(registry, monkeypatch):
    monkeypatch.setattr('app.ticker_extraction._extract_article_body', lambda _: 'Payment dates appear below.')
    wire, path = CASES[0]
    assert set(extract(path.read_text(encoding='utf-8'), article_url(wire), registry)) == EXPECTED


@pytest.fixture
def pipeline(registry, scratch_dir, monkeypatch):
    import httpx
    from types import SimpleNamespace
    from sqlalchemy.orm import Session
    from app import article_ingest, article_enrichment, ticker_extraction
    from app.models import Source, Ticker
    from app.ticker_context import load_ticker_context
    from tests.helpers import build_sqlite_session_factory, call_ingest, seed_source
    # This saved 17:00 release exercises the fresh-article enrichment path.
    replay_time = datetime(2026, 11, 2, 17, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(article_enrichment, 'utc_now', lambda: replay_time)
    engine, factory = build_sqlite_session_factory(scratch_dir/'invesco.db')
    with factory() as db:
        source = seed_source(db, code='prnewswire', name='PR Newswire', base_url='https://www.prnewswire.com')
        for r in registry:
            db.add(Ticker(symbol=r['ticker'], fund_name=r['fund_name'], sponsor=r['sponsor'], active=True,
                          validation_keywords=r.get('validation_keywords') or None))
        db.commit()
        source_id = source.id
    wire, path = CASES[0]
    body = path.read_text(encoding='utf-8')
    entries = [{'id': article_url(wire), 'link': article_url(wire), 'title': TITLE,
                'summary': SUMMARY, 'published': '2026-11-02T17:00:00Z'}]
    pages = {article_url(wire): body}
    body_requests, alerts = [], []
    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', lambda **_: httpx.Response(
        200, content=b'feed', request=httpx.Request('GET', FEED)))
    monkeypatch.setattr(article_ingest.feedparser, 'parse', lambda _: SimpleNamespace(feed={}, entries=entries))
    def fetch_page(url, *_):
        body_requests.append(url)
        assert url in pages
        return pages[url]
    monkeypatch.setattr(ticker_extraction, '_fetch_source_page_html', fetch_page)
    monkeypatch.setattr(article_enrichment, '_fetch_source_page_html', fetch_page)
    monkeypatch.setattr(article_ingest, 'notify_new_articles', lambda db, count: alerts.append(count))
    monkeypatch.setattr(article_enrichment, 'notify_new_articles', lambda db, count: alerts.append(count))
    def ingest():
        with factory() as db:
            ctx = load_ticker_context(db)
            return call_ingest(db, db.get(Source, source_id), FEED, known_symbols=ctx.known_symbols,
                               symbol_to_id=ctx.symbol_to_id, symbol_keywords=ctx.symbol_keywords)
    yield SimpleNamespace(factory=factory, ingest=ingest, entries=entries, pages=pages,
                          body_requests=body_requests, alerts=alerts)
    engine.dispose()


def persisted_symbols(case):
    from sqlalchemy import select
    from app.models import ArticleTicker, Ticker
    with case.factory() as db:
        return set(db.scalars(select(Ticker.symbol).join(ArticleTicker)))


def test_initial_ingestion_and_duplicate_preserve_all_thirteen(pipeline):
    from sqlalchemy import func, select
    from app.models import Article, RawFeedItem
    first = pipeline.ingest()
    assert first['items_inserted'] == 1
    assert persisted_symbols(pipeline) == EXPECTED
    assert pipeline.alerts == [1]
    assert len(pipeline.body_requests) == 1
    assert pipeline.ingest()['items_inserted'] == 0
    assert persisted_symbols(pipeline) == EXPECTED and pipeline.alerts == [1]
    with pipeline.factory() as db:
        assert db.scalar(select(func.count()).select_from(Article)) == 1
        assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 1


def test_postcommit_enrichment_adds_the_other_twelve_without_replacing_the_article(pipeline):
    from sqlalchemy import select
    from app import article_enrichment
    from app.models import Article
    pipeline.entries[0]['summary'] = 'Invesco Value Municipal Income Trust (NYSE: IIM) declares its dividend.'
    assert pipeline.ingest()['items_inserted'] == 1
    assert persisted_symbols(pipeline) == {'IIM'}
    with pipeline.factory() as db:
        a = db.scalars(select(Article)).one()
        original = (a.id, a.first_seen_at, a.created_at, a.first_alert_sent_at)
    assert pipeline.body_requests == []
    assert article_enrichment.process_one(pipeline.factory)['added_tickers'] == 12
    assert persisted_symbols(pipeline) == EXPECTED
    with pipeline.factory() as db:
        a = db.scalars(select(Article)).one()
        assert (a.id, a.first_seen_at, a.created_at, a.first_alert_sent_at) == original
    assert pipeline.alerts == [1, 0]
    assert article_enrichment.process_one(pipeline.factory) is None


def test_enrichment_reloads_disabled_and_changed_registry_rows(pipeline):
    from sqlalchemy import select
    from app import article_enrichment
    from app.models import Ticker
    pipeline.entries[0]['summary'] = 'Invesco Value Municipal Income Trust (NYSE: IIM) declares its dividend.'
    assert pipeline.ingest()['items_inserted'] == 1
    with pipeline.factory() as db:
        db.scalar(select(Ticker).where(Ticker.symbol == 'VKI')).active = False
        db.scalar(select(Ticker).where(Ticker.symbol == 'VGM')).fund_name = 'Different Fund'
        db.scalar(select(Ticker).where(Ticker.symbol == 'VMO')).sponsor = 'Different Sponsor'
        db.scalar(select(Ticker).where(Ticker.symbol == 'VCV')).validation_keywords = 'invesco'
        db.commit()
    article_enrichment.process_one(pipeline.factory)
    assert persisted_symbols(pipeline) == EXPECTED - {'VKI', 'VGM', 'VMO', 'VCV'}


def test_distinct_wire_ids_with_the_same_headline_remain_separate(pipeline):
    from sqlalchemy import func, select
    from app.models import Article, ArticleTicker, RawFeedItem, Ticker
    assert pipeline.ingest()['items_inserted'] == 1
    original = article_url(CASES[0][0])
    other = article_url('302926054')
    pipeline.pages[other] = pipeline.pages[original].replace(original, other)
    pipeline.entries[:] = [{**pipeline.entries[0], 'id': other, 'link': other}]
    assert pipeline.ingest()['items_inserted'] == 1
    with pipeline.factory() as db:
        articles = db.scalars(select(Article)).all()
        assert len(articles) == 2
        assert {a.canonical_url for a in articles} == {original, other}
        for a in articles:
            assert set(db.scalars(select(Ticker.symbol).join(ArticleTicker).where(ArticleTicker.article_id == a.id))) == EXPECTED
        assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 2
    assert pipeline.alerts == [1, 1] and len(pipeline.body_requests) == 2
