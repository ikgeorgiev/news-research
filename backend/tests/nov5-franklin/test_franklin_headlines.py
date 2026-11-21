"""Exercise the two November 5 BW entries with a local database and recorded input."""
from datetime import datetime
from email.utils import format_datetime
import json
from pathlib import Path
import socket
from xml.etree import ElementTree as ET

import httpx
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

HERE = Path(__file__).resolve().parent
from app import article_ingest, ticker_context
from app.database import Base
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.ticker_extraction import _extract_entry_tickers, _verified_ticker_hits

DATA = json.loads((HERE / 'fixture.json').read_text(encoding='utf-8'))
FUND_NAMES = {row['symbol']: row['aliases'][0] for row in DATA['reviewed_names']}
ENTRY_BY_SYMBOL = {
    symbol: next(row for row in DATA['entries'] if row['raw_title'].startswith(name + ' Announces '))
    for symbol, name in FUND_NAMES.items()
}
REGISTRY = [(row['id'], row['symbol'], row['fund_name'], row['sponsor'], row['validation_keywords'])
            for row in DATA['registry']]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def reject_network(*args, **kwargs):
        raise AssertionError('Unexpected network access in saved-entry replay')
    monkeypatch.setattr(socket.socket, 'connect', reject_network)


def rss_document(entry):
    rss = ET.Element('rss', version='2.0')
    channel = ET.SubElement(rss, 'channel')
    ET.SubElement(channel, 'title').text = 'Business Wire'
    item = ET.SubElement(channel, 'item')
    ET.SubElement(item, 'title').text = entry['raw_title']
    ET.SubElement(item, 'guid', isPermaLink='false').text = entry['raw_guid']
    ET.SubElement(item, 'link').text = entry['raw_link']
    ET.SubElement(item, 'pubDate').text = format_datetime(datetime.fromisoformat(entry['raw_pub_date']))
    ET.SubElement(item, 'description').text = entry['raw_payload_json']['summary']
    return ET.tostring(rss, encoding='utf-8')


@pytest.mark.parametrize('symbol', ['PMM', 'PMO'])
def test_real_feed_entry_persists_only_its_fund_and_repeat_is_unique(monkeypatch, symbol):
    entry = ENTRY_BY_SYMBOL[symbol]
    feed = DATA['feed_url']
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    requests = []
    alerts = []

    def saved_feed(**kwargs):
        assert kwargs['feed_url'] == feed
        return httpx.Response(200, content=rss_document(entry), request=httpx.Request('GET', feed))

    def unavailable_page(*args, **kwargs):
        requests.append(entry['raw_guid'])
        return None

    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', saved_feed)
    monkeypatch.setattr(article_ingest, '_extract_source_fallback_tickers', unavailable_page)
    monkeypatch.setattr(article_ingest, 'notify_new_articles', lambda db, count: alerts.append(count))
    with Session(engine) as db:
        source = Source(code='businesswire', name='Business Wire', base_url='https://www.businesswire.com', enabled=True)
        db.add(source)
        db.add_all(Ticker(**row, active=True) for row in DATA['registry'])
        db.commit()
        context = ticker_context.load_ticker_context(db)
        for _ in range(2):
            result = article_ingest.ingest_feed(db, source=source, feed_url=feed, known_symbols=context.known_symbols,
                symbol_keywords=context.symbol_keywords, symbol_to_id=context.symbol_to_id, timeout_seconds=1,
                fetch_max_attempts=1)
            assert result['status'] == 'success' and result['items_seen'] == 1, result
        tags = set(db.scalars(select(Ticker.symbol).join(ArticleTicker)))
        assert db.scalar(select(func.count()).select_from(Article)) == 1
        assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 1
        assert tags == {symbol}
        assert requests == []
        assert alerts == [1]
    engine.dispose()


def headline_matches(title, registry=REGISTRY, body='Category: Fund Announcement'):
    profiles = ticker_context._build_symbol_keywords(registry)
    raw = _extract_entry_tickers(title, body, '', DATA['feed_url'], set(profiles), symbol_keywords=profiles)
    return set(_verified_ticker_hits('businesswire', raw))


@pytest.mark.parametrize('title', [
    'Franklin Managed Municipal Income Announces a Tender Offer',
    'Franklin Municipal Opportunities ETF Announces Results',
    'Franklin Managed Municipal Income Trust II Announces a Distribution',
    'Franklin Advisers Announces Municipal Income Results',
    'Franklin Managed Municipal Income Trustworthiness Announces Results',
    'Franklin Municipal Opportunities Trust Series B Announces Results',
])
def test_similar_titles_do_not_gain_a_fund(title):
    assert headline_matches(title).isdisjoint(FUND_NAMES)


@pytest.mark.parametrize('symbol', ['PMM', 'PMO'])
@pytest.mark.parametrize('change', ['registry_name','sponsor','override','inactive'])
def test_reviewed_identity_must_still_match(symbol, change):
    rows = []
    for original in REGISTRY:
        row = list(original)
        if row[1] == symbol:
            if change == 'inactive':
                continue
            row[{'registry_name':2,'sponsor':3,'override':4}[change]] = 'Changed reference'
        rows.append(row)
    assert symbol not in headline_matches(ENTRY_BY_SYMBOL[symbol]['raw_title'], rows)


def test_summary_only_reference_is_insufficient():
    assert not headline_matches('Adviser Consolidation', body=ENTRY_BY_SYMBOL['PMM']['raw_title'])


def test_existing_registry_name_and_explicit_exchange_evidence_survive():
    assert headline_matches('Putnam Managed Muni Income announces results') == {'PMM'}
    assert headline_matches('Other fund announces results', body='NYSE: PMO') == {'PMO'}


@pytest.mark.parametrize('symbol', ['PMM', 'PMO'])
def test_another_active_registry_identity_takes_precedence(symbol):
    rows = REGISTRY + [(9000, 'ZZQ', FUND_NAMES[symbol], 'Another adviser', None)]
    assert headline_matches(ENTRY_BY_SYMBOL[symbol]['raw_title'], rows) == {'ZZQ'}
