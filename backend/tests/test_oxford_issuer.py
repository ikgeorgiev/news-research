"""Verify the issuer-feed refusal and retain archived, offline transport tests."""
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app import article_ingest, ticker_extraction
from app.config import Settings
from app.database import Base
from app.ingestion import _run_feed_ingestion
from app.models import Article, ArticleTicker, FeedPollState, RawFeedItem, Source, Ticker
from app.sources import build_source_feeds
from app.ticker_context import load_ticker_context
from app.utils import to_utc

FIXTURES = Path(__file__).parent / 'fixtures'
ISSUER = 'https://www.globenewswire.com/rssfeed/organization/MqpMjrUEB9C-kuKoReZgvA=='
GUGGENHEIM = 'https://www.globenewswire.com/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=='
BROAD = 'https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies'
PUB = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
SAVED = (FIXTURES/'issuer_feeds/oxford.xml').read_bytes()
BODY = (FIXTURES/'issuer_feeds/oxford.html').read_text(encoding='utf-8')


@pytest.fixture
def setup(monkeypatch):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        source = Source(code='globenewswire', name='GlobeNewswire', base_url='https://www.globenewswire.com')
        db.add(source)
        registry = FIXTURES/'correctness/registry-at-start.csv'
        with registry.open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.DictReader(handle):
                if row['active'].upper() == 'TRUE':
                    db.add(Ticker(symbol=row['ticker'], fund_name=row['fund_name'], sponsor=row['sponsor'],
                                  validation_keywords=row.get('validation_keywords') or None, active=True))
        db.commit()
        calls = []
        body_calls = []
        def fetch(url, timeout, headers):
            assert url == ISSUER
            calls.append(dict(url=url, timeout=timeout, headers=dict(headers)))
            return httpx.Response(200, content=SAVED, headers={'ETag':'saved-oxlc'}, request=httpx.Request('GET',url))
        def body(url, *_):
            assert '/3391193/' in url, 'Historical or unrelated body request'
            body_calls.append(url)
            return BODY
        monkeypatch.setattr(article_ingest, '_fetch_issuer_feed', fetch)
        monkeypatch.setattr(ticker_extraction, '_fetch_source_page_html', body)
        monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries',
                            lambda **_: httpx.Response(304,request=httpx.Request('GET',BROAD)))
        def run(cutoff=PUB-timedelta(seconds=1), session=None):
            session = session or db
            ctx = load_ticker_context(session)
            source_row = session.scalar(select(Source).where(Source.code == 'globenewswire'))
            return article_ingest.ingest_feed(session, source=source_row, feed_url=ISSUER,
                known_symbols=ctx.known_symbols, symbol_to_id=ctx.symbol_to_id,
                symbol_keywords=ctx.symbol_keywords, timeout_seconds=5, issuer_feed_start=cutoff)
        yield db, run, calls, body_calls, engine
    engine.dispose()


def gnw_feeds(settings, db):
    return next(row for row in build_source_feeds(settings,db,
        now=settings.guggenheim_issuer_feed_start or PUB) if row.code == 'globenewswire').feeds


def settings(**kwargs):
    return Settings(_env_file=None,source_enable_yahoo=False,source_enable_prn=False,source_enable_bw=False,**kwargs)


@pytest.mark.parametrize('legacy_setting', [None, '2026-10-29T14:00:00+02:00', 'invalid-old-value'])
def test_stale_oxford_setting_never_enrolls_refused_feed(setup, monkeypatch, legacy_setting):
    db, *_ = setup
    monkeypatch.delenv('OXFORD_LANE_ISSUER_FEED_START', raising=False)
    if legacy_setting is not None:
        monkeypatch.setenv('OXFORD_LANE_ISSUER_FEED_START', legacy_setting)
    assert [(row.url,row.published_after) for row in gnw_feeds(settings(),db)] == [(BROAD,None)]


def test_normal_cycle_uses_broad_feed_with_legacy_oxford_setting(setup, monkeypatch):
    db, _, issuer_requests, body_requests, _ = setup
    monkeypatch.setenv('OXFORD_LANE_ISSUER_FEED_START', PUB.isoformat())
    broad_requests = []
    def saved_broad(**kwargs):
        broad_requests.append(kwargs['feed_url'])
        return httpx.Response(304, request=httpx.Request('GET', BROAD))
    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', saved_broad)
    config = settings()
    _run_feed_ingestion(db,config,source_feeds=build_source_feeds(config,db))
    assert broad_requests == [BROAD]
    assert issuer_requests == [] and body_requests == []
    assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 0


def test_guggenheim_opt_in_does_not_enroll_refused_oxford_feed(setup):
    db, *_ = setup
    later = PUB + timedelta(days=4)
    enrolled = gnw_feeds(settings(guggenheim_issuer_feed_start=later,oxford_lane_issuer_feed_start=PUB),db)
    assert [(row.url,row.published_after) for row in enrolled] == [(BROAD,None),(GUGGENHEIM,later)]


def test_master_gnw_switch_excludes_all_gnw_feeds(setup):
    db, *_ = setup
    config = settings(source_enable_gn=False,guggenheim_issuer_feed_start=PUB,oxford_lane_issuer_feed_start=PUB)
    assert all(row.code != 'globenewswire' for row in build_source_feeds(config,db))


@pytest.mark.parametrize('value',['2026-11-03T14:00:00','not-a-date'])
def test_remaining_guggenheim_opt_in_requires_aware_date(value):
    with pytest.raises(ValidationError):
        settings(guggenheim_issuer_feed_start=value)


def test_remaining_guggenheim_opt_in_preserves_enrollment_instant(setup):
    db, *_ = setup
    config = settings(guggenheim_issuer_feed_start='2026-10-29T14:00:00+02:00')
    feed = next(row for row in gnw_feeds(config,db) if row.url == GUGGENHEIM)
    assert feed.published_after == PUB


@pytest.mark.parametrize('cutoff',[PUB,PUB+timedelta(seconds=1),PUB+timedelta(days=5)])
def test_twenty_saved_items_are_baseline_history(setup,cutoff):
    db, run, _, body_calls, _ = setup
    result = run(cutoff)
    assert result['status'] == 'success' and result['items_seen'] == 20
    assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 0
    assert db.scalar(select(func.count()).select_from(Article)) == 0
    assert body_calls == []


def test_legacy_setting_does_not_fetch_or_enqueue_history(setup):
    db, _, calls, body_calls, _ = setup
    config = settings(oxford_lane_issuer_feed_start=PUB)
    _run_feed_ingestion(db,config,source_feeds=build_source_feeds(config,db))
    assert not calls and not body_calls
    assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 0


def test_conditional_request_and_cadence_survive_new_session(setup):
    db, run, calls, _, engine = setup
    assert run()['items_inserted'] == 1
    with Session(engine) as new_session:
        assert run(session=new_session)['status'] == 'skipped_backoff'
        assert len(calls) == 1
        state = new_session.scalar(select(FeedPollState).where(FeedPollState.feed_url == ISSUER))
        assert (to_utc(state.backoff_until)-datetime.now(timezone.utc)).total_seconds() > 55
        state.backoff_until = datetime.now(timezone.utc)-timedelta(seconds=1)
        new_session.commit()
        assert run(session=new_session)['items_inserted'] == 0
        assert calls[-1]['headers']['If-None-Match'] == 'saved-oxlc'
        assert new_session.scalar(select(func.count()).select_from(Article)) == 1


@pytest.mark.parametrize('status',[401,403,404,429])
def test_refusal_and_retry_after_survive_new_session(setup,monkeypatch,status):
    db, run, calls, body_calls, engine = setup
    def refuse(url, *_):
        calls.append(url)
        response = httpx.Response(status,headers={'Retry-After':'3600'},request=httpx.Request('GET',url))
        response.raise_for_status()
    monkeypatch.setattr(article_ingest,'_fetch_issuer_feed',refuse)
    assert run()['status'] == 'failed'
    with Session(engine) as next_session:
        state = next_session.scalar(select(FeedPollState).where(FeedPollState.feed_url == ISSUER))
        until = to_utc(state.backoff_until)
        assert (until-datetime.now(timezone.utc)).total_seconds() > 3590
        if status != 429:
            assert until.year == 9999
        assert run(session=next_session)['status'] == 'skipped_backoff'
    assert len(calls) == 1 and not body_calls


@pytest.mark.parametrize('payload',[b'<html>Unavailable</html>',b'<rss><channel>broken'])
def test_failed_document_does_not_persist_article_or_validators(setup,monkeypatch,payload):
    db, run, _, body_calls, _ = setup
    monkeypatch.setattr(article_ingest,'_fetch_issuer_feed',lambda url,*_: httpx.Response(
        200,content=payload,headers={'ETag':'bad'},request=httpx.Request('GET',url)))
    assert run()['status'] == 'failed'
    state = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == ISSUER))
    assert state.etag is None and state.failure_count == 1
    assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 0 and not body_calls


def test_later_broad_original_copy_preserves_article_and_tickers(setup,monkeypatch):
    db, run, _, _, _ = setup
    assert run()['items_inserted'] == 1
    first = db.scalars(select(Article)).one()
    snapshot = (first.id,first.first_seen_at,first.created_at,first.source_name,first.provider_name)
    xml = ET.fromstring(SAVED)
    channel = xml.find('channel')
    for item in list(channel.findall('item')):
        if '/3391193/' not in item.findtext('link'):
            channel.remove(item)
    monkeypatch.setattr(article_ingest,'_fetch_feed_with_retries',lambda **_: httpx.Response(
        200,content=ET.tostring(xml),request=httpx.Request('GET',BROAD)))
    ctx = load_ticker_context(db)
    result = article_ingest.ingest_feed(db,source=db.scalar(select(Source)),feed_url=BROAD,
        known_symbols=ctx.known_symbols,symbol_to_id=ctx.symbol_to_id,symbol_keywords=ctx.symbol_keywords,timeout_seconds=5)
    assert result['items_inserted'] == 0
    article = db.scalars(select(Article)).one()
    assert (article.id,article.first_seen_at,article.created_at,article.source_name,article.provider_name) == snapshot
    assert db.scalar(select(func.count()).select_from(RawFeedItem)) == 1
    assert set(db.scalars(select(Ticker.symbol).join(ArticleTicker))) == {'OXLC'}
