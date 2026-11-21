import asyncio
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path

import httpx
import pytest

from app.free_news import (DiscoveryStore, ROUTES, HELD_GNW_FEED, fetch_metadata,
                           bounded_routes, load_routes, next_delay, parse_metadata, parse_issuer)


def payload(item_id='first', **changes):
    row = dict(id=item_id,title='Fund announces distribution',
               url='https://www.globenewswire.com/news-release/2026/11/16/1234567/0/en/Fund.html',
               published_at='2026-11-16T10:00:00Z',publisher={'name':'GlobeNewswire'})
    row.update(changes)
    return json.dumps({'data':[row]}).encode()


def test_metadata_retains_original_identity_but_never_assigns_query_tickers():
    row=parse_metadata(payload())[0]
    assert row['wire_identity'] == ['globenewswire','1234567','en']
    assert row['validated_tickers'] == []
    assert row['review_only']


def test_bw_label_and_yahoo_url_do_not_fabricate_wire_identity():
    row=parse_metadata(payload(url='https://finance.yahoo.com/news/fund.html',publisher={'name':'Business Wire'}))[0]
    assert row['wire_identity'] is None
    assert row['identity_basis'] == 'unresolved'


@pytest.mark.parametrize('url',['http://unsafe.test/a','https://user:password@safe.test/a','https://safe.test:8080/a','https://safe.test/a\n'])
def test_unsafe_metadata_links_fail_closed(url):
    with pytest.raises(ValueError): parse_metadata(payload(url=url))


def test_provider_ids_are_unique_and_unknown_dates_remain_unknown():
    item=json.loads(payload())['data'][0]
    with pytest.raises(ValueError): parse_metadata(json.dumps({'data':[item,item]}).encode())
    assert parse_metadata(payload(published_at='2026-11-16'))[0]['published_at'] is None


def test_saved_historical_metadata_replays_without_promoting_provider_labels():
    fixture=json.loads((Path(__file__).parent/'fixtures/free_metadata.json').read_text())
    rows=parse_metadata(json.dumps(fixture).encode())
    assert len(rows)==7
    assert sum(row['wire_identity'] is not None for row in rows)==4
    assert all(not row['validated_tickers'] for row in rows)


def test_baselines_versions_and_new_ids_stay_distinct(tmp_path):
    store=DiscoveryStore(tmp_path/'review.sqlite')
    try:
        now=datetime(2026,11,16,10,tzinfo=timezone.utc).timestamp()
        store.begin_session('pressreleasefeed','a')
        def save(at,body):
            assert store.claim('pressreleasefeed',at)
            store.finish('pressreleasefeed',at,200,{},body,parse_metadata(body))
        save(now,payload())
        save(now+300,payload())  # Exact duplicate adds a response, not an observation.
        save(now+600,payload('second'))
        save(now+900,payload('second',title='Corrected title'))
        kinds=[r[0] for r in store.db.execute('SELECT kind FROM observation')]
        assert kinds == ['baseline','matching_metadata_new_id','updated']
        first=store.db.execute("SELECT first_seen FROM item WHERE provider_id='first'").fetchone()[0]
        assert first == now
        store.begin_session('pressreleasefeed','b')
        save(now+1200,payload())
        assert store.db.execute('SELECT kind FROM observation ORDER BY id DESC LIMIT 1').fetchone()[0]=='baseline'
        assert store.db.execute('SELECT count(*) FROM response').fetchone()[0]==5
    finally:store.close()


@pytest.mark.parametrize('status,error',[(400,None),(401,None),(403,None),(404,None),(422,None),(302,None),(200,'schema_failure')])
def test_refusals_and_schema_failures_survive_reopen(tmp_path,status,error):
    path=tmp_path/'review.sqlite'
    store=DiscoveryStore(path)
    store.begin_session('pressreleasefeed','a')
    assert store.claim('pressreleasefeed',100)
    store.finish('pressreleasefeed',101,status,{},b'',[],error)
    store.close()
    store=DiscoveryStore(path)
    try:
        store.begin_session('pressreleasefeed','b')
        assert not store.claim('pressreleasefeed',999999)
        assert store.snapshot()['routes'][0]['hold']
    finally:store.close()


def test_uncertain_inflight_request_is_not_replayed(tmp_path):
    path=tmp_path/'review.sqlite'
    store=DiscoveryStore(path);store.begin_session('pressreleasefeed','a')
    assert store.claim('pressreleasefeed',100)
    store.close()
    store=DiscoveryStore(path)
    try:
        store.begin_session('pressreleasefeed','b')
        assert not store.claim('pressreleasefeed',99999)
    finally:store.close()


def test_retry_after_and_cache_are_host_wide(tmp_path):
    store=DiscoveryStore(tmp_path/'review.sqlite')
    try:
        for key in ('one','two'):store.begin_session(key,'a','https://www.globenewswire.com/rssfeed/'+key)
        assert store.claim('one',100)
        assert not store.claim('two',101)
        store.finish('one',102,429,{'retry-after':'900','cache-control':'max-age=1200'},b'',[])
        assert not store.claim('two',1301)
        assert store.claim('two',1302)
        assert next_delay({'cache-control':'public, s-maxage=800, max-age=60'},100)==800
    finally:store.close()


def test_transport_failure_requires_new_success_baseline(tmp_path):
    store=DiscoveryStore(tmp_path/'review.sqlite')
    try:
        store.begin_session('pressreleasefeed','a')
        assert store.claim('pressreleasefeed',100)
        store.finish('pressreleasefeed',100,None,{},b'',[],error='ReadTimeout')
        assert store.claim('pressreleasefeed',400)
        body=payload()
        store.finish('pressreleasefeed',400,200,{},body,parse_metadata(body))
        assert store.db.execute('SELECT kind FROM observation').fetchone()[0]=='baseline'
    finally:store.close()


def test_http_does_not_follow_redirect_or_read_error_body():
    calls=[]
    class ExplodingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError('Error body must not be consumed')
            yield b''
    def handle(request):
        calls.append(request)
        assert 'cookie' not in request.headers and 'authorization' not in request.headers
        return httpx.Response(302,headers={'location':'https://example.invalid'},stream=ExplodingBody())
    result=asyncio.run(fetch_metadata(ROUTES['pressreleasefeed'],transport=httpx.MockTransport(handle)))
    assert result[0]==302 and result[2]==b'' and len(calls)==1


def test_http_byte_limit_and_total_deadline(monkeypatch):
    from app import free_news
    monkeypatch.setattr(free_news,'MAX_BYTES',10)
    with pytest.raises(ValueError):
        asyncio.run(fetch_metadata(ROUTES['pressreleasefeed'],transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'a'*11))))
    timeout=asyncio.timeout
    monkeypatch.setattr(free_news.asyncio,'timeout',lambda _:timeout(.02))
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            await asyncio.sleep(1)
            yield b'a'
    with pytest.raises(TimeoutError):
        asyncio.run(fetch_metadata(ROUTES['pressreleasefeed'],transport=httpx.MockTransport(lambda r:httpx.Response(200,stream=Slow()))))


def test_custom_feeds_require_supplied_urls_and_keep_oxford_hold(tmp_path):
    path=tmp_path/'feeds.json'
    path.write_text(json.dumps({'version':1,'feeds':[]}))
    assert load_routes(path)==ROUTES
    row={'url':HELD_GNW_FEED,'provenance':'user_supplied_reader_feed'}
    path.write_text(json.dumps({'version':1,'feeds':[row]}))
    with pytest.raises(ValueError,match='oxford'):load_routes(path)
    row['url']='https://www.globenewswire.com/rssfeed/organization/SYNTHETIC123456'
    path.write_text(json.dumps({'version':1,'feeds':[row]}))
    assert len(load_routes(path))==3
    row['url']='https://attacker.test/rssfeed/a'
    path.write_text(json.dumps({'version':1,'feeds':[row]}))
    with pytest.raises(ValueError):load_routes(path)


def test_xml_entities_and_wrong_issuer_links_are_rejected():
    with pytest.raises(ValueError):parse_issuer(b'<!DOCTYPE rss><rss/>')
    body=b'<rss version="2.0"><channel><title>XAI</title><item><title>News</title><link>https://other.test/a</link></item></channel></rss>'
    with pytest.raises(ValueError):parse_issuer(body)


def test_metadata_query_is_bounded_and_stable_through_the_day():
    from urllib.parse import parse_qs, urlsplit
    morning=datetime(2026,11,16,10,tzinfo=timezone.utc)
    evening=datetime(2026,11,16,21,tzinfo=timezone.utc)
    urls=bounded_routes(ROUTES,morning)
    assert urls==bounded_routes(ROUTES,evening)
    assert urls['xai']==ROUTES['xai']
    query=parse_qs(urlsplit(urls['pressreleasefeed']).query)
    assert query=={'q':['closed-end'],'per_page':['100'],'since':['2026-11-13']}


def test_existing_store_preserves_controls_and_records_actual_request_url(tmp_path):
    path=tmp_path/'old.sqlite'
    store=DiscoveryStore(path)
    store.begin_session('pressreleasefeed','a')
    assert store.claim('pressreleasefeed',100)
    store.finish('pressreleasefeed',101,429,{'retry-after':'900'},b'',[])
    controls=[tuple(r) for r in store.db.execute('SELECT * FROM control')]
    store.close()
    with sqlite3.connect(path) as db:
        db.execute('ALTER TABLE response DROP COLUMN url')
    store=DiscoveryStore(path)
    try:
        assert [tuple(r) for r in store.db.execute('SELECT * FROM control')]==controls
        assert not store.claim('pressreleasefeed',1000)
        assert store.snapshot()['discovery_status']=='unavailable'
        assert store.claim('pressreleasefeed',1001)
        url=bounded_routes(ROUTES,datetime(2026,11,16,tzinfo=timezone.utc))['pressreleasefeed']
        store.finish('pressreleasefeed',1002,200,{},b'{}',[],url=url)
        assert store.db.execute('SELECT url FROM response ORDER BY id DESC LIMIT 1').fetchone()[0]==url
        assert store.snapshot()['discovery_status']=='available'
    finally:store.close()
