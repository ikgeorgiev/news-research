from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
from email.utils import format_datetime
import threading
from types import SimpleNamespace

import httpx,pytest
from sqlalchemy import select,text
from sqlalchemy.orm import sessionmaker

from app.models import SourcePageControl,RawFeedItem,Article
from app import sources,page_access,article_ingest
from app.enrichment_retry import pending_retry,retry_is_due,due_enrichment_entries
from app.page_access import PageAccessStore,PageOutcome,Permit,control_keys
from tests.helpers import seed_source,seed_ticker,call_ingest

URL='https://www.businesswire.com/news/home/20261030264836/en/Saved-Fixture'
OTHER='https://www.businesswire.com/news/home/20261030123456/en/Other'
CFG=sources.PAGE_FETCH_CONFIGS['businesswire']
NOW=datetime(2026,10,30,20,tzinfo=timezone.utc)

@pytest.fixture
def pg(disposable_postgres_engine):
    factory = sessionmaker(bind=disposable_postgres_engine, autoflush=False)
    store = PageAccessStore(factory)
    page_access.configure_page_access(store)
    yield store, factory, disposable_postgres_engine
    page_access.configure_page_access(None)

@pytest.fixture
def transport(pg,monkeypatch):
    store,factory,_=pg;clock=[NOW.timestamp()];calls=[];cache=OrderedDict()
    def fetch(status,url=URL,retry_after=None):
        def get(request_url,**kwargs):
            calls.append(request_url)
            return httpx.Response(status,content=b'<p>Complete saved fixture</p>' if status==200 else b'',headers={'Retry-After':retry_after} if retry_after else {})
        monkeypatch.setattr(sources.http_client,'get_http_client',lambda:SimpleNamespace(get=get))
        return sources._fetch_source_page_html(url,4,CFG,cache=cache,cache_lock=threading.Lock(),headers={},cache_ttl_seconds=600,failure_cache_ttl_seconds=60,cache_max_items=10,now_fn=lambda:clock[0])
    return fetch,clock,calls,cache

@pytest.mark.parametrize('status',[401,403,404])
@pytest.mark.parametrize('restart',[False,True])
def test_original_six_refusal_failures_are_fixed(pg,transport,status,restart):
    fetch,clock,calls,cache=transport
    assert fetch(status) is None
    clock[0]+=61
    if restart:
        cache.clear();page_access.configure_page_access(PageAccessStore(pg[1]))
    assert fetch(status) is None
    assert len(calls)==1
    assert page_access.get_page_outcome().kind=='refused'

@pytest.mark.parametrize('restart',[False,True])
def test_original_two_retry_after_failures_are_fixed(pg,transport,restart):
    fetch,clock,calls,cache=transport
    assert fetch(429,retry_after='300') is None
    clock[0]+=61
    if restart:cache.clear();page_access.configure_page_access(PageAccessStore(pg[1]))
    assert fetch(200,OTHER) is None
    assert len(calls)==1
    clock[0]+=240
    assert fetch(200,OTHER)
    assert len(calls)==2

@pytest.mark.parametrize('variant',[
 'http://www.businesswire.com/news/home/20261030264836/en/Different-Slug?ref=copy',
 'https://businesswire.com/news/home/20261030264836/en/',
 'https://www.businesswire.com/news/home/20261030264836/EN/Trademark%C2%AE',
])
def test_original_wire_identity_preserves_refusal_across_variants(transport,variant):
    fetch,clock,calls,cache=transport
    fetch(403);clock[0]+=1000;cache.clear()
    assert fetch(200,variant) is None and len(calls)==1

def test_other_release_and_other_language_are_not_expired_or_deleted(transport):
    fetch,clock,calls,_=transport
    fetch(404)
    assert fetch(200,OTHER)
    assert fetch(200,URL.replace('/en/','/es/'))
    assert len(calls)==3

def test_concurrent_same_page_has_one_request_owner(pg):
    store,factory,_=pg
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes=list(pool.map(lambda _:PageAccessStore(factory).claim(URL,CFG,NOW,4),range(16)))
    assert sum(isinstance(o,Permit) for o in outcomes)==1
    assert sum(isinstance(o,PageOutcome) and o.kind=='in_flight' for o in outcomes)==15

def test_success_cannot_clear_another_inflight_requests_429(pg):
    store,_,_=pg
    a=store.claim(URL,CFG,NOW,4);b=store.claim(OTHER,CFG,NOW,4)
    store.finish(a,status=429,retry_after='300',now=NOW)
    store.finish(b,status=200,usable=True,now=NOW+timedelta(seconds=1))
    assert store.claim(OTHER,CFG,NOW+timedelta(seconds=61),4).kind=='backoff'

def test_http_date_retry_after_is_respected(pg):
    store,_,_=pg;permit=store.claim(URL,CFG,NOW,4)
    result=store.finish(permit,status=429,retry_after=format_datetime(NOW+timedelta(hours=2),usegmt=True),now=NOW)
    assert result.retry_at==NOW+timedelta(hours=2)

@pytest.mark.parametrize('value',['bad','inf','-10',None])
def test_invalid_retry_after_still_backs_off(pg,value):
    store,_,_=pg;permit=store.claim(URL,CFG,NOW,4)
    assert store.finish(permit,status=429,retry_after=value,now=NOW).retry_at==NOW+timedelta(seconds=60)

def test_crashed_inflight_request_is_held_for_review_after_restart(pg):
    store,factory,_=pg;store.claim(URL,CFG,NOW,4)
    outcome=PageAccessStore(factory).claim(URL,CFG,NOW+timedelta(minutes=10),4)
    assert outcome.kind=='unknown_in_flight'
    retry=pending_retry({'attempts':8,'queued_at':'original','next_retry_at':'original-due'},attempted=False,now=NOW,outcome=outcome)
    assert retry['attempts']==8 and retry['queued_at']=='original' and retry['previous_next_retry_at']=='original-due'
    assert not retry_is_due(retry,NOW+timedelta(days=10))

def test_unconfigured_store_fails_closed(pg,transport):
    fetch,_,calls,_=transport;page_access.configure_page_access(None)
    assert fetch(200) is None and calls==[]
    assert page_access.get_page_outcome().kind=='control_unavailable'

def test_store_outage_fails_closed(pg,transport,monkeypatch):
    fetch,_,calls,_=transport
    def fail(*a):raise RuntimeError('Simulated control DB outage')
    monkeypatch.setattr(pg[0],'inspect',fail)
    assert fetch(200) is None and calls==[]

def test_control_commit_survives_unrelated_ingestion_rollback(pg):
    store,factory,_=pg
    with factory() as article_tx:
        article_tx.execute(text('SELECT 1'))
        p=store.claim(URL,CFG,NOW,4);store.finish(p,status=403,now=NOW)
        article_tx.rollback()
    assert PageAccessStore(factory).claim(URL,CFG,NOW+timedelta(days=1),4).kind=='refused'

def test_prior_refusal_import_is_idempotent_and_makes_no_network_attempt(pg):
    store,factory,_=pg
    for _ in range(2):store.seed_refusal(URL,CFG,status=403,observed_at=NOW)
    with factory() as db:
        row=db.get(SourcePageControl,control_keys(URL,'businesswire','businesswire.com')[1])
        assert row.network_attempts==0 and row.refused_at==NOW
    assert store.claim(URL,CFG,NOW+timedelta(days=2),4).kind=='refused'

def test_transient_error_recovers_without_discarding_pending(transport):
    fetch,clock,calls,_=transport
    assert fetch(500) is None
    clock[0]+=61
    assert fetch(200) and len(calls)==2

def test_fresh_valid_feed_evidence_can_resolve_a_held_item(pg,monkeypatch):
    store,factory,_=pg;feed='https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg=='
    entries=[{'id':'20261030264836en','title':'A fund distribution announcement','link':URL,'published':NOW.isoformat(),'summary':'Details on article page.'}]
    def get(url,**kwargs):
        return httpx.Response(403 if '/news/home/' in url else 200,content=b'feed',request=httpx.Request('GET',url))
    monkeypatch.setattr(sources.http_client,'get_http_client',lambda:SimpleNamespace(get=get))
    monkeypatch.setattr(article_ingest,'_fetch_feed_with_retries',lambda **kw:get(feed))
    monkeypatch.setattr(article_ingest.feedparser,'parse',lambda _:SimpleNamespace(feed={},entries=entries))
    monkeypatch.setattr(article_ingest,'notify_new_articles',lambda *_:None)
    with factory() as db:
        source=seed_source(db);ticker=seed_ticker(db,symbol='UTF')
        kwargs=dict(persistence_policy_override='validated_mapping_required',known_symbols={'UTF'},symbol_to_id={'UTF':ticker.id})
        first=call_ingest(db,source,feed,**kwargs)
        assert first['items_inserted']==0
        row=db.scalar(select(RawFeedItem));before=row.raw_payload_json['_page_enrichment']
        assert before['control_state']=='held' and before['last_http_status']==403
        assert due_enrichment_entries(db,source.id,feed)==[]
        entries[0]['summary']='Cohen & Steers Infrastructure Fund (NYSE: UTF) declares its distribution.'
        second=call_ingest(db,source,feed,**kwargs)
        assert second['items_inserted']==1
        db.refresh(row)
        assert row.article_id is not None and '_page_enrichment' not in row.raw_payload_json
    assert store.inspect(URL,CFG,datetime.now(timezone.utc)).kind=='refused'
