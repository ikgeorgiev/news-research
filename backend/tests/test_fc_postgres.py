"""Only an explicitly disposable database; real concurrent writes and PG LISTEN."""
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
import httpx
import psycopg
import pytest
from sqlalchemy import func,select
from sqlalchemy.orm import Session,sessionmaker
from app import article_ingest
from app.pg_utils import to_psycopg_conninfo
from app.models import Article,ArticleTicker,RawFeedItem,Source
from tests.helpers import call_ingest,seed_source,seed_ticker
from app.financialcontent.delivery import deliver
from tests.test_fc_delivery import saved_entry


@pytest.fixture
def database(disposable_postgres_engine):
    factory = sessionmaker(bind=disposable_postgres_engine, autoflush=False)
    url = disposable_postgres_engine.url.render_as_string(hide_password=False)
    with psycopg.connect(to_psycopg_conninfo(url), autocommit=True) as listener:
        listener.execute('LISTEN new_articles')
        yield factory, listener


@pytest.mark.parametrize('symbol,folder',[('SABA','validation-1902-saba'),('CET','validation-2059-cet')])
@pytest.mark.parametrize('order',[('financialcontent','yahoo','businesswire'),('businesswire','financialcontent','yahoo'),('yahoo','businesswire','financialcontent')])
def test_three_source_race_and_replay_deliver_exactly_once(database,monkeypatch,symbol,folder,order):
    factory,listener=database
    entry=saved_entry(folder)
    with factory() as db:
        sources={code:seed_source(db,code=code,name=code).id for code in order}
        ticker_id=seed_ticker(db,symbol=symbol).id
    wire_id=entry['id'].split(':')[1]
    by_source={'yahoo':{**entry,'id':'yahoo-'+wire_id,'link':f'https://finance.yahoo.com/news/{wire_id}.html'},
               'businesswire':{**entry,'link':f'https://www.businesswire.com/news/home/{wire_id}/en/'}}
    monkeypatch.setattr(article_ingest,'_fetch_feed_with_retries',lambda **k:httpx.Response(200,content=k['feed_url'].encode(),request=httpx.Request('GET',k['feed_url'])))
    monkeypatch.setattr(article_ingest.feedparser,'parse',lambda content:SimpleNamespace(feed={},entries=[by_source[content.decode().split('/')[-1]]]))
    monkeypatch.setattr(article_ingest,'_source_page_fetch_requires_network',lambda *a:False)
    def forbidden(*a,**k):raise AssertionError('No HTTP fallback in saved fixture race')
    monkeypatch.setattr(article_ingest,'_extract_source_fallback_tickers',forbidden)
    gate=threading.Barrier(3)
    def task(code):
        gate.wait(timeout=3)
        if code=='financialcontent':return deliver(factory,entry,listing_seen_at='fixture',body_received_at='fixture')['created']
        with factory() as db:
            source=db.get(Source,sources[code])
            result=call_ingest(db,source=source,feed_url='https://offline.invalid/'+code,
                known_symbols={symbol},symbol_to_id={symbol:ticker_id},persistence_policy_override='validated_mapping_required',postcommit_enrichment_enabled=False)
            assert result['status']=='success',result
            return result['items_inserted']
    with ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(task,order))
    assert sum(results)==1
    with ThreadPoolExecutor(max_workers=3) as pool:assert sum(pool.map(task,order))==0
    notices=list(listener.notifies(timeout=.15,stop_after=2))
    assert [n.payload for n in notices]==['1']
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(Article))==1
        assert db.scalar(select(func.count()).select_from(ArticleTicker))==1
        raws=db.scalars(select(RawFeedItem)).all()
        assert len(raws)==3 and len({r.article_id for r in raws})==1
        assert all(r.article_id for r in raws)


def test_failed_commit_does_not_publish_or_notify(database,monkeypatch):
    factory,listener=database
    with factory() as db:
        seed_source(db,code='financialcontent');seed_ticker(db,symbol='SABA')
    original=Session.commit
    def fail_commit(self):raise RuntimeError('injected after notification enqueue')
    monkeypatch.setattr(Session,'commit',fail_commit)
    with pytest.raises(RuntimeError,match='injected'):
        deliver(factory,saved_entry('validation-1902-saba'),listing_seen_at='fixture',body_received_at='fixture')
    monkeypatch.setattr(Session,'commit',original)
    assert list(listener.notifies(timeout=.15,stop_after=1))==[]
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(Article))==0
        assert db.scalar(select(func.count()).select_from(RawFeedItem))==0
