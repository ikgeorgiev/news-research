"""Real locks and concurrent sessions, only on the explicit disposable DB."""
import threading
from pathlib import Path

import httpx
import pytest
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.scheduler import IngestionScheduler

@pytest.fixture
def pg(monkeypatch, disposable_postgres_engine):
    monkeypatch.setattr('app.scheduler.get_engine', lambda: disposable_postgres_engine)
    return disposable_postgres_engine, sessionmaker(bind=disposable_postgres_engine)


def test_leader_lock_survives_worker_commits_and_excludes_second_owner(pg):
    engine,factory=pg
    settings=Settings(_env_file=None)
    first,second=IngestionScheduler(settings,factory),IngestionScheduler(settings,factory)
    acquired,release=threading.Event(),threading.Event()
    errors=[]
    def own(connection):
        connection.exec_driver_sql('SELECT 1');connection.commit()
        with factory() as db:db.commit()
        acquired.set()
        assert release.wait(3)
    def run():
        try:first._run_with_global_lock(own)
        except Exception as exc:errors.append(exc)
    thread=threading.Thread(target=run);thread.start()
    try:
        assert acquired.wait(2)
        called=[]
        second._run_with_global_lock(lambda _:called.append(True))
        assert called==[]
    finally:
        release.set();thread.join(3)
    assert not thread.is_alive() and not errors
    second._run_with_global_lock(lambda _:called.append(True))
    assert called==[True]


def test_actual_ingestion_repolls_bw_while_gnw_is_slow(pg,monkeypatch):
    engine,factory=pg
    from app.sources import BUSINESSWIRE_FEEDS
    from app.models import Ticker
    settings=Settings(_env_file=None,scheduler_enabled=True,ingest_cooldown_seconds=.08,
        ingestion_max_workers=2,source_enable_yahoo=False,source_enable_prn=False,
        source_enable_gn=True,source_enable_bw=True,source_enable_financialcontent=False,
        tickers_csv_path=str(Path(__file__).parents[2]/'data/cef_tickers.csv'),
        raw_feed_prune_interval_seconds=0)
    # A tiny existing registry avoids depending on the user's changing CSV.
    with factory() as db:
        db.add(Ticker(symbol='GOF',fund_name='Guggenheim Strategic Opportunities Fund',active=True))
        db.commit()
    monkeypatch.setattr('app.source_scheduler._load_tickers_from_csv_if_changed',lambda *_:dict(loaded=1,created=0,updated=0,unchanged=1))
    release,slow_started,fast_repeated=threading.Event(),threading.Event(),threading.Event()
    counts={}
    def fetch(*,feed_url,**kwargs):
        counts[feed_url]=counts.get(feed_url,0)+1
        if 'globenewswire.com' in feed_url:
            slow_started.set()
            assert release.wait(5)
        elif feed_url==BUSINESSWIRE_FEEDS[0].url and counts[feed_url]>=2:
            fast_repeated.set()
        return httpx.Response(200,content=b'<rss version="2.0"><channel><title>Wire</title></channel></rss>',request=httpx.Request('GET',feed_url))
    monkeypatch.setattr('app.article_ingest._fetch_feed_with_retries',fetch)
    scheduler=IngestionScheduler(settings,factory)
    scheduler.start()
    try:
        assert slow_started.wait(3)
        assert fast_repeated.wait(3),scheduler.snapshot()
        assert sum(count for url,count in counts.items() if 'globenewswire.com' in url)==1
        assert scheduler.run_once() is None
    finally:
        release.set();scheduler.shutdown()
    assert scheduler._thread is None
    with engine.connect() as conn:
        result=conn.exec_driver_sql('SELECT pg_try_advisory_lock(%s)',(settings.ingestion_advisory_lock_key,)).scalar()
        assert result is True
        conn.exec_driver_sql('SELECT pg_advisory_unlock(%s)',(settings.ingestion_advisory_lock_key,));conn.commit()
