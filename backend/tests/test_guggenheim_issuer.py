"""Run against the staged app only, using saved GNW documents and local SQLite."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import httpx
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session
from app import article_ingest, feed_runtime, ticker_extraction
from app.config import Settings
from app.database import Base
from app.models import Article, ArticleTicker, FeedPollState, RawFeedItem, Source, Ticker
from app.sources import build_source_feeds
from app.ticker_context import load_ticker_context
from app.utils import to_utc

FIXTURES = Path(__file__).parent / 'fixtures'
ISSUER = "https://www.globenewswire.com/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=="
BROAD = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies"
PUB = datetime(2026, 11, 2, 21, 45, tzinfo=timezone.utc)
SAVED = (FIXTURES / "issuer_feeds/guggenheim.xml").read_bytes()
BODY = (FIXTURES / "publisher_reader/guggenheim.html").read_text(encoding="utf-8")


@pytest.fixture
def setup(monkeypatch):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        source = Source(code="globenewswire",name="GlobeNewswire",base_url="https://www.globenewswire.com")
        db.add(source)
        with (FIXTURES / "correctness/registry-at-start.csv").open(encoding="utf-8-sig",newline="") as f:
            for r in csv.DictReader(f):
                if r["active"].upper()=="TRUE":
                    db.add(Ticker(symbol=r["ticker"],fund_name=r["fund_name"],sponsor=r["sponsor"],validation_keywords=r.get("validation_keywords") or None,active=True))
        db.commit()
        ctx = load_ticker_context(db)
        calls = []
        def fetch(url, timeout, headers):
            assert url == ISSUER
            calls.append(url)
            return httpx.Response(200,content=SAVED,request=httpx.Request("GET",url))
        monkeypatch.setattr(article_ingest,"_fetch_issuer_feed",fetch)
        monkeypatch.setattr(ticker_extraction,"_fetch_source_page_html",lambda url, *_: BODY if "/3393802/" in url else pytest.fail("Historical body fetch"))
        def run(cutoff=PUB-timedelta(seconds=1), url=ISSUER):
            return article_ingest.ingest_feed(db,source=source,feed_url=url,known_symbols=ctx.known_symbols,
                symbol_to_id=ctx.symbol_to_id,symbol_keywords=ctx.symbol_keywords,timeout_seconds=5,issuer_feed_start=cutoff)
        yield db, run, calls
    engine.dispose()


def test_default_and_explicit_enrollment(setup):
    db, _, _ = setup
    settings=Settings(_env_file=None,source_enable_yahoo=False)
    def gn():
        return next(s for s in build_source_feeds(settings,db,now=PUB) if s.code=="globenewswire").feeds
    assert [f.url for f in gn()]==[BROAD]
    settings.guggenheim_issuer_feed_start=PUB
    assert [(f.url,f.published_after) for f in gn()]==[(BROAD,None),(ISSUER,PUB)]


@pytest.mark.parametrize("cutoff", [PUB, PUB+timedelta(seconds=1)])
def test_start_excludes_existing_twenty(setup,cutoff):
    db,run,_=setup
    result=run(cutoff)
    assert result["status"]=="success" and result["items_seen"]==20
    assert db.scalar(select(func.count()).select_from(RawFeedItem))==0


def test_discovery_tags_cadence_and_duplicate(setup):
    db,run,calls=setup
    res=run()
    assert res["status"]=="success" and res["items_inserted"]==1
    row=db.scalars(select(Article)).one()
    assert set(db.scalars(select(Ticker.symbol).join(ArticleTicker).where(ArticleTicker.article_id==row.id)))=={"AVK","GBAB","GOF","GUG"}
    assert db.scalar(select(func.count()).select_from(RawFeedItem))==1
    assert run()["status"]=="skipped_backoff" and len(calls)==1
    state=db.scalar(select(FeedPollState));state.backoff_until=datetime.now(timezone.utc)-timedelta(seconds=1);db.commit()
    assert run()["items_inserted"]==0 and len(calls)==2
    assert db.scalar(select(func.count()).select_from(Article))==1


@pytest.mark.parametrize("status", [401,403,404,429])
def test_terminal_and_retry_after(setup,monkeypatch,status):
    db,run,_=setup
    hits=[]
    def refuse(*_):
        hits.append(status)
        res=httpx.Response(status,request=httpx.Request("GET",ISSUER),headers={"Retry-After":"3600"})
        res.raise_for_status()
    monkeypatch.setattr(article_ingest,"_fetch_issuer_feed",refuse)
    assert run()["status"]=="failed"
    until=to_utc(db.scalar(select(FeedPollState)).backoff_until)
    assert (until-datetime.now(timezone.utc)).total_seconds()>=3598
    if status!=429: assert until.year==9999
    assert run()["status"]=="skipped_backoff" and hits==[status]


@pytest.mark.parametrize("status,size,expected_error", [(200,4194304,False),(200,4194305,True),(304,0,False),(302,0,True)])
def test_bounded_transport(monkeypatch,status,size,expected_error):
    original=httpx.AsyncClient
    def answer(req):
        assert str(req.url)==ISSUER and req.headers["If-None-Match"]=="abc"
        return httpx.Response(status,content=b"x"*size,headers={"ETag":"def"})
    def client(**opts):
        assert opts["timeout"]<=15 and opts["follow_redirects"] is False
        return original(transport=httpx.MockTransport(answer),**opts)
    monkeypatch.setattr(feed_runtime.httpx,"AsyncClient",client)
    if expected_error:
        with pytest.raises((ValueError,httpx.HTTPStatusError)):feed_runtime._fetch_issuer_feed(ISSUER,60,{"If-None-Match":"abc"})
    else:
        response=feed_runtime._fetch_issuer_feed(ISSUER,60,{"If-None-Match":"abc"})
        assert response.status_code==status and len(response.content)==size and response.headers["etag"]=="def"


def test_application_cycle_passes_activation_cutoff(setup, monkeypatch):
    from app.ingestion import _run_feed_ingestion
    import app.issuer_window as issuer_window
    class Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return PUB if tz else PUB.replace(tzinfo=None)
    monkeypatch.setattr(issuer_window, 'datetime', Fixed)
    monkeypatch.setattr(article_ingest, 'datetime', Fixed)
    db, _, calls = setup
    settings = Settings(_env_file=None, source_enable_yahoo=False, source_enable_bw=False,
                        source_enable_prn=False, guggenheim_issuer_feed_start=PUB)
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries",
                        lambda **_: httpx.Response(304,request=httpx.Request("GET",BROAD)))
    _run_feed_ingestion(db, settings, source_feeds=build_source_feeds(settings,db,now=PUB))
    assert calls==[ISSUER]
    assert db.scalar(select(func.count()).select_from(RawFeedItem))==0


def test_later_broad_feed_copy_deduplicates(setup, monkeypatch):
    import xml.etree.ElementTree as ET
    db, run, _ = setup
    assert run()["items_inserted"] == 1
    first = db.scalars(select(Article)).one()
    original_seen = first.first_seen_at
    xml = ET.fromstring(SAVED)
    channel = xml.find("channel")
    for item in list(channel.findall("item")):
        if "/3393802/" not in item.findtext("link"):
            channel.remove(item)
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries",
                        lambda **_: httpx.Response(200,content=ET.tostring(xml),request=httpx.Request("GET",BROAD)))
    assert run(cutoff=None,url=BROAD)["items_inserted"] == 0
    assert db.scalar(select(func.count()).select_from(Article))==1
    assert db.scalar(select(func.count()).select_from(RawFeedItem))==1
    assert db.scalar(select(func.count()).select_from(ArticleTicker))==4
    assert db.scalars(select(Article)).one().first_seen_at == original_seen
