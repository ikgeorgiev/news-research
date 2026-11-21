from datetime import timedelta
import os
import threading
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, event, func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app import article_enrichment as coverage, article_ingest
from app.database import Base
from app.models import Article, ArticleEnrichment, ArticleTicker, RawFeedItem, Source, Ticker
from app.ticker_context import load_ticker_context
from tests.helpers import build_sqlite_session_factory, call_ingest, seed_source, seed_ticker

HTML = '''<article><h1>AllianceBernstein Closed-End Funds Announce Distribution Rates</h1>
<table><tr><td>AllianceBernstein Global High Income Fund (NYSE: AWF)</td><td>$0.1</td></tr>
<tr><td>AllianceBernstein National Municipal Income Fund (NYSE: AFB)</td><td>$0.067</td></tr></table></article>'''
FEED = "https://www.prnewswire.com/rss/mutual-funds.rss"


@pytest.fixture()
def case(scratch_dir, monkeypatch):
    url = os.getenv("DATABASE_URL", "")
    schema = "coverage_test_" + uuid4().hex
    if url.startswith("postgresql+psycopg://"):
        admin = create_engine(url)
        with admin.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_engine(url, connect_args={"options": f"-c search_path={schema} -c statement_timeout=5000"})
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, autoflush=False)
    else:
        admin = None
        engine, factory = build_sqlite_session_factory(scratch_dir / "coverage.db")
    with factory() as db:
        source = seed_source(db, code="prnewswire", name="PR Newswire")
        seed_ticker(db, symbol="AWF", fund_name="AllianceBernstein Global High Income", sponsor="AllianceBernstein")
        seed_ticker(db, symbol="AFB", fund_name="AllianceBernstein National Mun Inc", sponsor="AllianceBernstein")
        source_id = source.id
    entries = [{
        "id": "coverage-one", "title": "AllianceBernstein Closed-End Funds Announce Distribution Rates",
        "link": "https://www.prnewswire.com/news-releases/funds-1.html",
        "published": coverage.utc_now().isoformat(),
        "summary": "AllianceBernstein Global High Income Fund (NYSE: AWF) declares a distribution...",
    }]
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries", lambda **_: httpx.Response(
        200, content=b"feed", request=httpx.Request("GET", FEED)))
    monkeypatch.setattr(article_ingest.feedparser, "parse", lambda _: SimpleNamespace(feed={}, entries=entries))
    # Feed evidence alone must commit without a synchronous fallback.
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", lambda *a, **k: pytest.fail("inline page request"))
    monkeypatch.setattr(coverage, "_fetch_source_page_html", lambda *a: HTML)
    # Exercise the table route even when readability omits the table.
    monkeypatch.setattr("app.ticker_extraction._extract_article_body", lambda _: "Distribution dates are listed in the table.")
    notifications = []
    monkeypatch.setattr(article_ingest, "notify_new_articles", lambda db, count: notifications.append(count))
    monkeypatch.setattr(coverage, "notify_new_articles", lambda db, count: notifications.append(count))

    def ingest(**kwargs):
        with factory() as db:
            ctx = load_ticker_context(db)
            return call_ingest(db, db.get(Source, source_id), FEED,
                               known_symbols=ctx.known_symbols, symbol_to_id=ctx.symbol_to_id,
                               symbol_keywords=ctx.symbol_keywords, **kwargs)

    try:
        yield SimpleNamespace(factory=factory, engine=engine, entries=entries,
                              source_id=source_id, notifications=notifications, ingest=ingest)
    finally:
        engine.dispose()
        if admin is not None:
            with admin.begin() as conn:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            admin.dispose()


def symbols(case):
    with case.factory() as db:
        return set(db.scalars(select(Ticker.symbol).join(ArticleTicker)))


def make_due(case):
    with case.factory() as db:
        for job in db.scalars(select(ArticleEnrichment)):
            job.available_at = coverage.utc_now() - timedelta(seconds=1)
        db.commit()


@pytest.mark.parametrize('kind,attempted,status', [('refused',True,403),('refused',True,404),('unknown_in_flight',False,None)])
def test_durable_page_hold_preserves_postcommit_job_without_expiring_or_notifying(case,monkeypatch,kind,attempted,status):
    from app.page_access import PageOutcome,set_page_outcome
    assert case.ingest()['items_inserted']==1
    before=list(case.notifications)
    def page(*args):
        set_page_outcome(PageOutcome(kind,attempted,status));return None
    monkeypatch.setattr(coverage,'_fetch_source_page_html',page)
    assert coverage.process_one(case.factory)['status']=='held'
    with case.factory() as db:
        job=db.scalar(select(ArticleEnrichment))
        assert job.status=='held' and job.completed_at is None and job.attempts==int(attempted)
        assert db.scalar(select(func.count()).select_from(Article))==1
    make_due(case)
    assert coverage.process_one(case.factory) is None
    assert case.notifications==before


def test_postcommit_rate_limit_does_not_spend_an_http_attempt(case,monkeypatch):
    from app.page_access import PageOutcome,set_page_outcome
    assert case.ingest()['items_inserted']==1
    deadline=coverage.utc_now()+timedelta(hours=2)
    def page(*args):
        set_page_outcome(PageOutcome('backoff',False,429,deadline));return None
    monkeypatch.setattr(coverage,'_fetch_source_page_html',page)
    assert coverage.process_one(case.factory)['status']=='retry'
    with case.factory() as db:
        job=db.scalar(select(ArticleEnrichment))
        from app.utils import to_utc
        assert job.attempts==0 and to_utc(job.available_at)>=deadline and job.completed_at is None


def test_initial_visibility_and_other_ingestion_while_page_is_blocked(case, monkeypatch):
    assert case.ingest()["items_inserted"] == 1
    entered, release = threading.Event(), threading.Event()
    def page(*_):
        entered.set()
        assert release.wait(5)
        return HTML
    monkeypatch.setattr(coverage, "_fetch_source_page_html", page)
    errors = []
    def worker():
        try:
            coverage.process_one(case.factory)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert entered.wait(5)
        assert symbols(case) == {"AWF"}
        assert case.notifications == [1]
        assert coverage.process_one(case.factory) is None  # competing consumer skips lease
        with case.factory() as db:
            # A separate connection can lock the job during page HTTP work.
            assert db.scalar(select(ArticleEnrichment).with_for_update(nowait=True)) is not None
        case.entries[:] = [{**case.entries[0], "id": "coverage-two",
                            "title": "AllianceBernstein new portfolio report NYSE: AWF",
                            "link": "https://www.prnewswire.com/news-releases/funds-2.html"}]
        assert case.ingest()["items_inserted"] == 1
    finally:
        release.set()
        thread.join(6)
    assert not thread.is_alive()
    assert not errors
    assert symbols(case) == {"AWF", "AFB"}
    assert case.notifications == [1, 1, 0]


def test_additive_coverage_survives_raw_retention_and_replay(case):
    assert case.ingest()["items_inserted"] == 1
    with case.factory() as db:
        db.execute(delete(RawFeedItem))
        db.commit()
    assert coverage.process_one(case.factory)["added_tickers"] == 1
    assert symbols(case) == {"AWF", "AFB"}
    assert coverage.process_one(case.factory) is None
    assert case.ingest()["items_inserted"] == 0
    with case.factory() as db:
        assert db.scalar(select(func.count()).select_from(Article)) == 1
        assert db.scalar(select(func.count()).select_from(ArticleEnrichment)) == 1
        assert db.scalar(select(ArticleEnrichment.status)) == "done"
    assert symbols(case) == {"AWF", "AFB"}
    assert case.notifications == [1, 0]


def test_failed_page_backoff_and_restart(case, monkeypatch):
    case.ingest()
    monkeypatch.setattr(coverage, "_fetch_source_page_html", lambda *_: None)
    assert coverage.process_one(case.factory)["status"] == "retry"
    assert coverage.process_one(case.factory) is None
    assert symbols(case) == {"AWF"}
    with case.factory() as db:
        job = db.scalar(select(ArticleEnrichment))
        assert job.attempts == 1
        assert (coverage.to_utc(job.available_at) - coverage.utc_now()).total_seconds() > 50
    make_due(case)
    monkeypatch.setattr(coverage, "_fetch_source_page_html", lambda *_: HTML)
    assert coverage.process_one(case.factory)["added_tickers"] == 1


def test_failed_commit_rolls_back_mapping_and_recovers_after_lease(case, monkeypatch):
    case.ingest()
    def fail_commit(db):
        if any(isinstance(row, ArticleEnrichment) and row.status == "notify" for row in db.dirty):
            raise SQLAlchemyError("controlled commit failure")
    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(SQLAlchemyError, match="controlled"):
            coverage.process_one(case.factory)
    finally:
        event.remove(Session, "before_commit", fail_commit)
    assert symbols(case) == {"AWF"}
    assert case.notifications == [1]
    assert coverage.process_one(case.factory) is None
    make_due(case)
    assert coverage.process_one(case.factory)["added_tickers"] == 1
    assert case.notifications == [1, 0]


def test_expired_claim_cannot_overwrite_new_worker(case):
    case.ingest()
    stale = coverage.claim_one(case.factory)
    make_due(case)
    current = coverage.claim_one(case.factory)
    assert stale.token != current.token
    assert coverage._finish(case.factory, stale, HTML) == ("stale_lease", 0)
    assert symbols(case) == {"AWF"}
    assert coverage._finish(case.factory, current, HTML) == ("notify", 1)


def test_initial_article_and_queue_commit_atomically(case):
    def fail_commit(db):
        if db.scalar(select(func.count()).select_from(ArticleEnrichment)):
            raise SQLAlchemyError("initial commit failure")
    event.listen(Session, "before_commit", fail_commit)
    try:
        # ingest_feed may also fail while finalizing the run after rollback.
        result = case.ingest()
        assert result["status"] == "failed"
    finally:
        event.remove(Session, "before_commit", fail_commit)
    with case.factory() as db:
        assert db.scalar(select(func.count()).select_from(Article)) == 0
        assert db.scalar(select(func.count()).select_from(ArticleEnrichment)) == 0
    assert case.notifications == []
    assert case.ingest()["items_inserted"] == 0  # feed-level error backoff


def test_concurrent_feed_replay_and_worker_keep_consistent_lock_order(case, monkeypatch):
    if case.engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL advisory-lock ordering")
    case.ingest()
    claim = coverage.claim_one(case.factory)
    with case.factory() as db:
        db.execute(delete(RawFeedItem))
        db.commit()
    producer_ready, consumer_started = threading.Event(), threading.Event()
    enqueue = article_ingest.enqueue_article_enrichment
    lock = coverage.acquire_article_dedupe_locks
    def held_producer(*args, **kwargs):
        producer_ready.set()
        assert consumer_started.wait(5)
        return enqueue(*args, **kwargs)
    def consumer_lock(*args):
        consumer_started.set()
        return lock(*args)
    monkeypatch.setattr(article_ingest, "enqueue_article_enrichment", held_producer)
    monkeypatch.setattr(coverage, "acquire_article_dedupe_locks", consumer_lock)
    results = []
    thread = threading.Thread(target=lambda: results.append(case.ingest()))
    thread.start()
    try:
        assert producer_ready.wait(5)
        assert coverage._finish(case.factory, claim, HTML) == ("notify", 1)
    finally:
        consumer_started.set()
        thread.join(6)
    assert not thread.is_alive()
    assert results[0]["status"] == "success"
    assert symbols(case) == {"AWF", "AFB"}


@pytest.mark.parametrize("change", ["inactive", "keywords"])
def test_validation_uses_context_changed_during_page_fetch(case, monkeypatch, change):
    case.ingest()
    def page(*_):
        with case.factory() as db:
            ticker = db.scalar(select(Ticker).where(Ticker.symbol == "AFB"))
            if change == "inactive":
                ticker.active = False
            else:
                ticker.fund_name, ticker.sponsor = "Unrelated Mining Trust", "Unrelated"
            db.commit()
        return HTML
    monkeypatch.setattr(coverage, "_fetch_source_page_html", page)
    assert coverage.process_one(case.factory)["added_tickers"] == 0
    assert symbols(case) == {"AWF"}
    assert case.notifications == [1]


def test_notification_retry_does_not_refetch_or_duplicate(case, monkeypatch):
    case.ingest()
    def fail(*_):
        raise RuntimeError("controlled notification failure")
    monkeypatch.setattr(coverage, "notify_new_articles", fail)
    assert coverage.process_one(case.factory)["notification_ok"] is False
    assert symbols(case) == {"AWF", "AFB"}
    make_due(case)
    monkeypatch.setattr(coverage, "_fetch_source_page_html", lambda *_: pytest.fail("refetch"))
    monkeypatch.setattr(coverage, "notify_new_articles", lambda db, count: case.notifications.append(count))
    assert coverage.process_one(case.factory)["notification_ok"] is True
    assert coverage.process_one(case.factory) is None
    assert case.notifications == [1, 0]


@pytest.mark.parametrize("reason", ["disabled", "old", "foreign"])
def test_eligibility_avoids_disabled_history_or_foreign_page_work(case, reason):
    if reason == "old":
        case.entries[0]["published"] = (coverage.utc_now() - timedelta(days=3)).isoformat()
    if reason == "foreign":
        case.entries[0]["link"] = "https://example.com/release"
    assert case.ingest(postcommit_enrichment_enabled=reason != "disabled")["items_inserted"] == 1
    assert coverage.process_one(case.factory) is None


@pytest.mark.parametrize("limit", ["age", "attempts", "source_disabled"])
def test_expiry_and_disabled_source_do_not_fetch(case, monkeypatch, limit):
    case.ingest()
    with case.factory() as db:
        job = db.scalar(select(ArticleEnrichment))
        if limit == "age":
            job.created_at = coverage.utc_now() - timedelta(days=2)
        elif limit == "attempts":
            job.attempts = coverage.MAX_ATTEMPTS
        else:
            db.get(Source, case.source_id).enabled = False
        db.commit()
    monkeypatch.setattr(coverage, "_fetch_source_page_html", lambda *_: pytest.fail("unexpected page request"))
    assert coverage.process_one(case.factory) is None
    assert symbols(case) == {"AWF"}
