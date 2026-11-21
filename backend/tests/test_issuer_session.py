from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
import httpx
import pytest
from sqlalchemy import select, func
from app import article_ingest, sources
from app.config import Settings
from app.issuer_window import issuer_session
from app.models import IngestionRun, FeedPollState, RawFeedItem, Source

UTC = timezone.utc
ENROLL = datetime(2026, 11, 9, 8, tzinfo=UTC)
URL = 'https://www.globenewswire.com/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=='
BODY = b'''<rss version="2.0"><channel><title>Guggenheim</title><item><title>Existing fund release</title><link>https://www.globenewswire.com/news-release/2026/11/09/3400000/0/en/Example.html</link><guid>3400000</guid><pubDate>Mon, 09 Nov 2026 08:01:00 GMT</pubDate></item></channel></rss>'''


@pytest.mark.parametrize('now,expected', [
    ('2026-11-07T17:00:00+00:00',False), ('2026-11-08T15:00:00+00:00',False),
    ('2026-11-09T07:59:59+00:00',False), ('2026-11-09T08:00:00+00:00',True),
    ('2026-11-09T21:59:29+00:00',True), ('2026-11-09T21:59:30+00:00',False),
    ('2026-11-09T22:00:00+00:00',False), ('2026-11-10T06:00:00+00:00',False),
    ('2026-11-10T08:00:00+00:00',True), ('2026-11-14T14:00:00+00:00',False),
])
def test_window_and_cutoff_margin(now, expected):
    assert issuer_session(ENROLL, datetime.fromisoformat(now))[0] is expected


def test_naive_timestamps_rejected():
    with pytest.raises(ValueError):
        issuer_session(ENROLL.replace(tzinfo=None), ENROLL)


def test_enrollment_later_than_session_open():
    enrolled = ENROLL + timedelta(hours=2)
    assert not issuer_session(enrolled, ENROLL)[0]
    assert issuer_session(enrolled, enrolled)[0]


def test_kyiv_summer_time_kept_local():
    summer = datetime(2027, 7, 5, 7, tzinfo=UTC)
    active, opened, cutoff = issuer_session(ENROLL, summer)
    assert active and opened.hour == 7 and cutoff.hour == 21


def test_build_only_enrolls_inside_window(db_session):
    settings = Settings(_env_file=None, source_enable_yahoo=False, guggenheim_issuer_feed_start=ENROLL)
    def urls(now):
        return [f.url for s in sources.build_source_feeds(settings, db_session, now=now) for f in s.feeds]
    assert URL not in urls(ENROLL - timedelta(seconds=1))
    assert URL in urls(ENROLL)
    assert URL not in urls(ENROLL + timedelta(days=5))


@pytest.fixture
def runtime(db_session, monkeypatch):
    source = Source(code='globenewswire',name='GlobeNewswire',base_url='https://www.globenewswire.com')
    db_session.add(source); db_session.commit()
    clock = [ENROLL + timedelta(minutes=2)]
    class Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0] if tz else clock[0].replace(tzinfo=None)
    import app.issuer_window as window
    monkeypatch.setattr(window, 'datetime', Fixed)
    monkeypatch.setattr(article_ingest, 'datetime', Fixed)
    fetch = Mock(side_effect=lambda url, timeout, headers: httpx.Response(200,
        request=httpx.Request('GET',url), content=BODY, headers={'ETag':'"baseline"'}))
    monkeypatch.setattr(article_ingest, '_fetch_issuer_feed', fetch)
    pending = Mock(return_value=[])
    monkeypatch.setattr(article_ingest, 'due_enrichment_entries', pending)
    def run():
        return article_ingest.ingest_feed(db_session, source=source, feed_url=URL,
            known_symbols=set(), symbol_to_id={}, timeout_seconds=5,
            issuer_feed_start=ENROLL, issuer_baseline_required=True)
    return run, clock, fetch, pending, source


def test_first_success_is_baseline_without_history_or_body_fetch(db_session, runtime):
    run, clock, fetch, pending, _ = runtime
    result = run()
    assert result['status']=='issuer_baseline' and result['items_seen']==1 and result['items_inserted']==0
    assert db_session.scalar(select(func.count()).select_from(RawFeedItem)) == 0
    assert db_session.scalar(select(IngestionRun.status)) == 'issuer_baseline'
    pending.assert_not_called()
    assert fetch.call_args.args[2] == {}


def test_same_day_recovery_uses_saved_baseline(db_session, runtime):
    run, clock, fetch, _, _ = runtime
    run(); clock[0] += timedelta(seconds=61)
    assert run()['status'] == 'success'
    assert fetch.call_args.args[2] == {'If-None-Match':'"baseline"'}
    assert db_session.scalar(select(func.count()).select_from(RawFeedItem)) == 0
    assert db_session.scalar(select(func.count()).select_from(IngestionRun).where(IngestionRun.status=='issuer_baseline'))==1


def test_next_day_full_baseline_before_new_processing(db_session, runtime):
    run, clock, fetch, _, _ = runtime
    run(); clock[0] += timedelta(days=1)
    assert run()['status'] == 'issuer_baseline'
    assert fetch.call_args.args[2] == {}


def test_refusal_no_baseline_and_no_retry(db_session, runtime):
    run, clock, fetch, _, _ = runtime
    response=httpx.Response(403,request=httpx.Request('GET',URL))
    fetch.side_effect=httpx.HTTPStatusError('refused',request=response.request,response=response)
    assert run()['status']=='failed'
    clock[0] += timedelta(days=1)
    assert run()['status']=='skipped_backoff'
    assert fetch.call_count==1
    assert db_session.scalar(select(func.count()).select_from(IngestionRun).where(IngestionRun.status=='issuer_baseline'))==0


def test_unsolicited_304_does_not_establish_baseline(db_session, runtime):
    run, clock, fetch, _, _ = runtime
    fetch.side_effect=lambda *args:httpx.Response(304,request=httpx.Request('GET',URL))
    assert run()['status']=='failed'
    assert db_session.scalar(select(func.count()).select_from(IngestionRun).where(IngestionRun.status=='issuer_baseline'))==0


def test_window_rechecked_before_request_without_database_writes(db_session, runtime):
    run, clock, fetch, pending, _ = runtime
    clock[0] = datetime(2026,11,9,21,59,30,tzinfo=UTC)
    assert run()['status']=='skipped_issuer_window'
    fetch.assert_not_called(); pending.assert_not_called()
    assert db_session.scalar(select(func.count()).select_from(IngestionRun))==0


def test_new_entry_after_baseline_reaches_normal_ingestion(db_session, runtime, monkeypatch):
    run, clock, fetch, _, _ = runtime
    run(); clock[0] += timedelta(minutes=2)
    body=BODY.replace(b'08:01:00',b'08:03:00')
    fetch.side_effect=lambda *args:httpx.Response(200,request=httpx.Request('GET',URL),content=body)
    process = Mock(wraps=article_ingest._process_single_entry)
    monkeypatch.setattr(article_ingest, '_process_single_entry', process)
    # The new row reaches ordinary classification; it has no validated fund.
    result=run()
    assert result['status']=='success' and result['items_inserted']==0
    process.assert_called_once()
    assert process.call_args.kwargs['prepared'].raw_pub_date == ENROLL + timedelta(minutes=3)
