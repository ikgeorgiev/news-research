import asyncio
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import httpx
import pytest

from app.financialcontent import runtime
from app.financialcontent.catchup import BASE, is_second_page, published_next_page
from app.financialcontent.financialcontent_adapter import parse_article, fetch_article, ArticleUnavailable, InvalidArticle
from app.financialcontent.store import Store

ROUTE = 'financialcontent-bw-index'
DAY = '2026-11-09'
TITLE = 'ZZZ Announces Monthly Distribution'
CTX = SimpleNamespace(known_symbols={'ZZZ'}, symbol_keywords={})


def article(n):
    return {'url': f'https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-11-9-fund-{n}', 'title': TITLE}


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / 'fc.sqlite')
    result.import_controls(evidence='isolated-test')
    return result


def front(store, n, now, day=DAY, **kwargs):
    assert store.begin_listing(ROUTE, now=now, day=day, interval=30) is not None
    return store.finish_listing(ROUTE, status=200, now=now, day=day,
        items=[article(i) for i in n], context=CTX, **kwargs)


def candidate(store, now=130):
    front(store, [0], now-30)
    front(store, [1, 0], now)
    return store.claim_body(now)


def row(store, url=None):
    with store.connect() as db:
        return dict(db.execute('SELECT * FROM fc_candidate WHERE url=?', (url or article(1)['url'],)).fetchone())


def test_import_required_and_no_existing_study_database(tmp_path):
    import sqlite3
    fresh = Store(tmp_path/'fresh.sqlite')
    assert fresh.begin_listing(ROUTE, now=100, day=DAY, interval=30) is None
    path = tmp_path/'study.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE candidates(id INTEGER)')
    with pytest.raises(ValueError, match='not the production'):
        Store(path)


def test_daily_baseline_cadence_and_restart(store):
    assert front(store, [0], 100) == 0
    assert store.begin_listing(ROUTE, now=129, day=DAY, interval=30) is None
    assert front(store, [1, 0], 130) == 1
    assert row(store)['last_absent'] == 100
    assert front(Store(store.path), [1, 0], 160) == 0
    assert front(store, [2, 1], 86400, day='2026-11-10') == 0
    assert row(store)['observed'] == 130


def test_no_fine_absence_across_downtime(store):
    front(store, [0], 100)
    front(store, [1, 0], 500)
    assert row(store)['last_absent'] is None


def test_conditional_response_does_not_advance_absence(store):
    front(store, [0], 100, headers={'etag':'v1'})
    assert store.begin_listing(ROUTE, now=130, day=DAY, interval=30) == {'If-None-Match':'v1'}
    store.finish_listing(ROUTE, status=304, now=130, day=DAY)
    front(store, [1, 0], 160)
    assert row(store)['last_absent'] == 100


@pytest.mark.parametrize('status', [401, 403])
def test_host_refusal_from_either_task_survives_restart(store, status):
    item = candidate(store)
    assert store.begin_body(item, 130)
    store.fail_body(item, 131, status=status)
    restarted = Store(store.path)
    assert restarted.begin_listing('financialcontent-franklin-index', now=500, day=DAY, interval=60) is None
    assert restarted.inspect()['controls']['disabled'] == str(status)


def test_429_applies_across_candidates_and_listings(store):
    item = candidate(store)
    store.begin_body(item, 130)
    store.fail_body(item, 131, status=429, retry_until=4000)
    store.import_controls(backoff_until=3000, evidence='older controls')
    assert store.claim_body(2000) is None
    assert store.begin_listing(ROUTE, now=2000, day=DAY, interval=30) is None
    assert float(store.inspect()['controls']['backoff_until']) == 4000


def test_404_terminal_per_url_and_historical_rows_not_replayed(store):
    item = candidate(store)
    store.begin_body(item, 130)
    store.fail_body(item, 131, status=404)
    store.import_controls(historical_urls=[article(3)['url']], refused_urls=[article(4)['url']], evidence='prior study')
    front(store, [1, 2, 3, 4], 160)
    fresh = store.claim_body(160)
    assert fresh['url'] == article(2)['url']
    assert row(store)['status'] == 'refused'
    assert row(store, article(3)['url'])['status'] == 'prior_history'
    assert row(store, article(4)['url'])['status'] == 'prior_refusal'


def test_unknown_inflight_is_not_automatically_retried(store):
    item = candidate(store)
    store.begin_body(item, 130)
    Store(store.path).recover_owned_work()
    assert row(store)['status'] == 'held_inflight'
    assert store.claim_body(160) is None
    assert store.begin_listing(ROUTE, now=160, day=DAY, interval=30) is not None
    Store(store.path).recover_owned_work()
    assert store.begin_listing(ROUTE, now=90000, day='2026-11-10', interval=30) is None


def test_saved_body_survives_retry_without_another_fetch(store):
    item = candidate(store)
    store.begin_body(item, 130)
    store.save_body(item, {'id':'bw:123456:en'}, 131)
    Store(store.path).recover_owned_work()
    recovered = store.claim_body(160)
    assert json.loads(recovered['entry'])['id'] == 'bw:123456:en'
    assert not store.begin_body(recovered, 160)
    store.finish_body(recovered, 'done', result={'article_id':1})
    assert Store(store.path).claim_body(1000) is None


def test_attempts_and_age_are_bounded(store):
    item = candidate(store)
    for now in [130, 190, 310, 550]:
        if now != 130:
            item = store.claim_body(now)
        assert item is not None
        store.begin_body(item, now)
        store.fail_body(item, now, error='timeout')
    assert row(store)['status'] == 'held_attempts'
    front(store, [2], 700)
    assert store.claim_body(1700) is None
    assert row(store, article(2)['url'])['status'] == 'held_age'


def test_page_two_is_review_only_and_throttled(store):
    link = '<a href="?CurrentPage=1">Next</a>'
    front(store, [0], 100)
    front(store, [1], 130, html=link)
    job = store.claim_catchup(130, 130)
    assert job and job['url'] == BASE + '?CurrentPage=1'
    store.finish_catchup(job, now=131, status=200, items=[article(9)])
    review = row(store, article(9)['url'])
    assert review['status'] == 'review_history' and review['last_absent'] is None
    assert store.claim_body(131)['url'] == article(1)['url']
    front(store, [2], 160, html=link)
    assert store.claim_catchup(160, 160) is None
    front(store, [3], 190, html=link)
    assert store.claim_catchup(190, 190)
    front(store, [4], 260, html=link)
    assert store.claim_catchup(260, 260) is None  # prior outcome unknown


@pytest.mark.parametrize("request_started", [False, True])
def test_restart_releases_only_unstarted_page_two_reservations(store, request_started):
    link = '<a href="?CurrentPage=1">Next</a>'
    front(store, [0], 100)
    front(store, [1], 130, html=link)
    if request_started:
        assert store.claim_catchup(130, 130) is not None

    restarted = Store(store.path)
    restarted.recover_owned_work()
    front(restarted, [2], 200, html=link)
    next_job = restarted.claim_catchup(200, 200)
    assert (next_job is None) == request_started


@pytest.mark.parametrize('status', [401, 403, 404, 429])
def test_catchup_controls_preserved(store, status):
    front(store, [0], 100)
    front(store, [1], 130, html='<a href="?CurrentPage=1">Next</a>')
    job = store.claim_catchup(130, 130)
    store.finish_catchup(job, now=131, status=status, retry_until=5000 if status == 429 else 0)
    controls = Store(store.path).inspect()['controls']
    if status in {401,403}:
        assert controls['disabled'] == str(status)
    elif status == 404:
        assert controls['catchup_disabled'] == '404' and not controls.get('disabled')
    else:
        assert float(controls['backoff_until']) == 5000


def test_pending_body_blocks_catchup_and_cutoff_cancels_unstarted(store):
    item = candidate(store)
    store.begin_body(item, 130)
    front(store, [2], 160, html='<a href="?CurrentPage=1">Next</a>')
    assert store.claim_catchup(160, 160) is None
    store.fail_body(item, 160, status=404)
    front(store, [3], 190, html='<a href="?CurrentPage=1">Next</a>')
    store.cancel_unstarted_catchup(190)
    assert store.claim_catchup(190, 190) is None


@pytest.mark.parametrize('href', ['?CurrentPage=2','?CurrentPage=1&extra=1','https://elsewhere.example/?CurrentPage=1','?CurrentPage=1#x'])
def test_unpublished_or_wrong_page_is_rejected(href):
    assert published_next_page(['a'], ['b'], f'<a href="{href}">Next</a>') is None
    assert not is_second_page(href)


def test_overlap_requires_explicit_link_and_not_baseline():
    assert published_next_page([], ['a'], '<a href="?CurrentPage=1">') is None
    assert published_next_page(list('abcdef'), list('abcdefg'), '<a href="?CurrentPage=1">') is None
    assert published_next_page(['a'], ['b'], '<a href="?CurrentPage=1">') == BASE+'?CurrentPage=1'


@pytest.mark.parametrize('when, active', [('2026-11-07T12:00:00+00:00', False), ('2026-11-09T07:59:59+00:00',False), ('2026-11-09T08:00:00+00:00',True), ('2026-11-09T21:59:29+00:00',True), ('2026-11-09T21:59:30+00:00',False), ('2026-11-09T22:00:00+00:00',False)])
def test_production_hours_keep_cutoff_margin(when, active):
    assert runtime.session_window(datetime.fromisoformat(when))[0] is active


def test_singleton_and_disabled_worker(tmp_path):
    path = tmp_path/'fc.lock'
    lock = runtime.singleton_lock(path)
    assert lock and runtime.singleton_lock(path) is None
    lock.close()
    lock = runtime.singleton_lock(path)
    assert lock
    lock.close()
    worker = runtime.FinancialContentWorker(SimpleNamespace(scheduler_enabled=True, source_enable_financialcontent=False), None)
    worker.start()
    assert worker.thread is None


@pytest.mark.parametrize('status', [301,401,403,404,429])
def test_body_request_is_one_bounded_nonredirecting_request(status):
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(status, headers={'location':'https://example.com','retry-after':'120'})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(ArticleUnavailable) as exc:
                await fetch_article(client, article(1)['url'])
            assert exc.value.status == status and exc.value.retry_after == '120'
    asyncio.run(run())
    assert len(calls) == 1


def test_transport_byte_and_wall_limits(monkeypatch):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b'x'*(1024*1024+1)))) as client:
            with pytest.raises(InvalidArticle, match='one MiB'):
                await fetch_article(client, article(1)['url'])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b'x'*(4*1024*1024+1)))) as client:
            with pytest.raises(ValueError, match='four MiB'):
                await runtime.fetch_listing(client, BASE, {})
        async def slow(request):
            await asyncio.sleep(.1)
            return httpx.Response(200, content=b'')
        monkeypatch.setattr(runtime, 'REQUEST_SECONDS', .01)
        async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
            with pytest.raises(TimeoutError):
                await runtime.fetch_listing(client, BASE, {})
    asyncio.run(run())


def test_archive_failure_does_not_replay_delivery(store, tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    item = candidate(store, now.timestamp())
    entry = {'id':'bw:123456:en', 'published':now.isoformat()}
    store.save_body(item, entry, now.timestamp())
    item = row(store)
    calls = []
    monkeypatch.setattr(runtime, 'deliver', lambda *a, **k: calls.append(1) or {'article_id':2, 'created':True})
    def fail(*a):
        raise OSError('disk full')
    monkeypatch.setattr(runtime, 'write_reader_archive', fail)
    worker = runtime.FinancialContentWorker(SimpleNamespace(article_body_archive_dir=str(tmp_path)), None)
    worker.store = store
    asyncio.run(worker.process_body(None, item))
    assert calls == [1] and row(store)['status'] == 'done'
    assert row(store)['archive_status'] == 'error:OSError'
    store.recover_owned_work()
    assert store.claim_body(now.timestamp()+1) is None
