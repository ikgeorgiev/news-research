"""GNW review collection: durable evidence, bounded access and no publication."""
import asyncio
from datetime import datetime, timezone, timedelta
import hashlib
import gzip
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from app.financialcontent import runtime, gnw_body
from app.financialcontent.listings import GNW_ROUTE, GNW_URL, REPUBLISHER_ROUTES, parse_republisher_index
from app.financialcontent.store import Store

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
END = datetime(2026, 11, 13, 22, tzinfo=timezone.utc)
DAY = '2026-11-10'
TITLE = 'Example Fund (Nasdaq-FUND) Declares Distribution'
CTX = SimpleNamespace(known_symbols={'FUND', 'ZZZ'}, symbol_keywords={})
BW = 'financialcontent-bw-index'


def article(n, route=GNW_ROUTE):
    provider = 'gnwcq' if route == GNW_ROUTE else 'bizwire'
    return {'url': f'https://markets.chroniclejournal.com/thepilotnews/article/{provider}-2026-11-10-example-{n}', 'title': TITLE}


def listing(items):
    return ('<div class="newsrpm-headlines1">' + ''.join(
        f'<div class="headline"><div class="title"><a href="{a["url"]}">{a["title"]}</a></div></div>'
        for a in items) + '</div><a href="?CurrentPage=1">Next</a>').encode()


def body(item, *, title=TITLE, published='2026-11-10T12:00:00Z'):
    data = {'@type': 'NewsArticle', 'mainEntityOfPage': {'@id': item['url']},
            'headline': title, 'datePublished': published}
    return (f'<script type="application/ld+json">{json.dumps(data)}</script>'
        '<div itemprop="articleBody"><p>Example Fund (Nasdaq-FUND) announces a distribution to common '
        'shareholders. This synthetic fixture exercises the collector without contacting any website.</p>'
        '<table><tr><th>Distribution</th><td>$0.15</td></tr></table></div>').encode()


@pytest.fixture
def clock(monkeypatch):
    value = [NOW.timestamp()]
    class DateTimeMeta(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, datetime)
    class FixedDateTime(datetime, metaclass=DateTimeMeta):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromtimestamp(value[0], tz or timezone.utc)
    monkeypatch.setattr(runtime, 'datetime', FixedDateTime)
    monkeypatch.setattr(runtime.time, 'time', lambda: value[0])
    return value


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path/'fc.sqlite')
    s.import_controls(evidence='isolated regression')
    return s


@pytest.fixture
def worker(store, clock, monkeypatch):
    w = runtime.FinancialContentWorker(SimpleNamespace(financialcontent_gnw_review_until=END), None)
    w.store = store
    w.source_enabled = True
    def no_delivery(*args, **kwargs):
        pytest.fail('GNW review must never publish or notify')
    monkeypatch.setattr(runtime, 'deliver', no_delivery)
    monkeypatch.setattr(runtime, 'write_reader_archive', no_delivery)
    return w


def front(store, items, now, route=GNW_ROUTE, day=DAY):
    assert store.begin_listing(route, now=now, day=day, interval=60 if route==GNW_ROUTE else 30) is not None
    return store.finish_listing(route, status=200, now=now, day=day, items=items, context=CTX,
        html=listing(items).decode())


def candidate(store, clock, n=1, route=GNW_ROUTE):
    front(store, [article(0, route)], clock[0]-60, route)
    front(store, [article(n, route),article(0, route)], clock[0], route)
    return store.claim_body(clock[0], allowed_routes=[route])


def row(store, url=None):
    with store.connect() as db:
        return dict(db.execute('SELECT * FROM fc_candidate WHERE url=?', (url or article(1)['url'],)).fetchone())


def run_response(worker, item, response):
    calls = []
    async def run():
        def respond(request):
            calls.append(str(request.url)); return response
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            await worker.process_body(client, item)
    asyncio.run(run())
    return calls


def test_disabled_until_explicit_finite_window(clock):
    w = runtime.FinancialContentWorker(SimpleNamespace(), None)
    assert set(w.enabled_routes()) == set(REPUBLISHER_ROUTES)
    for deadline in [NOW, NOW+timedelta(seconds=29), NOW.replace(tzinfo=None), None]:
        w.settings.financialcontent_gnw_review_until = deadline
        assert GNW_ROUTE not in w.enabled_routes()
    w.settings.financialcontent_gnw_review_until = END
    assert GNW_ROUTE in w.enabled_routes()
    assert GNW_ROUTE not in w.enabled_routes(datetime(2026,11,10,22,tzinfo=timezone.utc))
    assert GNW_ROUTE not in w.enabled_routes(datetime(2026,11,14,12,tzinfo=timezone.utc))


def test_configuration_requires_an_explicit_timezone():
    from pydantic import ValidationError
    from app.config import Settings
    assert Settings(_env_file=None,financialcontent_gnw_review_until=None).financialcontent_gnw_review_until is None
    assert Settings(_env_file=None,financialcontent_gnw_review_until='2026-11-13T22:00:00Z').financialcontent_gnw_review_until==END
    with pytest.raises(ValidationError):
        Settings(_env_file=None,financialcontent_gnw_review_until='2026-11-13T22:00:00')


def test_only_published_gnw_provider_is_parsed():
    rows = parse_republisher_index(listing([article(1), article(2,BW)]), GNW_ROUTE, lambda x:x)
    assert [r['url'] for r in rows] == [article(1)['url']]
    assert rows[0]['original_wire_id'] is None and rows[0]['publication_precision']=='date_only'
    with pytest.raises(ValueError, match='no recognized'):
        parse_republisher_index(listing([article(1,BW)]), GNW_ROUTE, lambda x:x)


def test_default_off_means_no_listing_request_or_route_baseline(worker):
    worker.settings.financialcontent_gnw_review_until = None
    asyncio.run(worker.poll_listing(None, GNW_ROUTE, GNW_URL, DAY, 60, CTX))
    assert worker.store.inspect()['routes'] == []


def test_baseline_new_day_restart_and_conditional_rules(store, clock):
    assert front(store,[article(0)],clock[0]-60)==0
    assert front(store,[article(1),article(0)],clock[0])==1
    s=Store(store.path)
    s.recover_owned_work()
    assert front(s,[article(1),article(0)],clock[0]+60)==0
    assert row(s)['observed']==clock[0] and row(s)['last_absent']==clock[0]-60
    assert front(s,[article(2),article(1)],clock[0]+86400,day='2026-11-11')==0
    assert s.inspect()['counts']=={'pending':1}
    assert s.inspect()['catchup']=={}  # GNW never borrows BW pagination.


def test_listing_baseline_requests_no_body_and_respects_sixty_seconds(worker, clock):
    requests=[]
    async def run():
        def respond(req):
            requests.append(str(req.url));return httpx.Response(200, content=listing([article(0)]))
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as c:
            await worker.poll_listing(c,GNW_ROUTE,GNW_URL,DAY,1,CTX)
            clock[0]+=59
            await worker.poll_listing(c,GNW_ROUTE,GNW_URL,DAY,1,CTX)
    asyncio.run(run())
    assert requests==[GNW_URL] and worker.store.inspect()['counts']=={}


def test_new_body_is_archived_atomically_without_publication(worker, store, clock):
    item=candidate(store,clock);content=body(item)
    assert run_response(worker,item,httpx.Response(200,content=content))==[item['url']]
    r=row(store)
    assert r['status']=='identity_review' and r['attempts']==1 and r['entry'] is None
    assert not r['request_pending'] and r['lease'] is None
    with store.connect() as db:
        archive=dict(db.execute('SELECT * FROM fc_review_body').fetchone())
    assert archive['html']==content and archive['html_sha256']==hashlib.sha256(content).hexdigest()
    review=json.loads(archive['review_json'])
    assert review['wire_identity'] is None and review['validated_fund_tickers']==[]
    assert review['discovery']['first_observed_at']==item['observed']
    assert review['discovery']['last_absent_at']==item['last_absent']
    assert review['network_requests']==1 and review['receipt_kind']=='network_response'
    assert review['discovery']['body_request_started_at']<=review['discovery']['body_received_at']
    assert any(b['type']=='table' for b in review['reader']['blocks'])
    Store(store.path).recover_owned_work()
    assert store.claim_body(clock[0]+120,allowed_routes=[GNW_ROUTE]) is None


def test_duplicate_listing_never_requeues_completed_body(worker, store, clock):
    item=candidate(store,clock)
    run_response(worker,item,httpx.Response(200,content=body(item)))
    for elapsed in [60,120,180]:
        assert front(store,[article(1),article(0)],clock[0]+elapsed)==0
    assert store.inspect()['review_bodies']==1 and row(store)['attempts']==1


def test_parser_failure_retains_raw_body_for_review(worker,store,clock):
    item=candidate(store,clock);content=body(item,title='A conflicting release')
    run_response(worker,item,httpx.Response(200,content=content))
    assert row(store)['status']=='invalid'
    with store.connect() as db:
        r=db.execute('SELECT html,review_json FROM fc_review_body').fetchone()
    assert r['html']==content and not json.loads(r['review_json'])['body_parse_valid']
    assert store.claim_body(clock[0]+120,allowed_routes=[GNW_ROUTE]) is None


@pytest.mark.parametrize('failure',['empty_html','reader_exception'])
def test_unexpected_parser_failure_cannot_stop_shared_worker(worker,store,clock,monkeypatch,failure):
    item=candidate(store,clock)
    content=b'' if failure=='empty_html' else body(item)
    if failure=='reader_exception':
        def reject(*args,**kwargs):
            raise RuntimeError('Unexpected reader structure')
        monkeypatch.setattr(gnw_body,'build_reader_document',reject)
    assert run_response(worker,item,httpx.Response(200,content=content))==[item['url']]
    assert row(store)['status']=='invalid'
    with store.connect() as db:
        saved=db.execute('SELECT html,review_json FROM fc_review_body').fetchone()
    assert saved['html']==content
    assert not json.loads(saved['review_json'])['body_parse_valid']
    assert front(store,[article(0,BW)],clock[0]+1,BW)==0
    assert front(store,[article(2,BW),article(0,BW)],clock[0]+31,BW)==1


@pytest.mark.parametrize('route',[GNW_ROUTE,BW])
def test_restart_clears_only_route_absence_bound_not_saved_discoveries(store,clock,route):
    item=candidate(store,clock,route=route)
    first=row(store,item['url'])
    reopened=Store(store.path)
    reopened.recover_owned_work()
    assert front(reopened,[article(2,route),article(1,route)],clock[0]+60,route)==1
    assert row(reopened,article(2,route)['url'])['last_absent'] is None
    saved=row(reopened,item['url'])
    assert saved['last_absent']==first['last_absent']
    assert saved['observed']==first['observed']


@pytest.mark.parametrize('status',[301,401,403,404,429])
def test_body_refusal_stops_redirects_and_preserves_controls(worker,store,clock,status):
    item=candidate(store,clock)
    calls=run_response(worker,item,httpx.Response(status,headers={'Location':'https://example.com','Retry-After':'120'}))
    assert calls==[item['url']] and store.inspect()['review_bodies']==0
    s=Store(store.path)
    if status in {401,403}:
        assert s.inspect()['controls']['disabled']==str(status)
        assert s.begin_listing(BW,now=clock[0]+500,day=DAY,interval=30) is None
    elif status==429:
        assert float(s.inspect()['controls']['backoff_until'])>=clock[0]+120
        assert s.begin_listing(BW,now=clock[0]+61,day=DAY,interval=30) is None
    else:
        assert row(s)['status']=='refused'


@pytest.mark.parametrize('status',[401,403,404,429])
def test_listing_controls_apply_to_same_host(worker,store,clock,status):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(status,headers={'Retry-After':'120'}))) as c:
            await worker.poll_listing(c,GNW_ROUTE,GNW_URL,DAY,60,CTX)
    asyncio.run(run())
    s=Store(store.path)
    assert s.begin_listing(GNW_ROUTE,now=clock[0]+61,day=DAY,interval=60) is None
    if status!=404:
        assert s.begin_listing(BW,now=clock[0]+61,day=DAY,interval=30) is None
    else:
        assert s.begin_listing(BW,now=clock[0]+61,day=DAY,interval=30)=={}


def test_no_absence_bound_across_downtime(store,clock):
    front(store,[article(0)],clock[0]-500)
    front(store,[article(1),article(0)],clock[0])
    assert row(store)['last_absent'] is None


def test_unknown_inflight_is_held_and_never_replayed(store,clock):
    item=candidate(store,clock)
    assert store.begin_body(item,clock[0])
    assert not store.begin_body(item,clock[0])
    s=Store(store.path);s.recover_owned_work()
    assert row(s)['status']=='held_inflight' and row(s)['attempts']==1
    assert s.claim_body(clock[0]+500,allowed_routes=[GNW_ROUTE]) is None


def test_unstarted_work_resumes_without_resetting_baseline(store,clock):
    item=candidate(store,clock)
    Store(store.path).recover_owned_work()
    recovered=store.claim_body(clock[0],allowed_routes=[GNW_ROUTE])
    assert recovered['observed']==item['observed'] and recovered['last_absent']==item['last_absent']
    assert recovered['attempts']==0 and recovered['lease']!=item['lease']


def test_archive_and_queue_state_roll_back_together(store,clock):
    item=candidate(store,clock);content=body(item)
    review=gnw_body.parse_body(content,article_url=item['url'],expected_title=TITLE)
    assert store.begin_body(item,clock[0])
    with store.connect() as db:
        db.execute("CREATE TRIGGER fail_review BEFORE UPDATE OF status ON fc_candidate WHEN NEW.status='identity_review' BEGIN SELECT RAISE(ABORT,'simulated commit failure'); END")
    with pytest.raises(sqlite3.IntegrityError,match='simulated'):
        store.save_review_body(item,content,review,clock[0])
    assert store.inspect()['review_bodies']==0 and row(store)['request_pending']==1


@pytest.mark.parametrize('after_commit',[False,True])
def test_archive_failure_or_lost_ack_does_not_refetch(worker,store,clock,monkeypatch,after_commit):
    item=candidate(store,clock);original=store.save_review_body
    def fail(*args):
        if after_commit:original(*args)
        raise OSError('simulated archive failure')
    monkeypatch.setattr(store,'save_review_body',fail)
    run_response(worker,item,httpx.Response(200,content=body(item)))
    assert row(store)['status']==('identity_review' if after_commit else 'held_review_archive')
    Store(store.path).recover_owned_work()
    assert store.claim_body(clock[0]+500,allowed_routes=[GNW_ROUTE]) is None


def test_new_gnw_body_rate_survives_restart_and_bw_has_priority(store,clock):
    first=candidate(store,clock)
    assert store.begin_body(first,clock[0])
    store.fail_body(first,clock[0],status=404)
    front(store,[article(2)],clock[0]+60)
    front(store,[article(0,BW)],clock[0]-30,BW)
    front(store,[article(1,BW)],clock[0]+1,BW)
    s=Store(store.path)
    assert s.claim_body(clock[0]+61,allowed_routes=[GNW_ROUTE,BW])['route']==BW
    second=s.claim_body(clock[0]+61,allowed_routes=[GNW_ROUTE])
    assert s.begin_body(second,clock[0]+61)
    assert float(s.inspect()['controls']['gnw_body_next'])==clock[0]+121


def test_feature_off_leaves_gnw_work_unclaimed(worker,store,clock):
    candidate(store,clock);store.recover_owned_work()
    worker.settings.financialcontent_gnw_review_until=None
    assert store.claim_body(clock[0],allowed_routes=worker.enabled_routes()) is None
    assert row(store)['status']=='pending' and row(store)['attempts']==0


@pytest.mark.parametrize('stop',[True,False])
def test_stop_or_expiry_between_claim_and_fetch_sends_nothing(worker,store,clock,stop):
    item=candidate(store,clock)
    if stop:worker.stop_event.set()
    else:worker.settings.financialcontent_gnw_review_until=NOW+timedelta(seconds=29)
    asyncio.run(worker.process_body(None,item))
    assert row(store)['status']=='pending' and row(store)['attempts']==0


def test_slow_gnw_body_does_not_block_bw_discovery(worker,store,clock):
    item=candidate(store,clock)
    front(store,[article(0,BW)],clock[0]-30,BW)
    async def run():
        started=asyncio.Event();release=asyncio.Event()
        async def respond(request):
            if str(request.url)==item['url']:
                started.set();await release.wait();return httpx.Response(200,content=body(item))
            return httpx.Response(200,content=listing([article(1,BW),article(0,BW)]))
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as c:
            task=asyncio.create_task(worker.process_body(c,item))
            await asyncio.wait_for(started.wait(),1)
            try:
                await asyncio.wait_for(worker.poll_listing(c,BW,REPUBLISHER_ROUTES[BW],DAY,30,CTX),1)
                assert row(store,article(1,BW)['url'])['status']=='pending'
            finally:
                release.set();await asyncio.wait_for(task,1)
    asyncio.run(run())
    assert row(store)['status']=='identity_review'


def test_total_timeout_and_size_bound_leave_discovery_record(worker,store,clock,monkeypatch):
    item=candidate(store,clock)
    async def run():
        async def slow(request):
            await asyncio.sleep(.1);return httpx.Response(200,content=body(item))
        monkeypatch.setattr(gnw_body,'REQUEST_SECONDS',.01)
        async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as c:
            await worker.process_body(c,item)
    asyncio.run(run())
    assert row(store)['status']=='pending' and row(store)['due']>=clock[0]+60
    clock[0]+=60
    item=store.claim_body(clock[0],allowed_routes=[GNW_ROUTE])
    run_response(worker,item,httpx.Response(200,content=b'x'*(gnw_body.MAX_BODY+1)))
    assert row(store)['status']=='invalid' and row(store)['observed']==NOW.timestamp()
    assert store.inspect()['review_bodies']==0


def test_real_saved_fund_body_keeps_history_and_three_tables():
    with gzip.open(Path(__file__).parent / 'fixtures/financialcontent/sprott.html.gz', 'rb') as capture:
        content = capture.read()
    review = gnw_body.parse_body(content,
        article_url='https://markets.chroniclejournal.com/thepilotnews/article/gnwcq-2026-9-2-sprott-focus-trust-inc-nasdaq-fund-declares-third-quarter-common-stock-distribution-of-01503-per-share',
        expected_title='Sprott Focus Trust, Inc. (Nasdaq-FUND) Declares Third Quarter Common Stock Distribution of $0.1503 Per Share')
    assert hashlib.sha256(content).hexdigest()=='8a48f5ae3895e2d050bf4f18a2ac5538d2c82c0c9caca7ff5a02ba358ae815c4'
    assert sum(b['type']=='table' for b in review['reader']['blocks'])==3
    assert review['publication_label'].startswith('2026-09-02')
    assert review['original_link_candidates']==[] and review['wire_identity'] is None
    assert not review['latency_eligible']
