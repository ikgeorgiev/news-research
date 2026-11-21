import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from app.financialcontent import runtime
from app.financialcontent.health import listing_health
from app.financialcontent.listings import GNW_ROUTE, GNW_URL, REPUBLISHER_ROUTES
from app.financialcontent.store import Store

BW='financialcontent-bw-index';FRANKLIN='financialcontent-franklin-index';DAY='2026-11-11'
ROUTES=dict(REPUBLISHER_ROUTES)|{GNW_ROUTE:GNW_URL}
CTX=SimpleNamespace(known_symbols={'ZZZ'},symbol_keywords={})


@pytest.fixture
def store(tmp_path):
    value=Store(tmp_path/'fc.sqlite');value.import_controls(evidence='isolated-unit-test');return value


def item(n):
    return dict(url=f'https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-11-11-fund-{n}',title='ZZZ Announces Distribution')


def full(store,route=BW,now=100,items=None,headers=None):
    assert store.begin_listing(route,now=now,day=DAY,interval=60) is not None
    store.finish_listing(route,status=200,now=now+1,day=DAY,items=items or [item(0)],context=CTX,headers=headers)


def assess(store,now=170,active=True,routes=ROUTES):
    return listing_health(store.inspect(),routes,active=active,now=now)


def find(report,name):return next(r for r in report['routes'] if r['name']==name)


def test_current_two_production_holds_are_not_hidden_by_review_route(store):
    for route in [BW,FRANKLIN]:
        assert store.begin_listing(route,now=100,day=DAY,interval=60) is not None
        store.finish_listing(route,status=404,now=101,day=DAY)
    full(store,GNW_ROUTE)
    h=assess(store)
    assert h['status']=='degraded' and h['attention_required'] and h['held_route_count']==2
    assert h['production_listing_status']=='unavailable' and h['review_listing_status']=='ok'
    assert find(h,BW)['hold_response_recorded_at_utc']=='1970-01-01T00:01:41+00:00'


@pytest.mark.parametrize('status',[301,302,303,307,308,400,401,403,404,410])
def test_terminal_hold_and_diagnostics_survive_restart_without_retry(store,status):
    assert store.begin_listing(BW,now=100,day=DAY,interval=30)=={}
    store.finish_listing(BW,status=status,now=101,day=DAY,headers={'content-type':'text/html'})
    saved=store.inspect()['listing_diagnostics']
    restarted=Store(store.path);restarted.recover_owned_work()
    assert restarted.begin_listing(BW,now=90000,day='2026-11-12',interval=30) is None
    assert restarted.inspect()['listing_diagnostics']==saved
    h=assess(restarted,90000,routes={BW:ROUTES[BW]})
    assert h['status']=='unavailable' and find(h,BW)['hold_reason']==str(status)
    assert saved[0]['http_status']==status and saved[0]['finished_at']==101


def test_legacy_hold_upgrade_preserves_rows_and_unknown_time(store):
    full(store,now=100);full(store,now=160,items=[item(1),item(0)])
    with store.connect() as db:
        db.execute('DROP TABLE fc_listing_diagnostic')
        db.execute('UPDATE fc_route SET disabled=?,next_due=? WHERE name=?',('404',123456,BW))
        before={t:[tuple(r) for r in db.execute(f'SELECT * FROM {t}')] for t in ['fc_route','fc_candidate','fc_seen','fc_control','fc_meta']}
    upgraded=Store(store.path)
    with upgraded.connect() as db:
        after={t:[tuple(r) for r in db.execute(f'SELECT * FROM {t}')] for t in before}
    assert after==before and upgraded.inspect()['listing_diagnostics']==[]
    route=find(assess(upgraded),BW)
    assert route['status']=='held' and route['last_attempt'] is None
    assert route['legacy_hold_without_response_time'] and route['hold_response_recorded_at_utc'] is None
    assert upgraded.claim_body(162)['url']==item(1)['url']


def test_previous_success_is_not_relabelled_as_imported_refusal_time(store):
    full(store)
    store.import_controls(blocked_routes=[BW],evidence='reviewed-external-hold')
    route=find(assess(store),BW)
    assert route['last_attempt']['http_status']==200
    assert route['hold_response_recorded_at_utc'] is None and route['legacy_hold_without_response_time']


def test_304_is_liveness_not_a_new_absence_bound(store):
    full(store,headers={'etag':'secret-validator-value'})
    assert store.begin_listing(BW,now=160,day=DAY,interval=30)=={'If-None-Match':'secret-validator-value'}
    store.finish_listing(BW,status=304,now=161,day=DAY)
    r=find(assess(store,now=170),BW)
    assert r['status']=='ok' and r['last_success_at_utc']=='1970-01-01T00:01:41+00:00'
    assert r['last_attempt']['http_status']==304 and r['last_attempt']['conditional_headers']==['If-None-Match']
    assert 'secret-validator-value' not in json.dumps(r)


def test_timeout_and_parser_failure_are_visible_with_separate_clocks(store):
    full(store)
    for now,status,error in [(160,None,'TimeoutError'),(221,200,'ValueError')]:
        assert store.begin_listing(BW,now=now,day=DAY,interval=30) is not None
        store.finish_listing(BW,status=status,now=now+1,day=DAY,error=error)
        r=find(assess(store,now=now+2),BW)
        assert r['status']=='failed' and r['last_success_at_utc'] is None
        assert r['last_attempt']['started_at']==now and r['last_attempt']['finished_at']==now+1
        assert r['last_attempt']['error_kind']==error and r['last_attempt']['http_status']==status


def test_unknown_inflight_remains_blocked_after_restart(store):
    assert store.begin_listing(BW,now=100,day=DAY,interval=30)=={}
    restarted=Store(store.path);restarted.recover_owned_work()
    assert restarted.begin_listing(BW,now=90000,day='2026-11-12',interval=30) is None
    r=find(assess(restarted,now=90000),BW)
    assert r['status']=='request_outcome_unknown' and r['last_attempt']['finished_at'] is None
    assert r['last_attempt']['http_status'] is None


def test_retry_after_remains_host_wide_and_explicit(store):
    assert store.begin_listing(BW,now=100,day=DAY,interval=30)=={}
    store.finish_listing(BW,status=429,now=101,day=DAY,retry_until=900,headers={'retry-after':'799'})
    assert store.begin_listing(GNW_ROUTE,now=500,day=DAY,interval=60) is None
    h=assess(store,now=500)
    assert h['status']=='backoff' and h['backoff_until_utc']=='1970-01-01T00:15:00+00:00'
    assert len(store.inspect()['listing_diagnostics'])==1


def test_stale_success_is_visible_and_schedule_is_not_changed(store):
    full(store)
    before=store.inspect();h=assess(store,now=1000)
    assert find(h,BW)['status']=='stale' and h['attention_required']
    assert store.inspect()==before


def test_inactive_and_expired_review_route_do_not_claim_live_coverage(store):
    for route in ROUTES:full(store,route)
    assert assess(store,active=False)['status']=='inactive'
    h=assess(store,routes=REPUBLISHER_ROUTES)
    assert h['status']=='ok' and h['review_listing_status']=='inactive'
    assert not find(h,GNW_ROUTE)['enabled'] and find(h,GNW_ROUTE)['status']=='inactive'
    assert h['freshness_is_not_complete_coverage']


def test_metadata_allowlist_and_lengths_exclude_sensitive_or_unbounded_data(store):
    headers={'content-type':'x'*3000,'cache-control':'max-age=600','last-modified':'Wed, 11 Nov 2026 15:38:31 GMT',
        'retry-after':'60','set-cookie':'private-cookie','authorization':'private-auth','location':'private-url','etag':'private-etag'}
    assert store.begin_listing(BW,now=100,day=DAY,interval=30)=={}
    store.finish_listing(BW,status=None,now=101,day=DAY,headers=headers,error='message with private token')
    row=store.inspect()['listing_diagnostics'][0]
    assert set(row['response_headers'])=={'content-type','cache-control','last-modified','retry-after'}
    assert len(row['response_headers']['content-type'])==512 and row['error_kind']=='OtherError'
    assert 'private' not in json.dumps(row)


def test_diagnostic_retention_is_one_row_per_route(store):
    for n in range(20):full(store,now=100+n*60)
    rows=store.inspect()['listing_diagnostics']
    assert len(rows)==1 and rows[0]['started_at']==1240 and rows[0]['finished_at']==1241


def test_diagnostic_and_hold_commit_roll_back_together(store):
    assert store.begin_listing(BW,now=100,day=DAY,interval=30)=={}
    with store.connect() as db:
        db.execute("CREATE TRIGGER fail_diag BEFORE UPDATE ON fc_listing_diagnostic BEGIN SELECT RAISE(ABORT,'test failure'); END")
    with pytest.raises(sqlite3.IntegrityError):store.finish_listing(BW,status=404,now=101,day=DAY)
    state=store.inspect();r=next(r for r in state['routes'] if r['name']==BW)
    assert r['request_pending']==1 and state['listing_diagnostics'][0]['finished_at'] is None
    assert store.begin_listing(BW,now=1000,day=DAY,interval=30) is None


def test_worker_retains_404_metadata_with_one_request_and_never_reads_error_body(store,monkeypatch):
    calls=[];clock=[100]
    monkeypatch.setattr(runtime.time,'time',lambda:clock[0])
    class UnreadBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError('Refusal response body must not be consumed')
            yield b''
    def respond(request):
        calls.append(str(request.url));clock[0]=101
        return httpx.Response(404,headers={'content-type':'text/html','cache-control':'max-age=30','set-cookie':'secret'},stream=UnreadBody())
    worker=runtime.FinancialContentWorker(SimpleNamespace(),None);worker.store=store;worker.stop_event.set()
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            await worker.poll_listing(client,BW,ROUTES[BW],DAY,30,CTX)
            clock[0]=10000
            await worker.poll_listing(client,BW,ROUTES[BW],DAY,30,CTX)
    asyncio.run(run())
    assert calls==[ROUTES[BW]]
    d=store.inspect()['listing_diagnostics'][0]
    assert d['started_at']==100 and d['finished_at']==101 and d['http_status']==404
    assert d['response_headers']=={'content-type':'text/html','cache-control':'max-age=30'}


def test_worker_snapshot_includes_route_health_without_starting_any_request(store,monkeypatch):
    store.import_controls(blocked_routes=[BW,FRANKLIN],evidence='saved-refusals')
    worker=runtime.FinancialContentWorker(SimpleNamespace(),None);worker.store=store;worker.source_enabled=False
    monkeypatch.setattr(runtime,'session_window',lambda now:(False,DAY,datetime(2026,11,11,22,tzinfo=timezone.utc)))
    async def end_cycle(seconds):worker.stop_event.set()
    monkeypatch.setattr(runtime.asyncio,'sleep',end_cycle)
    async def forbidden(*args,**kwargs):raise AssertionError('No source call expected')
    monkeypatch.setattr(worker,'poll_listing',forbidden)
    asyncio.run(worker._discovery_loop(None))
    h=worker.snapshot()['listing_health']
    assert worker.snapshot()['status']=='outside_hours' and h['status']=='inactive'
    assert h['held_route_count']==2 and h['attention_required']


def test_summary_does_not_mutate_the_input(store):
    full(store);original=store.inspect();copy=deepcopy(original)
    listing_health(original,ROUTES,active=True,now=170)
    assert original==copy
