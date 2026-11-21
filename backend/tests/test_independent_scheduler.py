import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.source_scheduler import SourceCoordinator, EMPTY_SYNC
from app.scheduler import IngestionScheduler


def settings(**changes):
    return SimpleNamespace(ingestion_max_workers=2, ingest_cooldown_seconds=.08,
                           source_plan_refresh_seconds=30, ingestion_maintenance_seconds=60,
                           ingestion_advisory_lock_key=123, scheduler_enabled=True, **changes)


def test_parallel_yahoo_is_preserved_inside_the_shared_worker_budget():
    from app.source_scheduler import source_plan, ScheduledChunk
    from app.sources import SourceFeed, FeedDef
    yahoo=SourceFeed('yahoo','Yahoo','https://example.invalid',[FeedDef('https://example.invalid/a'),FeedDef('https://example.invalid/b')])
    wire=SourceFeed('businesswire','BW','https://example.invalid',[FeedDef('https://example.invalid/wire')])
    assert len(source_plan([yahoo,wire],False))==2
    split=source_plan([yahoo,wire],True)
    assert len(split)==3 and len(yahoo.feeds)==2
    stop=threading.Event(); release=threading.Event(); both=threading.Event()
    coordinator=SourceCoordinator(settings(ingestion_parallel_yahoo=True),None,stop)
    coordinator._plan=lambda:{'source_feeds':[yahoo], 'ticker_sync':dict(EMPTY_SYNC)}
    coordinator._maintenance=lambda _:None
    started=[]
    def poll(job):
        assert isinstance(job,ScheduledChunk)
        started.append(job.key)
        if len(started)==2:both.set()
        assert release.wait(2)
        return time.monotonic()
    coordinator._poll=poll
    thread=threading.Thread(target=coordinator.run);thread.start()
    try:
        assert both.wait(1)
        assert len(set(started))==2
    finally:
        stop.set();release.set();thread.join(2)
    assert not thread.is_alive()


def test_slow_source_does_not_block_healthy_repoll_or_overlap_itself():
    stop = threading.Event()
    release = threading.Event()
    slow_started = threading.Event()
    fast_repeated = threading.Event()
    calls = []
    coordinator = SourceCoordinator(settings(), None, stop)
    coordinator._plan = lambda: {"source_feeds":[SimpleNamespace(code=s, feeds=[1]) for s in ("slow", "fast")], "ticker_sync":dict(EMPTY_SYNC)}
    coordinator._maintenance = lambda _: None
    def poll(source):
        calls.append((source.code, time.monotonic()))
        if source.code == "slow":
            slow_started.set()
            assert release.wait(2)
        elif len([c for c in calls if c[0] == "fast"]) >= 3:
            fast_repeated.set()
        return time.monotonic()
    coordinator._poll = poll
    worker = threading.Thread(target=coordinator.run)
    worker.start()
    try:
        assert slow_started.wait(1)
        assert fast_repeated.wait(1.5), "Healthy source was held behind the slow request"
        assert len([c for c in calls if c[0] == "slow"]) == 1
        fast = [t for s,t in calls if s == "fast"]
        assert min(b-a for a,b in zip(fast,fast[1:])) >= .08
    finally:
        stop.set()
        release.set()
        worker.join(2)
    assert not worker.is_alive()
    assert coordinator.snapshot()['status'] == 'stopped'


def test_due_yahoo_chunk_gets_a_turn_when_another_source_is_always_ready():
    from app.sources import FeedDef, SourceFeed

    config = settings(ingestion_parallel_yahoo=True)
    config.ingestion_max_workers = 1
    config.ingest_cooldown_seconds = 0
    feed = FeedDef("https://example.invalid/feed")
    sources = [SourceFeed(code, code, "", [feed]) for code in ("businesswire", "yahoo")]
    stop = threading.Event()
    coordinator = SourceCoordinator(config, None, stop)
    coordinator._plan = lambda: {"source_feeds": sources, "ticker_sync": EMPTY_SYNC}
    coordinator._maintenance = lambda _: None
    calls = []

    def poll(job):
        calls.append(job.key if hasattr(job, "key") else job.code)
        if len(calls) == 4:
            stop.set()
        return time.monotonic()

    coordinator._poll = poll
    coordinator.run()
    assert calls[0] == calls[2] == "businesswire"
    assert calls[1] == calls[3] and calls[1].startswith("yahoo/")


def test_failed_maintenance_retries_ticker_changes_from_prior_refreshes():
    config = settings()
    config.ingestion_maintenance_seconds = 1
    stop = threading.Event()
    coordinator = SourceCoordinator(config, None, stop)
    changed = {**EMPTY_SYNC, "created": 2}
    coordinator._plan = lambda: {"source_feeds": [], "ticker_sync": changed}
    attempts = []

    def maintenance(stats):
        attempts.append(stats)
        if len(attempts) == 1:
            raise RuntimeError("Temporary remap failure")
        stop.set()

    coordinator._maintenance = maintenance
    coordinator.run()
    assert attempts == [changed, changed]


def test_capacity_is_bounded_and_removed_sources_are_not_resubmitted():
    stop = threading.Event()
    coordinator = SourceCoordinator(settings(), None, stop)
    sources = [SimpleNamespace(code=str(i), feeds=[1]) for i in range(6)]
    coordinator._plan = lambda: {"source_feeds":sources, "ticker_sync":dict(EMPTY_SYNC)}
    coordinator._maintenance = lambda _: None
    count, peak, completed = 0, 0, 0
    lock = threading.Lock()
    def poll(_):
        nonlocal count,peak,completed
        with lock:
            count += 1
            peak = max(peak,count)
        time.sleep(.03)
        with lock:
            count -= 1
            completed += 1
            if completed >= 6: stop.set()
        return time.monotonic()
    coordinator._poll = poll
    coordinator.run()
    assert peak == 2
    assert completed >= 6


def test_loss_of_leader_before_plan_starts_no_sources():
    stop = threading.Event()
    coordinator = SourceCoordinator(settings(), None, stop)
    coordinator._plan = MagicMock()
    def lost(): raise RuntimeError('lost leader')
    with pytest.raises(RuntimeError, match='lost leader'):
        coordinator.run(lost)
    coordinator._plan.assert_not_called()
    assert stop.is_set()


def test_global_lock_keeps_dedicated_connection_and_unlocks_on_failure(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.scalar.return_value = True
    engine = MagicMock()
    engine.dialect.name = 'postgresql'
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr('app.scheduler.get_engine', lambda:engine)
    scheduler = IngestionScheduler(settings(), MagicMock())
    def fail(conn):
        assert conn is connection
        raise RuntimeError('test failure')
    with pytest.raises(RuntimeError): scheduler._run_with_global_lock(fail)
    assert connection.execute.call_count == 2
    assert connection.commit.call_count == 2


def test_second_instance_cannot_enter_source_loop(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.scalar.return_value = False
    engine = MagicMock()
    engine.dialect.name = 'postgresql'
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr('app.scheduler.get_engine',lambda:engine)
    operation = MagicMock()
    IngestionScheduler(settings(), MagicMock())._run_with_global_lock(operation)
    operation.assert_not_called()


def test_shutdown_retains_live_thread_to_prevent_second_owner():
    scheduler = IngestionScheduler(settings(), MagicMock())
    thread = MagicMock()
    thread.is_alive.return_value = True
    scheduler._thread = thread
    scheduler.shutdown()
    scheduler.start()
    assert scheduler._thread is thread
    thread.start.assert_not_called()


def test_source_poll_honors_cancellation_before_next_feed(db_session, monkeypatch):
    from app.ingestion import _run_feed_ingestion
    from app.config import Settings
    from app.sources import SourceFeed, FeedDef, seed_sources
    source = SourceFeed('businesswire','BW','https://example.invalid', [FeedDef('https://example.invalid/a'), FeedDef('https://example.invalid/b')])
    seed_sources(db_session,[source])
    calls=[]
    monkeypatch.setattr('app.ingestion.ingest_feed',lambda *a,**k: calls.append(k['feed_url']) or dict(source='businesswire',feed_url=k['feed_url'],status='success',items_seen=0,items_inserted=0,notify_failed=False,error=None))
    _run_feed_ingestion(db_session,Settings(_env_file=None),source_feeds=[source],should_stop=lambda: bool(calls))
    assert calls == ['https://example.invalid/a']
