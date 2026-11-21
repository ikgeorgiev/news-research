from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest
from sqlalchemy import event, func, insert, select, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.ingestion import run_ingestion_cycle
from app.ingestion_history import MAX_RECENT_RUNS, compact_ingestion_runs, daily_ingestion_activity
from app.models import IngestionPollDaily, IngestionRun, Source
from app.routes.admin import admin_ingestion_status
from app.source_scheduler import EMPTY_SYNC, SourceCoordinator


NOW = datetime(2026, 11, 21, 18, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 11, 19, tzinfo=timezone.utc)
OLD = CUTOFF - timedelta(microseconds=1)
BACKOFF = "Feed is in backoff until 2026-11-19T00:00:01.123456+00:00"


@pytest.fixture(params=["sqlite", "postgres"])
def history_db(request):
    if request.param == "sqlite":
        yield request.getfixturevalue("db_session")
    else:
        with Session(request.getfixturevalue("disposable_postgres_engine")) as session:
            yield session


def source(db, code):
    row = Source(code=code, name=code, base_url="https://example.invalid")
    db.add(row)
    db.flush()
    return row.id


def poll(source_id, label, started_at=OLD, **changes):
    row = dict(source_id=source_id, feed_url=label, started_at=started_at, finished_at=started_at,
               status="success", items_seen=0, items_inserted=0, error_text=None)
    return {**row, **changes}


def seed_recent(db, source_id, when=NOW):
    db.execute(insert(IngestionRun.__table__), [
        poll(source_id, f"recent-{i}", when) for i in range(MAX_RECENT_RUNS)
    ])


def daily(db):
    return [dict(row) for row in daily_ingestion_activity(db, days=7, now=NOW)]


def test_batches_preserve_exact_daily_totals_and_only_fold_routine_polls(history_db):
    db = history_db
    wire, yahoo, delivery = (source(db, code) for code in ("businesswire", "yahoo", "financialcontent"))
    rows = [
        poll(wire, "empty"), poll(wire, "repeat", items_seen=12, error_text=""),
        poll(wire, "wait", status="skipped_backoff", error_text=BACKOFF),
        poll(yahoo, "other-source", items_seen=4),
        poll(wire, "other-day", OLD - timedelta(days=1), items_seen=3),
        poll(wire, "at-boundary", CUTOFF),
        poll(wire, "crossed-midnight", finished_at=CUTOFF),
        poll(wire, "in-progress", status="running", finished_at=None),
        poll(wire, "no-finish", finished_at=None),
        poll(wire, "failure", status="failed"), poll(wire, "baseline", status="issuer_baseline"),
        poll(wire, "insert", items_seen=2, items_inserted=1),
        poll(wire, "warning", error_text="Skipped malformed entry."),
        poll(wire, "annotated-wait", status="skipped_backoff", error_text=BACKOFF + " (manual hold)"),
        poll(delivery, "delivery"),
    ]
    db.execute(insert(IngestionRun.__table__), rows)
    seed_recent(db, wire)
    db.commit()
    if db.get_bind().dialect.name == "postgresql":
        db.execute(text("SET LOCAL timezone='Asia/Tokyo'"))
    initial = daily(db)
    assert sum(r["runs"] for r in initial) == len(rows) + MAX_RECENT_RUNS
    assert compact_ingestion_runs(db, detail_days=0, now=NOW) == 0
    for expected in (2, 2, 1, 0):
        assert compact_ingestion_runs(db, detail_days=2, batch_size=2, now=NOW) == expected
        assert daily(db) == initial
        db.commit()
    kept = set(db.scalars(select(IngestionRun.feed_url)))
    assert {r["feed_url"] for r in rows if r["feed_url"] not in kept} == {
        "empty", "repeat", "wait", "other-source", "other-day",
    }
    wire_day = db.get(IngestionPollDaily, (OLD.date(), wire))
    assert (wire_day.runs, wire_day.items_seen, wire_day.backoff_skips) == (3, 12, 1)


def test_quiet_database_retains_the_exact_admin_history_including_timestamp_ties(history_db):
    db = history_db
    wire = source(db, "prnewswire")
    db.execute(insert(IngestionRun.__table__), [poll(wire, "one older id"), poll(wire, "another older id")])
    seed_recent(db, wire, OLD)
    db.commit()
    recent = admin_ingestion_status(limit=MAX_RECENT_RUNS, db=db)
    assert compact_ingestion_runs(db, detail_days=2, now=NOW) == 2
    db.commit()
    assert recent == admin_ingestion_status(limit=MAX_RECENT_RUNS, db=db)


def test_failed_summary_write_rolls_back_removal_of_detail(history_db):
    db = history_db
    wire = source(db, "globenewswire")
    db.execute(insert(IngestionRun.__table__), poll(wire, "claim"))
    seed_recent(db, wire)
    db.commit()

    def fail_summary(_conn, _cursor, statement, *_args):
        if statement.startswith("INSERT INTO ingestion_poll_daily"):
            raise RuntimeError("injected summary failure")

    event.listen(db.get_bind(), "before_cursor_execute", fail_summary)
    try:
        with pytest.raises(RuntimeError, match="injected summary failure"), db.begin():
            compact_ingestion_runs(db, detail_days=2, now=NOW)
    finally:
        event.remove(db.get_bind(), "before_cursor_execute", fail_summary)
    assert db.scalar(select(IngestionRun.id).where(IngestionRun.feed_url == "claim")) is not None
    assert db.scalar(select(IngestionPollDaily.runs)) is None
    assert compact_ingestion_runs(db, detail_days=2, now=NOW) == 1
    db.commit()
    assert db.scalar(select(IngestionPollDaily.runs)) == 1


def test_concurrent_postgres_workers_account_for_each_run_once(disposable_postgres_engine):
    factory = sessionmaker(disposable_postgres_engine)
    with factory.begin() as setup:
        wire = source(setup, "businesswire")
        setup.execute(insert(IngestionRun.__table__), [poll(wire, f"old-{i}", items_seen=1) for i in range(12)])
        seed_recent(setup, wire)

    reached_summary = Event()
    def second_worker():
        with factory.begin() as worker:
            @event.listens_for(worker.connection(), "before_cursor_execute")
            def observe(_conn, _cursor, statement, *_args):
                if statement.startswith("INSERT INTO ingestion_poll_daily"):
                    reached_summary.set()
            return compact_ingestion_runs(worker, detail_days=2, batch_size=6, now=NOW)

    with ThreadPoolExecutor(max_workers=1) as pool, factory() as first:
        assert compact_ingestion_runs(first, detail_days=2, batch_size=6, now=NOW) == 6
        future = pool.submit(second_worker)
        try:
            assert reached_summary.wait(3), "The second worker did not skip the first worker's claimed runs"
        finally:
            first.commit()
        assert future.result(timeout=5) == 6
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(IngestionRun)) == MAX_RECENT_RUNS
        assert [(r.runs, r.items_seen) for r in db.scalars(select(IngestionPollDaily))] == [(12, 12)]


@pytest.mark.parametrize("independent", [True, False])
def test_normal_ingestion_modes_compact_without_fetching_sources(db_session, independent):
    db = db_session
    wire = source(db, "prnewswire")
    when = datetime.now(timezone.utc)
    db.execute(insert(IngestionRun.__table__), poll(wire, "expired", when - timedelta(days=10)))
    seed_recent(db, wire, when)
    db.commit()
    settings = Settings(_env_file=None, raw_feed_prune_interval_seconds=0, source_enable_bw=False,
                        source_enable_yahoo=False, source_enable_gn=False, source_enable_prn=False,
                        tickers_csv_path="missing-in-history-test.csv")
    if independent:
        coordinator = SourceCoordinator(settings, sessionmaker(db.get_bind()), Event())
        coordinator._maintenance(EMPTY_SYNC)
    else:
        run_ingestion_cycle(db, settings)
    assert db.scalar(select(IngestionPollDaily.runs)) == 1
