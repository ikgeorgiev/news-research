"""Retention and daily reporting for the feed polling ledger."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Date, cast, delete, func, literal, or_, select, type_coerce, union_all
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.models import ROUTINE_POLL_PREDICATE, IngestionPollDaily, IngestionRun, Source
from app.utils import to_utc


MAX_RECENT_RUNS = 300
RSS_SOURCES = ("businesswire", "globenewswire", "prnewswire", "yahoo")
# Only the timestamp-only message emitted by ingest_feed is routine.
_BACKOFF_NOTE = (
    r"^Feed is in backoff until [0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{6})?[+]00:00$"
)


@dataclass
class _PollCounts:
    runs: int = 0
    items_seen: int = 0
    backoff_skips: int = 0


def compact_ingestion_runs(
    db: Session, *, detail_days: int, batch_size: int = 5000, now: datetime | None = None,
) -> int:
    """Fold one bounded batch. Deletion and counting belong to the caller's transaction."""
    if detail_days <= 0:
        return 0
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    cutoff = to_utc(now or datetime.now(timezone.utc)).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff -= timedelta(days=detail_days)
    run = IngestionRun.__table__.c
    latest = select(run.id).order_by(run.started_at.desc(), run.id.desc()).limit(MAX_RECENT_RUNS)
    candidates = select(run.id).where(
        ROUTINE_POLL_PREDICATE,
        run.finished_at < cutoff,
        run.started_at < cutoff,
        run.source_id.in_(select(Source.id).where(Source.code.in_(RSS_SOURCES))),
        run.id.not_in(latest),
        or_(run.status != "skipped_backoff", run.error_text.is_(None),
            run.error_text == "", run.error_text.regexp_match(_BACKOFF_NOTE)),
    ).order_by(run.finished_at).limit(batch_size)

    postgres = db.get_bind().dialect.name == "postgresql"
    if postgres:
        # Freeze this worker's bounded set of rows; other workers can take another batch.
        claimed = candidates.with_for_update(skip_locked=True).cte("claimed").prefix_with("MATERIALIZED")
        candidates = select(claimed.c.id)
    removed = db.execute(
        delete(IngestionRun.__table__).where(run.id.in_(candidates))
        .returning(run.source_id, run.started_at, run.items_seen, run.status)
    )
    counts: dict[tuple[date, int], _PollCounts] = defaultdict(_PollCounts)
    for source_id, started_at, seen, status in removed:
        group = counts[to_utc(started_at).date(), source_id]
        group.runs += 1
        group.items_seen += seen
        group.backoff_skips += int(status == "skipped_backoff")
    if not counts:
        return 0

    daily = IngestionPollDaily.__table__
    insert = (pg_insert if postgres else sqlite_insert)(daily)
    add_counts = insert.on_conflict_do_update(
        index_elements=[daily.c.day, daily.c.source_id],
        set_={key: daily.c[key] + insert.excluded[key] for key in ("runs", "items_seen", "backoff_skips")},
    )
    # Acquire daily-row locks in the same order across concurrent workers.
    db.execute(add_counts, [
        dict(day=day, source_id=source_id, runs=count.runs, items_seen=count.items_seen,
             backoff_skips=count.backoff_skips)
        for (day, source_id), count in sorted(counts.items())
    ])
    return sum(count.runs for count in counts.values())


def daily_ingestion_activity(db: Session, *, days: int, now: datetime | None = None):
    """Read retained and compacted runs in one snapshot, including partially folded days."""
    today = to_utc(now or datetime.now(timezone.utc)).date()
    since = today - timedelta(days=days - 1)
    until = today + timedelta(days=1)
    run = IngestionRun.__table__.c
    summary = IngestionPollDaily.__table__.c
    if db.get_bind().dialect.name == "postgresql":
        day = cast(func.timezone("UTC", run.started_at), Date)
    else:
        # SQLite stores application timestamps as UTC text. Preserve the day even
        # for the final microsecond before midnight.
        day = type_coerce(func.substr(run.started_at, 1, 10), Date)
    detail = select(
        day.label("day"), run.source_id, func.count().label("runs"),
        func.sum(run.items_seen).label("items_seen"), func.sum(run.items_inserted).label("items_inserted"),
        func.count().filter(run.status == "failed").label("failed_runs"),
        func.count().filter(run.status == "skipped_backoff").label("backoff_skips"),
    ).where(
        run.started_at >= datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc),
        run.started_at < datetime.combine(until, datetime.min.time(), tzinfo=timezone.utc),
    ).group_by(day, run.source_id)
    folded = select(summary.day, summary.source_id, summary.runs, summary.items_seen,
                    literal(0), literal(0), summary.backoff_skips).where(summary.day.between(since, today))
    totals = union_all(detail, folded).subquery()
    query = select(
        totals.c.day, Source.code.label("source"),
        *(func.sum(totals.c[key]).label(key)
          for key in ("runs", "items_seen", "items_inserted", "failed_runs", "backoff_skips")),
    ).join_from(totals, Source, Source.id == totals.c.source_id).group_by(
        totals.c.day, Source.code,
    ).order_by(totals.c.day.desc(), Source.code)
    return db.execute(query).mappings().all()
