from datetime import datetime, timedelta, timezone
import json
import logging
from types import SimpleNamespace

from app import research_timing


def test_only_stage_diagnostics_are_copied_without_changing_console_records(scratch_dir, monkeypatch):
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    monkeypatch.setattr(research_timing, "utc_now", lambda: now)
    path = scratch_dir / "timing.jsonl"
    handler = research_timing.ResearchTimingHandler(path, now + timedelta(hours=1))
    try:
        for event in sorted(research_timing.EVENTS):
            record = logging.LogRecord("app.sources", logging.INFO, "", 0, "%s %s",
                                       (event, '{"stage_ms": 12.5}'), None)
            before = record.getMessage()
            handler.handle(record)
            assert record.getMessage() == before
        for message in ("Other application message", "article_latency not-json", "article_latency []"):
            handler.handle(logging.LogRecord("app", logging.INFO, "", 0, message, (), None))
    finally:
        handler.close()
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert {line['event'] for line in lines} == research_timing.EVENTS
    assert len(lines) == len(research_timing.EVENTS)
    assert all(line['data']['stage_ms'] == 12.5 for line in lines)
    assert all(line['process_id'] == record.process for line in lines)


def test_collection_stops_at_deadline_even_when_process_stays_running(scratch_dir, monkeypatch):
    now = datetime(2026, 10, 29, 20, tzinfo=timezone.utc)
    clock = [now]
    monkeypatch.setattr(research_timing, "utc_now", lambda: clock[0])
    path = scratch_dir / "timing.jsonl"
    handler = research_timing.ResearchTimingHandler(path, now + timedelta(hours=1))
    record = logging.LogRecord("app", logging.INFO, "", 0, 'article_latency {"n":1}', (), None)
    try:
        handler.handle(record)
        clock[0] += timedelta(hours=1)
        handler.handle(record)
    finally:
        handler.close()
    assert len(path.read_text().splitlines()) == 1


def test_unconfigured_or_expired_collection_does_not_attach_handler(scratch_dir, monkeypatch):
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(research_timing, "utc_now", lambda: now)
    before = list(logging.getLogger().handlers)
    for path, until in ((None, None), (str(scratch_dir / 'trace'), None),
                        (str(scratch_dir / 'trace'), now-timedelta(seconds=1))):
        assert research_timing.configure_research_timing(SimpleNamespace(
            research_timing_log_path=path, research_timing_log_until=until)) is None
    assert logging.getLogger().handlers == before
    assert not (scratch_dir / 'trace').exists()
