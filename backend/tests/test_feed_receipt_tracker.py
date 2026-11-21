from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import io
from types import SimpleNamespace

import pytest

from app import research_timing
from app.feed_receipts import FeedReceiptTracker, receipt_key

START = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
FEED = 'https://example.org/feed?secret=do-not-log'
A = {'link': 'https://www.businesswire.com/news/home/20261103123456/en/story'}
B = {'link': 'https://www.businesswire.com/news/home/20261103654321/en/story'}


@pytest.fixture(autouse=True)
def trace_clock(monkeypatch):
    monkeypatch.setattr(research_timing, 'utc_now', lambda: START)


def observe(tracker, seconds=0, entries=None, **kw):
    args = dict(source_code='businesswire', feed_url=FEED,
        request_started_at=START+timedelta(seconds=seconds-1),
        received_at=START+timedelta(seconds=seconds), status_code=200,
        entries=[A] if entries is None else entries,
        parsed_version='rss20', parse_bozo=False, feed_title_present=True)
    args.update(kw)
    return tracker.observe(**args)


@pytest.mark.parametrize('entry,source,expected', [
    (A, 'businesswire', 'businesswire:20261103123456:en'),
    ({'link': 'https://www.businesswire.com/news/home/20261103123456/de/other?x=1'}, 'businesswire', 'businesswire:20261103123456:de'),
    ({'id': 'bw:20261103123456:en'}, 'financialcontent', 'businesswire:20261103123456:en'),
    ({'link': 'https://www.globenewswire.com/news-release/2026/11/02/3393802/16530/en/title.html'}, 'globenewswire', 'globenewswire:3393802:en'),
    ({'link': 'https://www.prnewswire.com/news-releases/a-title-302928090.html'}, 'prnewswire', 'prnewswire:302928090:'),
])
def test_original_wire_identity_and_language(entry, source, expected):
    assert receipt_key(entry, source) == expected


def test_unknown_links_and_guids_are_opaque_not_claimed_wire_ids():
    link = 'https://example.org/story?token=never-print-me'
    assert receipt_key({'link': link}, 'yahoo') == 'urlsha256:' + hashlib.sha256(link.encode()).hexdigest()
    assert receipt_key({'id': 'opaque-private-guid'}, 'yahoo').startswith('guidsha256:')
    assert receipt_key({}, 'yahoo') is None
    assert receipt_key({'link': 'x' * 8193}, 'yahoo') is None


def test_changed_response_carries_immediately_previous_full_absence_without_logging_every_poll():
    tracker = FeedReceiptTracker()
    first = observe(tracker)
    assert first['baseline'] and not first['negative_evidence_usable']
    assert observe(tracker, 20) is None
    changed = observe(tracker, 40, [A, B])
    assert changed['negative_evidence_usable'] and not changed['baseline']
    previous = changed['previous_nonempty_full_response']
    assert previous['received_at'] == (START+timedelta(seconds=20)).isoformat()
    assert previous['keys'] == ['businesswire:20261103123456:en']
    assert 'businesswire:20261103654321:en' in changed['keys']
    assert 'do-not-log' not in json.dumps(changed)


def test_304_never_supplies_a_fresh_absence_even_after_valid_full_response():
    tracker = FeedReceiptTracker()
    observe(tracker)
    unchanged = observe(tracker, 20, [], status_code=304)
    assert unchanged['keys'] == [] and not unchanged['negative_evidence_usable']
    changed = observe(tracker, 40, [B])
    assert changed['previous_nonempty_full_response']['received_at'] == START.isoformat()


def test_304_after_restart_does_not_invent_a_baseline_representation():
    tracker = FeedReceiptTracker()
    event = observe(tracker, status_code=304, entries=[])
    assert event['previous_nonempty_full_response'] is None
    first = observe(tracker, 20, [B])
    assert first['baseline'] and not first['negative_evidence_usable']


@pytest.mark.parametrize('kw,entries', [
    ({}, []),
    ({'parse_bozo': True}, [A]),
    ({'parsed_version': ''}, [A]),
    ({'feed_title_present': False}, [A]),
    ({'status_code': 403, 'error_kind': 'HTTPStatusError'}, []),
    ({'status_code': 429, 'error_kind': 'HTTPStatusError'}, []),
    ({'status_code': None, 'error_kind': 'ReadTimeout'}, []),
    ({'status_code': 200, 'error_kind': 'ValueError'}, [A]),
    ({}, [A, {}]),
    ({}, [A] * 257),
])
def test_unusable_responses_do_not_bridge_negative_intervals(kw, entries):
    tracker = FeedReceiptTracker()
    first = observe(tracker)
    error = observe(tracker, 20, entries, **kw)
    assert not error['negative_evidence_usable']
    after = observe(tracker, 40, [A, B])
    assert after['baseline']
    assert after['observation_epoch'] != first['observation_epoch']
    assert after['previous_nonempty_full_response'] is None


@pytest.mark.parametrize('seconds', [0, -1, 121, 900])
def test_clock_regression_and_full_response_gaps_reset_bounds(seconds):
    tracker = FeedReceiptTracker()
    observe(tracker)
    later = observe(tracker, seconds, [B])
    assert later['baseline'] and later['previous_nonempty_full_response'] is None


def test_long_304_chain_preserves_explicit_full_response_gap_reason():
    tracker = FeedReceiptTracker()
    observe(tracker)
    observe(tracker, 140, [], status_code=304)
    after = observe(tracker, 160, [B])
    assert after['baseline_reason'] == 'full_response_gap'


def test_eviction_and_fresh_tracker_require_new_baselines():
    tracker = FeedReceiptTracker(max_feeds=2)
    observe(tracker, feed_url='https://example.org/a')
    observe(tracker, feed_url='https://example.org/b')
    observe(tracker, feed_url='https://example.org/c')
    assert len(tracker._states) == 2
    assert observe(tracker, 20, feed_url='https://example.org/a')['baseline']
    assert observe(FeedReceiptTracker(), 20)['baseline']


def test_interleaved_feeds_do_not_share_absence_state():
    tracker = FeedReceiptTracker()
    with ThreadPoolExecutor(max_workers=4) as executor:
        events = list(executor.map(lambda n: observe(tracker, entries=[A if n % 2 else B],
            feed_url=f'https://example.org/feed/{n}'), range(16)))
    assert all(event['baseline'] for event in events)
    next_a = observe(tracker, 20, entries=[B], feed_url='https://example.org/feed/1')
    assert next_a['previous_nonempty_full_response']['keys'] == ['businesswire:20261103123456:en']


def test_duplicate_entries_and_reordering_do_not_create_new_appearances():
    tracker = FeedReceiptTracker()
    observe(tracker, entries=[A, B, A])
    assert observe(tracker, 20, [B, A]) is None


def test_periodic_snapshot_retains_bounds_and_limits_output_volume():
    tracker = FeedReceiptTracker(snapshot_seconds=300)
    events = [observe(tracker, n) for n in range(0, 601, 20)]
    assert len([event for event in events if event]) == 3
    assert events[-1]['previous_nonempty_full_response']['received_at'] == (START+timedelta(seconds=580)).isoformat()


def test_configured_handler_expires_without_changing_rotation_limits(scratch_dir, monkeypatch):
    clock = [START]
    monkeypatch.setattr(research_timing, 'utc_now', lambda: clock[0])
    path = scratch_dir / 'receipt.jsonl'
    handler = research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path), research_timing_log_until=START+timedelta(seconds=30),
        research_feed_receipts_enabled=True, research_feed_receipt_dates='2026-11-03'))
    args = dict(source_code='businesswire', feed_url=FEED, request_started_at=START,
        received_at=START, status_code=200, entries=[A], parsed_version='rss20',
        parse_bozo=False, feed_title_present=True)
    try:
        assert handler.maxBytes == 8*1024*1024 and handler.backupCount == 3
        assert research_timing.record_feed_receipt(**args)
        clock[0] += timedelta(seconds=30)
        assert not research_timing.record_feed_receipt(**args)
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    assert len(path.read_text().splitlines()) == 1
    assert not research_timing.record_feed_receipt(**args)


def test_record_io_failure_does_not_escape_and_next_event_is_baseline(scratch_dir, monkeypatch):
    path = scratch_dir / 'receipt.jsonl'
    handler = research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path), research_timing_log_until=START+timedelta(hours=1),
        research_feed_receipts_enabled=True, research_feed_receipt_dates='2026-11-03'))
    args = dict(source_code='businesswire', feed_url=FEED, request_started_at=START,
        received_at=START, status_code=200, entries=[A], parsed_version='rss20',
        parse_bozo=False, feed_title_present=True)
    original = handler.handle
    try:
        monkeypatch.setattr(handler, 'handle', lambda _: (_ for _ in ()).throw(OSError('simulated full disk')))
        assert not research_timing.record_feed_receipt(**args)
        monkeypatch.setattr(handler, 'handle', original)
        assert research_timing.record_feed_receipt(**args)
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    row = json.loads(path.read_text())
    assert row['data']['baseline'] and row['data']['previous_nonempty_full_response'] is None


def test_size_limited_event_contains_no_raw_credentials_or_payload(scratch_dir, monkeypatch):
    tracker = FeedReceiptTracker()
    entries = [{'link': f'https://u:secret@example.org/{n}?password=secret', 'title': 'secret-title',
                'summary': 'secret-body'} for n in range(256)]
    observe(tracker, entries=entries)
    event = observe(tracker, 20, entries=entries[1:]+[{'link': 'https://example.org/new'}])
    output = json.dumps(event)
    assert len(output.encode()) < 64*1024
    assert 'secret' not in output and 'do-not-log' not in output


def test_real_stream_failure_is_detected_even_when_logging_swallows_it(scratch_dir):
    class FullDisk(io.StringIO):
        def write(self, _):
            raise OSError('simulated full disk')
    path = scratch_dir / 'receipt.jsonl'
    handler = research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path), research_timing_log_until=START+timedelta(hours=1),
        research_feed_receipts_enabled=True, research_feed_receipt_dates='2026-11-03'))
    args = dict(source_code='businesswire', feed_url=FEED, request_started_at=START,
        received_at=START, status_code=200, entries=[A], parsed_version='rss20',
        parse_bozo=False, feed_title_present=True)
    try:
        handler.stream = FullDisk()
        assert not research_timing.record_feed_receipt(**args)
        assert not handler.receipt_tracker._states
        handler.stream.close()
        handler.stream = None
        assert research_timing.record_feed_receipt(**args)
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    assert json.loads(path.read_text())['data']['baseline']


@pytest.mark.parametrize('now,enabled,dates,allowed', [
    ('2026-11-03T07:59:59+00:00', True, '2026-11-03', False),
    ('2026-11-03T08:00:00+00:00', True, '2026-11-03', True),
    ('2026-11-03T21:59:59+00:00', True, '2026-11-03', True),
    ('2026-11-03T22:00:00+00:00', True, '2026-11-03', False),
    ('2026-11-04T12:00:00+00:00', True, '2026-11-03', False),
    ('2026-11-07T12:00:00+00:00', True, '2026-11-03,2026-11-06', False),
    ('2026-11-03T12:00:00+00:00', False, '2026-11-03', False),
    ('2026-11-03T12:00:00+00:00', True, '', False),
    ('2026-11-03T12:00:00+00:00', True, 'invalid-date', False),
])
def test_explicit_date_window_and_default_off(scratch_dir, monkeypatch, now, enabled, dates, allowed):
    moment = datetime.fromisoformat(now)
    monkeypatch.setattr(research_timing, 'utc_now', lambda: moment)
    path = scratch_dir / 'window.jsonl'
    handler = research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path), research_timing_log_until=datetime(2026,11,8,tzinfo=timezone.utc),
        research_feed_receipts_enabled=enabled, research_feed_receipt_dates=dates))
    try:
        result = research_timing.record_feed_receipt(source_code='businesswire', feed_url=FEED,
            request_started_at=moment, received_at=moment, status_code=200, entries=[A],
            parsed_version='rss20', parse_bozo=False, feed_title_present=True)
        assert result is allowed
        if not allowed:
            assert not handler.receipt_tracker._states
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    assert path.exists() is allowed


def test_settings_default_off_and_existing_trace_does_not_enable_receipts(scratch_dir):
    from app.config import Settings
    settings = Settings(_env_file=None)
    assert settings.research_feed_receipts_enabled is False
    assert settings.research_feed_receipt_dates == ''
    path = scratch_dir / 'default.jsonl'
    handler = research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path), research_timing_log_until=START+timedelta(hours=1)))
    try:
        assert not research_timing.record_feed_receipt(source_code='businesswire', feed_url=FEED,
            request_started_at=START, received_at=START, status_code=200, entries=[A])
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    assert not path.exists()
