from datetime import datetime, timedelta, timezone
import json
import logging
import os
from types import SimpleNamespace

import pytest

from app import research_timing as timing
from app.news_validation_timing import validation_timing_window


DATES = '2026-11-09,2026-11-10,2026-11-11,2026-11-12,2026-11-13'


def moment(value):
    return datetime.fromisoformat(value + '+00:00')


def config(root, **overrides):
    values = dict(news_validation_timing_enabled=True,
                  news_validation_timing_log_path=str(root / 'new' / 'timing.jsonl'),
                  news_validation_timing_dates=DATES,
                  research_timing_log_path=str(root / 'old.jsonl'),
                  research_timing_log_until=moment('2026-11-06T22:00:00'))
    values.update(overrides)
    return SimpleNamespace(**values)


def close(handler):
    if handler is not None:
        logging.getLogger().removeHandler(handler)
        handler.close()


@pytest.mark.parametrize('at,allowed', [
    ('2026-11-08T12:00:00', False),
    ('2026-11-09T07:59:59', False),
    ('2026-11-09T08:00:00', True),
    ('2026-11-09T21:59:59', True),
    ('2026-11-09T22:00:00', False),
    ('2026-11-10T00:00:00', False),
    ('2026-11-10T08:00:00', True),
    ('2026-11-13T21:59:59', True),
    ('2026-11-13T22:00:00', False),
    ('2026-11-14T12:00:00', False),
    ('2026-11-16T08:00:00', False),
])
def test_all_stages_obey_daily_and_final_cutoffs_with_no_restart(scratch_dir, monkeypatch, at, allowed):
    clock = [moment('2026-11-08T12:00:00')]
    monkeypatch.setattr(timing, 'utc_now', lambda: clock[0])
    settings = config(scratch_dir)
    old = scratch_dir / 'old.jsonl'
    old.write_bytes(b'original study evidence\n')
    handler = timing.configure_research_timing(settings)
    try:
        assert handler is not None  # Sunday preparation attaches without recording.
        assert not handler.baseFilename.endswith('old.jsonl')
        clock[0] = moment(at)
        for event in timing.EVENTS:
            handler.handle(logging.LogRecord('app', logging.INFO, '', 0, event + ' {"n":1}', (), None))
        status = timing.research_timing_status()
        assert status['recording_now'] is allowed
        assert status['process_id'] == os.getpid()
        assert status['until'] == '2026-11-13T22:00:00+00:00'
    finally:
        close(handler)
    target = scratch_dir / 'new' / 'timing.jsonl'
    assert target.exists() is allowed
    if allowed:
        rows = [json.loads(line) for line in target.read_text().splitlines()]
        assert {r['event'] for r in rows} == timing.EVENTS
        assert all(r['configuration_fingerprint'] == status['configuration_fingerprint'] for r in rows)
    assert old.read_bytes() == b'original study evidence\n'


@pytest.mark.parametrize('overrides', [
    {'news_validation_timing_dates': ''},
    {'news_validation_timing_dates': '20261109'},
    {'news_validation_timing_dates': '2026-11-31'},
    {'news_validation_timing_dates': '2026-11-09,'},
    {'news_validation_timing_dates': '2026-11-09,2026-11-09'},
    {'news_validation_timing_dates': '2026-11-14'},
    {'news_validation_timing_dates': '2026-11-09,2026-11-23'},
    {'news_validation_timing_dates': ','.join(['2026-11-09'] * 11)},
    {'news_validation_timing_start_hour_utc': 22, 'news_validation_timing_end_hour_utc': 8},
    {'news_validation_timing_start_hour_utc': -1},
    {'news_validation_timing_end_hour_utc': 25},
    {'news_validation_timing_start_hour_utc': True},
    {'news_validation_timing_log_path': None},
])
def test_invalid_windows_fail_closed_without_legacy_fallback(scratch_dir, monkeypatch, overrides):
    monkeypatch.setattr(timing, 'utc_now', lambda: moment('2026-11-09T12:00:00'))
    settings = config(scratch_dir, research_timing_log_until=moment('2027-01-01T00:00:00'), **overrides)
    before = list(logging.getLogger().handlers)
    assert timing.configure_research_timing(settings) is None
    assert logging.getLogger().handlers == before
    assert not (scratch_dir / 'old.jsonl').exists()


@pytest.mark.parametrize('name', ['old.jsonl', 'old.jsonl.1', 'old.jsonl.backup'])
def test_cannot_append_to_any_old_log_path(scratch_dir, name):
    with pytest.raises(ValueError, match='legacy_path_collision'):
        validation_timing_window(config(scratch_dir, news_validation_timing_log_path=str(scratch_dir / name)))


def test_only_explicit_dates_and_timezone_aware_times_are_valid(scratch_dir):
    window = validation_timing_window(config(scratch_dir, news_validation_timing_dates='2026-11-09,2026-11-11'))
    assert not window.accepts(moment('2026-11-10T12:00:00'))
    assert not window.accepts(datetime(2026, 11, 9, 12))
    assert window.accepts(datetime.fromisoformat('2026-11-09T10:00:00+02:00'))
    assert not window.accepts(datetime.fromisoformat('2026-11-10T00:00:00+02:00'))


def test_start_after_final_cutoff_does_not_attach_or_make_directory(scratch_dir, monkeypatch):
    monkeypatch.setattr(timing, 'utc_now', lambda: moment('2026-11-13T22:00:00'))
    assert timing.configure_research_timing(config(scratch_dir)) is None
    assert not (scratch_dir / 'new').exists()


def test_status_is_loaded_configuration_even_if_saved_settings_change(scratch_dir, monkeypatch):
    monkeypatch.setattr(timing, 'utc_now', lambda: moment('2026-11-08T12:00:00'))
    settings = config(scratch_dir)
    handler = timing.configure_research_timing(settings)
    try:
        expected = timing.research_timing_status()
        settings.news_validation_timing_dates = '2026-11-16'
        assert timing.research_timing_status() == expected
        assert 'path' not in expected and 'log_path' not in expected
    finally:
        close(handler)
    assert not timing.research_timing_status()['handler_attached']


def test_receipts_reset_after_overnight_and_after_process_restart(scratch_dir, monkeypatch):
    clock = [moment('2026-11-09T21:59:40')]
    monkeypatch.setattr(timing, 'utc_now', lambda: clock[0])
    settings = config(scratch_dir)
    def receipt():
        return timing.record_feed_receipt(source_code='businesswire', feed_url='https://example.org/feed',
            request_started_at=clock[0]-timedelta(seconds=1), received_at=clock[0], status_code=200,
            entries=[{'id':'bw:20261109123456:en'}], parsed_version='rss20', parse_bozo=False, feed_title_present=True)
    handler = timing.configure_research_timing(settings)
    try:
        assert receipt()
        clock[0] = moment('2026-11-10T08:00:05')
        assert receipt()
    finally:
        close(handler)
    clock[0] += timedelta(seconds=20)
    handler = timing.configure_research_timing(settings)
    try:
        assert receipt()
    finally:
        close(handler)
    rows = [json.loads(line) for line in (scratch_dir/'new/timing.jsonl').read_text().splitlines()]
    assert len(rows) == 3
    assert all(row['data']['baseline'] and row['data']['previous_nonempty_full_response'] is None for row in rows)
    assert len({row['data']['observation_epoch'] for row in rows}) == 3


def test_request_started_before_window_cannot_supply_absence(scratch_dir, monkeypatch):
    now = moment('2026-11-09T08:00:02')
    monkeypatch.setattr(timing, 'utc_now', lambda: now)
    handler = timing.configure_research_timing(config(scratch_dir))
    try:
        assert not timing.record_feed_receipt(feed_url='https://example.org/feed',
            request_started_at=now-timedelta(seconds=3), received_at=now)
        assert not handler.receipt_tracker._states
    finally:
        close(handler)


def test_close_between_receipt_check_and_write_does_not_claim_success(scratch_dir, monkeypatch):
    clock = [moment('2026-11-13T21:59:59')]
    monkeypatch.setattr(timing, 'utc_now', lambda: clock[0])
    handler = timing.configure_research_timing(config(scratch_dir))
    original = handler.handle
    def cutoff(record):
        clock[0] = moment('2026-11-13T22:00:00')
        return original(record)
    monkeypatch.setattr(handler, 'handle', cutoff)
    try:
        assert not timing.record_feed_receipt(source_code='businesswire', feed_url='https://example.org/feed',
            request_started_at=clock[0]-timedelta(seconds=1), received_at=clock[0], status_code=200,
            entries=[{'id':'bw:20261113123456:en'}], parsed_version='rss20', parse_bozo=False, feed_title_present=True)
        assert not handler.receipt_tracker._states
    finally:
        close(handler)
    assert not (scratch_dir/'new/timing.jsonl').exists()


def test_new_capture_defaults_off():
    from app.config import Settings
    settings = Settings(_env_file=None)
    assert not settings.news_validation_timing_enabled
    assert settings.news_validation_timing_dates == ''
    assert validation_timing_window(settings) is None
