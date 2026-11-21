"""Offline receipt evidence: real ingest path, mocked HTTP, disposable SQLite."""
import json
import logging
import io
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from app import article_ingest, research_timing
from app.models import Article, RawFeedItem, Source

FEED = 'https://www.globenewswire.com/RssFeed/public-companies'
XML = b'''<rss version="2.0"><channel><title>GlobeNewswire</title>
<item><title>Unmatched company release</title><link>https://www.globenewswire.com/news-release/2026/11/03/3399991/0/en/item.html</link><guid>one</guid></item>
<item><title>Communique francais</title><link>https://www.globenewswire.com/news-release/2026/11/03/3399992/0/fr/item.html</link><guid>two</guid></item>
<item><link>https://www.globenewswire.com/news-release/2026/11/03/3399993/0/en/item.html</link><guid>three</guid></item>
</channel></rss>'''

TRACE_NOW = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def trace_clock(monkeypatch):
    monkeypatch.setattr(research_timing, 'utc_now', lambda: TRACE_NOW)


def configured_trace(path):
    return research_timing.configure_research_timing(SimpleNamespace(
        research_timing_log_path=str(path),
        research_timing_log_until=TRACE_NOW + timedelta(hours=1),
        research_feed_receipts_enabled=True, research_feed_receipt_dates='2026-11-03'))


def finish(handler):
    logging.getLogger().removeHandler(handler)
    handler.close()


def ingest(db, monkeypatch, payload=XML, status=200):
    source = db.scalar(select(Source).where(Source.code == 'globenewswire'))
    if source is None:
        source = Source(code='globenewswire', name='GlobeNewswire', base_url='https://www.globenewswire.com')
        db.add(source)
        db.commit()
    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', lambda **_: httpx.Response(
        status, content=payload, request=httpx.Request('GET', FEED)))
    return article_ingest.ingest_feed(db, source=source, feed_url=FEED,
        known_symbols=set(), symbol_to_id={}, timeout_seconds=5,
        globenewswire_source_page_max_fetches_per_feed=0,
        postcommit_enrichment_enabled=False)


def test_receipt_is_retained_before_unmatched_language_and_malformed_filters(db_session, scratch_dir, monkeypatch):
    path = scratch_dir / 'trace.jsonl'
    handler = configured_trace(path)
    try:
        result = ingest(db_session, monkeypatch)
    finally:
        finish(handler)
    assert result['status'] == 'success'
    assert db_session.scalars(select(Article)).all() == []
    assert len(db_session.scalars(select(RawFeedItem)).all()) == 1
    lines = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    receipts = [row for row in lines if row['event'] == 'feed_receipt']
    assert len(receipts) == 1, 'Missing immutable receipt evidence for filtered/unattached releases'
    assert receipts[0]['data']['keys'] == ['globenewswire:3399991:en',
        'globenewswire:3399992:fr', 'globenewswire:3399993:en']
    assert receipts[0]['data']['baseline'] is True
    assert receipts[0]['data']['previous_nonempty_full_response'] is None


def test_empty_or_malformed_response_is_not_successful_absence(db_session, scratch_dir, monkeypatch):
    path = scratch_dir / 'trace.jsonl'
    handler = configured_trace(path)
    try:
        ingest(db_session, monkeypatch)
        ingest(db_session, monkeypatch, b'<html><title>Unavailable</title></html>')
        ingest(db_session, monkeypatch)
    finally:
        finish(handler)
    receipts = [json.loads(line)['data'] for line in path.read_text().splitlines()
                if json.loads(line)['event'] == 'feed_receipt']
    assert len(receipts) == 3
    assert receipts[1]['quality'] == 'unusable_parse'
    assert receipts[-1]['baseline'] and receipts[-1]['previous_nonempty_full_response'] is None


def test_request_failure_is_recorded_without_credentials(db_session, scratch_dir, monkeypatch):
    path = scratch_dir / 'trace.jsonl'
    source = Source(code='globenewswire', name='GlobeNewswire', base_url='https://www.globenewswire.com')
    db_session.add(source)
    db_session.commit()
    def fail(**_):
        raise httpx.ReadTimeout('password=must-not-appear', request=httpx.Request('GET', FEED))
    monkeypatch.setattr(article_ingest, '_fetch_feed_with_retries', fail)
    handler = configured_trace(path)
    try:
        result = article_ingest.ingest_feed(db_session, source=source, feed_url=FEED,
            known_symbols=set(), symbol_to_id={}, timeout_seconds=5)
    finally:
        finish(handler)
    assert result['status'] == 'failed'
    output = path.read_text()
    assert 'must-not-appear' not in output
    receipt = json.loads(output)['data']
    assert receipt['quality'] == 'request_or_parse_error'
    assert receipt['error_kind'] == 'ReadTimeout'
    assert not receipt['negative_evidence_usable']


def test_disk_failure_does_not_change_ingestion_or_retry_decisions(db_session, scratch_dir, monkeypatch):
    class FullDisk(io.StringIO):
        def write(self, _):
            raise OSError('full')
    handler = configured_trace(scratch_dir / 'trace.jsonl')
    handler.stream = FullDisk()
    try:
        result = ingest(db_session, monkeypatch)
    finally:
        finish(handler)
    assert result['status'] == 'success'
    assert len(db_session.scalars(select(RawFeedItem)).all()) == 1
    assert db_session.scalars(select(Article)).all() == []


def test_pending_retry_items_are_not_reported_as_current_feed_receipts(db_session, scratch_dir, monkeypatch):
    pending_url = 'https://www.globenewswire.com/news-release/2026/11/02/3398888/0/en/old.html'
    monkeypatch.setattr(article_ingest, 'due_enrichment_entries', lambda *_: [
        {'id': 'old-pending', 'link': pending_url, 'title': 'Old pending release',
         '_enrichment_queued_at': '2026-11-02T20:00:00Z'}])
    path = scratch_dir / 'trace.jsonl'
    handler = configured_trace(path)
    try:
        result = ingest(db_session, monkeypatch)
    finally:
        finish(handler)
    assert result['status'] == 'success'
    receipts = [json.loads(line)['data'] for line in path.read_text().splitlines()
                if json.loads(line)['event'] == 'feed_receipt']
    assert len(receipts) == 1
    assert 'globenewswire:3398888:en' not in receipts[0]['keys']
    assert len(db_session.scalars(select(RawFeedItem)).all()) == 2
