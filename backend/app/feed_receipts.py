"""Bounded evidence from existing feed responses, never a network collector.

Only a nonempty, identified, well-formed full response supplies a negative
snapshot. A304 is deliberately not promoted to a fresh absence. Each emitted
full-response event carries its own previous snapshot; rotated logs need not
be joined to reconstruct that particular interval.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
import hashlib
import threading
from uuid import uuid4

from app.release_matching import release_identity


def digest(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def receipt_key(entry, source_code: str) -> str | None:
    if not hasattr(entry, 'get'):
        return None
    link, guid = entry.get('link'), entry.get('id') or entry.get('guid')
    link = link.strip() if isinstance(link, str) else ''
    guid = guid.strip() if isinstance(guid, str) else ''
    if max(len(link), len(guid)) > 8192:
        return None
    identity = release_identity(link, guid, source_code)
    if identity is not None:
        key = ':'.join(identity)
        if len(key) <= 256:
            return key
    # Do not log titles, URL query values, tokens, or arbitrary publisher GUIDs.
    if link:
        return 'urlsha256:' + digest(link)
    if guid:
        return 'guidsha256:' + digest(guid)
    return None


def utc_stamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError('Receipt timestamps must be timezone-aware')
    return value.astimezone(timezone.utc).isoformat()


class FeedReceiptTracker:
    def __init__(self, *, max_feeds=64, max_entries=256,
                 snapshot_seconds=300, absence_gap_seconds=120):
        self.max_feeds = max_feeds
        self.max_entries = max_entries
        self.snapshot_seconds = snapshot_seconds
        self.absence_gap_seconds = absence_gap_seconds
        self._states = OrderedDict()
        self._lock = threading.Lock()

    def forget(self, feed_url):
        with self._lock:
            self._states.pop(digest(feed_url), None)

    def observe(self, *, source_code, feed_url, request_started_at, received_at,
                status_code, entries=(), parsed_version='', parse_bozo=True,
                feed_title_present=False, error_kind=None):
        feed_hash = digest(feed_url)
        received = utc_stamp(received_at)
        started = utc_stamp(request_started_at)
        entry_keys = [receipt_key(entry, source_code) for entry in entries[:self.max_entries]]
        keys = sorted({key for key in entry_keys if key is not None})
        unkeyed = entry_keys.count(None)
        if error_kind is not None:
            quality = 'request_or_parse_error'
        elif status_code == 304:
            quality = 'not_modified_no_new_absence'
            keys = []
        elif status_code != 200:
            quality = 'non_full_success'
        elif parse_bozo or not str(parsed_version).startswith(('rss', 'atom')) or not feed_title_present:
            quality = 'unusable_parse'
        elif not entries:
            quality = 'empty_full_response'
        elif len(entries) > self.max_entries:
            quality = 'entry_limit_exceeded'
        elif unkeyed:
            quality = 'unidentified_entries'
        else:
            quality = 'nonempty_full_response'
        if received_at < request_started_at:
            quality = 'non_monotonic_clock'

        with self._lock:
            state = self._states.pop(feed_hash, None)
            if state is None:
                state = {'epoch': uuid4().hex, 'previous': None, 'last_emit': None,
                         'signature': None, 'reset_reason': 'first_in_memory'}
            previous = state['previous']
            reset_reason = state['reset_reason']
            if previous is not None:
                elapsed = (received_at - datetime.fromisoformat(previous['received_at'])).total_seconds()
                if elapsed <= 0 or elapsed > self.absence_gap_seconds:
                    previous = None
                    state['previous'] = None
                    state['epoch'] = uuid4().hex
                    reset_reason = 'non_monotonic_clock' if elapsed <= 0 else 'full_response_gap'
                    state['reset_reason'] = reset_reason

            full = quality == 'nonempty_full_response'
            if not full and quality != 'not_modified_no_new_absence':
                # No interval may silently bridge an unusable/error response.
                state['previous'] = None
                state['epoch'] = uuid4().hex
                state['reset_reason'] = 'after_unusable_response'
                previous = None
            baseline = full and previous is None
            signature = (quality, tuple(keys))
            emit = (signature != state['signature'] or baseline or state['last_emit'] is None
                    or (received_at - state['last_emit']).total_seconds() >= self.snapshot_seconds)
            data = {
                'schema_version': 1, 'source': str(source_code)[:64], 'feed_sha256': feed_hash,
                'observation_epoch': state['epoch'], 'request_started_at': started,
                'response_received_at': received, 'http_status': status_code,
                'quality': quality, 'parsed_entry_count': len(entries),
                'unidentified_entry_count': unkeyed,
                'keys_truncated': len(entries) > self.max_entries,
                'keys': keys, 'baseline': baseline,
                'baseline_reason': reset_reason if baseline else None,
                'previous_nonempty_full_response': previous if full else None,
                'negative_evidence_usable': full and previous is not None,
                # Exception class only; never a message carrying a URL/token.
                'error_kind': error_kind if error_kind in {
                    'ReadTimeout', 'ConnectTimeout', 'ConnectError', 'HTTPStatusError',
                    'ValueError', 'TypeError', 'UnicodeDecodeError', 'RuntimeError'} else ('other' if error_kind else None),
            }
            if full:
                state['previous'] = {'received_at': received, 'keys': keys}
                state['reset_reason'] = None
            if emit:
                state['last_emit'] = received_at
                state['signature'] = signature
            self._states[feed_hash] = state
            while len(self._states) > self.max_feeds:
                self._states.popitem(last=False)
            return data if emit else None
