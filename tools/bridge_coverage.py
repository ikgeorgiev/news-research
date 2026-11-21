"""Local-only diagnostics for passive news copies; never fetches a source.

Filter eligibility is not verified release coverage. The source's own ticker
tags, pagination and native update behavior still bound what can be observed.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
STALE_SECONDS = 180


def universe(path=None):
    path = Path(path or ROOT / 'data/cef_tickers.csv')
    raw = path.read_bytes()
    rows = csv.DictReader(raw.decode('utf-8-sig').splitlines())
    values = sorted({r['ticker'].strip().upper() for r in rows
                     if r.get('active', '').strip().upper() in ('TRUE', '1', 'YES')})
    if not values or len(values) > 1000 or any(not re.fullmatch(r'[A-Z0-9.^-]{1,20}', x) for x in values):
        raise ValueError('Invalid current CEF universe')
    return {'symbols': values, 'count': len(values), 'csv_sha256': hashlib.sha256(raw).hexdigest()}


def filter_scope(source, filters, expected):
    """Recognize narrow, explicit patterns; ambiguous queries stay unknown."""
    target = set(expected['symbols'])
    mode, included, restrictions = 'unverified_expression', set(), {}
    if source == 'godel-browser':
        chosen = filters.get('symbols')
        restrictions = {k: filters.get(k) for k in ('sources', 'languages', 'legal', 'dateTimeRange')}
        if 'symbols' not in filters:
            mode = 'symbols_omitted_unverified'
        elif chosen == []:
            mode, included = 'all_symbols', target
        elif isinstance(chosen, list) and all(isinstance(s, str) and re.fullmatch(r'[A-Z0-9.^-]{1,20}', s) for s in chosen):
            mode, included = 'explicit_symbols', target.intersection(chosen)
    elif source == 'newsfilter-browser':
        query = filters.get('queryString', '').strip()
        restrictions = {'query': query}
        if query in ('*', '*:*', 'NOT source.id:"sec-api"'):
            mode, included = 'all_symbols', target
        elif re.fullmatch(r'symbols:\("[A-Z0-9.^-]+"(?: OR "[A-Z0-9.^-]+")*\)', query):
            mode, included = 'explicit_symbols', target.intersection(re.findall(r'"([A-Z0-9.^-]+)"', query))
        elif re.fullmatch(r'symbols:"?[A-Z0-9.^-]+"?', query):
            mode, included = 'explicit_symbols', target.intersection({query.split(':', 1)[1].strip('"')})
        else:
            match = re.fullmatch(r'title:"([a-zA-Z0-9.^-]+)" OR description:"\1" OR symbols:"\1"', query)
            if match:
                mode, included = 'explicit_symbol_with_text', target.intersection({match[1].upper()})
    return {'mode': mode, 'eligible_symbols': sorted(included), 'eligible_count': len(included),
            'universe_count': expected['count'], 'universe_sha256': expected['csv_sha256'],
            'missing_symbols': sorted(target - included), 'restrictions': restrictions,
            'meaning': 'query eligibility only; not verified original-release or ticker coverage'}


def continuity(db, source, scope, observed, session, capture_id, ids, capacity):
    now = datetime.fromisoformat(observed.replace('Z', '+00:00'))
    previous = db.execute('SELECT * FROM streams WHERE source=? AND scope=? AND day=?',
                          (source, scope, now.date().isoformat())).fetchone()
    reason, gap, overlap = 'first_sample', None, None
    full = capacity is not None and len(ids) >= capacity
    if previous:
        gap = (now - datetime.fromisoformat(previous['last_observed'])).total_seconds()
        old_ids = set(json.loads(previous['last_ids']))
        overlap = len(old_ids.intersection(ids))
        if previous['session'] != session + ':' + capture_id:
            reason = 'new_capture_or_receiver'
        elif gap < 0:
            reason = 'clock_reversal'
        elif gap > STALE_SECONDS:
            reason = 'native_update_gap'
        elif old_ids and ids and overlap == 0 and (full or (capacity and len(old_ids) >= capacity)):
            reason = 'full_page_without_overlap'
        else:
            reason = 'repeated_native_sample'
    return {'reason': reason, 'baseline_required': reason != 'repeated_native_sample',
            'previous_sample_at': previous['last_observed'] if previous else None,
            'sample_gap_seconds': gap, 'overlap_ids': overlap, 'page_capacity': capacity,
            'full_page': full, 'finite_page': True,
            'global_absence_proven': False}


def assess(db, source, scope, rows, observed, session, capture_id, filters, capacity):
    return {'continuity': continuity(db, source, scope, observed, session, capture_id,
                                    {r['local_id'] for r in rows}, capacity),
            'filter_scope': filter_scope(source, filters, universe())}


def status(db, source, session, now=None, capture_id=None):
    """Bounded status derived from saved copies, never from connection liveness."""
    now = now or datetime.now(timezone.utc)
    rows = db.execute('SELECT scope,observed,meta FROM responses WHERE source=? AND (status=200 OR (status IS NULL AND scope NOT IN (\'native_stream_batch\',\'native_worker_batch\',\'unparsed\'))) AND error IS NULL ORDER BY id DESC LIMIT 2000', (source,)).fetchall()
    scopes, seen = [], set()
    expected = universe()
    eligible = set()
    for row in rows:
        meta = json.loads(row['meta'])
        if meta.get('session') != session or (capture_id is not None and meta.get('capture_id') != capture_id) or row['scope'] in seen:
            continue
        seen.add(row['scope'])
        age = (now - datetime.fromisoformat(row['observed'])).total_seconds()
        assessment = meta.get('assessment') or {}
        matching = assessment.get('filter_scope') or filter_scope(source, meta.get('filters', {}), expected)
        fresh = 0 <= age <= STALE_SECONDS
        if fresh:
            eligible.update(matching['eligible_symbols'])
        scopes.append({'scope': row['scope'], 'last_native_copy_at': row['observed'],
                       'age_seconds': round(age, 2), 'fresh': fresh,
                       'filter_scope': matching, 'continuity': assessment.get('continuity')})
    return {'native_status': 'no_native_updates' if not scopes else 'fresh' if any(s['fresh'] for s in scopes) else 'stale',
            'checked_at': now.isoformat(), 'stale_after_seconds': STALE_SECONDS,
            'fresh_filter_eligible_count': len(eligible), 'universe_count': expected['count'],
            'fresh_filter_missing_symbols': sorted(set(expected['symbols']) - eligible),
            'scopes': scopes, 'scan_truncated_possible': len(rows) == 2000,
            'continuous_coverage_verified': False,
            'limitation': 'Repeated copies demonstrate sampling only; native auto-update mode and full release coverage require separate validation.'}
