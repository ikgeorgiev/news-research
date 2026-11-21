"""Offline comparison of explicitly reviewed, independent source observations.

This module never fetches a source or validates a vendor's permission. A reviewed
original URL/title reference is required; vendor IDs and related tickers alone
are insufficient. A missing route is unobserved, not a proven coverage miss.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlsplit

MAX_BYTES = 5 * 1024 * 1024
MAX_ROWS = 1000
HOSTS = {'businesswire.com': ('bw', r'^/news/home/(\d+)/([a-z]{2})(?:/|$)'),
         'globenewswire.com': ('gnw', r'^/news-release/\d{4}/\d{2}/\d{2}/(\d+)/\d+/([a-z]{2})(?:/|$)'),
         'prnewswire.com': ('prn', r'^/news-releases/.+-(\d+)\.html$')}


def timestamp(value):
    if not isinstance(value, str) or len(value) > 80 or 'T' not in value:
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return result.astimezone(timezone.utc) if result.tzinfo is not None else None
    except ValueError:
        return None


def identity(value):
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        url = urlsplit(value)
        if url.scheme != 'https' or url.username or url.password or url.port:
            return None
        provider = HOSTS.get((url.hostname or '').removeprefix('www.'))
        match = re.match(provider[1], url.path, re.I) if provider else None
        return ':'.join((provider[0], *[v.lower() for v in match.groups()])) if match else None
    except ValueError:
        return None


def label(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split()) if isinstance(value, str) else ''


def read_document(path):
    # Read a fixed bound even if a file grows between stat and open.
    with Path(path).open('rb') as stream:
        data = stream.read(MAX_BYTES+1)
    if len(data) > MAX_BYTES:
        raise ValueError('Observation file exceeds 5 MiB')
    try:
        return json.loads(data)
    except (ValueError, UnicodeError) as exc:
        raise ValueError('Invalid observation JSON') from exc


def compare(document):
    if not isinstance(document, dict) or document.get('version') != 1:
        raise ValueError('Observation schema version 1 required')
    rows = document.get('observations')
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise ValueError('At most 1000 observations allowed')
    excluded, candidates = [], []
    for index, row in enumerate(rows):
        reason = None
        if not isinstance(row, dict):
            excluded.append({'row': index, 'reason': 'invalid_row'})
            continue
        wire = identity(row.get('original_url'))
        title = label(row.get('title'))
        evidence = row.get('identity_evidence')
        route, clock = row.get('route'), row.get('clock_id')
        received = timestamp(row.get('received_at'))
        if not wire or not title or len(title) > 2000:
            reason = 'missing_original_wire_or_title'
        elif not isinstance(evidence, dict) or identity(evidence.get('original_url')) != wire \
                or label(evidence.get('title')) != title or not isinstance(evidence.get('reference'), str) \
                or not evidence['reference'].strip() or len(evidence['reference']) > 4096:
            reason = 'missing_matching_reviewed_evidence'
        elif not isinstance(route, str) or not route.strip() or len(route) > 120:
            reason = 'invalid_route'
        elif row.get('classification') != 'new':
            reason = 'baseline_or_history_not_speed_sample'
        elif row.get('receipt_kind') != 'network_response':
            reason = 'not_an_independent_network_receipt'
        elif not received or not isinstance(clock, str) or not clock.strip() or len(clock) > 120:
            reason = 'missing_receipt_time_or_clock'
        if reason:
            excluded.append({'row': index, 'reason': reason})
            continue
        candidates.append(dict(index=index, wire_key=wire, title=title, route=route,
            clock_id=clock, received=received, row=row))

    # Conflicting headlines invalidate every occurrence of that wire identity.
    titles = defaultdict(set)
    for c in candidates:
        titles[c['wire_key']].add(c['title'])
    first, duplicates = {}, []
    for c in candidates:
        if len(titles[c['wire_key']]) != 1:
            excluded.append({'row': c['index'], 'reason': 'conflicting_title_for_wire'})
            continue
        # Do not merge different clocks into a manufactured earlier first sighting.
        key = (c['wire_key'], c['route'], c['clock_id'])
        previous = first.get(key)
        if previous is None:
            first[key] = c
        elif c['received'] < previous['received']:
            duplicates.append(previous['index'])
            first[key] = c
        else:
            duplicates.append(c['index'])

    routes, releases = defaultdict(list), defaultdict(list)
    for c in first.values():
        row, received = c['row'], c['received']
        published = timestamp(row.get('publication_label'))
        proxy = None
        proxy_status = 'missing_or_imprecise_publication_label'
        if row.get('publication_precision') == 'second' and published is not None:
            if received >= published:
                proxy = round((received-published).total_seconds(), 6)
                proxy_status = 'publisher_label_proxy_only'
            else:
                proxy_status = 'negative_label_delta_clock_or_metadata_error'
        entry = dict(wire_key=c['wire_key'], route=c['route'], title=c['title'],
            clock_id=c['clock_id'], received_at=received.isoformat(),
            source_row=c['index'], publication_label_to_receipt_s=proxy, proxy_status=proxy_status)
        routes[c['route']].append(entry)
        releases[c['wire_key']].append(entry)

    pairs, incomparable = [], []
    for wire, sightings in releases.items():
        for a,b in combinations(sightings,2):
            if a['route'] == b['route']:
                continue
            a,b = sorted([a,b], key=lambda e:(e['route'],e['clock_id']))
            item = dict(wire_key=wire, route_a=a['route'], route_b=b['route'])
            if a['clock_id'] != b['clock_id']:
                incomparable.append({**item,'reason':'different_unreconciled_clocks'})
                continue
            pairs.append({**item, 'clock_id':a['clock_id'],
                'b_minus_a_receipt_seconds':round((timestamp(b['received_at'])-timestamp(a['received_at'])).total_seconds(),6)})
    summaries = {}
    for route, entries in sorted(routes.items()):
        deltas = [e['publication_label_to_receipt_s'] for e in entries if e['publication_label_to_receipt_s'] is not None]
        successes = sum(d <= 60 for d in deltas)
        summaries[route] = dict(independent_receipts=len(entries), original_wires=len({e['wire_key'] for e in entries}),
            second_precision_label_proxy_samples=len(deltas), within_60s=successes,
            fraction_within_60s=successes/len(deltas) if deltas else None,
            label_proxy_outliers=[e for e in entries if e['publication_label_to_receipt_s'] is not None and e['publication_label_to_receipt_s']>60],
            entries=entries)
    return dict(version=1, input_rows=len(rows), accepted_first_receipts=len(first),
        excluded=excluded, excluded_reasons=dict(Counter(e['reason'] for e in excluded)),
        duplicate_input_rows=sorted(duplicates), routes=summaries,
        matched_receipt_comparisons=pairs, incomparable_receipt_pairs=incomparable,
        known_original_wires=len(releases),
        limitations=[
            'Identity evidence is supplied by a reviewer; this offline tool does not verify a public body.',
            'Publisher labels are proxies, not first public availability or delivery guarantees.',
            'Minute-only and date-only labels are excluded from subminute fractions.',
            'Different clocks are not compared unless reconciled before input; shared clock IDs are an explicit input claim.',
            'Releases from one feed response are correlated; receipt counts are not independent statistical trials.',
            'Missing routes are unobserved, not coverage misses. No complete source census is supplied.',
            'Related-ticker labels are not validated body tickers; ticker correctness is not inferred.',
            'Commit, notification, body availability and physical screen delivery remain separate and unmeasured here.',
        ])
