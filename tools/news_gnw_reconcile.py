"""Reconcile saved FC/GNW discoveries with reviewed original bodies, without publishing.

No network client, PostgreSQL connection, replay, or source-control mutation.
Both exact-body associations and literal original-link corroboration remain
review decisions. This tool does not authorize live publication.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from app.financialcontent.gnw_body import (  # noqa: E402
    MAX_BODY, aware_time, article_date, parse_body, read_bounded, tidy,
    _original_link_candidate, EXCHANGE_LABEL,
)
from app.financialcontent.structured_body import document_text  # noqa: E402
from app.financialcontent.selection import candidate_reason  # noqa: E402
from app.ticker_context import _build_symbol_keywords  # noqa: E402

ROUTE = 'financialcontent-gnw-index'
MAX_ROWS = 5000
MAX_BODIES = 100
MAX_BODY_TOTAL = 8 * MAX_BODY
MAX_JSON = 32 * MAX_BODY


def digest(data):
    return hashlib.sha256(data).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON key')
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError('Non-finite JSON number: ' + value)


def read_json(path, cap=MAX_JSON):
    raw = read_bounded(path, cap)
    return json.loads(raw, object_pairs_hook=_unique_object,
                      parse_constant=_reject_constant), digest(raw)


def window(since, until):
    start, end = aware_time(since), aware_time(until)
    if not timedelta(0) < end - start <= timedelta(days=7):
        raise ValueError('Choose a positive window of at most seven days')
    return start, end


def _bounded_rows(db, sql, args=(), limit=MAX_ROWS):
    rows = [dict(row) for row in db.execute(sql + ' LIMIT ?', (*args, limit + 1))]
    if len(rows) > limit:
        raise ValueError('Row limit exceeded; narrow the saved-evidence window')
    return rows


def snapshot_store(path, since, until):
    start, end = window(since, until)
    if end > datetime.now(timezone.utc):
        raise ValueError('Cannot snapshot a future observation window')
    # mode=ro also prevents accidentally creating a nonexistent store. Do not
    # instantiate the application's Store: its constructor performs migrations.
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    db = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        deadline = time.monotonic() + 5
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute('BEGIN')
        args = (ROUTE, start.timestamp(), end.timestamp())
        candidates = _bounded_rows(db, '''SELECT url,title,route,observed,last_absent,
            reason,status,attempts,body_received,error,request_pending
            FROM fc_candidate WHERE route=? AND observed>=? AND observed<?
            ORDER BY observed,url''', args)
        sightings = _bounded_rows(db, '''SELECT route,url,title,first_seen,initial
            FROM fc_seen WHERE route=? AND first_seen>=? AND first_seen<?
            ORDER BY first_seen,url''', args)
        bodies = _bounded_rows(db, '''SELECT b.*, c.title,c.status,c.observed,c.last_absent,
            c.reason,c.request_pending FROM fc_review_body b
            JOIN fc_candidate c ON c.url=b.url
            WHERE c.route=? AND b.received>=? AND b.received<?
            ORDER BY b.received,b.url''', args, MAX_BODIES)
        total_bytes = 0
        for body in bodies:
            raw = body.pop('html')
            total_bytes += len(raw)
            if len(raw) > MAX_BODY or total_bytes > MAX_BODY_TOTAL:
                raise ValueError('Saved body byte limit exceeded')
            if digest(raw) != body['html_sha256']:
                raise ValueError('Stored source HTML digest mismatch')
            body['html_base64'] = base64.b64encode(raw).decode('ascii')
        routes = _bounded_rows(db, 'SELECT * FROM fc_route ORDER BY name', limit=20)
        controls = _bounded_rows(db, 'SELECT * FROM fc_control ORDER BY key', limit=100)
        diagnostics = _bounded_rows(db, 'SELECT * FROM fc_listing_diagnostic ORDER BY route', limit=20)
        return dict(version=1, kind='fc_gnw_saved_snapshot', since=start.isoformat(),
                    until_exclusive=end.isoformat(), captured_at=datetime.now(timezone.utc).isoformat(),
                    store=str(Path(path).resolve()), mode='ro', query_only=True,
                    deadline_seconds=5, truncated=False, candidates=candidates,
                    sightings=sightings, bodies=bodies, routes=routes, controls=controls,
                    diagnostics=diagnostics,
                    controls_note='Current state at snapshot time; not reconstructed historical controls.')
    finally:
        db.rollback()
        db.close()


def validate_snapshot(snapshot):
    if (snapshot.get('version') != 1 or snapshot.get('kind') != 'fc_gnw_saved_snapshot'
            or snapshot.get('truncated') is not False):
        raise ValueError('An untruncated version 1 GNW snapshot is required')
    start, end = window(snapshot['since'], snapshot['until_exclusive'])
    for key, limit in [('candidates', MAX_ROWS), ('sightings', MAX_ROWS), ('bodies', MAX_BODIES)]:
        if not isinstance(snapshot.get(key), list) or len(snapshot[key]) > limit:
            raise ValueError('Snapshot row limit exceeded')
        urls = [r['url'] for r in snapshot[key]]
        if len(set(urls)) != len(urls):
            raise ValueError('Duplicate snapshot evidence identity')
        clock = {'candidates': 'observed', 'sightings': 'first_seen', 'bodies': 'received'}[key]
        for row in snapshot[key]:
            stamp = row.get(clock)
            if (type(stamp) not in {int, float} or not math.isfinite(stamp)
                    or not start.timestamp() <= stamp < end.timestamp()):
                raise ValueError('Snapshot evidence is outside its observation window')
            article_date(row['url'])
            if key != 'bodies' and row.get('route') != ROUTE:
                raise ValueError('Unexpected route in GNW evidence')
    return snapshot


def read_registry(path):
    raw = read_bounded(path, MAX_BODY)
    rows = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    if not rows or len(rows) > 5000:
        raise ValueError('Bounded registry required')
    active = [r for r in rows if r.get('active', '').upper() == 'TRUE']
    symbols = {r['ticker'] for r in active}
    if len(symbols) != len(active):
        raise ValueError('Duplicate active registry symbols')
    context = SimpleNamespace(known_symbols=symbols,
        symbol_keywords=_build_symbol_keywords([(i, r['ticker'], r.get('fund_name'), r.get('sponsor'),
                                                 r.get('validation_keywords')) for i, r in enumerate(active)]),
        fund_names={r['ticker']: r.get('fund_name') for r in active},
        sponsors={r['ticker']: r.get('sponsor') for r in active})
    return context, dict(path=str(Path(path).resolve()), sha256=digest(raw), rows=len(rows),
                         active_symbols=len(symbols))


def _original(case, archives, symbols):
    """An explicit reviewed case plus an unchanged independently saved native body."""
    if (case.get('classification') != 'genuine_new_release'
            or case.get('original_identity_title_date_consistent') is not True
            or type(case.get('article_id')) is not int or case['article_id'] <= 0):
        raise ValueError('Reviewed original identity/title/date attestation required')
    published = aware_time(case['publisher_body_second_label'])
    link = _original_link_candidate(case['original_url'], published)
    if not link or link['candidate_wire_identity'] != case['original_wire_identity']:
        raise ValueError('Reviewed original URL and wire identity disagree')
    if case['date'] != published.date().isoformat():
        raise ValueError('Reviewed publication day disagrees')
    path = Path(archives).resolve() / f"{case['article_id']}.json"
    # Never follow an arbitrary path supplied by a case document.
    if not path.resolve().is_relative_to(Path(archives).resolve()):
        raise ValueError('Original archive escapes archive directory')
    doc, sha = read_json(path, MAX_BODY)
    if sha != case['archive']['sha256']:
        raise ValueError('Reviewed original archive digest mismatch')
    if (doc.get('article_id') != case['article_id'] or doc.get('wire_key') != case['original_wire_identity']
            or tidy(doc.get('title')) != tidy(case['title']) or doc.get('original_url') != case['original_url']
            or doc.get('source_url') != case['original_url'] or doc.get('body_kind') != 'publisher_body'
            or doc.get('identity_validation') != 'canonical_title_date'
            or aware_time(doc.get('body_published_at')) != published):
        raise ValueError('Original archive provenance disagrees with reviewed case')
    blocks = doc.get('blocks')
    if not isinstance(blocks, list) or not 0 < len(blocks) <= 2000:
        raise ValueError('Original body structure is absent or excessive')
    body_text = document_text(blocks)
    if len(body_text) < 100 or digest(body_text.encode()) != doc.get('text_sha256'):
        raise ValueError('Original body text digest mismatch')
    tickers = case.get('primary_body_fund_tickers')
    evidence = tidy(case.get('body_exchange_evidence'))
    labels = {m[2].upper() for m in EXCHANGE_LABEL.finditer(evidence)}
    if (not isinstance(tickers, list) or not tickers or len(set(tickers)) != len(tickers)
            or not set(tickers) <= symbols or not set(tickers) <= labels
            or len(evidence) < 40 or evidence not in body_text):
        raise ValueError('Reviewed fund evidence must occur in the unchanged primary body')
    return dict(wire_identity=case['original_wire_identity'], title=tidy(case['title']),
                original_url=case['original_url'], published=published.isoformat(),
                article_id=case['article_id'], primary_body_fund_tickers=tickers,
                body_exchange_evidence=evidence, body_text=body_text, blocks=blocks,
                archive_sha256=sha, archive_path=str(path))


def read_originals(paths, archives, symbols):
    if not 1 <= len(paths) <= 4:
        raise ValueError('Use one to four reviewed case files')
    originals, inputs, seen = [], [], {}
    for path in paths:
        data, sha = read_json(path)
        cases = data.get('cases')
        if not isinstance(cases, list) or len(cases) > MAX_ROWS:
            raise ValueError('Bounded reviewed cases array required')
        inputs.append(dict(path=str(Path(path).resolve()), sha256=sha))
        for case in cases:
            # An archive can contain other wires; they are not GNW references.
            if not str(case.get('original_wire_identity', '')).startswith('gnw:'):
                continue
            original = _original(case, archives, symbols)
            identity = original['wire_identity']
            if identity in seen:
                if original != seen[identity]:
                    raise ValueError('Conflicting reviewed records for one original identity')
                continue  # Overlapping daily archives must not double count.
            seen[identity] = original
            originals.append(original)
    if len(originals) > MAX_ROWS:
        raise ValueError('Too many reviewed original releases')
    return originals, inputs


def _tables(blocks):
    return [b for b in blocks if b.get('type') == 'table']


def reconcile_body(review, originals):
    """Conservative corroboration, not a feed-entry adapter or publication permit."""
    result = dict(source_url=review.get('source_url'), title=review.get('title'),
                  status='unresolved', original_identity_validated=False,
                  production_eligible=False, latency_eligible=False,
                  validated_fund_tickers=[], possible_originals=[])
    if review.get('body_parse_valid') is False:
        return dict(result, status='stored_parser_rejection', issues=review.get('issues', []))
    body = review.get('body_text', '')
    if (digest(body.encode()) != review.get('body_sha256')
            or document_text(review['reader']['blocks']) != body):
        raise ValueError('Syndicated body text digest/structure mismatch')
    published = aware_time(review['publication_label'])
    related = [o for o in originals if o['title'] == tidy(review['title'])]
    if not related:
        return dict(result, status='no_reviewed_original_title_match')
    result['possible_originals'] = [dict(wire_identity=o['wire_identity'], article_id=o['article_id']) for o in related]
    dated = [o for o in related if aware_time(o['published']) == published]
    if not dated:
        return dict(result, status='publication_label_conflict')
    identical = [o for o in dated if o['body_text'] == body and _tables(o['blocks']) == _tables(review['reader']['blocks'])]
    if not identical:
        return dict(result, status='body_or_table_conflict')
    if len(identical) != 1:
        return dict(result, status='ambiguous_original_identity')
    original = identical[0]
    links = review.get('original_link_candidates', [])
    identities = {v['candidate_wire_identity'] for v in links}
    if identities and identities != {original['wire_identity']}:
        return dict(result, status='conflicting_literal_original_links')
    result.update(associated_original_identity=original['wire_identity'],
                  original_archive_sha256=original['archive_sha256'],
                  associated_article_id=original['article_id'],
                  reviewed_primary_body_fund_tickers=original['primary_body_fund_tickers'],
                  duplicate_alert_permitted=False,
                  association_basis='exact title, second-resolution label, full body and table structure')
    if not identities:
        return dict(result, status='exact_body_association_requires_identity_review',
                    identity_note='No literal original link; independent case association is not original-wire validation.')
    result.update(status='literal_original_link_corroborated', original_identity_validated=True,
                  validated_fund_tickers=original['primary_body_fund_tickers'])
    return result


def reconcile_snapshot(snapshot, originals, selection_context=None):
    validate_snapshot(snapshot)
    start, end = window(snapshot['since'], snapshot['until_exclusive'])
    originals = [o for o in originals if start <= aware_time(o['published']) < end]
    decisions = []
    total_bytes = 0
    for saved in snapshot['bodies']:
        raw = base64.b64decode(saved['html_base64'], validate=True)
        total_bytes += len(raw)
        if len(raw) > MAX_BODY or total_bytes > MAX_BODY_TOTAL or digest(raw) != saved['html_sha256']:
            raise ValueError('Saved body bounds or source HTML digest mismatch')
        old = json.loads(saved['review_json'], object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        if old.get('source_url') != saved['url'] or old.get('source_html_sha256') != saved['html_sha256']:
            raise ValueError('Saved body provenance mismatch')
        if old.get('body_sha256') != saved['body_sha256']:
            raise ValueError('Stored body digest disagrees with review')
        # A new parser must not silently promote a previously rejected body.
        if old.get('body_parse_valid') is False or saved['status'] != 'identity_review' or saved['request_pending']:
            decisions.append(dict(source_url=saved['url'], title=saved['title'],
                                  status='retained_candidate_hold_or_rejection', stored_status=saved['status'],
                                  production_eligible=False, latency_eligible=False))
            continue
        review = parse_body(raw, article_url=saved['url'], expected_title=saved['title'])
        for key in ['body_sha256', 'body_text', 'publication_label']:
            if review[key] != old[key]:
                raise ValueError('Current reader disagrees with immutable saved review')
        decision = reconcile_body(review, originals)
        decision['observation'] = dict(first_observed=saved['observed'], body_received=saved['received'],
                                     last_absent=saved['last_absent'], selection_reason=saved['reason'],
                                     clocks_note='Saved clocks; no speed winner or availability bound inferred.')
        decision['historical_or_startup'] = old.get('appearance_classification') != 'recent_label_identity_pending'
        decisions.append(decision)
    by_url = {r['url']: r for r in snapshot['candidates']}
    discovery = []
    for original in originals:
        matches = [r for r in snapshot['sightings'] if tidy(r['title']) == original['title']
                   and article_date(r['url']) == aware_time(original['published']).date()]
        discovery.append(dict(original_identity=original['wire_identity'], article_id=original['article_id'],
                              title=original['title'],
                              status='title_date_association_only' if matches else 'unobserved_in_saved_front_pages',
                              matches=[dict(source_url=r['url'], first_observed=r['first_seen'], baseline=bool(r['initial']),
                                            selected=bool(by_url.get(r['url'], {}).get('reason')),
                                            candidate_status=by_url.get(r['url'], {}).get('status')) for r in matches],
                              original_identity_validated_by_listing=False))
        if selection_context is not None:
            discovery[-1]['current_title_selection_replay'] = dict(
                reason=candidate_reason(original['title'], selection_context),
                scope='Offline current CSV registry replay; no synthetic discovery or body request.')
    return dict(body_decisions=decisions, reviewed_discovery_checks=discovery,
                counts=dict(distinct_sightings=len(snapshot['sightings']),
                            baseline_sightings=sum(bool(r['initial']) for r in snapshot['sightings']),
                            candidates=len(snapshot['candidates']),
                            selected_candidates=sum(bool(r['reason']) for r in snapshot['candidates']),
                            candidate_states=dict(Counter(r['status'] for r in snapshot['candidates'])),
                            retained_bodies=len(snapshot['bodies']), reviewed_originals=len(originals),
                            original_titles_observed=sum(bool(r['matches']) for r in discovery),
                            body_decisions=dict(Counter(r['status'] for r in decisions))),
                production_articles_written=0, notifications_sent=0, source_requests=0,
                coverage_miss_count=None, within60_fraction=None,
                limitations=['Finite front pages are not complete GNW or CEF coverage.',
                             'No native original match does not prove a non-CEF story.',
                             'Title/date and exact-body associations without a literal link remain identity review.',
                             'Reviewed original body evidence usually arrives after native ingestion; no earlier delivery is established.',
                             'Startup/history, immutable parser failures, source holds and gaps are retained.',
                             'No decision is a production publication authorization.'])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--store', type=Path)
    mode.add_argument('--snapshot', type=Path, help='Prior output containing its frozen snapshot')
    parser.add_argument('--since')
    parser.add_argument('--until')
    parser.add_argument('--cases', action='append', required=True, type=Path)
    parser.add_argument('--archives', required=True, type=Path)
    parser.add_argument('--registry', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise ValueError('Refusing to overwrite existing evidence')
    source_snapshot_hash = None
    if args.store:
        if not args.since or not args.until:
            parser.error('--store requires --since and --until')
        snapshot = snapshot_store(args.store, args.since, args.until)
    else:
        if args.since or args.until:
            parser.error('A frozen snapshot retains its original observation window')
        prior, source_snapshot_hash = read_json(args.snapshot)
        snapshot = prior['snapshot']
    context, registry = read_registry(args.registry)
    originals, inputs = read_originals(args.cases, args.archives, context.known_symbols)
    report = dict(version=1, generated_at=datetime.now(timezone.utc).isoformat(),
                  snapshot=snapshot, source_snapshot_sha256=source_snapshot_hash,
                  reviewed_case_inputs=inputs, registry=registry,
                  report=reconcile_snapshot(snapshot, originals, context))
    encoded = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    if len(encoded.encode()) > MAX_JSON:
        raise ValueError('Output exceeds bounded evidence size')
    with args.output.open('x', encoding='utf-8') as stream:
        stream.write(encoded)
    print(json.dumps(dict(output=str(args.output.resolve()), **report['report']['counts'],
                          source_requests=0, production_articles_written=0)))
    return report


if __name__ == '__main__':
    main()
