"""Offline reader coverage and browser-delivery reports. No app imports or I/O clients."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
from urllib.parse import urlsplit

MAX_INPUT_BYTES = 5 * 1024 * 1024
MAX_ARCHIVE_BYTES = 1024 * 1024
MAX_ARTICLES = 1000
SESSION_MS = 20 * 60 * 1000


def _invalid_constant(value):
    raise ValueError(f'Non-finite JSON number: {value}')


def read_json(path, maximum=MAX_INPUT_BYTES):
    """The digest describes exactly the bounded bytes parsed, not a later read."""
    path = Path(path)
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError(f'Input exceeds {maximum} bytes: {path.name}')
    try:
        document = json.loads(raw.decode('utf-8-sig'), parse_constant=_invalid_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError(f'Invalid JSON: {path.name}') from exc
    return document, dict(path=str(path.resolve()), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def timestamp(value):
    if not isinstance(value, str) or len(value) > 80:
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except (ValueError, OverflowError):
        return None


def finite(value):
    try:
        return type(value) in (float, int) and math.isfinite(value)
    except OverflowError:
        return False


def article_id(value):
    return type(value) is int and 0 < value < 10**10


def request_id(value):
    return value is None or (type(value) is int and value > 0)


def text(value, maximum=60_000):
    return isinstance(value, str) and len(value) <= maximum


def wire_identity(value):
    if not text(value, 4096):
        return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {'http', 'https'} or parsed.username or parsed.password or parsed.port:
            return None
        host = (parsed.hostname or '').removeprefix('www.')
        rules = {
            'businesswire.com': ('bw', r'^/news/home/(\d{12,20})/([a-z]{2})(?:/|$)'),
            'globenewswire.com': ('gnw', r'^/news-release/\d{4}/\d{2}/\d{2}/(\d+)/\d+/([a-z]{2})/'),
            'prnewswire.com': ('prn', r'^/news-releases/.+-(\d+)\.html$'),
        }
        rule = rules.get(host)
        match = re.match(rule[1], parsed.path, re.I) if rule else None
        return ':'.join([rule[0], *[part.lower() for part in match.groups()]]) if match else None
    except ValueError:
        return None


def fraction(numerator, denominator):
    return dict(numerator=numerator, denominator=denominator,
                fraction=numerator / denominator if denominator else None)


def duration_summary(values):
    values = sorted(values)
    return dict(n=len(values), minimum_ms=values[0] if values else None,
                median_ms=statistics.median(values) if values else None,
                p95_nearest_rank_ms=values[math.ceil(.95 * len(values)) - 1] if values else None,
                maximum_ms=values[-1] if values else None,
                within_60_seconds=fraction(sum(value <= 60_000 for value in values), len(values)))


def _array(value, maximum, name):
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError(f'{name} must contain at most {maximum} rows')
    return value


def delivery_report(document):
    if not isinstance(document, dict) or type(document.get('version')) is not int or document['version'] != 1 or document.get('type') != 'cef_local_delivery_check':
        raise ValueError('Delivery check export version 1 required')
    rows = _array(document.get('articles'), 400, 'articles')
    events = _array(document.get('events'), 1200, 'events')
    session = document.get('session')
    if not isinstance(session, dict) or type(session.get('active')) is not bool:
        raise ValueError('A delivery session is required')
    start, expiry = timestamp(session.get('startedAt')), timestamp(session.get('expiresAt'))
    if not start or not expiry or abs((expiry - start).total_seconds() - 1200) > .001:
        raise ValueError('A bounded 20-minute session is required')
    drift = document.get('max_wall_vs_monotonic_drift_ms')
    dropped = session.get('dropped')
    if not finite(drift) or drift < 0 or type(dropped) is not int or dropped < 0:
        raise ValueError('Invalid clock or truncation metadata')
    if type(document.get('clock_warning')) is not bool:
        raise ValueError('Clock warning must be explicit')
    # The collector owns the monotonic duration; wall-clock subtraction is never used.
    viewport_events = set()
    responses = {}
    event_counts = Counter()
    for event in events:
        if not isinstance(event, dict) or not text(event.get('type'), 100) or not timestamp(event.get('at')) \
                or not finite(event.get('elapsed_ms')) or event['elapsed_ms'] < 0 \
                or not request_id(event.get('request_id')) \
                or ('detail' in event and not text(event['detail'], 1000)):
            raise ValueError('Invalid event record')
        event_counts[event['type']] += 1
        if event['type'] == 'viewport_after_two_frames':
            viewport_events.add((event.get('detail'), event.get('request_id')))
        if event['type'] == 'response_received':
            responses[event.get('request_id')] = event.get('detail')
    ids = Counter(row.get('article_id') for row in rows
                  if isinstance(row, dict) and article_id(row.get('article_id')))
    invalid, classified = [], []
    for index, row in enumerate(rows):
        reason = None
        if not isinstance(row, dict) or not article_id(row.get('article_id')):
            reason = 'invalid_article_id'
        elif ids[row['article_id']] != 1:
            reason = 'duplicate_article_id'
        elif not text(row.get('provider'), 120) or not row['provider'] or not text(row.get('title'), 1000):
            reason = 'invalid_article_metadata'
        elif not timestamp(row.get('received_at')):
            reason = 'invalid_receipt_timestamp'
        elif not isinstance(row.get('classification'), str) or row['classification'] not in {'baseline', 'automatic_new', 'manual_or_history'}:
            reason = 'invalid_classification'
        elif type(row.get('awaiting_user')) is not bool:
            reason = 'missing_user_action_classification'
        elif not request_id(row.get('request_id')):
            reason = 'invalid_request_id'
        elif row['classification'] == 'automatic_new' and (row.get('request_id') is None
                or (dropped == 0 and row['request_id'] not in responses)):
            reason = 'automatic_response_event_missing'
        elif row['classification'] == 'automatic_new' and row['request_id'] in responses \
                and responses[row['request_id']] not in {'automatic', 'queued'}:
            reason = 'response_kind_mismatch'
        elif row['classification'] == 'automatic_new' and responses.get(row['request_id']) == 'queued' and not row['awaiting_user']:
            reason = 'queued_response_requires_user_action'
        elif 'viewport_at' not in row:
            reason = 'missing_viewport_field'
        elif row.get('viewport_at') is None and row.get('receipt_to_viewport_ms') is not None:
            reason = 'duration_without_viewport'
        elif row.get('viewport_at') is not None:
            if not timestamp(row['viewport_at']) or not finite(row.get('receipt_to_viewport_ms')) \
                    or not 0 <= row['receipt_to_viewport_ms'] <= SESSION_MS:
                reason = 'invalid_viewport_duration'
            elif dropped == 0 and (str(row['article_id']), row.get('request_id')) not in viewport_events:
                reason = 'viewport_event_missing'
        if reason:
            invalid.append(dict(row=index, article_id=row.get('article_id') if isinstance(row, dict) else None, reason=reason))
            continue
        if row['classification'] != 'automatic_new':
            group = row['classification']
        elif row['awaiting_user']:
            group = 'awaiting_user_action'
        elif row['viewport_at'] is None:
            group = 'automatic_viewport_unobserved'
        else:
            group = 'automatic_viewport_observed'
        classified.append(dict(article_id=row['article_id'], provider=row['provider'],
                               date_utc=timestamp(row['received_at']).date().isoformat(),
                               group=group, received_at=row['received_at'], viewport_at=row.get('viewport_at'),
                               receipt_to_viewport_ms=row.get('receipt_to_viewport_ms')))

    def summarize(subset):
        counts = Counter(row['group'] for row in subset)
        durations = [row['receipt_to_viewport_ms'] for row in subset if row['group'] == 'automatic_viewport_observed']
        return dict(counts=dict(counts), eligible_automatic=counts['automatic_viewport_observed'] + counts['automatic_viewport_unobserved'],
                    viewport_unobserved=counts['automatic_viewport_unobserved'],
                    measured_receipt_to_viewport=duration_summary(durations))

    groups = defaultdict(list)
    for row in classified:
        groups[(row['date_utc'], row['provider'])].append(row)
    return dict(version=1, report='browser_delivery', synthetic_fixture=document.get('synthetic_fixture') is True,
                input_rows=len(rows), invalid_rows=invalid,
                totals=summarize(classified), by_date_and_provider=[dict(date_utc=key[0], provider=key[1], **summarize(value))
                    for key, value in sorted(groups.items())], rows=classified,
                outliers_over_60_seconds=sorted([r for r in classified if r['group'] == 'automatic_viewport_observed'
                                                and r['receipt_to_viewport_ms'] > 60_000], key=lambda r: -r['receipt_to_viewport_ms']),
                session=dict(active_at_export=session['active'], started_at=session['startedAt'], expires_at=session['expiresAt'],
                             stopped_reason=session.get('stoppedReason'), dropped_records=dropped,
                             event_counts=dict(event_counts), local_clock_warning=document['clock_warning'] or drift > 2000,
                             maximum_wall_monotonic_drift_ms=drift),
                limitations=[
                    'Measured values are local response-to-viewport durations, not physical screen delivery or publisher-to-screen latency.',
                    'The within-60-second denominator contains measured automatic rows only; unobserved rows are censored, not successful or slow.',
                    'Baseline, manual/history and click-to-load rows are excluded from automatic durations.',
                    'Automatic-new classification is inherited from the collector and depends on unverified browser/server clock alignment; independently verify new releases.',
                    'Provider groups describe the displayed delivery provider, not independent publisher-route arrival times.',
                    'A hidden-tab interval or user interaction can affect local delivery; this report does not attribute its cause to the backend.',
                    'An active or truncated export is incomplete. No server commit/notification stages are inferred from labels.',
                ])


def archive_summary(document, article, expected_wire=None):
    """Validate renderable structure and independent snapshot identity, not text completeness."""
    if not isinstance(document, dict) or type(document.get('version')) is not int or document['version'] != 1 \
            or not article_id(document.get('article_id')) or document['article_id'] != article['id']:
        raise ValueError('article_identity_mismatch')
    if not text(document.get('title')) or document['title'].strip() != article['title'].strip():
        raise ValueError('title_mismatch')
    wire = wire_identity(document.get('original_url'))
    if wire is None or wire != document.get('wire_key'):
        raise ValueError('original_wire_identity_mismatch')
    snapshot_wire = wire_identity(article.get('canonical_url', article.get('url')))
    if (expected_wire and expected_wire != wire) or (snapshot_wire and snapshot_wire != wire):
        raise ValueError('snapshot_wire_identity_mismatch')
    published = timestamp(document.get('published_at'))
    if published is None or not text(document.get('source_url')):
        raise ValueError('invalid_publisher_metadata')
    snapshot_date = timestamp(article.get('published_at'))
    if snapshot_date and abs((published - snapshot_date).total_seconds()) > 60:
        raise ValueError('publication_label_mismatch')
    blocks = _array(document.get('blocks'), 2000, 'body_blocks')
    if not blocks:
        raise ValueError('empty_body')
    stats = dict(blocks=len(blocks), tables=0, nonempty_table_cells=0, text_characters=0)
    cells = 0
    for block in blocks:
        if not isinstance(block, dict):
            raise ValueError('invalid_block')
        kind = block.get('type')
        if not isinstance(kind, str):
            raise ValueError('invalid_block_type')
        if kind in {'paragraph', 'heading'}:
            if not text(block.get('text')) or (kind == 'heading' and (type(block.get('level')) is not int or block['level'] not in {2, 3, 4})):
                raise ValueError('invalid_text_block')
            stats['text_characters'] += len(block['text'].strip())
        elif kind == 'list':
            if type(block.get('ordered')) is not bool or not all(text(item) for item in _array(block.get('items'), 2000, 'list_items')):
                raise ValueError('invalid_list')
            stats['text_characters'] += sum(len(item.strip()) for item in block['items'])
        elif kind == 'table':
            if not text(block.get('caption')):
                raise ValueError('invalid_table_caption')
            stats['tables'] += 1
            for row in _array(block.get('rows'), 2000, 'table_rows'):
                for cell in _array(row, 80, 'table_cells'):
                    cells += 1
                    if cells > 15000 or not isinstance(cell, dict) or not text(cell.get('text')) \
                            or type(cell.get('header')) is not bool \
                            or any(type(cell.get(key)) is not int or not 1 <= cell[key] <= 40 for key in ['colspan', 'rowspan']):
                        raise ValueError('invalid_table_cell')
                    stats['nonempty_table_cells'] += bool(cell['text'].strip())
                    stats['text_characters'] += len(cell['text'].strip())
        else:
            raise ValueError('unknown_block_type')
    if stats['text_characters'] == 0:
        raise ValueError('empty_body')
    return dict(**stats, wire_key=wire, original_url=document['original_url'],
                independent_snapshot_wire_match=bool(expected_wire or snapshot_wire))


def reviewed_case_wire(case):
    if not case:
        return None
    declared = case.get('wire_identity')
    original = wire_identity(case.get('canonical_url'))
    # The retained study used bare PRN numbers. Accept only with its independently
    # supplied original PRN URL; a vendor-local numeric ID is never sufficient.
    if isinstance(declared, str) and re.fullmatch(r'\d+', declared):
        if original == 'prn:' + declared:
            return original
        raise ValueError('unverified_bare_case_identity')
    if declared is not None:
        if not isinstance(declared, str) or not re.fullmatch(r'(?:(?:bw|gnw):\d+:[a-z]{2}|prn:\d+)', declared):
            raise ValueError('unsupported_reviewed_case_identity')
        if original and original != declared:
            raise ValueError('reviewed_case_identity_conflict')
        return declared
    return original


def reader_report(document, archives, cases_document=None):
    if not isinstance(document, dict):
        raise ValueError('Article snapshot object required')
    present = [key for key in ('articles', 'items') if key in document]
    if len(present) != 1:
        raise ValueError('Exactly one articles or items list is required')
    articles = _array(document[present[0]], MAX_ARTICLES, 'articles')
    ids = set()
    for row in articles:
        if not isinstance(row, dict) or not article_id(row.get('id')) or not text(row.get('title')) or not row['title'].strip():
            raise ValueError('Invalid article snapshot row')
        provider = row.get('provider_name', row.get('provider', 'unknown'))
        if not text(provider, 120) or not provider or (row.get('summary') is not None and not text(row['summary'], MAX_INPUT_BYTES)):
            raise ValueError('Invalid article provider or summary')
        if row['id'] in ids:
            raise ValueError('Duplicate article ID in snapshot')
        ids.add(row['id'])
    cases = {}
    if cases_document is not None:
        if not isinstance(cases_document, dict):
            raise ValueError('Invalid reviewed case document')
        for case in _array(cases_document.get('cases'), MAX_ARTICLES, 'cases'):
            if not isinstance(case, dict) or not article_id(case.get('article_id')) or case['article_id'] in cases:
                raise ValueError('Invalid or duplicate reviewed case')
            cases[case['article_id']] = case
    archives = Path(archives).resolve()
    if not archives.is_dir():
        raise ValueError('Archive directory does not exist')
    rows = []
    for article in articles:
        aid = article['id']
        case = cases.get(aid)
        row = dict(article_id=aid, title=article['title'], provider=article.get('provider_name', article.get('provider', 'unknown')),
                   published_at=article.get('published_at'), summary_characters=len(article.get('summary') or ''),
                   case_mode=case.get('mode') if case else None, reviewed_study_case=case is not None,
                   retained_case_wire_identity=case.get('wire_identity') if case else None,
                   original_366_cohort=case.get('original_366_cohort') if case else None)
        path = archives / f'{aid}.json'
        if not path.resolve().is_relative_to(archives):
            row.update(status='archive_invalid', reason='archive_path_outside_directory')
        elif not path.exists():
            row['status'] = 'archive_missing'
        else:
            try:
                body, evidence = read_json(path, MAX_ARCHIVE_BYTES)
                stats = archive_summary(body, article, reviewed_case_wire(case))
                row.update(status='archive_validated', archive=evidence, **stats)
            except (ValueError, OSError) as exc:
                row.update(status='archive_invalid', reason=str(exc))
        rows.append(row)

    def summarize(subset):
        counts = Counter(row['status'] for row in subset)
        valid = [r for r in subset if r['status'] == 'archive_validated']
        return dict(articles=len(subset), statuses=dict(counts), validated_archive_fraction=fraction(len(valid), len(subset)),
                    tables=sum(r['tables'] for r in valid), nonempty_table_cells=sum(r['nonempty_table_cells'] for r in valid),
                    summaries_at_most_900_characters=sum(r['summary_characters'] <= 900 for r in subset))

    providers = defaultdict(list)
    for row in rows:
        providers[row['provider']].append(row)
    return dict(version=1, report='reader_coverage', totals=summarize(rows),
                by_provider={key: summarize(value) for key, value in sorted(providers.items())},
                reviewed_study_cases=summarize([r for r in rows if r['reviewed_study_case']]),
                reviewed_cases_absent_from_snapshot=sorted(set(cases) - ids), rows=rows,
                limitations=[
                    'This is present reader availability for the supplied snapshot, not original live coverage or a new latency sample.',
                    'A validated archive has consistent metadata and renderable structure; full publisher-text completeness is not proven.',
                    'Missing archives do not prove a failed fetch or permission to fetch. No network request or backfill was performed.',
                    'Short summary counts are length diagnostics, not proof that a release was truncated.',
                    'Manual recoveries and historical cohort membership remain as recorded in the optional reviewed cases.',
                ])


def render_summary(report):
    if report['report'] == 'browser_delivery':
        totals = report['totals']
        measured = totals['measured_receipt_to_viewport']
        rate = measured['within_60_seconds']
        prefix = 'SYNTHETIC EXAMPLE: ' if report.get('synthetic_fixture') else ''
        return (prefix + f"Browser response-to-viewport: {measured['n']} measured automatic rows; "
                f"{rate['numerator']}/{rate['denominator']} within 60 seconds. "
                f"{totals['viewport_unobserved']} eligible rows unobserved; {len(report['invalid_rows'])} invalid rows. "
                'This does not measure publisher-to-screen delivery.')
    totals = report['totals']
    rate = totals['validated_archive_fraction']
    return (f"Reader coverage: {rate['numerator']}/{rate['denominator']} supplied articles have validated archives; "
            f"{totals['tables']} tables and {totals['nonempty_table_cells']} nonempty table cells. "
            'This is current saved-body coverage, not a live delivery sample.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    delivery = sub.add_parser('delivery', help='Analyze an exported local Delivery check')
    delivery.add_argument('input', type=Path)
    delivery.add_argument('output', type=Path)
    reader = sub.add_parser('reader', help='Audit saved bodies against an article snapshot')
    reader.add_argument('input', type=Path)
    reader.add_argument('output', type=Path)
    reader.add_argument('--archives', required=True, type=Path)
    reader.add_argument('--cases', type=Path)
    args = parser.parse_args(argv)
    try:
        document, evidence = read_json(args.input)
        sources = [evidence]
        if args.command == 'delivery':
            report = delivery_report(document)
        else:
            cases = None
            if args.cases:
                cases, case_evidence = read_json(args.cases)
                sources.append(case_evidence)
            report = reader_report(document, args.archives, cases)
        report.update(generated_at_utc=datetime.now(timezone.utc).isoformat(), inputs=sources,
                      network_requests=0, database_connections=0)
        with args.output.open('x', encoding='utf8') as stream:
            json.dump(report, stream, indent=2, ensure_ascii=False, allow_nan=False)
            stream.write('\n')
        print(render_summary(report))
        print(f'Saved {args.output.resolve()}')
        return 0
    except (ValueError, OSError) as exc:
        parser.exit(2, f'news-validation: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())
