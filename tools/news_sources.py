"""Bounded, offline intake for source material; never publishes or fetches news.

Run with the repository backend Python. This deliberately uses a separate SQLite
review store. A terminal capture is not an API response or a network receipt.
"""
from __future__ import annotations

import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sqlite3
import sys
import unicodedata
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from app.source_research.yahoo_direct import assess as assess_yahoo
from app.source_research.observations import identity as wire_identity

MAX_MANIFEST = 64 * 1024
MAX_PAYLOAD = 4 * 1024 * 1024
MAX_ITEMS = 250
APP_ID = 0x43454653
FC = 'https://markets.chroniclejournal.com/thepilotnews/news/provider/gnwcq'
NEWSNET = 'https://lasvegas.newsnetmedia.com/story/category/partners/globe-newswire/feed/'
GUGGENHEIM = 'https://www.globenewswire.com/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=='
XAI = 'https://xainvestments.com/news-distributions/feed/'
YAHOO = 'https://nexus-finance-prod.media.yahoo.com/'
ROUTES = {
    'yahoo-direct': {'urls': [YAHOO], 'access': 'held_prior_direct_yahoo_429', 'format': 'yahoo_graphql'},
    'godel-terminal': {'urls': ['https://app.godelterminal.com/'], 'access': 'held_prior_app_403', 'format': 'godel_review_capture'},
    'financialcontent-gnw': {'urls': [FC], 'access': 'offline_review_only', 'format': 'fc_html'},
    'newsnet-gnw': {'urls': [NEWSNET], 'access': 'offline_review_only', 'format': 'rss'},
    'guggenheim-gnw': {'urls': [GUGGENHEIM], 'access': 'production_opt_in_not_enrolled', 'format': 'rss'},
    'xai-distributions': {'urls': [XAI], 'access': 'offline_review_only', 'format': 'rss'},
}
LIMITATIONS = [
    'Offline import does not enable a source, contact a website or clear an access refusal.',
    'Godel input is our reviewed capture format, not a claimed native Godel API schema.',
    'Imported records never create production articles, ticker assignments or notifications.',
    'A local ID, related ticker, tracking pixel or matching title is not an original wire identity.',
    'Publication/display labels, manual observations and network receipt clocks remain separate.',
    'Missing cases in supplied finite material are unobserved, not established coverage misses.',
    'Matching an original link and title does not validate body ticker evidence or body completeness.',
    'Retained, cached, startup and unknown-history records cannot become new speed samples.',
]


def read_bounded(path, cap):
    with Path(path).open('rb') as stream:
        data = stream.read(cap + 1)
    if len(data) > cap:
        raise ValueError(f'File exceeds {cap} bytes')
    return data


def digest(data):
    return hashlib.sha256(data).hexdigest()


def label(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split()) if isinstance(value, str) else ''


def timestamp(value):
    if not isinstance(value, str) or len(value) > 100:
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        try:
            result = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
    return result.astimezone(timezone.utc).isoformat() if result.tzinfo else None


def url(value):
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        p = urlsplit(value)
        if p.scheme != 'https' or not p.hostname or p.username or p.password or p.port:
            return None
        return urlunsplit((p.scheme, p.netloc.lower(), p.path, p.query, ''))
    except ValueError:
        return None


def wire_url(value):
    """RSS HTTP links may identify an original wire, without following a redirect."""
    if isinstance(value, str) and value.startswith('http://'):
        value = 'https://' + value[7:]
    return value if wire_identity(value) else None


def item(local_id, title, source_url, *, original_url=None, published=None,
         display=None, related=None, issues=None):
    title = label(title)
    if len(title) > 2000:
        raise ValueError('Headline exceeds 2000 characters')
    if not isinstance(local_id, str) or not local_id or len(local_id) > 4096:
        raise ValueError('Missing or oversized local identity')
    original_url = wire_url(original_url)
    return dict(local_id=local_id, title=title, source_url=url(source_url),
                original_url=original_url, wire_identity=wire_identity(original_url),
                publication_label=timestamp(published), display_label=display,
                publication_precision='unverified', related_tickers=related or [],
                validated_fund_tickers=[], issues=list(issues or []))


class Listing(HTMLParser):
    """Only titles within the published FinancialContent headline widget."""
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.widget = self.headline = self.title = None
        self.anchor = None
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = attrs.get('class', '').split()
        if tag == 'div':
            self.depth += 1
            if 'newsrpm-headlines1' in classes:
                self.widget = self.depth
            if self.widget and 'headline' in classes:
                self.headline = self.depth
            if self.headline and 'title' in classes:
                self.title = self.depth
        if tag == 'a' and self.title and attrs.get('href'):
            self.anchor = [attrs['href'], '']

    def handle_data(self, data):
        if self.anchor is not None:
            self.anchor[1] += data

    def handle_endtag(self, tag):
        if tag == 'a' and self.anchor is not None:
            self.links.append(self.anchor)
            self.anchor = None
        if tag == 'div':
            for key in ('widget', 'headline', 'title'):
                if getattr(self, key) == self.depth:
                    setattr(self, key, None)
            self.depth = max(0, self.depth - 1)


def parse_fc(body):
    parser = Listing()
    parser.feed(body.decode('utf8'))
    rows, seen = [], set()
    for href, title in parser.links:
        link = url(urljoin(FC, href))
        if not link:
            continue
        parts = urlsplit(link)
        match = re.fullmatch(r'/thepilotnews/article/gnwcq-(\d{4})-(\d{1,2})-(\d{1,2})-.+', parts.path)
        if parts.hostname != 'markets.chroniclejournal.com' or not match:
            continue
        day = date(*(int(v) for v in match.groups())).isoformat()
        link = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
        if link in seen:
            continue
        seen.add(link)
        row = item(link, title, link, issues=['original_wire_identity_unverified'])
        row.update(publication_date=day, publication_precision='date_only',
                   publication_date_source='article_url_date')
        rows.append(row)
    if not rows:
        raise ValueError('No recognized FC GNW headlines; not a successful empty feed')
    return rows


def parse_rss(body):
    text = body.decode('utf-8-sig')
    if re.search(r'<!\s*(?:DOCTYPE|ENTITY)', text, re.I):
        raise ValueError('DTD/entity declarations are not accepted')
    root = ET.fromstring(text)
    channel = root.find('channel') if root.tag == 'rss' else None
    if channel is None or not label(channel.findtext('title')):
        raise ValueError('Expected RSS channel with a title')
    rows = []
    for node in channel.findall('item'):
        link = (node.findtext('link') or '').strip()
        original = wire_url(link)
        # Local GUIDs and GNW media/tracking IDs must not be promoted to wire IDs.
        row = item(node.findtext('guid') or link, node.findtext('title'), original or link,
                   original_url=original, published=node.findtext('pubDate'),
                   issues=[] if original else ['original_wire_identity_unverified'])
        body_text = node.findtext('{http://purl.org/rss/1.0/modules/content/}encoded')
        row['body_present'] = bool(body_text)
        row['body_sha256'] = digest(body_text.encode()) if body_text else None
        row['body_used_for_ticker_validation'] = False
        rows.append(row)
    return rows


def parse_godel(body):
    data = json.loads(body)
    # Accept the actual retained October capture, plus a versioned reviewed format.
    if not isinstance(data, dict):
        raise ValueError('Godel capture must be an object')
    if data.get('version') != 1 and data.get('method') != 'Manual anonymous public UI review. Historical rows; no prospective latency observation.':
        raise ValueError('Reviewed Godel capture version 1 required')
    entries = data.get('cases') if 'cases' in data else data.get('entries')
    if not isinstance(entries, list):
        raise ValueError('Reviewed capture entries required')
    rows = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('Invalid Godel capture entry')
        original = entry.get('original_source_link')
        key = entry.get('local_id') or original
        observed = timestamp(entry.get('observed_at') or entry.get('godel_ui_observed_at_utc'))
        if not observed:
            raise ValueError('Each Godel capture requires an explicit observed-at timezone')
        row = item(key, entry.get('title'), original,
                   original_url=original, published=entry.get('publication_label'),
                   display=entry.get('displayed_time_local'), related=entry.get('related_tickers'),
                   issues=[] if label(entry.get('title')) else ['title_not_retained'])
        row.update(manual_observed_at=observed, displayed_source=entry.get('source_label'),
                   filter_state=entry.get('filters', data.get('filter_values', 'unknown')),
                   body_present=bool(entry.get('reader_paragraphs')),
                   body_used_for_ticker_validation=False)
        rows.append(row)
    return rows


def parse_payload(route, body, manifest):
    if route == 'yahoo-direct':
        result = assess_yahoo(200, body, received_at=manifest['received_at'],
                              requested_symbol=manifest.get('requested_symbol'),
                              tab=manifest.get('tab', 'press releases'),
                              network_response=manifest['receipt_kind'] != 'cached')
        if result['status'] == 'invalid':
            raise ValueError('Invalid Yahoo GraphQL response: ' + ','.join(result['issues']))
        rows = [item(v['id'], v['title'], v['url'],
                     original_url=wire_url(v['url']) or wire_url(v['clickthrough_url']), published=v['published_at'],
                     display=v['display_at'], related=v['related_tickers'],
                     issues=list(result['issues'])) for v in result['items']]
        for row in rows:
            row['has_next_page'] = result.get('has_next_page')
        for row, source in zip(rows, result['items']):
            original_keys = {wire_identity(value) for value in [source['url'], source['clickthrough_url']]
                             if wire_identity(value)}
            if len(original_keys) > 1:
                row['issues'].append('conflicting_original_urls')
                row['original_url'] = row['wire_identity'] = None
        issues = list(result['issues'])
    elif route == 'financialcontent-gnw':
        rows, issues = parse_fc(body), []
    elif route == 'godel-terminal':
        rows, issues = parse_godel(body), []
    else:
        rows, issues = parse_rss(body), []
    if len(rows) > MAX_ITEMS:
        raise ValueError('More than 250 items in one response')
    return rows, issues


def load_capture(manifest_path):
    manifest_path = Path(manifest_path).resolve()
    raw = read_bounded(manifest_path, MAX_MANIFEST)
    m = json.loads(raw)
    if not isinstance(m, dict) or m.get('version') != 1 or m.get('route') not in ROUTES:
        raise ValueError('Version 1 and a reviewed route are required')
    if m.get('request_url') not in ROUTES[m['route']]['urls']:
        raise ValueError('URL is not the retained route; no inferred endpoint')
    if m.get('receipt_kind') not in {'network_response', 'retained_response', 'manual_capture', 'cached'}:
        raise ValueError('Explicit receipt kind required')
    if m.get('classification') not in {'baseline', 'new', 'historical', 'unknown_history'}:
        raise ValueError('Explicit new/history classification required')
    received = timestamp(m.get('received_at'))
    if not received:
        raise ValueError('Explicit timezone required for receipt/capture')
    m['received_at'] = received
    if m['route'] == 'godel-terminal' and m['receipt_kind'] != 'manual_capture':
        raise ValueError('Reviewed terminal captures cannot masquerade as network receipts')
    if m['receipt_kind'] == 'network_response':
        started = timestamp(m.get('request_started_at'))
        if not started or datetime.fromisoformat(started) > datetime.fromisoformat(received) or not label(m.get('clock_id')):
            raise ValueError('Network response requires ordered start/receipt and clock identity')
    status = m.get('http_status')
    if type(status) is not int or not 100 <= status <= 599:
        raise ValueError('Explicit HTTP status required; manual captures use their retained page status')
    if not isinstance(m.get('payload_file'), str) or not m['payload_file']:
        raise ValueError('Payload filename required')
    rel = Path(m['payload_file'])
    payload = (manifest_path.parent / rel).resolve()
    if rel.is_absolute() or not payload.is_relative_to(manifest_path.parent):
        raise ValueError('Payload must be inside the capture directory')
    body = read_bounded(payload, MAX_PAYLOAD)
    if not isinstance(m.get('payload_sha256'), str) or digest(body) != m['payload_sha256']:
        raise ValueError('Payload digest mismatch')
    if status == 200:
        rows, issues = parse_payload(m['route'], body, m)
    else:
        rows, issues = [], [f'http_{status}_not_a_listing']
    return m, rows, issues, digest(raw + b'\0' + body)


SCHEMA = '''
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE imports (id TEXT PRIMARY KEY, route TEXT NOT NULL, receipt TEXT NOT NULL,
 manifest TEXT NOT NULL, issues TEXT NOT NULL, imported_at TEXT NOT NULL);
CREATE TABLE observations (import_id TEXT NOT NULL REFERENCES imports(id), position INTEGER NOT NULL,
 record TEXT NOT NULL, PRIMARY KEY(import_id, position));
CREATE TABLE controls (route TEXT PRIMARY KEY, status TEXT NOT NULL);
'''


def open_store(path, *, create=False):
    p = Path(path).resolve()
    if create:
        # Never initialize, overwrite or mutate an arbitrary existing SQLite DB.
        with p.open('xb'):
            pass
        db = sqlite3.connect(p, timeout=5)
        try:
            db.executescript(SCHEMA)
            db.execute(f'PRAGMA application_id={APP_ID}')
            db.execute("INSERT INTO metadata VALUES ('kind','cef-source-review-v1')")
            db.executemany('INSERT INTO controls VALUES (?,?)', [(k, v['access']) for k, v in ROUTES.items()])
            db.commit()
        except Exception:
            db.close()
            raise
        return db
    if not p.is_file():
        raise ValueError('Initialize a dedicated source review store first')
    db = sqlite3.connect(p.as_uri() + '?mode=rw', uri=True, timeout=5)
    try:
        if db.execute('PRAGMA application_id').fetchone()[0] != APP_ID:
            raise ValueError('This is not a source review store; no changes made')
        if db.execute("SELECT value FROM metadata WHERE key='kind'").fetchone() != ('cef-source-review-v1',):
            raise ValueError('Unrecognized review-store version')
        db.execute('PRAGMA foreign_keys=ON')
        return db
    except Exception:
        db.close()
        raise


def ingest(path, manifest_path):
    m, rows, issues, capture_id = load_capture(manifest_path)
    db = open_store(path)
    try:
        with db:
            cur = db.execute('INSERT OR IGNORE INTO imports VALUES (?,?,?,?,?,?)',
                (capture_id, m['route'], m['received_at'], json.dumps(m), json.dumps(issues),
                 datetime.now(timezone.utc).isoformat()))
            inserted = bool(cur.rowcount)
            if inserted:
                db.executemany('INSERT INTO observations VALUES (?,?,?)',
                    [(capture_id, n, json.dumps(row)) for n, row in enumerate(rows)])
                # Refusals are sticky. Importing a later 200 never clears a hold.
                if m['http_status'] in {401, 403, 404, 429}:
                    db.execute('UPDATE controls SET status=? WHERE route=?',
                               (f'held_observed_http_{m["http_status"]}', m['route']))
        return dict(capture_id=capture_id, inserted=inserted,
                    observations_added=len(rows) if inserted else 0, issues=issues,
                    production_articles_written=0, network_requests=0)
    finally:
        db.close()


def report(path, cases_path):
    cases_data = json.loads(read_bounded(cases_path, 5 * 1024 * 1024))
    cases = cases_data.get('cases')
    if not isinstance(cases, list) or len(cases) > 1000:
        raise ValueError('At most 1000 reviewed cases required')
    by_wire, by_title = {}, {}
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError('Invalid reviewed case')
        if type(case.get('article_id')) is not int or case['article_id'] <= 0:
            raise ValueError('Reviewed cases require positive integer article IDs')
        key = case.get('wire_identity') or wire_identity(case.get('canonical_url'))
        if key:
            by_wire.setdefault(key, []).append(case)
        if label(case.get('title')):
            by_title.setdefault(label(case['title']), []).append(case)
    db = open_store(path)
    try:
        # Protect an interactive report from accidentally reading an unbounded archive.
        count = db.execute('SELECT count(*) FROM observations').fetchone()[0]
        if count > 10000:
            raise ValueError('Review store exceeds 10,000 observations; split by date')
        controls = dict(db.execute('SELECT route,status FROM controls'))
        imported = db.execute('SELECT id,route,receipt,manifest,issues FROM imports ORDER BY receipt,id').fetchall()
        rows = []
        for capture_id, route, receipt, manifest, capture_issues in imported:
            m = json.loads(manifest)
            for position, data in db.execute('SELECT position,record FROM observations WHERE import_id=? ORDER BY position', (capture_id,)):
                row = json.loads(data)
                candidates = by_wire.get(row['wire_identity'], []) if row['wire_identity'] else []
                match = 'unmatched'
                exact = [c for c in candidates if label(c.get('title')) == row['title'] and row['title']]
                if len(exact) == 1:
                    match, candidates = 'original_link_and_title_match', exact
                elif len(exact) > 1:
                    match, candidates = 'ambiguous_reviewed_cases', exact
                elif candidates:
                    match = 'original_link_only_title_missing' if not row['title'] else 'conflicting_title_for_original_link'
                else:
                    day = (row.get('publication_date') or row.get('publication_label') or '')[:10]
                    candidates = [c for c in by_title.get(row['title'], [])
                                  if day and str(c.get('date') or c.get('publisher_label_utc', ''))[:10] == day]
                    if candidates:
                        match = 'title_date_candidate_identity_unverified'
                entry = dict(route=route, capture_id=capture_id, position=position,
                    received_or_captured_at=receipt, receipt_kind=m['receipt_kind'],
                    classification=m['classification'], clock_id=m.get('clock_id'),
                    synthetic_fixture=m.get('synthetic_fixture') is True,
                    payload_sha256=m['payload_sha256'], match=match,
                    matching_article_ids=[c.get('article_id') for c in candidates],
                    source_record=row, independent_receipt_eligible=(
                        m['receipt_kind'] == 'network_response' and m['classification'] == 'new'
                        and not m.get('synthetic_fixture')
                        and match == 'original_link_and_title_match' and not row['issues']
                        and not json.loads(capture_issues)))
                rows.append(entry)
        route_reports = {}
        for route in ROUTES:
            selected = [r for r in rows if r['route'] == route]
            matched = {aid for r in selected if r['match'] == 'original_link_and_title_match' for aid in r['matching_article_ids']}
            first = {}
            for row in selected:
                key = (row['source_record']['wire_identity'] or row['source_record']['local_id'],
                       row['clock_id'], row['receipt_kind'], row['classification'])
                first.setdefault(key, row)
            route_reports[route] = dict(
                captures=sum(r[1] == route for r in imported), observations=len(selected),
                repeated_observations=len(selected) - len(first),
                access_status=controls[route], verified_link_title_case_count=len(matched),
                verified_link_title_article_ids=sorted(matched),
                unobserved_in_supplied_material=[c.get('article_id') for c in cases if c.get('article_id') not in matched],
                eligible_independent_receipts=sum(r['independent_receipt_eligible'] for r in first.values()),
                first_observations=[dict(local_or_wire_key=k[0], clock_id=k[1], receipt_kind=k[2],
                    classification=k[3], observed_at=v['received_or_captured_at'], capture_id=v['capture_id'])
                    for k, v in first.items()])
        return dict(version=1, reviewed_cases=len(cases), captures=len(imported), observations=len(rows),
                    routes=route_reports, records=rows, limitations=LIMITATIONS,
                    production_articles_written=0, network_requests=0)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('init'); p.add_argument('store')
    p = sub.add_parser('import'); p.add_argument('store'); p.add_argument('manifest')
    p = sub.add_parser('report'); p.add_argument('store'); p.add_argument('cases'); p.add_argument('output')
    args = parser.parse_args()
    if args.command == 'init':
        open_store(args.store, create=True).close()
        result = dict(status='created', store=str(Path(args.store).resolve()))
    elif args.command == 'import':
        result = ingest(args.store, args.manifest)
    else:
        result = report(args.store, args.cases)
        with Path(args.output).open('x', encoding='utf8') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
        result = {k: result[k] for k in ('captures', 'observations', 'reviewed_cases')}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
