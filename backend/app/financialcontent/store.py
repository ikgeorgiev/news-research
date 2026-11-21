"""Durable production discovery and delivery state, separate from study evidence."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid

from app.financialcontent.selection import candidate_reason
from app.financialcontent.catchup import published_next_page
from app.financialcontent.listings import GNW_ROUTE
from app.financialcontent.health import diagnostic_error, diagnostic_headers


class Store:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables and 'fc_meta' not in tables:
                raise ValueError('Refusing a database that is not the production FC store')
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS fc_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fc_control(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fc_route(
                    name TEXT PRIMARY KEY,day TEXT,last_success REAL,last_keys TEXT,
                    next_due REAL NOT NULL DEFAULT 0,request_pending INTEGER NOT NULL DEFAULT 0,
                    disabled TEXT,etag TEXT,modified TEXT);
                CREATE TABLE IF NOT EXISTS fc_listing_diagnostic(
                    route TEXT PRIMARY KEY,started_at REAL NOT NULL,finished_at REAL,
                    http_status INTEGER,error_kind TEXT,conditional_headers TEXT NOT NULL,
                    response_headers TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fc_seen(
                    route TEXT NOT NULL,url TEXT NOT NULL,title TEXT NOT NULL,first_seen REAL NOT NULL,
                    initial INTEGER NOT NULL,PRIMARY KEY(route,url));
                CREATE TABLE IF NOT EXISTS fc_candidate(
                    url TEXT PRIMARY KEY,title TEXT NOT NULL,route TEXT NOT NULL,
                    observed REAL NOT NULL,last_absent REAL,reason TEXT,status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,due REAL NOT NULL,
                    lease TEXT,request_pending INTEGER NOT NULL DEFAULT 0,
                    entry TEXT,body_received REAL,result TEXT,error TEXT,archive_status TEXT);
                CREATE INDEX IF NOT EXISTS fc_candidate_due ON fc_candidate(status,due);
                CREATE TABLE IF NOT EXISTS fc_review_body(
                    url TEXT PRIMARY KEY,received REAL NOT NULL,
                    html_sha256 TEXT NOT NULL,body_sha256 TEXT NOT NULL,
                    html BLOB NOT NULL,review_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fc_catchup(
                    id TEXT PRIMARY KEY,day TEXT NOT NULL,url TEXT NOT NULL,
                    front_received REAL NOT NULL,status TEXT NOT NULL,
                    finished REAL,http_status INTEGER,error TEXT,items TEXT);
            ''')
            old = db.execute("SELECT value FROM fc_meta WHERE key='schema'").fetchone()
            if old and old[0] != '1':
                raise ValueError('Unknown FC store schema')
            db.execute("INSERT OR IGNORE INTO fc_meta VALUES('schema','1')")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(str(self.path), timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _controls(db):
        return {r['key']: r['value'] for r in db.execute('SELECT * FROM fc_control')}

    @staticmethod
    def _gate(db, now):
        controls = Store._controls(db)
        if controls.get('disabled'):
            return 'disabled'
        if float(controls.get('backoff_until', 0)) > now:
            return 'backoff'
        return None

    def inspect(self):
        with self.connect() as db:
            return {
                'controls': self._controls(db),
                'routes': [dict(r) for r in db.execute('SELECT * FROM fc_route')],
                'listing_diagnostics': [dict(r) | {
                    'conditional_headers': json.loads(r['conditional_headers']),
                    'response_headers': json.loads(r['response_headers'])}
                    for r in db.execute('SELECT * FROM fc_listing_diagnostic ORDER BY route')],
                'counts': {r[0]: r[1] for r in db.execute('SELECT status,count(*) FROM fc_candidate GROUP BY status')},
                'catchup': {r[0]: r[1] for r in db.execute('SELECT status,count(*) FROM fc_catchup GROUP BY status')},
                'review_bodies': db.execute('SELECT count(*) FROM fc_review_body').fetchone()[0],
                'seed_reviewed': db.execute("SELECT value FROM fc_meta WHERE key='seed_reviewed'").fetchone() is not None,
            }

    def import_controls(self, *, disabled=None, backoff_until=None, refused_urls=(), blocked_routes=(), historical_urls=(), catchup_disabled=None, evidence):
        """Explicit maintenance import; never clears newer controls or imports old work."""
        if not evidence:
            raise ValueError('Reviewed evidence is required')
        refused_urls, historical_urls = set(refused_urls), set(historical_urls)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if catchup_disabled:
                db.execute("INSERT OR IGNORE INTO fc_control VALUES('catchup_disabled',?)", (str(catchup_disabled),))
            if disabled:
                db.execute("INSERT OR IGNORE INTO fc_control VALUES('disabled',?)", (str(disabled),))
            if backoff_until:
                existing = float(self._controls(db).get('backoff_until', 0))
                db.execute("INSERT INTO fc_control VALUES('backoff_until',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(max(existing, backoff_until)),))
            for route in blocked_routes:
                db.execute('INSERT OR IGNORE INTO fc_route(name,disabled) VALUES(?,?)', (route, 'prior_refusal'))
                db.execute("UPDATE fc_route SET disabled=COALESCE(disabled,'prior_refusal') WHERE name=?", (route,))
            for url in set(refused_urls) | set(historical_urls):
                state = 'prior_refusal' if url in refused_urls else 'prior_history'
                db.execute('''INSERT OR IGNORE INTO fc_candidate
                    (url,title,route,observed,status,due,error) VALUES(?,?,?,?,?,?,?)''',
                    (url, '', 'imported-evidence', 0, state, 0, evidence))
            db.execute("INSERT INTO fc_meta VALUES('seed_reviewed',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (evidence,))

    def recover_owned_work(self):
        """Call only while holding the application singleton lock."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE fc_candidate SET status='held_inflight',lease=NULL,error='Process ended with an unknown body request' WHERE status='working' AND request_pending=1")
            db.execute("UPDATE fc_candidate SET status=CASE WHEN entry IS NULL THEN 'pending' ELSE 'ready' END,lease=NULL WHERE status='working' AND request_pending=0")
            # A reservation belongs to the caller that read the front page.
            # Only 'fetching' can represent a request with an unknown outcome.
            db.execute("UPDATE fc_catchup SET status='cancelled' WHERE status='reserved'")
            # Keep seen keys and original observations, but obtain a fresh full
            # response before using a route's absence bound after any restart.
            db.execute('UPDATE fc_route SET last_success=NULL WHERE last_success IS NOT NULL')
            # Unknown listing requests remain explicitly blocked, including across days.

    def begin_listing(self, route, *, now, day, interval):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute("SELECT 1 FROM fc_meta WHERE key='seed_reviewed'").fetchone():
                return None
            if self._gate(db, now):
                return None
            db.execute('INSERT OR IGNORE INTO fc_route(name) VALUES(?)', (route,))
            row = db.execute('SELECT * FROM fc_route WHERE name=?', (route,)).fetchone()
            if row['disabled'] or row['request_pending'] or row['next_due'] > now:
                return None
            # A fresh daily baseline requires a full response, not a 304.
            headers = {}
            if row['day'] == day and row['last_success'] is not None:
                if row['etag']:
                    headers['If-None-Match'] = row['etag']
                if row['modified']:
                    headers['If-Modified-Since'] = row['modified']
            db.execute('UPDATE fc_route SET request_pending=1,next_due=? WHERE name=?', (now + interval, route))
            db.execute('''INSERT INTO fc_listing_diagnostic VALUES(?,?,NULL,NULL,NULL,?,'{}')
                ON CONFLICT(route) DO UPDATE SET started_at=excluded.started_at,finished_at=NULL,
                http_status=NULL,error_kind=NULL,conditional_headers=excluded.conditional_headers,response_headers='{}'
                ''', (route, now, json.dumps(sorted(headers))))
            return headers

    def finish_listing(self, route, *, status, now, day, items=None, context=None, headers=None, retry_until=0, error=None, html=None):
        headers = headers or {}
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM fc_route WHERE name=?', (route,)).fetchone()
            if row is None or not row['request_pending']:
                raise ValueError('Listing was not claimed')
            db.execute('UPDATE fc_route SET request_pending=0 WHERE name=?', (route,))
            db.execute('''UPDATE fc_listing_diagnostic SET finished_at=?,http_status=?,error_kind=?,response_headers=?
                WHERE route=?''', (now, status, diagnostic_error(error),
                json.dumps(diagnostic_headers(headers)), route))
            self._record_response(db, status, now, retry_until)
            if status in {301,302,303,307,308,400,401,403,404,410}:
                db.execute('UPDATE fc_route SET disabled=? WHERE name=?', (str(status), route))
                return 0
            if status == 304:
                # Retain validators, but never advance a negative availability bound.
                return 0
            if status != 200 or error or not items:
                db.execute('UPDATE fc_route SET last_success=NULL,next_due=MAX(next_due,?) WHERE name=?', (max(now + 60, retry_until), route))
                return 0
            baseline = row['day'] != day or row['last_keys'] is None
            previous = set(json.loads(row['last_keys'] or '[]'))
            last_absent = row['last_success'] if not baseline and row['last_success'] and 0 <= now-row['last_success'] <= 120 else None
            added = 0
            for item in items:
                unseen = db.execute('SELECT 1 FROM fc_seen WHERE route=? AND url=?', (route, item['url'])).fetchone() is None
                db.execute('INSERT OR IGNORE INTO fc_seen VALUES(?,?,?,?,?)', (route, item['url'], item['title'], now, int(baseline)))
                if baseline or not unseen:
                    continue
                reason = candidate_reason(item['title'], context)
                state = 'pending' if reason else 'review'
                # Exact publication time is unavailable until the body is validated.
                inserted = db.execute('''INSERT OR IGNORE INTO fc_candidate
                    (url,title,route,observed,last_absent,reason,status,due) VALUES(?,?,?,?,?,?,?,?)''',
                    (item['url'], item['title'], route, now, last_absent if item['url'] not in previous else None, reason, state, now))
                added += inserted.rowcount
            db.execute('''UPDATE fc_route SET day=?,last_success=?,last_keys=?,etag=?,modified=? WHERE name=?''',
                (day, now, json.dumps([i['url'] for i in items]), headers.get('etag'), headers.get('last-modified'), route))
            if route == 'financialcontent-bw-index' and not baseline and html:
                url = published_next_page(previous, [i['url'] for i in items], html)
                controls = self._controls(db)
                unknown = (db.execute('SELECT 1 FROM fc_route WHERE request_pending=1 LIMIT 1').fetchone()
                    or db.execute('SELECT 1 FROM fc_candidate WHERE request_pending=1 LIMIT 1').fetchone()
                    or db.execute("SELECT 1 FROM fc_catchup WHERE status IN ('reserved','fetching') LIMIT 1").fetchone())
                if (url and not unknown and not self._gate(db, now)
                        and not controls.get('catchup_disabled')
                        and float(controls.get('catchup_next', 0)) <= now):
                    db.execute('INSERT INTO fc_catchup(id,day,url,front_received,status) VALUES(?,?,?,?,?)',
                        (uuid.uuid4().hex, day, url, now, 'reserved'))
                    db.execute("INSERT INTO fc_control VALUES('catchup_next',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(now+60),))
            return added

    def claim_catchup(self, front_received, now):
        """Only the caller that just committed this front response may fetch its page."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM fc_catchup WHERE front_received=? AND status='reserved'", (front_received,)).fetchone()
            if row is None:
                return None
            unknown = (db.execute('SELECT 1 FROM fc_route WHERE request_pending=1 LIMIT 1').fetchone()
                or db.execute('SELECT 1 FROM fc_candidate WHERE request_pending=1 LIMIT 1').fetchone())
            if self._gate(db, now) or unknown or now-front_received > 30:
                db.execute("UPDATE fc_catchup SET status='blocked' WHERE id=?", (row['id'],))
                return None
            db.execute("UPDATE fc_catchup SET status='fetching' WHERE id=?", (row['id'],))
            return dict(row)

    def cancel_unstarted_catchup(self, front_received):
        with self.connect() as db:
            db.execute("UPDATE fc_catchup SET status='cutoff' WHERE front_received=? AND status='reserved'", (front_received,))

    def finish_catchup(self, job, *, now, status, items=None, retry_until=0, error=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM fc_catchup WHERE id=? AND status='fetching'", (job['id'],)).fetchone()
            if row is None:
                return False
            self._record_response(db, status, now, retry_until)
            if status in {301,302,303,307,308,400,401,403,404,410}:
                db.execute("INSERT OR IGNORE INTO fc_control VALUES('catchup_disabled',?)", (str(status),))
            for item in items or []:
                # Unknown page-two history is retained for review, never fetched or delivered.
                if db.execute('SELECT 1 FROM fc_seen WHERE url=? LIMIT 1', (item['url'],)).fetchone():
                    continue
                db.execute('INSERT OR IGNORE INTO fc_seen VALUES(?,?,?,?,?)',
                    ('financialcontent-bw-index:page2', item['url'], item['title'], now, 0))
                db.execute('''INSERT OR IGNORE INTO fc_candidate
                    (url,title,route,observed,reason,status,due) VALUES(?,?,?,?,?,?,?)''',
                    (item['url'], item['title'], 'financialcontent-bw-index:page2', now,
                     'unknown_history', 'review_history', now))
            db.execute('UPDATE fc_catchup SET status=?,finished=?,http_status=?,error=?,items=? WHERE id=?',
                ('done' if status == 200 and not error else 'failed', now, status, error, json.dumps(items or []), job['id']))
            return True

    @staticmethod
    def _record_response(db, status, now, retry_until):
        if status in {401,403}:
            db.execute("INSERT OR IGNORE INTO fc_control VALUES('disabled',?)", (str(status),))
        if status == 429 or (status == 503 and retry_until):
            existing = float(Store._controls(db).get('backoff_until', 0))
            value = max(existing, now + 60, retry_until)
            db.execute("INSERT INTO fc_control VALUES('backoff_until',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(value),))

    def claim_body(self, now, *, allowed_routes=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            gate = self._gate(db, now)
            routes = tuple(allowed_routes) if allowed_routes is not None else None
            if routes == ():
                return None
            route_clause = (' AND route IN (' + ','.join('?' for _ in routes) + ')') if routes else ''
            review_due = float(self._controls(db).get('gnw_body_next', 0))
            # Validated saved bodies may still commit without issuing another request.
            row = db.execute('''SELECT * FROM fc_candidate WHERE status IN ('pending','ready') AND request_pending=0 AND due<=?
                AND (? IS NULL OR entry IS NOT NULL)
                AND (route!=? OR ?<=?)''' + route_clause + '''
                ORDER BY CASE WHEN route=? THEN 1 ELSE 0 END,observed LIMIT 1''',
                (now, gate, GNW_ROUTE, review_due, now, *(routes or ()), GNW_ROUTE)).fetchone()
            if row is None:
                return None
            if row['entry'] is None and now - row['observed'] > 900:
                db.execute("UPDATE fc_candidate SET status='held_age' WHERE url=?", (row['url'],))
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE fc_candidate SET status='working',lease=? WHERE url=?", (token, row['url']))
            return {**dict(row), 'lease': token}

    def begin_body(self, item, now):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._gate(db, now):
                db.execute("UPDATE fc_candidate SET status='pending',lease=NULL WHERE url=? AND lease=? AND request_pending=0", (item['url'], item['lease']))
                return False
            if item['route'] == GNW_ROUTE:
                due = float(self._controls(db).get('gnw_body_next', 0))
                if due > now:
                    db.execute("UPDATE fc_candidate SET status='pending',lease=NULL,due=MAX(due,?) WHERE url=? AND lease=? AND request_pending=0",
                               (due, item['url'], item['lease']))
                    return False
            started = db.execute('''UPDATE fc_candidate SET request_pending=1,attempts=attempts+1
                WHERE url=? AND lease=? AND status='working' AND entry IS NULL AND request_pending=0''',
                (item['url'], item['lease'])).rowcount == 1
            if started and item['route'] == GNW_ROUTE:
                db.execute("INSERT INTO fc_control VALUES('gnw_body_next',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                           (str(now + 60),))
            return started

    def save_review_body(self, item, content, review, received):
        """Atomically retain the immutable response and end review work; no delivery."""
        from app.financialcontent.gnw_body import MAX_BODY, tidy
        if (item['route'] != GNW_ROUTE or len(content) > MAX_BODY
                or review.get('status') != 'review_only' or review.get('source_url') != item['url']
                or review.get('title') != tidy(item['title'])
                or review.get('production_eligible') is not False
                or review.get('wire_identity') is not None or review.get('original_url') is not None
                or review.get('validated_fund_tickers') != []):
            raise ValueError('Invalid identity-pending review')
        digest = hashlib.sha256(content).hexdigest()
        body_digest = hashlib.sha256(review['body_text'].encode()).hexdigest()
        if (review.get('source_html_sha256') != digest or review.get('body_sha256') != body_digest
                or review.get('reader', {}).get('source_html_sha256') != digest
                or review['reader'].get('wire_key') is not None):
            raise ValueError('Review evidence digest or identity mismatch')
        encoded = json.dumps(review, ensure_ascii=False)
        if len(encoded.encode()) > 2 * MAX_BODY:
            raise ValueError('Review archive exceeds bound')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM fc_candidate WHERE url=? AND lease=? AND status='working' AND request_pending=1",
                             (item['url'], item['lease'])).fetchone()
            if row is None:
                return False
            if row['route'] != GNW_ROUTE or tidy(row['title']) != review['title']:
                raise ValueError('Claimed review identity changed')
            existing = db.execute('SELECT html_sha256 FROM fc_review_body WHERE url=?', (item['url'],)).fetchone()
            if existing and existing['html_sha256'] != digest:
                raise ValueError('Existing review response is immutable')
            db.execute('INSERT OR IGNORE INTO fc_review_body VALUES(?,?,?,?,?,?)',
                       (item['url'], received, digest, body_digest, content, encoded))
            result = {'review_only': True, 'article_id': None, 'notification_sent': False,
                      'html_sha256': digest, 'issues': review['issues']}
            state = 'identity_review' if review.get('body_parse_valid', True) else 'invalid'
            db.execute("""UPDATE fc_candidate SET status=?,body_received=?,result=?,
                archive_status='review_archived',error=NULL,lease=NULL,request_pending=0 WHERE url=? AND lease=?""",
                (state, received, json.dumps(result), item['url'], item['lease']))
            return True

    def save_body(self, item, entry, received):
        with self.connect() as db:
            return db.execute('''UPDATE fc_candidate SET entry=?,body_received=?,request_pending=0
                WHERE url=? AND lease=? AND status='working' ''',
                (json.dumps(entry), received, item['url'], item['lease'])).rowcount == 1

    def finish_body(self, item, state, *, result=None, error=None, due=0, archive_status=None):
        with self.connect() as db:
            return db.execute('''UPDATE fc_candidate SET status=?,result=?,error=?,due=?,archive_status=?,lease=NULL,request_pending=0
                WHERE url=? AND lease=? AND status='working' ''',
                (state, json.dumps(result) if result is not None else None, error, due, archive_status, item['url'], item['lease'])).rowcount == 1

    def fail_body(self, item, now, *, status=None, retry_until=0, error=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM fc_candidate WHERE url=? AND lease=? AND status='working'", (item['url'], item['lease'])).fetchone()
            if row is None:
                return False
            self._record_response(db, status, now, retry_until)
            state = 'ready' if row['entry'] else 'pending'
            if status in {301,302,303,307,308,400,401,403,404,410}:
                state = 'refused'
            elif not row['entry'] and row['attempts'] >= 4:
                state = 'held_attempts'
            delay = 60 * 2 ** min(max(row['attempts'] - 1, 0), 5)
            db.execute('''UPDATE fc_candidate SET status=?,error=?,due=?,lease=NULL,request_pending=0 WHERE url=?''',
                (state, error, max(now + delay, retry_until), item['url']))
            return True
