"""Bounded live source comparison. Never writes production articles or credentials.

Yahoo Search uses its verified public endpoint. Godel has NO HTTP client: its
only live input is a passive copy from a user-opened terminal, via the bridge.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import uuid

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import bridge_coverage
import bridge_rejection

sys.path.insert(0, str(ROOT / 'backend'))
from app.source_research.observations import identity as wire_identity

YAHOO = 'https://query2.finance.yahoo.com/v1/finance/search'
GODEL = 'https://terminal-api.godelterminal.com/api/news/items'
MAX_BODY = 1024 * 1024
APP_ID = 0x4345464C
ALLOWED_DAYS = {f'2026-11-{d:02}' for d in range(9, 14)}
DEFAULT_SYMBOLS = ['OXLC', 'GOF', 'PDI', 'UTG']


def utc():
    return datetime.now(timezone.utc)


def stamp(value=None):
    return (value or utc()).isoformat()


def dt(value):
    if not isinstance(value, str) or len(value) > 80:
        raise ValueError('Expected an aware ISO timestamp')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Naive timestamp')
    return result.astimezone(timezone.utc)


def epoch(value, divisor=1):
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = datetime.fromtimestamp(float(value) / divisor, timezone.utc)
        return stamp(result) if 2000 <= result.year <= 2100 else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def text(value, cap=2000):
    if not isinstance(value, str) or not value.strip() or len(value) > cap:
        raise ValueError('Missing or oversized text field')
    return ' '.join(value.split())


def symbols(values, limit=250):
    if not isinstance(values, list) or len(values) > limit:
        raise ValueError('Invalid symbol list')
    return sorted(set(text(x, 30) for x in values))


def safe_url(value):
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        p = urlsplit(value)
        return value if p.scheme == 'https' and p.hostname and not p.username and not p.password and not p.port else None
    except ValueError:
        return None


def row(key, title, link, published, related, *, related_limit=250, **extra):
    link = safe_url(link)
    return dict(local_id=text(key, 4096), title=text(title), link=link,
                wire_identity=wire_identity(link), published_at_label=published,
                related_tickers=symbols(related or [], related_limit), validated_fund_tickers=[],
                body_available=False, **extra)


def parse_yahoo(data):
    news = data.get('news') if isinstance(data, dict) else None
    if not isinstance(news, list) or len(news) > 250:
        raise ValueError('Missing Yahoo news array; not an empty successful response')
    result = []
    for x in news:
        if not isinstance(x, dict):
            raise ValueError('Invalid Yahoo news row')
        result.append(row(x.get('uuid'), x.get('title'), x.get('link'),
                          epoch(x.get('providerPublishTime')), x.get('relatedTickers'),
                          publisher=x.get('publisher'), vendor_received_at=None))
    return result


def godel_history_reason(event):
    """Classify a validated native page without promoting it to live evidence."""
    scope = event['scope']
    page = scope['pageable']
    if page.get('afterCursor') or page.get('beforeCursor'):
        return 'paginated_history'
    end_label = epoch((scope.get('dateTimeRange') or {}).get('endTime'), 1000)
    if end_label and dt(end_label) < dt(event['browser_request_observed_at']) - timedelta(seconds=120):
        return 'historical_range'
    return None


def parse_godel(event, *, allow_history=False):
    if not isinstance(event, dict) or event.get('version') != 1 or event.get('endpoint') != GODEL or event.get('receipt_kind') != 'passive_browser_copy':
        raise ValueError('Expected passive Godel bridge event')
    scope = event.get('scope')
    if not isinstance(scope, dict):
        raise ValueError('Missing original request filter scope')
    for key in ['symbols', 'sources', 'languages']:
        if key == 'symbols' and key not in scope:
            diagnostic = event.get('request_scope_diagnostic') or {}
            fields = diagnostic.get('fields') or {}
            if diagnostic.get('status') != 'selected' or fields.get('symbols') != 'missing':
                raise ValueError('Unverified omitted symbol filter')
            continue
        vals = scope.get(key)
        limit = 1000 if key == 'symbols' else 250
        if not isinstance(vals, list) or len(vals) > limit or any(not isinstance(v, str) or len(v) > 300 for v in vals):
            raise ValueError('Invalid filter scope')
    page = scope.get('pageable')
    if not isinstance(page, dict):
        raise ValueError('Missing native pagination metadata')
    if any(page.get(k) is not None and (not isinstance(page[k], str) or len(page[k]) > 4096) for k in ('afterCursor', 'beforeCursor')):
        raise ValueError('Invalid native pagination cursor')
    if not allow_history and (page.get('afterCursor') or page.get('beforeCursor')):
        raise ValueError('Paginated history is not live front-page news')
    started = dt(event.get('browser_request_observed_at'))
    end_label = epoch((scope.get('dateTimeRange') or {}).get('endTime'),1000)
    if not end_label or (dt(end_label)-started).total_seconds() > 120 or (not allow_history and (started-dt(end_label)).total_seconds() > 120):
        raise ValueError('Native query selects a historical range, not current news')
    if event.get('status') != 200 or event.get('error'):
        raise ValueError('Unsuccessful native capture')
    observed = dt(event.get('browser_body_observed_at'))
    if not started <= observed <= started + timedelta(seconds=30):
        raise ValueError('Unordered or excessively delayed browser copy')
    content = event.get('payload', {}).get('content')
    if not isinstance(content, list) or len(content) > 250:
        raise ValueError('Missing Godel content array')
    result = []
    for x in content:
        if not isinstance(x, dict):
            raise ValueError('Invalid Godel row')
        received = x.get('receivedTime')
        if received is not None:
            received = stamp(dt(received))
        related = x.get('securityIds')
        if related is None:
            related = []
        if not isinstance(related,list) or len(related)>1000:
            raise ValueError('Invalid Godel security ID list')
        empty_ids=sum(isinstance(v,str) and not v.strip() for v in related)
        related=[v for v in related if not (isinstance(v,str) and not v.strip())]
        result.append(row(x.get('id'), x.get('title'), x.get('originalUrl'),
                          epoch(x.get('publicationTime'), 1000), related,
                          related_limit=1000,
                          empty_security_id_count=empty_ids,
                          publisher=x.get('providerName'), vendor_received_at=received))
    # Moving endTime is an ordinary native query bound, not a new filter scope.
    scope_key = {k: sorted(scope[k]) for k in ['symbols', 'sources', 'languages'] if k in scope}
    scope_key.update(legal=scope.get('legal'), startTime=(scope.get('dateTimeRange') or {}).get('startTime'), page_size=page.get('size'))
    return result, hashlib.sha256(json.dumps(scope_key, sort_keys=True).encode()).hexdigest()


def day_close(now):
    if now.date().isoformat() not in ALLOWED_DAYS or not 8 <= now.hour < 22:
        raise ValueError('Outside the authorized Nov 9–13, 08:00–22:00 UTC comparison windows')
    return now.replace(hour=22, minute=0, second=0, microsecond=0)


def cache_delay(headers):
    """Retain the full advertised TTL; do not try to force a fresh CDN response."""
    match = re.search(r'(?:^|,)\s*max-age\s*=\s*"?(\d+)', headers.get('cache-control', ''), re.I)
    return max(600, int(match[1]) if match else 0)


def retry_delay(value, now):
    if isinstance(value, str):
        if value.strip().isdigit():
            return max(60, int(value.strip()))
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo:
                return max(60, (parsed - now).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    return 3600


def validate_bridge_diagnostics(events):
    """Bounded, selected local transport events; never accept arbitrary payloads."""
    if not isinstance(events,list) or len(events)>6:
        raise ValueError('Invalid diagnostics')
    for event in events:
        if not isinstance(event,dict) or set(event)-{'id','at','kind','capture_id','http_status','gap_ms'}:
            raise ValueError('Invalid diagnostic fields')
        if not re.fullmatch(r'[a-z0-9-]{1,80}',str(event.get('id',''))):
            raise ValueError('Invalid diagnostic id')
        if not re.fullmatch(r'[a-z_]{1,64}',str(event.get('kind',''))) or not dt(event.get('at')):
            raise ValueError('Invalid diagnostic event')
        capture=event.get('capture_id')
        if capture is not None and (not isinstance(capture,str) or not re.fullmatch(r'[a-f0-9-]{36}',capture)):
            raise ValueError('Invalid diagnostic capture')
        for name,maximum in (('http_status',599),('gap_ms',24*3600*1000)):
            if name in event and (type(event[name]) is not int or not 0<=event[name]<=maximum):
                raise ValueError('Invalid diagnostic number')
    return events


def open_store(directory, readonly=False):
    directory = Path(directory).resolve()
    if not readonly:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / 'responses').mkdir(exist_ok=True)
    path = directory / 'live.sqlite'
    new = not path.exists()
    if readonly and new:
        raise ValueError('Comparison store has not been initialized')
    db = sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5) if readonly else sqlite3.connect(path,timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=5000')
    if new and not readonly:
        db.execute(f'PRAGMA application_id={APP_ID}')
    if db.execute('PRAGMA application_id').fetchone()[0] != APP_ID:
        db.close()
        raise ValueError('Not a live-comparison store')
    if readonly:
        db.execute('PRAGMA query_only=ON')
        return db
    db.executescript('''
      CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS responses(id INTEGER PRIMARY KEY, source TEXT, scope TEXT,
        started TEXT, observed TEXT, local_received TEXT, status INTEGER, path TEXT,
        sha256 TEXT, meta TEXT, error TEXT);
      CREATE TABLE IF NOT EXISTS streams(source TEXT, scope TEXT, day TEXT, session TEXT,
        last_observed TEXT, last_ids TEXT, PRIMARY KEY(source,scope,day));
      CREATE TABLE IF NOT EXISTS items(source TEXT, local_id TEXT, first_observed TEXT,
        data TEXT, PRIMARY KEY(source,local_id));
      CREATE TABLE IF NOT EXISTS observations(response_id INTEGER, source TEXT, local_id TEXT,
        classification TEXT, previous_sample TEXT, observed TEXT, data TEXT,
        PRIMARY KEY(response_id,source,local_id));
      CREATE TABLE IF NOT EXISTS bridge_sessions(session TEXT PRIMARY KEY, source TEXT,
        started_at TEXT, planned_end TEXT, mode TEXT, pid INTEGER, closed_at TEXT,
        close_reason TEXT, last_client_at TEXT, client_status TEXT);
      CREATE TABLE IF NOT EXISTS bridge_diagnostics(session TEXT, event_id TEXT, received_at TEXT,
        data TEXT, PRIMARY KEY(session,event_id));
    ''')
    return db


def get(db, key, default=None):
    result = db.execute('SELECT value FROM control WHERE key=?', (key,)).fetchone()
    return json.loads(result[0]) if result else default


def put(db, key, value):
    db.execute('INSERT INTO control VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, json.dumps(value)))


def active_source_hold(db, source):
    """A reviewed local filter-copy bug is distinct from a source refusal.

    Keep the original hold and failed response intact. Only an explicit review
    naming that exact HTTP200 failure may permit a new baseline. A subsequent
    failure, any access refusal, and all other sources remain held.
    """
    hold=get(db,source+':hold')
    if source!='godel-browser' or not isinstance(hold,dict) or hold.get('status')!=200 or hold.get('reason')!='godel_schema_failure':
        return hold
    review=get(db,source+':hold_review')
    expected=hashlib.sha256(json.dumps(hold,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    resolutions={'local_request_scope_copy_repaired','local_related_symbol_bound_repaired','local_history_classification_repaired'}
    if not isinstance(review,dict) or review.get('resolution') not in resolutions or review.get('hold_sha256')!=expected:
        return hold
    failed=db.execute('SELECT source,status,error,sha256 FROM responses WHERE id=?',(hold.get('response_id'),)).fetchone()
    if not failed or failed['source']!=source or failed['status']!=200 or failed['error']!='godel_schema_failure' or failed['sha256']!=review.get('response_sha256'):
        return hold
    return None


def save_response(db, directory, source, scope, body, meta, error=None):
    digest = hashlib.sha256(body).hexdigest()
    path = Path(directory) / 'responses' / (uuid.uuid4().hex + '.json')
    with path.open('xb') as stream:
        stream.write(body)
    cursor = db.execute('INSERT INTO responses(source,scope,started,observed,local_received,status,path,sha256,meta,error) VALUES(?,?,?,?,?,?,?,?,?,?)',
        (source, scope, meta.get('started_at'), meta.get('observed_at'), stamp(), meta.get('status'), str(path), digest, json.dumps(meta), error))
    return cursor.lastrowid


def ingest(db, response_id, source, scope, rows, observed, session, gap_seconds, compatibility=False, *, force_baseline=False):
    """Never borrow a fine absence bound over restarts, long gaps, or filter changes."""
    now = dt(observed)
    day = now.date().isoformat()
    previous = db.execute('SELECT * FROM streams WHERE source=? AND scope=? AND day=?', (source, scope, day)).fetchone()
    if previous and now < dt(previous['last_observed']):
        raise ValueError('Out-of-order observation; stream cursor preserved')
    continuous = not force_baseline and previous is not None and previous['session'] == session and 0 <= (now - dt(previous['last_observed'])).total_seconds() <= gap_seconds
    prior_ids = set(json.loads(previous['last_ids'])) if continuous else set()
    counts = {}
    seen = set()
    for entry in rows:
        key = entry['local_id']
        if key in seen:
            counts['duplicate_in_response'] = counts.get('duplicate_in_response', 0) + 1
            continue
        seen.add(key)
        existing = db.execute('SELECT data FROM items WHERE source=? AND local_id=?', (source, key)).fetchone()
        if compatibility:
            kind = 'compatibility_history'
        elif not continuous:
            kind = 'baseline'
        elif existing:
            kind = 'retained' if json.loads(existing['data']) == entry else 'updated_same_id'
        else:
            published = entry.get('published_at_label')
            if published and dt(published) > now:
                kind = 'future_publication_label'
            else:
                kind = 'newly_observed' if published and dt(published).date() == now.date() else 'history_or_unknown_date'
        # Search is ranked/finite and terminal filters are selected: an absent ID
        # is only a preceding sampled response, never proof of global absence.
        preceding = previous['last_observed'] if continuous and key not in prior_ids else None
        db.execute('INSERT OR IGNORE INTO items VALUES(?,?,?,?)', (source,key,observed,json.dumps(entry,sort_keys=True)))
        db.execute('INSERT INTO observations VALUES(?,?,?,?,?,?,?)', (response_id,source,key,kind,preceding,observed,json.dumps(entry,sort_keys=True)))
        counts[kind] = counts.get(kind,0) + 1
    db.execute('INSERT INTO streams VALUES(?,?,?,?,?,?) ON CONFLICT(source,scope,day) DO UPDATE SET session=excluded.session,last_observed=excluded.last_observed,last_ids=excluded.last_ids',
               (source,scope,day,session,observed,json.dumps(sorted(seen))))
    return counts


async def fetch_yahoo(query, transport=None):
    params = dict(q=query,quotesCount=0,newsCount=20,listsCount=0,enableFuzzyQuery='false',
        quotesQueryId='tss_match_phrase_query',newsQueryId='news_cie_vespa',enableCb='false',
        enableNavLinks='false',enableResearchReports='false',enableCulturalAssets='false',recommendedCount=0)
    meta = dict(started_at=stamp(),endpoint=YAHOO,query=query,receipt_kind='network_response',
                clock_id='local_system_utc',credentials=False,redirects=False,retries=0)
    body = bytearray()
    # A fresh client each time prevents received cookies from being sent later.
    async with asyncio.timeout(15):
        async with httpx.AsyncClient(timeout=15,follow_redirects=False,trust_env=False,transport=transport,
                headers={'User-Agent':'CEF-News-Research/1.0','Accept':'application/json'}) as client:
            async with client.stream('GET',YAHOO,params=params) as response:
                meta.update(status=response.status_code,headers={k:v for k,v in response.headers.items() if k in {'date','cache-control','age','retry-after','content-type'}})
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_BODY:
                        raise ValueError('Yahoo body exceeds 1 MiB')
                    body.extend(chunk)
    meta['observed_at'] = stamp()
    return bytes(body), meta


def yahoo_once(db, directory, query, session, compatibility=False, fetcher=fetch_yahoo):
    query = text(query,30)
    if not re.fullmatch(r'[A-Z0-9.^-]{1,20}',query):
        raise ValueError('An explicit ticker query is required')
    now = utc()
    if get(db,'yahoo:hold'):
        return {'status':'held', 'reason':get(db,'yahoo:hold')}
    due = max(get(db,'yahoo:host_next',0),get(db,'yahoo:query:'+query,0))
    if now.timestamp() < due:
        return {'status':'backoff','next_at':stamp(datetime.fromtimestamp(due,timezone.utc))}
    # Persist before networking: interruption cannot trigger an immediate retry.
    with db:
        put(db,'yahoo:host_next',now.timestamp()+60)
        put(db,'yahoo:query:'+query,now.timestamp()+600)
    try:
        body,meta = asyncio.run(fetcher(query))
    except (TimeoutError,httpx.HTTPError,ValueError) as exc:
        with db:
            rid = save_response(db,directory,'yahoo-search',query,b'',dict(started_at=stamp(now),observed_at=stamp(),status=None),type(exc).__name__)
            put(db,'yahoo:host_next',utc().timestamp()+300)
        return {'status':'error','error':type(exc).__name__,'response_id':rid}
    now = utc()
    with db:
        meta.update(collector_pid=os.getpid(),session=session)
        rid = save_response(db,directory,'yahoo-search',query,body,meta)
        code = meta['status']
        headers = meta.get('headers',{})
        if code in (401,403,404):
            put(db,'yahoo:hold',{'status':code,'at':stamp(now),'response_id':rid})
            return {'status':'held','http_status':code,'response_id':rid}
        if code == 429:
            put(db,'yahoo:host_next',now.timestamp()+retry_delay(headers.get('retry-after'),now))
            return {'status':'backoff','http_status':code,'response_id':rid}
        if code != 200:
            put(db,'yahoo:host_next',now.timestamp()+300)
            return {'status':'error','http_status':code,'response_id':rid}
        delay = cache_delay(headers)
        put(db,'yahoo:query:'+query,now.timestamp()+delay)
        try:
            rows = parse_yahoo(json.loads(body))
        except (ValueError,TypeError) as exc:
            db.execute('UPDATE responses SET error=? WHERE id=?', ('schema:'+str(exc),rid))
            put(db,'yahoo:hold',{'reason':'schema_changed','response_id':rid})
            return {'status':'held','reason':'schema_changed','response_id':rid}
        if not compatibility:
            try:
                day_close(dt(meta['observed_at']))
                if dt(meta['started_at']).date() != dt(meta['observed_at']).date():
                    raise ValueError('Cross-day receipt')
            except ValueError:
                db.execute('UPDATE responses SET error=? WHERE id=?',('outside_window_receipt_excluded',rid))
                return {'status':'excluded','reason':'outside_window_receipt','response_id':rid}
        counts = ingest(db,rid,'yahoo-search',query,rows,meta['observed_at'],session,max(900,delay+120),compatibility)
    return {'status':'ok','http_status':code,'response_id':rid,'articles':len(rows),'classifications':counts,'cache_seconds':delay}


def godel_event(db, directory, event, session, compatibility=False, retained=False):
    # This method has no network operation and never retries the native endpoint.
    if isinstance(event,dict) and event.get('version') in (2,3):
        import godel_stream
        return godel_stream.ingest_batch(SimpleNamespace(**globals()),db,directory,event,session,compatibility,retained)
    now = utc()
    raw = json.dumps(event,ensure_ascii=False).encode()
    if len(raw) > MAX_BODY:
        raise ValueError('Bridge event exceeds 1 MiB')
    if not isinstance(event,dict) or event.get('version') != 1 or event.get('endpoint') != GODEL or event.get('receipt_kind') != 'passive_browser_copy':
        raise ValueError('Expected passive Godel event')
    digest = hashlib.sha256(raw).hexdigest()
    duplicate = db.execute('SELECT id FROM responses WHERE source=? AND sha256=?',('godel-browser',digest)).fetchone()
    if duplicate:
        return {'status':'duplicate_capture','response_id':duplicate[0]}
    if active_source_hold(db,'godel-browser'):
        return {'status':'held','reason':active_source_hold(db,'godel-browser')}
    if event.get('status') != 200 or event.get('error'):
        with db:
            rid = save_response(db,directory,'godel-browser','unparsed',raw,
                dict(started_at=event.get('browser_request_observed_at'),observed_at=stamp(),status=event.get('status')),event.get('error'))
            if event.get('status') in (401,403,404,429):
                put(db,'godel-browser:hold',{'status':event['status'],'response_id':rid,'reason':'native_refusal_manual_review'})
        return {'status':'native_error','response_id':rid}
    observed = dt(event['browser_body_observed_at'])
    if not retained and abs((now-observed).total_seconds()) > 120:
        raise ValueError('Stale event or unsynchronized browser clock')
    try:
        rows, scope = parse_godel(event, allow_history=True)
    except (ValueError, TypeError, KeyError):
        with db:
            rid = save_response(db,directory,'godel-browser','unparsed',raw,
                dict(started_at=event.get('browser_request_observed_at'),observed_at=stamp(),status=event.get('status')), 'godel_schema_failure')
            put(db,'godel-browser:hold',{'status':event.get('status'),'response_id':rid,'reason':'godel_schema_failure'})
        return {'status':'native_error','response_id':rid}
    if retained and not compatibility:
        raise ValueError('Retained imports are compatibility history only')
    capture_id = event.get('capture_id')
    if not retained and (not isinstance(capture_id,str) or not re.fullmatch(r'[a-f0-9-]{36}',capture_id)):
        raise ValueError('Missing browser capture generation')
    history_reason = godel_history_reason(event)
    if history_reason:
        # A terminal may prefetch older pages without a user scroll. Archive the
        # copy but do not stop live capture, ingest its IDs, or move any cursor.
        with db:
            rid = save_response(db,directory,'godel-browser',scope,raw,
                dict(started_at=event['browser_request_observed_at'],observed_at=stamp(observed),status=200,
                     receipt_kind='passive_browser_copy',collector_pid=os.getpid(),session=session,
                     capture_id=capture_id,filters=event['scope'],history_excluded=True,
                     history_reason=history_reason,history_rows=len(rows)), 'native_history_excluded')
        return {'status':'excluded','reason':history_reason,'response_id':rid,'articles':0}
    capacity = event.get('payload', {}).get('pageSize') or event['scope'].get('pageable', {}).get('size')
    capacity = capacity if type(capacity) is int and 1 <= capacity <= 250 else None
    assessment = bridge_coverage.assess(db,'godel-browser',scope,rows,stamp(observed),session,capture_id or 'retained',event['scope'],capacity)
    with db:
        rid = save_response(db,directory,'godel-browser',scope,raw,
            dict(started_at=event['browser_request_observed_at'],observed_at=stamp(observed),status=200,
                 receipt_kind='passive_browser_copy',clock_id='browser_system_utc',
                 collector_pid=os.getpid(),session=session,capture_id=capture_id,
                 filters=event['scope'],headers=event.get('headers',{}),assessment=assessment,compatibility_history=compatibility,retrospective_import=retained))
        counts = ingest(db,rid,'godel-browser',scope,rows,stamp(observed),session+':'+(capture_id or 'retained'),180,compatibility,
                        force_baseline=assessment['continuity']['baseline_required'])
    return {'status':'ok','response_id':rid,'articles':len(rows),'classifications':counts,'assessment':assessment}


def report(db):
    started=time.monotonic()
    db.set_progress_handler(lambda:int(time.monotonic()-started > 5),1000)
    try:
        return _report(db)
    finally:
        db.set_progress_handler(None,0)


def _report(db):
    return dict(responses=[dict(x) for x in db.execute('SELECT source,status,count(*) n FROM responses GROUP BY source,status')],
        classifications=[dict(x) for x in db.execute('SELECT source,classification,count(*) n FROM observations GROUP BY source,classification')],
        daily_classifications=[dict(x) for x in db.execute('SELECT substr(observed,1,10) day,source,classification,count(*) observations,count(distinct local_id) distinct_ids FROM observations GROUP BY day,source,classification')],
        unique_source_ids=[dict(x) for x in db.execute('SELECT source,count(*) n FROM items GROUP BY source')],
        latest_responses=[dict(x) for x in db.execute('SELECT id,source,scope,started,observed,local_received,status,error FROM responses ORDER BY id DESC LIMIT 15')],
        controls={x[0]:json.loads(x[1]) for x in db.execute('SELECT key,value FROM control')},
        effective_browser_holds={source:active_source_hold(db,source) for source in ('godel-browser','newsfilter-browser')},
        limitations=['Finite selected query/filter coverage, not the 367-symbol universe.',
            'Publisher and vendor times are labels; browser copy and local receiver times are separate.',
            'Related symbols are unvalidated. No production article/notification is written.',
            'Baseline, restart, history and unknown dates are excluded from speed samples.',
            'Screen delivery, original body availability and exact transaction commit are unmeasured.'])


def bridge_deadline(now, minutes=20, compatibility=False, until_close=False):
    if until_close:
        if compatibility:
            raise ValueError('All-day sessions are weekday validation only')
        return day_close(now), 'until_close'
    if not 1 <= minutes <= 60:
        raise ValueError('Short bridge sessions are limited to 1–60 minutes')
    end = now + timedelta(minutes=minutes)
    return (end if compatibility else min(end,day_close(now))), 'short'


def serve(directory, minutes, compatibility=False, port=8766, *, route='/godel', event_handler=None, until_close=False):
    if route not in ('/godel','/newsfilter'):
        raise ValueError('Unknown comparison bridge')
    event_handler = event_handler or godel_event
    started = utc()
    end, mode = bridge_deadline(started, minutes, compatibility, until_close)
    monotonic_end = time.monotonic() + (end-started).total_seconds()
    pair_until = min(end, started+timedelta(minutes=10))
    source = 'godel-browser' if route == '/godel' else 'newsfilter-browser'
    db = open_store(directory)
    if active_source_hold(db,source):
        db.close()
        raise ValueError('Saved source refusal/failure requires review; no receiver started')
    token = secrets.token_urlsafe(32)
    connect_code = ''.join(secrets.choice('ABCDEFGHJKLMNPQRSTUVWXYZ23456789') for _ in range(8))
    pairing_attempts = 0
    paired_client = None
    session = uuid.uuid4().hex
    close_reason = 'deadline'
    last_client = None
    rate = []
    rejection_count = 0
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass

        def reply(self,code,data):
            body = json.dumps(data).encode()
            self.send_response(code)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store')
            self.send_header('Connection','close')
            origin = self.headers.get('Origin','')
            if re.fullmatch(r'chrome-extension://[a-p]{32}',origin):
                self.send_header('Access-Control-Allow-Origin',origin)
            self.end_headers()
            self.wfile.write(body)

        def reject(self,raw,event,error,stage='capture'):
            nonlocal rejection_count
            diagnostic=bridge_rejection.describe(raw,event,error,stage)
            # Bounded immutable metadata, even if SQLite itself caused the
            # rejection. No rejected article enters observations or cursors.
            if rejection_count<128:
                rejection_count+=1
                try:
                    archive=Path(directory)/'rejections';archive.mkdir(exist_ok=True)
                    with (archive/(diagnostic['id']+'.json')).open('x',encoding='utf8') as stream:
                        json.dump({'session':session,'source':source,**diagnostic},stream,indent=2)
                except OSError:
                    # Logging failure must not replace the original rejection.
                    print(json.dumps({'status':'rejection_archive_unavailable','diagnostic':diagnostic}),flush=True)
                try:
                    with db:db.execute('INSERT INTO bridge_diagnostics VALUES(?,?,?,?)',
                        (session,diagnostic['id'],stamp(),json.dumps(diagnostic)))
                except sqlite3.Error:pass  # The independent metadata file remains.
            self.reply(400,{'error':'invalid_'+stage,'diagnostic':{k:diagnostic[k] for k in ('id','reason','record_index') if k in diagnostic}})

        def do_OPTIONS(self):
            if not re.fullmatch(r'chrome-extension://[a-p]{32}',self.headers.get('Origin','')):
                return self.reply(403,{'error':'origin'})
            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin',self.headers['Origin'])
            self.send_header('Access-Control-Allow-Headers','authorization,content-type,x-cef-bridge')
            self.send_header('Access-Control-Allow-Methods','POST')
            self.end_headers()

        def do_GET(self):
            self.reply(200 if self.path=='/health' else 404,{'status':'ready','expires_at':stamp(end),'mode':mode,'session':session} if self.path=='/health' else {})

        def do_POST(self):
            nonlocal end, pairing_attempts, paired_client, close_reason, last_client
            # Drain only a bounded body before rejecting it. Closing a Windows
            # socket with unread data can reset the connection before the 4xx.
            try:
                size = int(self.headers.get('Content-Length','-1'))
                if not 0 < size <= MAX_BODY+1 or self.headers.get('Transfer-Encoding'):
                    return self.reply(413,{'error':'size'})
                self.connection.settimeout(min(5,max(.1,(end-utc()).total_seconds())))
                raw = self.rfile.read(size)
                if len(raw) != size:
                    raise ValueError('Truncated body')
            except (ValueError,TimeoutError):
                return self.reply(400,{'error':'invalid_body'})
            if size > MAX_BODY or self.headers.get_content_type() != 'application/json':
                return self.reply(413,{'error':'size_or_type'})
            if self.path not in (route,'/stop','/pair','/status') or utc() >= end or time.monotonic() >= monotonic_end:
                return self.reply(410,{})
            origin = self.headers.get('Origin','')
            if origin and not re.fullmatch(r'chrome-extension://[a-p]{32}',origin):
                return self.reply(403,{'error':'origin'})
            if self.path == '/pair':
                if utc() >= pair_until:
                    return self.reply(410,{'error':'pairing_expired'})
                if self.headers.get('X-CEF-Bridge') != '1' or size > 1024:
                    return self.reply(403,{'error':'pairing_request'})
                if pairing_attempts >= 5:
                    return self.reply(429,{'error':'pairing_locked'})
                try:
                    request = json.loads(raw)
                    code = request.get('code','').upper().replace('-','').replace(' ','')
                    client_id = request.get('client_id','')
                    if not re.fullmatch(r'[a-p]{32}',client_id) or (origin and origin != 'chrome-extension://'+client_id):
                        return self.reply(403,{'error':'client'})
                    if not hmac.compare_digest(code,connect_code):
                        pairing_attempts += 1
                        return self.reply(401,{'error':'connection_code'})
                except (ValueError,TypeError,AttributeError):
                    pairing_attempts += 1
                    return self.reply(400,{'error':'connection_code'})
                if paired_client and client_id != paired_client:
                    return self.reply(409,{'error':'already_paired'})
                paired_client = client_id
                last_client = time.monotonic()
                return self.reply(200,{'url':f'http://127.0.0.1:{port}{route}','token':token,'expires_at':stamp(end),'mode':mode,'session':session})
            if not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+token):
                return self.reply(401,{'error':'pairing'})
            last_client = time.monotonic()
            if self.path == '/stop':
                try:
                    stop_request=json.loads(raw)
                    reasons={'user_stopped','tab_closed','left_news_page','expired_connection','receiver_error'}
                    if size>256 or not isinstance(stop_request,dict) or set(stop_request)-{'reason'} or ('reason' in stop_request and stop_request['reason'] not in reasons):
                        raise ValueError('Invalid stop reason')
                except (ValueError,TypeError):
                    return self.reply(400,{'error':'invalid_stop'})
                close_reason = stop_request.get('reason','user_or_bridge_stop')
                end = utc()
                return self.reply(200,{'status':'stopping'})
            if self.path == '/status':
                if size > 2048:
                    return self.reply(413,{'error':'status_size'})
                client=None
                try:
                    client = json.loads(raw)
                    if not isinstance(client,dict) or set(client)-{'active','capture_id','dropped','diagnostics'}:
                        raise ValueError('Unexpected status fields')
                    if type(client.get('active')) is not bool or type(client.get('dropped',0)) is not int or not 0 <= client.get('dropped',0) <= 1000000:
                        raise ValueError('Invalid status')
                    capture = client.get('capture_id')
                    if capture is not None and (not isinstance(capture,str) or not re.fullmatch(r'[a-f0-9-]{36}',capture)):
                        raise ValueError('Invalid capture')
                    diagnostics = validate_bridge_diagnostics(client.pop('diagnostics',[]))
                    with db:
                        db.execute('UPDATE bridge_sessions SET last_client_at=?,client_status=? WHERE session=?', (stamp(),json.dumps(client),session))
                        db.executemany('INSERT OR IGNORE INTO bridge_diagnostics VALUES(?,?,?,?)',
                            [(session,e['id'],stamp(),json.dumps(e)) for e in diagnostics])
                    health = bridge_coverage.status(db,source,session,capture_id=capture)
                    return self.reply(200,{'status':'held' if active_source_hold(db,source) else 'ok','mode':mode,'expires_at':stamp(end),**health})
                except (ValueError,TypeError,sqlite3.Error) as error:
                    return self.reject(raw,client,error,'status')
            while rate and time.monotonic()-rate[0] > 60:
                rate.pop(0)
            if len(rate) >= 20:
                return self.reply(429,{'error':'bridge_rate'})
            event=None
            try:
                event = json.loads(raw)
                if not isinstance(event,dict):raise ValueError('Expected capture object')
                if not compatibility:
                    copied = event.get('browser_body_observed_at') or event.get('browser_response_completed_at') or event.get('browser_headers_observed_at') or event.get('browser_request_observed_at')
                    observed = dt(copied)
                    if observed.date()!=started.date() or not started.replace(hour=8,minute=0,second=0,microsecond=0)<=observed<end:
                        return self.reply(410,{'error':'outside_session_day'})
                rate.append(time.monotonic())
                result = event_handler(db,directory,event,session,compatibility)
                self.reply(200,result)
            except (ValueError,TypeError,TimeoutError,sqlite3.Error) as error:
                self.reject(raw,event,error)
    class BoundedServer(HTTPServer):
        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(min(2,max(.1,(end-utc()).total_seconds())))
            return connection,address
    server = BoundedServer(('127.0.0.1',port),Handler)
    server.timeout = 1
    with db:
        db.execute('INSERT INTO bridge_sessions(session,source,started_at,planned_end,mode,pid) VALUES(?,?,?,?,?,?)',
                   (session,source,stamp(started),stamp(end),mode,os.getpid()))
    pairing = dict(url=f'http://127.0.0.1:{port}{route}',token=token,expires_at=stamp(end),pid=os.getpid(),
                   code=connect_code[:4]+'-'+connect_code[4:],mode=mode,session=session,pair_until=stamp(pair_until))
    pairing_path = Path(directory)/'pairing.json'
    pairing_path.write_text(json.dumps(pairing,indent=2),encoding='utf8')
    print(json.dumps({'status':'ready','pid':os.getpid(),'pairing_file':str(pairing_path),
                      'connection_code':pairing['code'],'expires_at':stamp(end),'mode':mode,'session':session,'pair_until':stamp(pair_until)}),flush=True)
    try:
        while utc() < end and time.monotonic() < monotonic_end:
            server.handle_request()
            if mode == 'until_close':
                if last_client is None and utc() >= pair_until:
                    close_reason = 'unpaired_code_expired'
                    break
                # A paired Godel daily session waits through browser/loopback gaps
                # until its ORIGINAL deadline. It never initiates source requests.
                # Other bridges retain their existing five-minute lease policy.
                if source != 'godel-browser' and last_client is not None and time.monotonic()-last_client > 300:
                    close_reason = 'browser_lease_lost'
                    break
    finally:
        server.server_close()
        with db:
            db.execute('UPDATE bridge_sessions SET closed_at=?,close_reason=? WHERE session=?',(stamp(),close_reason,session))
        db.close()
        # The old token stays in its evidence file but is invalid after shutdown.
        print(json.dumps({'status':'stopped','started_at':stamp(started),'stopped_at':stamp()}),flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store',type=Path,required=True)
    subs = parser.add_subparsers(dest='command',required=True)
    once = subs.add_parser('yahoo-once')
    once.add_argument('symbol')
    once.add_argument('--compatibility-check',action='store_true')
    imported = subs.add_parser('godel-import')
    imported.add_argument('capture',type=Path)
    run = subs.add_parser('yahoo-run')
    run.add_argument('--symbols',nargs='+',default=DEFAULT_SYMBOLS)
    bridge = subs.add_parser('godel-serve')
    bridge.add_argument('--minutes',type=int,default=20)
    bridge.add_argument('--until-close',action='store_true',help='Explicit user-started passive capture until22:00UTC on Nov9–13 only')
    bridge.add_argument('--compatibility-check',action='store_true')
    bridge.add_argument('--port',type=int,default=8766)
    subs.add_parser('report')
    args = parser.parse_args(argv)
    if args.command=='godel-serve':
        serve(args.store,args.minutes,args.compatibility_check,args.port,until_close=args.until_close)
        return
    if args.command=='report' and not (args.store/'live.sqlite').exists():
        print(json.dumps({'status':'not_initialized','responses':[],'classifications':[],'controls':{}}))
        return
    db = open_store(args.store,readonly=args.command=='report')
    session = uuid.uuid4().hex
    try:
        if args.command=='report':
            print(json.dumps(report(db),indent=2))
        elif args.command=='godel-import':
            with args.capture.open('rb') as stream:
                raw = stream.read(MAX_BODY+1)
            if len(raw)>MAX_BODY:
                raise ValueError('Import exceeds 1 MiB')
            events = json.loads(raw)
            if not isinstance(events,list) or len(events)>100:
                raise ValueError('Expected at most 100 retained bridge events')
            print(json.dumps([godel_event(db,args.store,x,session,True,True) for x in events]))
        elif args.command=='yahoo-once':
            if not args.compatibility_check:
                if (day_close(utc())-utc()).total_seconds() < 17:
                    raise ValueError('Closing request margin')
            lock = Path(args.store)/'yahoo.lock'
            with lock.open('x') as stream:
                json.dump(dict(pid=os.getpid(),created_at=stamp(),session=session,mode='once'),stream)
            try:
                print(json.dumps(yahoo_once(db,args.store,args.symbol,session,args.compatibility_check)))
            finally:
                lock.unlink()
        elif args.command=='yahoo-run':
            end = day_close(utc())
            selected = symbols(args.symbols)
            if not 1 <= len(selected) <= 12 or any(not re.fullmatch(r'[A-Z0-9.^-]{1,20}',q) for q in selected):
                raise ValueError('Use 1–12 explicit ticker queries')
            lock = Path(args.store)/'yahoo.lock'
            # Never clear a stale lock blindly: the launcher verifies full process identity.
            with lock.open('x') as stream:
                json.dump(dict(pid=os.getpid(),created_at=stamp(),session=session,cutoff=stamp(end),symbols=selected),stream)
            print(json.dumps({'status':'running','pid':os.getpid(),'session':session,'cutoff':stamp(end),'symbols':selected}),flush=True)
            try:
                while (end-utc()).total_seconds() >= 17:
                    if get(db,'yahoo:hold'):
                        print(json.dumps({'status':'held','control':get(db,'yahoo:hold')}),flush=True)
                        break
                    for query in selected:
                        if (end-utc()).total_seconds() < 17:
                            break
                        result = yahoo_once(db,args.store,query,session)
                        if result['status'] != 'backoff':
                            print(json.dumps({'query':query,**result}),flush=True)
                    time.sleep(min(5,max(0,(end-utc()).total_seconds())))
            finally:
                lock.unlink()
    finally:
        db.close()


if __name__=='__main__':
    main()
