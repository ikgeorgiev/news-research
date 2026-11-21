"""One bounded, read-only production snapshot. No source requests or observers."""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.request import build_opener, ProxyHandler, HTTPRedirectHandler

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from app.source_research.observations import identity


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def matching_trace(line, pids, since, until, window=None):
    """Read the actual ResearchTimingHandler schema, not inferred event clocks."""
    try:
        event=json.loads(line)
        at=datetime.fromisoformat(str(event.get('recorded_at') or '').replace('Z','+00:00'))
        if event.get('process_id') in pids and at.tzinfo and since<=at<until:
            if window is not None and (not window.accepts(at)
                    or event.get('configuration_fingerprint') != window.fingerprint
                    or event.get('collection_id') != window.collection_id):
                return None
            return event
    except (ValueError,TypeError,AttributeError):
        pass
    return None


def trace_configuration(settings, health, pids):
    """A saved setting is a plan. Only the identified app can confirm it loaded."""
    from app.news_validation_timing import validation_timing_window
    try:
        window = validation_timing_window(settings)
    except ValueError as exc:
        return None, dict(mode='invalid_validation', error=str(exc), active_configuration_verified=False), None
    active = health.get('timing') or {}
    verified = (len(pids) == 1 and active.get('process_id') in pids
                and active.get('handler_attached') is True)
    if window is not None:
        verified = (verified and active.get('mode') == 'weekday_validation'
                    and active.get('configuration_fingerprint') == window.fingerprint)
        planned = dict(mode='weekday_validation', **window.description(),
                       configuration_fingerprint=window.fingerprint)
        path = window.path
    else:
        expiry = settings.research_timing_log_until
        if expiry is not None and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        verified = (verified and active.get('mode') == 'legacy_study'
                    and active.get('until') == (expiry.isoformat() if expiry else None))
        planned = dict(mode='legacy_study', until=expiry.isoformat() if expiry else None)
        path = Path(settings.research_timing_log_path) if settings.research_timing_log_path else None
    planned.update(active_configuration_verified=bool(verified),
                   recording_now_verified=bool(verified and active.get('recording_now') is True),
                   feed_receipts_enabled_verified=bool(verified and active.get('feed_receipts_enabled') is True))
    return path, planned, window


def inventory():
    # Read commands only for the project backend's listener tree, not every app.
    script = r'''
$ErrorActionPreference='Stop'
$all=@(Get-CimInstance Win32_Process)
$listener=@(Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 8001)
$owners=@($listener.OwningProcess | Sort-Object -Unique)
$selected=@($all | Where-Object { $_.ProcessId -in $owners -and $_.CommandLine -match 'uvicorn.*app\.main:app' -and $_.CommandLine -match 'sec-research' })
$ids=@($selected.ProcessId)
$children=@($all | Where-Object { $_.ParentProcessId -in $ids -and $_.CommandLine -match 'spawn_main' })
$parents=@($all | Where-Object { $_.ProcessId -in $selected.ParentProcessId -and $_.CommandLine -match 'uvicorn|dev-backend\.ps1' })
@{processes=@($selected+$children+$parents | Select-Object ProcessId,ParentProcessId,@{Name='CreatedUtc';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}},ExecutablePath,CommandLine);listener=@($listener | Select-Object LocalAddress,LocalPort,OwningProcess)} | ConvertTo-Json -Depth 5
'''
    completed = subprocess.run(['pwsh', '-NoProfile', '-NonInteractive', '-Command', script],
                               capture_output=True, text=True, timeout=15, check=True)
    return json.loads(completed.stdout)


def snapshot(minutes):
    if not 1 <= minutes <= 360:
        raise ValueError('Choose a recent window of 1–360 minutes')
    captured = datetime.now(timezone.utc)
    since = captured - timedelta(minutes=minutes)
    out = dict(version=1, captured_at=captured.isoformat(), since=since.isoformat(),
               minutes=minutes, database_read_only=True, statement_timeout_ms=5000,
               external_source_requests=0, production_mutations=0)
    out['runtime'] = inventory()
    with build_opener(ProxyHandler({}),NoRedirect()).open('http://127.0.0.1:8001/health', timeout=5) as response:
        health = response.read(256 * 1024 + 1)
        if len(health) > 256 * 1024:
            raise ValueError('Health response exceeds 256 KiB')
        out['health'] = json.loads(health)
    from app.config import get_settings
    from scripts.local_database import local_report_database
    settings = get_settings()
    with local_report_database(settings.database_url) as db:
        out['database_identity'] = db.execute('SELECT current_database() AS database,inet_server_port() AS server_port').fetchone()
        out['database_identity']['published_port'] = db.info.port
        out['articles'] = db.execute('''SELECT id,title,canonical_url,provider_name,source_name,
            published_at,first_seen_at,created_at,first_alert_sent_at,left(summary,6000) AS summary
            FROM articles WHERE first_seen_at >= %s AND first_seen_at < %s
            ORDER BY first_seen_at,id LIMIT 1001''', (since,captured)).fetchall()
        out['article_window_truncated'] = len(out['articles']) > 1000
        out['articles'] = out['articles'][:1000]
        ids = [a['id'] for a in out['articles']]
        out['ticker_associations'] = db.execute('''SELECT at.article_id,t.symbol,at.match_type,at.confidence
            FROM article_tickers at JOIN tickers t ON t.id=at.ticker_id
            WHERE at.article_id=ANY(%s) ORDER BY at.article_id,t.symbol LIMIT 10001''', (ids,)).fetchall()
        out['ticker_associations_truncated'] = len(out['ticker_associations']) > 10000
        out['ticker_associations'] = out['ticker_associations'][:10000]
        out['feed_runs'] = db.execute('''SELECT s.code,r.feed_url,r.status,count(*) AS runs,
            sum(r.items_seen) AS items_seen,sum(r.items_inserted) AS items_inserted,
            min(r.started_at) AS first_run,max(r.finished_at) AS last_finished
            FROM ingestion_runs r JOIN sources s ON s.id=r.source_id
            WHERE r.started_at >= %s AND r.started_at < %s
            GROUP BY s.code,r.feed_url,r.status ORDER BY s.code,r.feed_url,r.status LIMIT 1001''',
            (since,captured)).fetchall()
        out['feed_groups_truncated'] = len(out['feed_runs']) > 1000
        out['feed_runs'] = out['feed_runs'][:1000]
        out['recent_failures'] = db.execute('''SELECT feed_url,status,started_at,finished_at,left(error_text,1200) AS error
            FROM ingestion_runs WHERE started_at >= %s AND started_at < %s AND status='failed'
            ORDER BY started_at DESC LIMIT 101''', (since,captured)).fetchall()
        out['failures_truncated'] = len(out['recent_failures']) > 100
        out['recent_failures'] = out['recent_failures'][:100]
        out['active_tickers'] = db.execute('SELECT count(*) AS n FROM tickers WHERE active').fetchone()['n']
    grouped = defaultdict(list)
    for article in out['articles']:
        article['original_wire_from_url'] = identity(article['canonical_url'])
        if article['original_wire_from_url']:
            grouped[article['original_wire_from_url']].append(article['id'])
    out['possible_duplicate_wire_rows'] = {k:v for k,v in grouped.items() if len(v)>1}
    out['by_provider'] = dict(Counter(a['provider_name'] for a in out['articles']))
    out['reader_archives_present'] = [a['id'] for a in out['articles']
        if (Path(settings.article_body_archive_dir) / f"{a['id']}.json").is_file()]
    state = Path(settings.financialcontent_state_path).resolve()
    if state.is_file():
        with sqlite3.connect(state.as_uri()+'?mode=ro',uri=True,timeout=5) as db:
            db.execute('PRAGMA query_only=ON')
            deadline = time.monotonic()+5
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            out['financialcontent_candidate_counts'] = dict(db.execute('SELECT status,count(*) FROM fc_candidate GROUP BY status'))
            out['financialcontent_control_count'] = db.execute('SELECT count(*) FROM fc_control').fetchone()[0]
    pids = {p['ProcessId'] for p in out['runtime']['processes'] if 'spawn_main' in p['CommandLine']}
    out['actual_app_pids'] = sorted(pids)
    out['actual_app_identity_unambiguous'] = len(pids) == 1
    out['saved_trace_expiry'] = settings.research_timing_log_until
    base, out['timing_configuration'], window = trace_configuration(settings, out['health'], pids)
    out['fresh_feed_receipts_enabled'] = out['timing_configuration'].get('feed_receipts_enabled_verified', False)
    out['trace_events'] = []
    out['trace_scan'] = dict(bytes_read=0, files=[], truncated=False)
    if base:
        budget=8*1024*1024
        files=[base]+[base.with_name(base.name+'.'+str(n)) for n in range(1,4)]
        files=sorted((p for p in files if p.is_file()),key=lambda p:p.stat().st_mtime,reverse=True)
        for path in files:
            size=path.stat().st_size
            take=min(size,budget,2*1024*1024)
            if take <= 0:
                out['trace_scan']['truncated']=True
                break
            with path.open('rb') as stream:
                stream.seek(max(0,size-take)); raw=stream.read(take)
            if size>take:
                raw=raw.partition(b'\n')[2]
                out['trace_scan']['truncated']=True
            budget-=take
            out['trace_scan']['bytes_read']+=take
            out['trace_scan']['files'].append(str(path))
            for line in raw.splitlines():
                event=matching_trace(line,pids,since,captured,window)
                if event:
                    out['trace_events'].append(event)
    out['limitations'] = [
        'Database first_seen_at/created_at are stored labels, not measured transaction commit.',
        'The selected first_seen window is not a complete census of publisher releases.',
        'Startup, catch-up and retrospective rows require classification before speed comparisons.',
        'Ticker associations and URL identities are candidates for primary-body verification.',
        'Possible duplicate wire rows are review flags; translations and changed releases need inspection.',
        'Presence of an archive file is not validation; use news_validation.py reader on this snapshot.',
        'Trace scan is bounded and restricted to verified current app PIDs; no other stage is inferred.',
        'The old study trace expiry is unchanged. Browser exports remain separate; physical screen delivery is unmeasured.',
    ]
    return out


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('output');p.add_argument('--minutes',type=int,default=65)
    args=p.parse_args()
    path=Path(args.output)
    if path.exists():
        raise FileExistsError('Choose a new snapshot filename')
    result=snapshot(args.minutes)
    with path.open('x',encoding='utf8') as stream:
        json.dump(result,stream,indent=2,ensure_ascii=False,default=str);stream.write('\n')
    print(json.dumps(dict(output=str(path.resolve()),articles=len(result['articles']),
        app_pids=result['actual_app_pids'],health=result['health'].get('status'),
        trace_events=len(result['trace_events']),production_mutations=0)))


if __name__=='__main__':
    main()
