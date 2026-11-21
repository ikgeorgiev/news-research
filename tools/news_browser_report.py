"""Bounded read-only, date-specific passive-browser comparison report.

Sampling intervals are not latency measurements or proof of complete coverage.
No web requests, article bodies, production writes or browser actions.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time
import bridge_coverage

SOURCES=('godel-browser','newsfilter-browser')
APP_ID=0x4345464C
MAX_ROWS=20000


def dt(value):return datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(timezone.utc)


def merged(intervals):
    result=[]
    for start,end in sorted(intervals):
        if end<=start:continue
        if result and start<=result[-1][1]:result[-1][1]=max(end,result[-1][1])
        else:result.append([start,end])
    return result


def seconds(intervals):return sum((end-start).total_seconds() for start,end in merged(intervals))


def read_store(directory,start,end):
    path=Path(directory).resolve()/'live.sqlite'
    if not path.exists():return {'store':str(directory),'status':'not_initialized','responses':[],'sessions':[],'controls':{},'truncated':False}
    db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)
    db.row_factory=sqlite3.Row
    began=time.monotonic();db.set_progress_handler(lambda:int(time.monotonic()-began>5),1000)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        if db.execute('PRAGMA application_id').fetchone()[0]!=APP_ID:raise ValueError('Unrecognized comparison store')
        rows=db.execute('SELECT id,source,scope,observed,local_received,status,meta,error FROM responses WHERE local_received>=? AND local_received<? AND source IN (?,?) ORDER BY id LIMIT ?',
                        (start.isoformat(),end.isoformat(),*SOURCES,MAX_ROWS+1)).fetchall()
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        sessions=[dict(r) for r in db.execute('SELECT * FROM bridge_sessions WHERE started_at>=? AND started_at<? ORDER BY started_at LIMIT 1000',(start.isoformat(),end.isoformat()))] if 'bridge_sessions' in tables else []
        controls={r[0]:json.loads(r[1]) for r in db.execute('SELECT key,value FROM control WHERE key IN (?,?)',tuple(s+':hold' for s in SOURCES))}
        diagnostics=[dict(r) for r in db.execute('SELECT session,event_id,received_at,data FROM bridge_diagnostics WHERE received_at>=? AND received_at<? ORDER BY received_at LIMIT 1001',(start.isoformat(),end.isoformat()))] if 'bridge_diagnostics' in tables else []
        return {'store':str(directory),'status':'read','responses':[dict(r) for r in rows[:MAX_ROWS]],'sessions':sessions,'controls':controls,
                'transport_diagnostics':[{**r,'data':json.loads(r['data'])} for r in diagnostics[:1000]],'diagnostics_truncated':len(diagnostics)>1000,'truncated':len(rows)>MAX_ROWS or len(diagnostics)>1000}
    finally:db.close()


def summarize(stores,day,now=None):
    start=dt(day+'T08:00:00Z');cutoff=dt(day+'T22:00:00Z')
    now=now or datetime.now(timezone.utc);end=max(start,min(cutoff,now))
    input_data=[read_store(p,start,cutoff) for p in stores]
    sources={}
    for source in SOURCES:
        samples=[];excluded=[];unique=set();transport_counts={};batch_counts={}
        for store in input_data:
            for row in store['responses']:
                identity=(str(Path(store['store']).resolve()),row['id'])
                if row['source']!=source or identity in unique:continue
                unique.add(identity)
                meta=json.loads(row['meta'])
                kind=meta.get('receipt_kind','passive_browser_copy')
                if kind in ('passive_browser_stream_batch','passive_browser_worker_batch') and not row['error'] and not meta.get('compatibility_history'):
                    batch_counts[kind]=batch_counts.get(kind,0)+1
                    continue
                push=kind in ('passive_browser_stream_copy','passive_browser_worker_copy')
                if (row['status']!=200 and not (push and row['status'] is None)) or row['error']:
                    excluded.append({'response':identity,'reason':row['error'] or 'native_non200'});continue
                observed=dt(row['observed'])
                if not start<=observed<cutoff or observed>end or meta.get('retrospective_import') or meta.get('compatibility_history'):
                    excluded.append({'response':identity,'reason':'outside_window_or_retrospective'});continue
                samples.append((observed,row,meta))
                transport_counts[kind]=transport_counts.get(kind,0)+1
        intervals=[];by_scope={};breaks=[]
        for observed,row,meta in sorted(samples,key=lambda x:x[0]):
            assessment=meta.get('assessment',{});continuity=assessment.get('continuity',{})
            if meta.get('receipt_kind') in ('passive_browser_stream_copy','passive_browser_worker_copy'):
                assessment={'filter_scope':bridge_coverage.filter_scope(source,meta.get('filters',{}),bridge_coverage.universe())}
                continuity={'reason':'native_push_copy_no_listing_absence'}
            key=(meta.get('session'),meta.get('capture_id'),row['scope'])
            scope=by_scope.setdefault(key,{'receiver_session':key[0],'capture_id':key[1],'scope':key[2],'responses':0,'filters':meta.get('filters',{}),'filter_eligibility':assessment.get('filter_scope'),'sampled_intervals':[],'full_page_responses':0})
            scope['responses']+=1;scope['last_native_copy_at']=observed.isoformat()
            scope.setdefault('first_native_copy_at',observed.isoformat())
            scope['full_page_responses']+=int(bool(continuity.get('full_page')))
            if continuity.get('reason')=='repeated_native_sample' and continuity.get('previous_sample_at'):
                previous=dt(continuity['previous_sample_at'])
                if start<=previous<=observed and (observed-previous).total_seconds()<=180:
                    intervals.append((previous,observed));scope['sampled_intervals'].append((previous,observed))
            else:
                breaks.append({'at':observed.isoformat(),'reason':continuity.get('reason','legacy_diagnostics_unavailable'),'previous_sample_at':continuity.get('previous_sample_at')})
        for scope in by_scope.values():
            scope['sampled_interval_seconds']=seconds(scope['sampled_intervals'])
            scope['sampled_intervals']=[[a.isoformat(),b.isoformat()] for a,b in merged(scope['sampled_intervals'])]
        unions=merged(intervals)
        available=(end-start).total_seconds()
        first=min((s[0] for s in samples),default=None);last=max((s[0] for s in samples),default=None)
        sources[source]={'successful_native_copies':len(samples),'transport_counts':transport_counts,'local_batch_counts':batch_counts,'excluded_responses':excluded,'sampled_interval_seconds':seconds(unions),
          'assessed_window_seconds':available,'unobserved_or_censored_seconds':available-seconds(unions),
          'pre_first_sample_seconds':(first-start).total_seconds() if first else available,
          'after_last_sample_seconds':(end-last).total_seconds() if last else available,
          'scopes':list(by_scope.values()),'continuity_breaks':breaks,
          'sampled_intervals':[[a.isoformat(),b.isoformat()] for a,b in unions],
          'within_60_fraction':None,'validated_release_count':None,'complete_cef_coverage_proven':False}
    intersections=[]
    for a,b in sources[SOURCES[0]]['sampled_intervals']:
        for c,d in sources[SOURCES[1]]['sampled_intervals']:
            lo,hi=max(dt(a),dt(c)),min(dt(b),dt(d))
            if lo<hi:intersections.append((lo,hi))
    return {'date':day,'as_of_utc':now.isoformat(),'window_start':start.isoformat(),'window_cutoff':cutoff.isoformat(),
        'database_read_only':True,'statement_timeout_seconds':5,'source_requests':0,
        'stores':[{k:v for k,v in s.items() if k!='responses'} for s in input_data],
        'sources':sources,'simultaneously_sampled_seconds':seconds(intersections),
        'truncated':any(s['truncated'] for s in input_data),
        'limitations':['Sampled intervals connect successive native copies; they are not proof that every intervening release was available or captured.',
          'Filter eligibility is separate from original wire identity and primary-body ticker validation.',
          'Missing copies, pauses, new captures, full-page turnover and downtime cannot provide fine absence bounds.',
          'No latency winner, release-miss count or physical screen-delivery measurement is inferred.',
          'Publication labels, vendor timestamps, browser body-copy observation and local receipt remain separate clocks.']}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--store',action='append',type=Path,required=True)
    p.add_argument('--date',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if not 1<=len(a.store)<=2 or a.date not in {f'2026-11-{d:02}' for d in range(9,14)}:raise ValueError('Use one or two stores and an authorized November9–13 date')
    report=summarize(a.store,a.date)
    with a.output.open('x',encoding='utf-8') as f:json.dump(report,f,indent=2);f.write('\n')
    print(json.dumps({'output':str(a.output),'truncated':report['truncated'],'source_requests':0}))


if __name__=='__main__':main()
