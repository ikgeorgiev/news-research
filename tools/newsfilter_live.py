"""Passive Newsfilter comparison intake. No Newsfilter HTTP client or credentials.

Accepts only selected news fields from the user's signed-in browser. No article
body requests, pagination, production inserts or notification publishing.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import hashlib
import html
import json
from pathlib import Path
import re
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import news_live as live

ENDPOINT = 'https://api.newsfilter.io/actions'
SOURCE = 'newsfilter-browser'
FIELDS = {'id','title','sourceUrl','url','publishedAt','symbols','source'}
EVENT_FIELDS = {'version','capture_id','endpoint','status','receipt_kind','scope','transport',
                'browser_request_observed_at','browser_headers_observed_at','browser_body_observed_at','browser_response_completed_at',
                'headers','payload','native_response_bytes','error'}


def scope_of(event):
    scope = event.get('scope')
    if not isinstance(scope,dict) or set(scope) != {'type','queryString','from','size'}:
        raise ValueError('Expected selected Newsfilter query fields')
    if scope['type'] != 'filterArticles' or scope['from'] != 0 or type(scope['from']) is not int:
        raise ValueError('Only first-page news queries are accepted')
    if type(scope['size']) is not int or not 1 <= scope['size'] <= 100:
        raise ValueError('Newsfilter page size exceeds bound')
    live.text(scope['queryString'],8192)
    return scope, hashlib.sha256(json.dumps(scope,sort_keys=True).encode()).hexdigest()


def checked_event(event):
    """Reject extra fields before anything is written to disk."""
    if not isinstance(event,dict) or set(event)-EVENT_FIELDS:
        raise ValueError('Unexpected capture fields')
    if event.get('version') != 1 or event.get('endpoint') != ENDPOINT or event.get('receipt_kind') != 'passive_browser_copy':
        raise ValueError('Expected passive Newsfilter news capture')
    if type(event.get('status')) is not int or not 0 <= event['status'] <= 599:
        raise ValueError('Invalid native status')
    if not isinstance(event.get('capture_id'),str) or not re.fullmatch(r'[a-f0-9-]{36}',event['capture_id']):
        raise ValueError('Missing capture generation')
    headers = event.get('headers',{})
    if not isinstance(headers,dict) or set(headers)-{'date','age','cache-control','retry-after'}:
        raise ValueError('Unexpected response headers')
    if any(not isinstance(v,str) or len(v)>1000 for v in headers.values()):
        raise ValueError('Invalid response header')
    if event.get('error') not in (None,'news_response_copy_or_schema_error','native_network_error'):
        raise ValueError('Unrecognized error code')
    if event.get('browser_response_completed_at') is not None:
        completed=live.dt(event['browser_response_completed_at'])
        if not live.dt(event['browser_request_observed_at']) <= completed <= live.dt(event.get('browser_body_observed_at') or event['browser_response_completed_at']):
            raise ValueError('Invalid native completion clock ordering')
    scope_of(event)
    if 'payload' in event:
        p=event['payload']
        if not isinstance(p,dict) or set(p)-{'articles','total','from','size'} or not isinstance(p.get('articles'),list):
            raise ValueError('Unrecognized news envelope')
        if len(p['articles'])>100:
            raise ValueError('Oversized article array')
        total=p.get('total')
        if total is not None and (not isinstance(total,dict) or set(total)-{'value','relation'}):
            raise ValueError('Invalid total metadata')
        for x in p['articles']:
            if not isinstance(x,dict) or set(x)-FIELDS:
                raise ValueError('Unexpected article fields')
            source=x.get('source')
            if not isinstance(source,dict) or set(source)-{'id','name'}:
                raise ValueError('Unexpected source fields')
    raw=json.dumps(event,ensure_ascii=False).encode()
    if len(raw)>live.MAX_BODY:
        raise ValueError('Capture exceeds 1 MiB')
    return raw


def parse_newsfilter(event):
    checked_event(event)
    filters,scope=scope_of(event)
    payload=event.get('payload')
    if event['status'] != 200 or event.get('error') or not payload:
        raise ValueError('Not a successful news response')
    if payload.get('from') != 0 or type(payload.get('from')) is not int or payload.get('size') != filters['size']:
        raise ValueError('Response does not match requested first page')
    if len(payload['articles'])>filters['size']:
        raise ValueError('Response exceeds requested size')
    observed=live.dt(event['browser_body_observed_at'])
    started=live.dt(event['browser_request_observed_at'])
    if not timedelta(0) <= observed-started <= timedelta(seconds=60):
        raise ValueError('Invalid browser clock ordering or response duration')
    result=[]
    for x in payload['articles']:
        published=x.get('publishedAt')
        try: published=live.stamp(live.dt(published)) if published else None
        except (ValueError,TypeError): published=None
        source=x['source']
        original=live.safe_url(x.get('sourceUrl'))
        entry=live.row(x.get('id'),html.unescape(live.text(x.get('title'))),original,published,x.get('symbols'),
            publisher=live.text(source.get('name'),150),source_id=live.text(source.get('id'),150),
            republisher_url=live.safe_url(x.get('url')),vendor_received_at=None,
            publication_label_issue=('future_label' if published and live.dt(published)>observed else
                                     'missing_or_invalid_label' if not published else None))
        result.append(entry)
    return result,scope


def newsfilter_event(db,directory,event,session,compatibility=False,retained=False):
    raw=checked_event(event)
    if retained and not compatibility:
        raise ValueError('Saved imports are compatibility history only')
    now=live.utc()
    started=live.dt(event['browser_request_observed_at'])
    observed=live.dt(event.get('browser_body_observed_at') or event.get('browser_response_completed_at') or event.get('browser_headers_observed_at') or event['browser_request_observed_at'])
    if not retained and (abs((now-observed).total_seconds())>120 or started>observed):
        raise ValueError('Stale capture or invalid browser clock')
    if live.get(db,SOURCE+':hold'):
        return {'status':'held','reason':live.get(db,SOURCE+':hold')}
    digest=hashlib.sha256(raw).hexdigest()
    duplicate=db.execute('SELECT id FROM responses WHERE source=? AND sha256=?',(SOURCE,digest)).fetchone()
    if duplicate:
        return {'status':'duplicate_capture','response_id':duplicate[0]}
    scope_fields,scope=scope_of(event)
    meta=dict(started_at=live.stamp(started),observed_at=live.stamp(observed),status=event['status'],
        receipt_kind='passive_browser_copy',clock_id='browser_system_utc',session=session,
        capture_id=event['capture_id'],filters=scope_fields,headers=event.get('headers',{}),
        browser_response_completed_at=event.get('browser_response_completed_at'),
        retrospective_import=retained,compatibility_history=compatibility,finite_page=True)
    error=None
    if event['status']!=200 or event.get('error'):
        error='native_refusal_or_failure'
    else:
        try: rows,_=parse_newsfilter(event)
        except (ValueError,KeyError,TypeError): error='news_schema_failure'
    with db:
        rid=live.save_response(db,directory,SOURCE,scope,raw,meta,error)
        if error:
            live.put(db,SOURCE+':hold',{'status':event['status'],'response_id':rid,'reason':error})
            return {'status':'native_error','response_id':rid}
        assessment=live.bridge_coverage.assess(db,SOURCE,scope,rows,live.stamp(observed),session,event['capture_id'],scope_fields,scope_fields['size'])
        meta['assessment']=assessment
        db.execute('UPDATE responses SET meta=? WHERE id=?',(json.dumps(meta),rid))
        counts=live.ingest(db,rid,SOURCE,scope,rows,live.stamp(observed),session+':'+event['capture_id'],180,compatibility,
                          force_baseline=assessment['continuity']['baseline_required'])
    return {'status':'ok','response_id':rid,'articles':len(rows),'classifications':counts,
            'publication_label_issues':sum(bool(r['publication_label_issue']) for r in rows),'assessment':assessment}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--store',type=Path,required=True)
    sub=p.add_subparsers(dest='command',required=True)
    serve=sub.add_parser('serve');serve.add_argument('--minutes',type=int,default=20)
    serve.add_argument('--until-close',action='store_true')
    serve.add_argument('--compatibility-check',action='store_true');serve.add_argument('--port',type=int,default=8767)
    imp=sub.add_parser('import');imp.add_argument('capture',type=Path)
    sub.add_parser('report')
    a=p.parse_args(argv)
    if a.command=='serve':
        if not a.until_close and not 1<=a.minutes<=20: raise ValueError('Newsfilter short sessions are limited to 20 minutes')
        live.serve(a.store,a.minutes,a.compatibility_check,a.port,route='/newsfilter',event_handler=newsfilter_event,until_close=a.until_close)
        return
    db=live.open_store(a.store,readonly=a.command=='report')
    try:
        if a.command=='report': result=live.report(db)
        else:
            with a.capture.open('rb') as f: raw=f.read(live.MAX_BODY+1)
            if len(raw)>live.MAX_BODY: raise ValueError('Import exceeds 1 MiB')
            events=json.loads(raw)
            if not isinstance(events,list) or len(events)>100: raise ValueError('Expected at most 100 captures')
            result=[newsfilter_event(db,a.store,e,uuid.uuid4().hex,True,True) for e in events]
        print(json.dumps(result,indent=2))
    finally: db.close()


if __name__=='__main__': main()
