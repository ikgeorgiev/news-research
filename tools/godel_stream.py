"""Validate passive native news frames. No source networking or production writes."""
from __future__ import annotations
import hashlib
import json
import re
import os
from datetime import timedelta

ENDPOINT = 'wss://terminal-api.godelterminal.com/events'
WORKER_ENDPOINT = 'https://app.godelterminal.com/'
FIELDS = {'id','title','providerName','publicationTime','receivedTime','originalUrl','sources','securityIds','feedId','language'}


class InvalidStreamRecord(ValueError):
    def __init__(self,index,error):
        super().__init__(str(error))
        self.record_index=index


def parse_record(live, record, batch_observed, *, allow_delayed=False):
    if not isinstance(record, dict) or set(record)-{'socket_id','subscription','destination','message_id','scope','subscription_observed_at','browser_message_observed_at','browser_body_observed_at','native_frame_bytes','article'}:
        raise ValueError('Unexpected stream fields')
    socket = live.text(record.get('socket_id'),36)
    if not re.fullmatch(r'[a-f0-9-]{36}',socket):
        raise ValueError('Invalid socket identity')
    subscription = live.text(record.get('subscription'),100)
    destination = record.get('destination')
    if destination != '/topic/news/godel-headlines' and not re.fullmatch(r'/user/queue/filterResponse/\d{1,12}',destination or ''):
        raise ValueError('Not a news subscription')
    filters=record.get('scope')
    if not isinstance(filters,dict) or set(filters)-{'symbols','sources','languages'}:
        raise ValueError('Invalid subscription scope')
    for key in ('symbols','sources','languages'):
        if key=='symbols' and key not in filters:
            continue
        values=filters.get(key)
        if not isinstance(values,list) or len(values)>1000 or any(not isinstance(x,str) or len(x)>300 for x in values):
            raise ValueError('Invalid subscription scope')
    subscribed=live.dt(record.get('subscription_observed_at'))
    message=live.dt(record.get('browser_message_observed_at'))
    copied=live.dt(record.get('browser_body_observed_at'))
    if not subscribed<=message<=copied<=batch_observed or (not allow_delayed and batch_observed-message>timedelta(seconds=30)):
        raise ValueError('Unordered or delayed stream copy')
    if type(record.get('native_frame_bytes')) is not int or not 0<record['native_frame_bytes']<=live.MAX_BODY:
        raise ValueError('Invalid native frame bound')
    x=record.get('article')
    if not isinstance(x,dict) or set(x)-FIELDS:
        raise ValueError('Unexpected news fields')
    related=x.get('securityIds') or []
    if not isinstance(related,list) or len(related)>1000:
        raise ValueError('Invalid related symbols')
    empty=sum(isinstance(s,str) and not s.strip() for s in related)
    related=[s for s in related if not (isinstance(s,str) and not s.strip())]
    received=x.get('receivedTime')
    entry=live.row(x.get('id'),x.get('title'),x.get('originalUrl'),live.epoch(x.get('publicationTime'),1000),related,
        related_limit=1000,publisher=x.get('providerName'),vendor_received_at=live.stamp(live.dt(received)) if received else None,
        empty_security_id_count=empty,browser_message_observed_at=live.stamp(message),
        browser_body_observed_at=live.stamp(copied),receipt_kind='passive_browser_stream_copy',
        message_to_batch_seconds=(batch_observed-message).total_seconds())
    scope_key={'transport':'native_websocket','destination_kind':'headlines' if destination=='/topic/news/godel-headlines' else 'filtered_news',
               'filters':{k:sorted(v) for k,v in filters.items()}}
    scope=hashlib.sha256(json.dumps(scope_key,sort_keys=True).encode()).hexdigest()
    return entry,scope,live.stamp(message),socket+':'+subscription,filters


def parse_worker_record(live, record, batch_observed, *, allow_delayed=False):
    allowed={'worker_id','subscription','destination','scope','subscription_observed_at',
             'browser_message_observed_at','browser_body_observed_at','selected_article_bytes',
             'worker_initial','worker_delivery_mode','article'}
    if not isinstance(record,dict) or set(record)!=allowed:
        raise ValueError('Unexpected worker news fields')
    if type(record['worker_initial']) is not bool or record['worker_delivery_mode'] not in ('EMIT:BUFFERED','EMIT:CONFLATED'):
        raise ValueError('Invalid worker delivery metadata')
    # Reuse the bounded article/scope/clock validator without describing the
    # worker-to-page copy as a socket frame or network completion timestamp.
    adapted={k:v for k,v in record.items() if k not in {'worker_id','selected_article_bytes','worker_initial','worker_delivery_mode'}}
    adapted.update(socket_id=record['worker_id'],native_frame_bytes=record['selected_article_bytes'],message_id='')
    entry,scope,message,connection,filters=parse_record(live,adapted,batch_observed,allow_delayed=allow_delayed)
    entry.update(receipt_kind='passive_browser_worker_copy',worker_initial=record['worker_initial'],
                 worker_delivery_mode=record['worker_delivery_mode'])
    scope=hashlib.sha256(('worker:'+scope).encode()).hexdigest()
    return entry,scope,message,connection,filters


def ingest_batch(live, db, directory, event, session, compatibility=False, retained=False):
    raw=json.dumps(event,ensure_ascii=False).encode()
    worker=event.get('version')==3
    expected_endpoint=WORKER_ENDPOINT if worker else ENDPOINT
    batch_kind='passive_browser_worker_batch' if worker else 'passive_browser_stream_batch'
    copy_kind='passive_browser_worker_copy' if worker else 'passive_browser_stream_copy'
    if len(raw)>live.MAX_BODY or event.get('version') not in (2,3) or event.get('endpoint')!=expected_endpoint or event.get('receipt_kind')!=batch_kind:
        raise ValueError('Invalid stream batch')
    if set(event)-{'version','endpoint','receipt_kind','capture_id','browser_body_observed_at','records','dropped_frames'}:
        raise ValueError('Unexpected stream batch fields')
    if live.active_source_hold(db,'godel-browser'):
        return {'status':'held','reason':live.active_source_hold(db,'godel-browser')}
    if retained and not compatibility:
        raise ValueError('Retained frames are compatibility history only')
    capture=event.get('capture_id')
    if not isinstance(capture,str) or not re.fullmatch(r'[a-f0-9-]{36}',capture):
        raise ValueError('Missing capture generation')
    observed=live.dt(event.get('browser_body_observed_at'))
    if not retained and abs((live.utc()-observed).total_seconds())>120:
        raise ValueError('Stale stream batch')
    records=event.get('records')
    if not isinstance(records,list) or len(records)>100:
        raise ValueError('Invalid stream batch size')
    dropped=event.get('dropped_frames')
    if type(dropped) is not int or not 0<=dropped<=1000000:
        raise ValueError('Invalid stream drop count')
    # Validate the complete batch before storing any row.
    parser=parse_worker_record if worker else parse_record
    parsed=[]
    for index,record in enumerate(records):
        try:parsed.append(parser(live,record,observed,allow_delayed=True))
        except (ValueError,TypeError) as error:raise InvalidStreamRecord(index,error) from error
    digest=hashlib.sha256(raw).hexdigest()
    if db.execute('SELECT 1 FROM responses WHERE source=? AND sha256=?',('godel-browser',digest)).fetchone():
        return {'status':'duplicate_capture','articles':0}
    counts={};assessment=None;delayed_count=0;accepted_count=0
    batch_delayed=sum(entry['message_to_batch_seconds']>30 for entry,*_ in parsed)
    with db:
        batch_id=live.save_response(db,directory,'godel-browser','native_worker_batch' if worker else 'native_stream_batch',raw,
            dict(observed_at=live.stamp(observed),status=None,receipt_kind=batch_kind,
                 collector_pid=os.getpid(),session=session,capture_id=capture,dropped_frames=dropped,
                 compatibility_history=compatibility,retained_replay=retained,delayed_records=batch_delayed))
        for record,(entry,scope,message,connection,filters) in zip(records,parsed):
            # Each native message keeps an independent evidence row, even when an
            # identical ID arrives on two panels inside the same local batch.
            frame_raw=json.dumps(record,ensure_ascii=False).encode()
            delayed=entry['message_to_batch_seconds']>30
            day=live.dt(message).date().isoformat()
            gap_key='godel-browser:delayed-copy:'+day+':'+scope
            gap=live.get(db,gap_key,{})
            rid=live.save_response(db,directory,'godel-browser',scope,frame_raw,
                dict(started_at=record['subscription_observed_at'],observed_at=message,status=None,
                     receipt_kind=copy_kind,collector_pid=os.getpid(),session=session,capture_id=capture,
                     socket_subscription=connection,filters=filters,batch_response_id=batch_id,
                     browser_body_observed_at=record['browser_body_observed_at'],dropped_frames=dropped,
                     compatibility_history=compatibility,retained_replay=retained,
                     message_to_batch_seconds=entry['message_to_batch_seconds'],review_only=delayed,
                     baseline_after_delayed_response=gap.get('response_id') if gap.get('pending') else None),
                error='delayed_browser_copy_review_only' if delayed else None)
            if delayed:
                # Ordered but late page delivery is a timing gap, not a source
                # refusal. Preserve the copy without adding an article, moving
                # its discovery cursor, or claiming a timely/absent release.
                delayed_count+=1;counts['delayed_copy_review_only']=delayed_count
                live.put(db,gap_key,{'pending':True,'response_id':rid,'message_at':message,'batch_at':live.stamp(observed)})
                if assessment is None:
                    assessment={'filter_scope':live.bridge_coverage.filter_scope('godel-browser',filters,live.bridge_coverage.universe()),
                        'continuity':{'reason':'delayed_browser_copy_review_only','baseline_required':True,'global_absence_proven':False,'delivery_continuity_proven':False}}
                continue
            stream_session=session+':'+capture+':'+connection
            prior=db.execute('SELECT * FROM streams WHERE source=? AND scope=? AND day=?',('godel-browser',scope,day)).fetchone()
            if prior and live.dt(message)<live.dt(prior['last_observed']):
                raise ValueError('Out-of-order stream message')
            existing=db.execute('SELECT data FROM items WHERE source=? AND local_id=?',('godel-browser',entry['local_id'])).fetchone()
            baseline=not prior or prior['session']!=stream_session or entry.get('worker_initial',False) or bool(gap.get('pending'))
            if compatibility: kind='compatibility_history'
            elif baseline: kind='stream_baseline'
            elif existing:
                before=json.loads(existing['data'])
                compared=('local_id','title','link','published_at_label','related_tickers','publisher','vendor_received_at')
                kind='stream_repeated_id' if all(before.get(k)==entry.get(k) for k in compared) else 'stream_updated_same_id'
            elif entry['published_at_label'] and live.dt(entry['published_at_label'])>live.dt(message): kind='future_publication_label'
            elif entry['published_at_label'] and live.dt(entry['published_at_label']).date().isoformat()==day: kind='stream_newly_observed'
            else: kind='stream_history_or_unknown_date'
            db.execute('INSERT OR IGNORE INTO items VALUES(?,?,?,?)',('godel-browser',entry['local_id'],message,json.dumps(entry,sort_keys=True)))
            # A push message supplies no preceding complete listing/absence.
            db.execute('INSERT INTO observations VALUES(?,?,?,?,?,?,?)',(rid,'godel-browser',entry['local_id'],kind,None,message,json.dumps(entry,sort_keys=True)))
            db.execute('INSERT INTO streams VALUES(?,?,?,?,?,?) ON CONFLICT(source,scope,day) DO UPDATE SET session=excluded.session,last_observed=excluded.last_observed,last_ids=excluded.last_ids',
                ('godel-browser',scope,day,stream_session,message,json.dumps([entry['local_id']])))
            if gap.get('pending'):live.put(db,gap_key,{**gap,'pending':False,'consumed_by_response':rid})
            accepted_count+=1
            counts[kind]=counts.get(kind,0)+1
            assessment={'filter_scope':live.bridge_coverage.filter_scope('godel-browser',filters,live.bridge_coverage.universe()),
                'continuity':{'reason':'native_stream_message','baseline_required':baseline,'global_absence_proven':False,
                    'finite_page':False,'full_page':False,'delivery_continuity_proven':False,'dropped_frames':dropped}}
    return {'status':'ok','articles':accepted_count,'response_id':batch_id,'classifications':counts,'assessment':assessment,'stream_dropped':dropped,
            'delayed_records':delayed_count,'review_only':delayed_count>0 and accepted_count==0}
