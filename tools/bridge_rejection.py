"""Small, selected diagnostics for rejected local bridge payloads.

Never archives raw rejected input, exception text, headers, URLs or article text.
All original validation, refusal and shutdown decisions remain in the caller.
"""
from datetime import datetime,timezone
import hashlib
import json
import re
import uuid

# Only messages authored by our validators may become an exported reason.
REASONS={message:re.sub(r'[^a-z0-9]+','_',message.lower()).strip('_') for message in (
    'Expected capture object','Expected an aware ISO timestamp','Naive timestamp',
    'Missing or oversized text field','Invalid symbol list','Unexpected stream fields',
    'Invalid socket identity','Not a news subscription','Invalid subscription scope',
    'Unordered or delayed stream copy','Invalid native frame bound','Unexpected news fields',
    'Invalid related symbols','Unexpected worker news fields','Invalid worker delivery metadata',
    'Invalid stream batch','Unexpected stream batch fields','Retained frames are compatibility history only',
    'Missing capture generation','Stale stream batch','Invalid stream batch size','Invalid stream drop count',
    'Out-of-order stream message','Bridge event exceeds 1 MiB','Expected passive Godel event',
    'Stale event or unsynchronized browser clock','Missing browser capture generation',
    'Unexpected status fields','Invalid status','Invalid capture','Invalid diagnostics',
    'Invalid diagnostic fields','Invalid diagnostic id','Invalid diagnostic event','Invalid diagnostic capture','Invalid diagnostic number',
)}


def shape(value):
    kind=type(value).__name__
    result={'type':kind if kind in {'dict','list','str','int','float','bool','NoneType'} else 'other'}
    if isinstance(value,(str,list,dict)):result['length']=len(value)
    return result


def iso(value):
    if not isinstance(value,str) or len(value)>80:return None
    try:
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        if parsed.tzinfo is None:return None
        return parsed.astimezone(timezone.utc).isoformat()
    except (ValueError,OverflowError):return None


def describe(raw,event,error,stage='capture'):
    result={'id':uuid.uuid4().hex,'at':datetime.now(timezone.utc).isoformat(),
        'kind':stage+'_rejected','http_status':400,'raw_bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
        'reason':REASONS.get(str(error),'unclassified_validation_error'),
        'exception_class':type(error).__name__ if type(error).__name__ in {'ValueError','TypeError','TimeoutError','InvalidStreamRecord','JSONDecodeError','OperationalError','IntegrityError'} else 'other',
        'payload_shape':shape(event)}
    if isinstance(error,json.JSONDecodeError):result['reason']='invalid_json'
    if not isinstance(event,dict):return result
    if event.get('version') in (1,2,3) and type(event.get('version')) is int:result['version']=event['version']
    if event.get('receipt_kind') in ('passive_browser_worker_batch','passive_browser_stream_batch','passive_browser_copy'):
        result['transport']=event['receipt_kind']
    capture=event.get('capture_id')
    if isinstance(capture,str) and re.fullmatch(r'[a-f0-9-]{36}',capture):result['capture_id']=capture
    result['batch_copied_at']=iso(event.get('browser_body_observed_at'))
    records=event.get('records');result['records_shape']=shape(records)
    index=getattr(error,'record_index',None)
    if type(index) is int and 0<=index<100:result['record_index']=index
    if not isinstance(records,list) or not records:return result
    record=records[index if type(index) is int and 0<=index<len(records) else 0]
    result['record_shape']=shape(record)
    if not isinstance(record,dict):return result
    result['record_fields']={k:shape(record.get(k)) for k in (
        'worker_id','socket_id','subscription','destination','scope','subscription_observed_at',
        'browser_message_observed_at','browser_body_observed_at','selected_article_bytes','native_frame_bytes',
        'worker_initial','worker_delivery_mode','article')}
    result['record_times']={k:iso(record.get(k)) for k in (
        'subscription_observed_at','browser_message_observed_at','browser_body_observed_at')}
    message=result['record_times']['browser_message_observed_at'];batch=result['batch_copied_at']
    if message and batch:result['message_to_batch_seconds']=(datetime.fromisoformat(batch)-datetime.fromisoformat(message)).total_seconds()
    article=record.get('article')
    if isinstance(article,dict):
        result['article_fields']={k:shape(article.get(k)) for k in (
            'id','title','publicationTime','receivedTime','securityIds','originalUrl','sources','providerName','language','feedId')}
        result['vendor_received_time_parseable']=iso(article.get('receivedTime')) is not None
        tags=article.get('securityIds')
        if isinstance(tags,list):result['security_id_types']=sorted({shape(v)['type'] for v in tags[:1001]})
    return result
