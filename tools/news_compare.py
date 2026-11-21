"""Read-only cross-reference of live comparison captures and retained CEF evidence.

No source requests, body fetching, article insertion or automatic latency claims.
An optional local database lookup matches only captured Yahoo IDs/URLs in a
bounded publication window; mutable raw fetched_at is deliberately not selected.
"""
from __future__ import annotations
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'backend'))
from app.source_research.observations import identity, label, timestamp

APP_ID = 0x4345464C
OBSERVATION_FIELDS = {
    'browser_message_observed_at', 'browser_body_observed_at', 'receipt_kind',
    'message_to_batch_seconds', 'worker_initial', 'worker_delivery_mode', 'publication_label_issue',
}


def window(since, until):
    start, end = timestamp(since), timestamp(until)
    if not start or not end or not timedelta(0) < end-start <= timedelta(days=7):
        raise ValueError('Choose an aware observation window of at most seven days')
    return start, end


def clean_url(value):
    """Remove only Yahoo's archived, known RSS tracking query; preserve others."""
    if not isinstance(value, str) or len(value)>4096:
        return None
    try:
        p=urlsplit(value)
        if p.scheme!='https' or not p.hostname or p.username or p.password or p.port:
            return None
        if p.hostname=='finance.yahoo.com' and p.query=='.tsrc=rss':
            return urlunsplit(p._replace(query=''))
        return value
    except ValueError:
        return None


def publication_day(value):
    parsed=timestamp(value)
    return parsed.date().isoformat() if parsed else None


def case_wire(case):
    """Reconcile legacy numeric PRN IDs only against the archived original URL."""
    stored=case.get('wire_identity')
    from_url=identity(case.get('canonical_url'))
    if isinstance(stored,str) and stored.startswith(('bw:','gnw:','prn:')):
        if from_url and stored!=from_url:
            return None
        return stored
    if stored is None:
        return from_url
    if isinstance(stored,str) and stored.isdigit() and from_url=='prn:'+stored:
        return from_url
    return None


def read_cases(path):
    with Path(path).open('rb') as stream:
        raw=stream.read(8*1024*1024+1)
    if len(raw)>8*1024*1024:
        raise ValueError('Case archive exceeds 8 MiB')
    cases=json.loads(raw)['cases']
    if not isinstance(cases,list) or len(cases)>1000:
        raise ValueError('At most 1000 retained cases')
    if len({c['article_id'] for c in cases})!=len(cases):
        raise ValueError('Case archive must contain distinct article IDs')
    return cases,hashlib.sha256(raw).hexdigest()


def read_live(directory,since,until):
    start,end=window(since,until)
    path=(Path(directory)/'live.sqlite').resolve()
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        if db.execute('PRAGMA application_id').fetchone()[0]!=APP_ID:
            raise ValueError('Not a live-comparison store')
        deadline=time.monotonic()+5
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        # julianday handles equivalent Z/+00:00 labels without lexical-boundary errors.
        rows=db.execute('''SELECT o.response_id,o.source,o.local_id,o.classification,
            o.previous_sample,o.observed,o.data,r.scope,r.meta,r.local_received,r.path,r.sha256
            FROM observations o JOIN responses r ON r.id=o.response_id
            WHERE julianday(o.observed)>=julianday(?) AND julianday(o.observed)<julianday(?)
            ORDER BY o.observed,o.response_id LIMIT 5001''',(start.isoformat(),end.isoformat())).fetchall()
    if len(rows)>5000:
        raise ValueError('More than 5000 observations; narrow the window')
    result=[]
    for r in rows:
        row=dict(r);row['data']=json.loads(row['data']);row['meta']=json.loads(row['meta'])
        row['store']=str(path);result.append(row)
    return result


def lookup_local_yahoo(rows,since,until):
    """Exact observed identifiers only; requires an explicit --with-local-db flag."""
    start,end=window(since,until)
    start-=timedelta(days=14)
    observed=[r['data'] for r in rows if r['source']=='yahoo-search']
    ids=sorted({r['local_id'] for r in observed})
    links=set()
    for r in observed:
        link=clean_url(r.get('link'))
        if link and urlsplit(link).hostname=='finance.yahoo.com':
            links.add(link)
            if not urlsplit(link).query:
                links.add(link+'?.tsrc=rss')
    if len(ids)>250 or len(links)>500:
        raise ValueError('At most 250 observed Yahoo IDs per database lookup')
    out=dict(captured_at=datetime.now(timezone.utc).isoformat(),read_only=True,
        statement_timeout_ms=5000,publication_since=start.isoformat(),publication_until=end.isoformat(),
        lookup_ids=ids,lookup_links=sorted(links),raw_rows=[],ticker_associations=[],
        raw_fetched_at_used=False,source_requests=0,production_mutations=0)
    if not ids and not links:
        return out
    from app.config import get_settings
    from scripts.local_database import local_report_database
    with local_report_database(get_settings().database_url) as db:
        out['database_identity']=db.execute('SELECT current_database() AS database,inet_server_port() AS server_port').fetchone()
        out['database_identity']['published_port'] = db.info.port
        out['raw_rows']=db.execute('''SELECT r.id AS raw_id,r.article_id,r.feed_url,r.raw_guid,r.raw_title,
            r.raw_link,r.raw_pub_date,a.title AS article_title,a.canonical_url,a.published_at,
            a.first_seen_at AS stored_article_first_seen,a.created_at AS stored_article_created,
            left(r.raw_payload_json->>'summary',16000) AS retained_summary,
            length(r.raw_payload_json->>'summary') AS retained_summary_length
            FROM raw_feed_items r JOIN sources s ON s.id=r.source_id
            LEFT JOIN articles a ON a.id=r.article_id
            WHERE s.code='yahoo' AND r.raw_pub_date >= %s AND r.raw_pub_date < %s
            AND (r.raw_guid=ANY(%s) OR r.raw_link=ANY(%s)) ORDER BY r.id LIMIT 1001''',
            (start,end,ids,sorted(links))).fetchall()
        if len(out['raw_rows'])>1000:
            raise ValueError('Database lookup exceeded 1000 rows; narrow the window')
        article_ids=sorted({r['article_id'] for r in out['raw_rows'] if r['article_id'] is not None})
        out['ticker_associations']=db.execute('''SELECT at.article_id,t.symbol,at.match_type,at.confidence
            FROM article_tickers at JOIN tickers t ON t.id=at.ticker_id
            WHERE at.article_id=ANY(%s) ORDER BY at.article_id,t.symbol LIMIT 10001''',(article_ids,)).fetchall()
        if len(out['ticker_associations'])>10000:
            raise ValueError('Ticker snapshot exceeded bound')
    return json.loads(json.dumps(out,default=lambda value:value.isoformat() if isinstance(value,datetime) else str(value)))


def cross_reference(row,cases,raw_rows):
    data=row['data'];title=label(data.get('title'));day=publication_day(data.get('published_at_label'))
    url=clean_url(data.get('link'));wire=identity(data.get('link'))
    raw=[]
    if row['source']=='yahoo-search':
        raw=[r for r in raw_rows if (r.get('raw_guid')==data['local_id'] or
             (url and clean_url(r.get('raw_link'))==url)) and label(r.get('raw_title'))==title and
             day and publication_day(r.get('raw_pub_date'))==day]
    linked_ids={r['article_id'] for r in raw if r.get('article_id') is not None}
    matches=[];conflicts=[]
    for case in cases:
        case_url=clean_url(case.get('canonical_url'));reference_wire=case_wire(case)
        wire_match=bool(wire and reference_wire==wire)
        url_match=bool(url and url==case_url)
        raw_match=case['article_id'] in linked_ids
        title_match=bool(title and title==label(case.get('title')))
        date_match=bool(day and day==publication_day(case.get('publisher_label_utc')))
        archived_url_wire=identity(case.get('canonical_url'))
        stored_wire=case.get('wire_identity')
        if (url_match or raw_match or (title_match and date_match)) and archived_url_wire and stored_wire and reference_wire is None:
            conflicts.append({'article_id':case['article_id'],'reason':'conflicting_archive_identity'})
            continue
        if title_match and date_match and wire and reference_wire and wire!=reference_wire:
            conflicts.append({'article_id':case['article_id'],'reason':'different_original_wire_identity'})
            continue
        if (wire_match or url_match or raw_match) and not (title_match and date_match):
            conflicts.append({'article_id':case['article_id'],'title_matches':title_match,'publication_date_matches':date_match})
            continue
        if (wire_match or url_match) and linked_ids and not raw_match:
            conflicts.append({'article_id': case['article_id'], 'reason': 'conflicting_stored_article_association'})
            continue
        if not title_match or not date_match:
            continue
        tier='original_wire_url' if wire_match else 'same_archived_url' if url_match else 'stored_yahoo_association' if raw_match else 'title_date_candidate'
        matches.append({'article_id':case['article_id'],'tier':tier,'case_wire_identity':case.get('wire_identity'),
            'reference_wire_identity':reference_wire,
            'canonical_url':case.get('canonical_url'),'reference_evidence':case.get('input_evidence') or case.get('evidence')})
    strong=[m for m in matches if m['tier']!='title_date_candidate']
    selected=strong if strong else matches
    if conflicts:
        status='identity_conflict_review'
    elif len(linked_ids)>1 or len(selected)>1:
        status='ambiguous_review'
    elif len(selected)==1:
        status=selected[0]['tier']
    elif linked_ids:
        status='stored_article_outside_case_archive'
    else:
        status='unmatched_in_retained_cases'
    return dict(status=status,matches=selected,conflicts=conflicts,raw_ids=[r['raw_id'] for r in raw],
        stored_article_ids=sorted(linked_ids),source_wire_identity=wire,
        archived_identity_inherited=False,validated_fund_tickers=[],speed_result=None)


def compare(rows,cases,raw_rows=()):
    groups={}
    for row in rows:
        article_data = {k: v for k, v in row['data'].items() if k not in OBSERVATION_FIELDS}
        key=(row['source'],row['local_id'],json.dumps(article_data,sort_keys=True))
        groups.setdefault(key,[]).append(row)
    result=[]
    for versions in groups.values():
        first=min(versions,key=lambda r:timestamp(r['observed']))
        result.append(dict(source=first['source'],local_id=first['local_id'],data=first['data'],
            observations=len(versions),first_observed_in_window=first['observed'],
            classifications=dict(Counter(r['classification'] for r in versions)),
            scopes=sorted({r['scope'] for r in versions}),
            filters=[json.loads(x) for x in sorted({json.dumps(r['meta'].get('filters'),sort_keys=True) for r in versions if r['meta'].get('filters')})],
            evidence=[dict(store=r['store'],response_id=r['response_id'],observed=r['observed'],
                local_received=r['local_received'],path=r['path'],sha256=r['sha256'],previous_sample=r['previous_sample'],
                observation_metadata={k: v for k, v in r['data'].items() if k in OBSERVATION_FIELDS}) for r in versions],
            **cross_reference(first,cases,raw_rows)))
    return dict(version=1,observations=len(rows),unique_source_ids=len({(r['source'],r['local_id']) for r in rows}),
        distinct_item_versions=len(result),classifications=dict(Counter(r['classification'] for r in rows)),
        reference_status_counts=dict(Counter(r['status'] for r in result)),items=result,
        speed_samples=0,coverage_misses_asserted=0,extra_tickers_asserted=0,
        limitations=[
            'An archived URL or stored Yahoo association cross-references an existing article; it does not independently validate the original wire identity.',
            'Title/date candidates remain review-only; ambiguous or conflicting references are not automatically joined.',
            'Related ticker tags and stored DB associations are not primary-body evidence. No ticker error count is inferred.',
            'Finite selected queries/filters are not a census. Unmatched items are not proven new CEF releases; unobserved cases are not coverage misses.',
            'Compatibility, startup/history and downtime remain excluded from speed samples. This report calculates no route winner or latency.',
            'Independent observation/receiver times are retained; publisher labels and stored DB times are not promoted to network arrival or transaction commit.',
            'Source bodies, notification delivery and physical screen delivery are not measured by this report.'
        ])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--store',type=Path,action='append',required=True)
    p.add_argument('--cases',type=Path,required=True)
    p.add_argument('--since',required=True);p.add_argument('--until',required=True)
    p.add_argument('--with-local-db',action='store_true')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.output.exists():
        raise FileExistsError('Choose a new report filename')
    if len(a.store)>4 or len(set(x.resolve() for x in a.store))!=len(a.store):
        raise ValueError('Choose 1–4 distinct live stores')
    cases,case_hash=read_cases(a.cases)
    rows=[r for store in a.store for r in read_live(store,a.since,a.until)]
    snapshot=lookup_local_yahoo(rows,a.since,a.until) if a.with_local_db else None
    result=compare(rows,cases,snapshot['raw_rows'] if snapshot else [])
    result.update(captured_at=datetime.now(timezone.utc).isoformat(),since=a.since,until=a.until,
        case_archive=str(a.cases.resolve()),case_archive_sha256=case_hash,local_database_snapshot=snapshot,
        external_requests=0,production_mutations=0)
    with a.output.open('x',encoding='utf8') as stream:
        json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')
    print(json.dumps({k:result[k] for k in ['observations','unique_source_ids','reference_status_counts','speed_samples','external_requests','production_mutations']}))


if __name__=='__main__':main()
