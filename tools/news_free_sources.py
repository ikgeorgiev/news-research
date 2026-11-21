"""Read-only report for free discovery candidates; never makes HTTP requests."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys
import time


def report(path, limit=100):
    path=Path(path).resolve(strict=True)
    if not 1 <= limit <= 500:
        raise ValueError('Limit must be between 1 and 500')
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5) as db:
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        deadline=time.monotonic()+5
        db.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
        controls=[dict(row) for row in db.execute('SELECT * FROM control LIMIT 20')]
        hosts=[dict(row) for row in db.execute('SELECT * FROM host_control LIMIT 20')]
        responses=[dict(row) for row in db.execute('SELECT id,route,started,received,status,error,item_count,session FROM response ORDER BY id DESC LIMIT ?',(min(limit,100),))]
        counts=[dict(row) for row in db.execute('SELECT route,kind,count(*) count FROM observation GROUP BY route,kind')]
        rows=[dict(row) for row in db.execute('SELECT o.*,r.received,r.started,r.session FROM observation o JOIN response r ON r.id=o.response_id ORDER BY o.id DESC LIMIT ?',(limit,))]
        for row in rows:row['data']=json.loads(row['data'])
        return dict(review_only=True,scope='Finite selected feeds, not full CEF coverage',
                    controls=controls,host_controls=hosts,counts=counts,recent_responses=responses,latest=rows,
                    warning='Metadata identity matches are not primary-body ticker validation or latency measurements.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    view=commands.add_parser('report');view.add_argument('--store',required=True);view.add_argument('--limit',type=int,default=100)
    check=commands.add_parser('check-config');check.add_argument('path')
    args=parser.parse_args()
    if args.command=='report':result=report(args.store,args.limit)
    else:
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
        from app.free_news import load_routes
        routes=load_routes(args.path)
        result={'valid':True,'route_count':len(routes),'source_requests':0,'custom_feed_count':len(routes)-2}
    print(json.dumps(result,indent=2,ensure_ascii=False))


if __name__=='__main__':main()
