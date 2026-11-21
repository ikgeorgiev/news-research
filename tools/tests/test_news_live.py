"""Offline failure, continuity and transport checks for the comparison connectors."""
import asyncio
import copy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import httpx

SPEC=importlib.util.spec_from_file_location('news_live',Path(__file__).resolve().parents[1]/'news_live.py')
N=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(N)
WIRE='https://www.businesswire.com/news/home/20261109999999/en/Example'


def yahoo(key='one',at=None):
    return {'news':[{'uuid':key,'title':'Example fund distribution','link':WIRE,
                     'providerPublishTime':int((at or N.utc()).timestamp()),'publisher':'Business Wire','relatedTickers':['FALSE']}]}


def godel(key='one'):
    now=N.utc()
    return dict(version=1,capture_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',endpoint=N.GODEL,status=200,
        receipt_kind='passive_browser_copy',browser_request_observed_at=N.stamp(now-timedelta(seconds=1)),
        browser_body_observed_at=N.stamp(now),
        scope=dict(symbols=['GOF'],sources=['native-source'],languages=['en'],pageable=dict(size=20,afterCursor=None,beforeCursor=None),dateTimeRange=dict(startTime='0',endTime=str(int(now.timestamp()*1000))),legal=False),
        payload={'content':[dict(id=key,title='Example fund distribution',publicationTime=str(int(now.timestamp()*1000)),
            receivedTime=N.stamp(now-timedelta(seconds=30)),originalUrl=WIRE,securityIds=['FALSE'],providerName='Business Wire')]})


class LiveTests(unittest.TestCase):
    def setUp(self):
        original_connect=socket.socket.connect
        def local_only(sock,address):
            # Windows asyncio creates a loopback socketpair even with MockTransport.
            if not isinstance(address,tuple) or address[0] not in ('127.0.0.1','::1'):
                raise AssertionError('No external network')
            return original_connect(sock,address)
        self.network=patch.object(socket.socket,'connect',local_only)
        self.network.start()
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)
        self.db=N.open_store(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()
        self.network.stop()

    def test_ticker_tags_never_validate_or_create_body(self):
        x=N.parse_yahoo(yahoo())[0]
        self.assertEqual(x['related_tickers'],['FALSE'])
        self.assertEqual(x['validated_fund_tickers'],[])
        self.assertFalse(x['body_available'])

    def test_wire_identity_only_original_url(self):
        x=yahoo();x['news'][0]['link']='https://finance.yahoo.com/news/local.html'
        self.assertIsNone(N.parse_yahoo(x)[0]['wire_identity'])
        self.assertEqual(N.parse_yahoo(yahoo())[0]['wire_identity'],'bw:20261109999999:en')

    def test_no_local_id_promoted_to_wire_id(self):
        x=godel();x['payload']['content'][0]['originalUrl']=None
        self.assertIsNone(N.parse_godel(x)[0][0]['wire_identity'])

    def test_bad_yahoo_schema_not_empty(self):
        for x in [{},{'news':None},{'news':'html'},{'news':[{}]}, {'news': [{}]*251}]:
            with self.assertRaises(ValueError):N.parse_yahoo(x)
        self.assertEqual(N.parse_yahoo({'news':[]}),[])

    def test_url_credentials_rejected(self):
        self.assertIsNone(N.safe_url('https://u:p@www.businesswire.com/news/home/1/en/'))
        self.assertIsNone(N.safe_url('https://www.businesswire.com:123/news/home/1/en/'))

    def test_vendor_clock_not_browser_receipt(self):
        event=godel()
        rows,_=N.parse_godel(event)
        self.assertNotEqual(rows[0]['vendor_received_at'],event['browser_body_observed_at'])

    def test_page_history_rejected(self):
        for key in ['beforeCursor','afterCursor']:
            event=godel();event['scope']['pageable'][key]='old'
            with self.assertRaises(ValueError):N.parse_godel(event)

    def test_native_history_archived_without_hold_or_live_cursor(self):
        first=godel('front');N.godel_event(self.db,self.path,first,'s')
        before=[tuple(x) for x in self.db.execute('SELECT * FROM streams')]
        for key in ['beforeCursor','afterCursor']:
            event=godel('old-'+key);event['scope']['pageable'][key]='1793014800269'
            result=N.godel_event(self.db,self.path,event,'s')
            self.assertEqual(result['status'],'excluded')
            self.assertEqual(result['articles'],0)
            self.assertIsNone(N.active_source_hold(self.db,'godel-browser'))
        self.assertEqual(before,[tuple(x) for x in self.db.execute('SELECT * FROM streams')])
        self.assertEqual(self.db.execute('SELECT count(*) FROM observations').fetchone()[0],1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM responses WHERE error='native_history_excluded'").fetchone()[0],2)
        self.assertEqual(N.godel_event(self.db,self.path,godel('live-after-history'),'s')['status'],'ok')

    def test_historical_range_excluded_but_bad_cursor_schema_stays_held(self):
        event=godel('old');event['scope']['dateTimeRange']['endTime']='1700000000000'
        result=N.godel_event(self.db,self.path,event,'s')
        self.assertEqual(result['reason'],'historical_range')
        self.assertIsNone(N.active_source_hold(self.db,'godel-browser'))
        bad=godel();bad['scope']['pageable']['afterCursor']={'unexpected':'object'}
        self.assertEqual(N.godel_event(self.db,self.path,bad,'s')['status'],'native_error')
        self.assertIsNotNone(N.active_source_hold(self.db,'godel-browser'))

    def test_history_never_masks_source_refusal_or_bad_article(self):
        event=godel();event['scope']['pageable']['afterCursor']='old';event['status']=403
        self.assertEqual(N.godel_event(self.db,self.path,event,'s')['status'],'native_error')
        self.assertEqual(N.active_source_hold(self.db,'godel-browser')['status'],403)
        self.assertEqual(N.godel_event(self.db,self.path,godel(),'s')['status'],'held')

    def test_scope_tracks_filters_but_not_moving_end(self):
        event=godel();a=N.parse_godel(event)[1]
        event['scope']['dateTimeRange']['endTime']=str(int(N.utc().timestamp()*1000)+10000)
        self.assertEqual(a,N.parse_godel(event)[1])
        event['scope']['symbols']=['UTG']
        self.assertNotEqual(a,N.parse_godel(event)[1])

    def test_native_bad_status_or_time_rejected(self):
        for code in [403,429,302]:
            x=godel();x['status']=code
            with self.assertRaises(ValueError):N.parse_godel(x)
        x=godel();x['browser_body_observed_at']=N.stamp(N.utc()-timedelta(minutes=3))
        with self.assertRaises(ValueError):N.parse_godel(x)

    def test_observed_symbol_omission_has_distinct_scope_and_no_coverage_claim(self):
        event=godel();del event['scope']['symbols']
        with self.assertRaises(ValueError):N.parse_godel(event)
        event['request_scope_diagnostic']={'status':'selected','fields':{'symbols':'missing'}}
        rows,omitted=N.parse_godel(event)
        self.assertEqual(rows[0]['validated_fund_tickers'],[])
        saved=N.godel_event(self.db,self.path,event,'omitted-baseline')
        self.assertEqual(saved['classifications'],{'baseline':1})
        self.assertEqual(saved['assessment']['filter_scope']['eligible_count'],0)
        self.assertEqual(saved['assessment']['filter_scope']['mode'],'symbols_omitted_unverified')
        event['scope']['symbols']=[]
        self.assertNotEqual(omitted,N.parse_godel(event)[1])
        for bad in (None,'GOF',[42]):
            event['scope']['symbols']=bad
            with self.assertRaises(ValueError):N.parse_godel(event)

    def test_lost_original_scope_cannot_be_reconstructed_from_diagnostic(self):
        event=godel();event['scope']=None
        event['request_scope_diagnostic']={'status':'unavailable','fields':{'symbols':'missing','sources':'array','languages':'array'}}
        with self.assertRaises(ValueError):N.parse_godel(event)

    def test_godel_market_roundup_retains_bounded_related_tags_without_validation(self):
        event=godel();tags=['T'+str(i) for i in range(367)]
        event['payload']['content'][0]['securityIds']=tags
        rows,_=N.parse_godel(event)
        self.assertEqual(set(rows[0]['related_tickers']),set(tags))
        self.assertEqual(rows[0]['validated_fund_tickers'],[])
        event['payload']['content'][0]['securityIds']=['T'+str(i) for i in range(1001)]
        with self.assertRaises(ValueError):N.parse_godel(event)
        y=yahoo();y['news'][0]['relatedTickers']=tags
        with self.assertRaises(ValueError):N.parse_yahoo(y)

    def test_blank_godel_security_id_is_missing_metadata_not_entire_response_failure(self):
        event=godel();event['payload']['content'][0]['securityIds']=['','  ','GOF']
        rows,_=N.parse_godel(event)
        self.assertEqual(rows[0]['related_tickers'],['GOF'])
        self.assertEqual(rows[0]['empty_security_id_count'],2)
        self.assertEqual(rows[0]['validated_fund_tickers'],[])
        self.assertEqual(event['payload']['content'][0]['securityIds'],['','  ','GOF'])
        event['payload']['content'][0]['securityIds']=[None]
        with self.assertRaises(ValueError):N.parse_godel(event)
        event['payload']['content'][0]['securityIds']=['']*1001
        with self.assertRaises(ValueError):N.parse_godel(event)

    def test_future_or_old_browser_clock_rejected_live(self):
        x=godel();x['browser_body_observed_at']=N.stamp(N.utc()+timedelta(minutes=10));x['browser_request_observed_at']=x['browser_body_observed_at']
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,x,'s')

    def test_baseline_new_retained_and_restart(self):
        now=N.utc();a=N.parse_yahoo(yahoo(at=now));b=N.parse_yahoo(yahoo('two',now))
        self.assertEqual(N.ingest(self.db,1,'y','q',a,N.stamp(now),'s',900),{'baseline':1})
        counts=N.ingest(self.db,2,'y','q',a+b,N.stamp(now+timedelta(seconds=600)),'s',900)
        self.assertEqual(counts,{'retained':1,'newly_observed':1})
        self.assertEqual(N.ingest(self.db,3,'y','q',a+b,N.stamp(now+timedelta(seconds=610)),'new',900),{'baseline':2})
        self.assertIsNone(self.db.execute('SELECT previous_sample FROM observations WHERE response_id=3 LIMIT 1').fetchone()[0])

    def test_gap_and_day_change_reset_baseline(self):
        now=N.dt('2026-11-09T21:00:00Z');rows=N.parse_yahoo(yahoo(at=now))
        N.ingest(self.db,1,'y','q',rows,N.stamp(now),'s',900)
        self.assertEqual(N.ingest(self.db,2,'y','q',rows,N.stamp(now+timedelta(seconds=901)),'s',900),{'baseline':1})
        self.assertEqual(N.ingest(self.db,3,'y','q',rows,'2026-11-10T08:00:00Z','s',900),{'baseline':1})

    def test_old_publish_date_not_speed_sample(self):
        now=N.utc()
        N.ingest(self.db,1,'y','q',[],N.stamp(now),'s',900)
        old=N.parse_yahoo(yahoo(at=now-timedelta(days=1)))
        self.assertEqual(N.ingest(self.db,2,'y','q',old,N.stamp(now+timedelta(seconds=20)),'s',900),{'history_or_unknown_date':1})

    def test_duplicate_rows_and_changed_id_preserve_first(self):
        now=N.utc();rows=N.parse_yahoo(yahoo())
        counts=N.ingest(self.db,1,'y','q',rows+rows,N.stamp(now),'s',900)
        self.assertEqual(counts,{'baseline':1,'duplicate_in_response':1})
        changed=copy.deepcopy(rows);changed[0]['title']='Corrected title'
        self.assertEqual(N.ingest(self.db,2,'y','q',changed,N.stamp(now+timedelta(seconds=1)),'s',900),{'updated_same_id':1})
        self.assertEqual(json.loads(self.db.execute('SELECT data FROM items').fetchone()[0])['title'],rows[0]['title'])

    def test_out_of_order_does_not_move_cursor(self):
        now=N.utc();N.ingest(self.db,1,'y','q',[],N.stamp(now),'s',900)
        with self.assertRaises(ValueError):N.ingest(self.db,2,'y','q',[],N.stamp(now-timedelta(seconds=1)),'s',900)
        self.assertEqual(self.db.execute('SELECT last_observed FROM streams').fetchone()[0],N.stamp(now))

    def test_native_refusal_is_saved_and_sticky(self):
        x=godel();x['status']=403
        self.assertEqual(N.godel_event(self.db,self.path,x,'s')['status'],'native_error')
        self.assertEqual(N.godel_event(self.db,self.path,godel('two'),'s')['status'],'held')
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)

    def test_duplicate_capture_not_second_response(self):
        x=godel();N.godel_event(self.db,self.path,x,'s',True)
        self.assertEqual(N.godel_event(self.db,self.path,x,'s',True)['status'],'duplicate_capture')
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)

    def test_browser_restart_new_baseline(self):
        x=godel();N.godel_event(self.db,self.path,x,'s')
        x=godel('two');x['capture_id']='bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
        self.assertEqual(N.godel_event(self.db,self.path,x,'s')['classifications'],{'baseline':1})

    def test_missing_capture_generation_not_live(self):
        x=godel();x.pop('capture_id')
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,x,'s')

    def test_retained_import_never_live(self):
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,godel(),'s',retained=True)

    def test_cache_ttl_honored_without_shortening_for_age(self):
        self.assertEqual(N.cache_delay({'cache-control':'public, max-age=1200','age':'1199'}),1200)
        self.assertEqual(N.cache_delay({'cache-control':'max-age=30'}),600)

    def test_retry_after_date_and_seconds(self):
        now=N.dt('2026-11-09T08:00:00Z')
        self.assertEqual(N.retry_delay('7200',now),7200)
        self.assertEqual(N.retry_delay('Mon, 09 Nov 2026 10:00:00 GMT',now),7200)
        self.assertEqual(N.retry_delay('bad',now),3600)

    def test_explicit_week_window(self):
        for value in ['2026-11-07T12:00:00Z','2026-11-09T07:59:59Z','2026-11-13T22:00:00Z','2026-11-16T08:00:00Z']:
            with self.assertRaises(ValueError):N.day_close(N.dt(value))
        self.assertEqual(N.stamp(N.day_close(N.dt('2026-11-09T08:00:00Z'))),'2026-11-09T22:00:00+00:00')

    def test_read_only_report_store_cannot_write(self):
        self.db.commit()
        readonly=N.open_store(self.path,readonly=True)
        try:
            self.assertEqual(N.report(readonly)['responses'],[])
            with self.assertRaises(sqlite3.OperationalError):readonly.execute("INSERT INTO control VALUES('no','null')")
        finally:readonly.close()

    def test_report_separates_dates_and_unique_ids(self):
        rows=N.parse_yahoo(yahoo())
        N.ingest(self.db,1,'y','q',rows,'2026-11-09T08:00:00Z','s',900)
        N.ingest(self.db,2,'y','q',rows,'2026-11-10T08:00:00Z','s',900)
        report=N.report(self.db)
        self.assertEqual(len(report['daily_classifications']),2)
        self.assertEqual(report['unique_source_ids'],[{'source':'y','n':1}])

    def fetcher(self,status=200,data=None,headers=None):
        async def fetch(query):
            return json.dumps(data if data is not None else yahoo()).encode(),dict(started_at=N.stamp(),observed_at=N.stamp(),status=status,headers=headers or {})
        return fetch

    def test_sticky_yahoo_refusal_survives_reopen(self):
        result=N.yahoo_once(self.db,self.path,'GOF','s',fetcher=self.fetcher(403))
        self.assertEqual(result['status'],'held')
        self.db.close();self.db=N.open_store(self.path)
        self.assertEqual(N.yahoo_once(self.db,self.path,'OXLC','s',fetcher=lambda q:self.fail('must not fetch'))['status'],'held')

    def test_yahoo_host_backoff_applies_other_symbol(self):
        N.yahoo_once(self.db,self.path,'GOF','s',fetcher=self.fetcher(429,headers={'retry-after':'3600'}))
        self.assertEqual(N.yahoo_once(self.db,self.path,'OXLC','s',fetcher=lambda q:self.fail('must not fetch'))['status'],'backoff')

    def test_yahoo_no_immediate_retry_after_parse_error(self):
        self.assertEqual(N.yahoo_once(self.db,self.path,'GOF','s',fetcher=self.fetcher(data={'wrong':[]}))['status'],'held')
        self.assertIsNotNone(N.get(self.db,'yahoo:hold'))

    def test_yahoo_no_immediate_retry_after_network_error(self):
        async def broken(q):raise httpx.ReadTimeout('failure')
        self.assertEqual(N.yahoo_once(self.db,self.path,'GOF','s',fetcher=broken)['status'],'error')
        self.assertEqual(N.yahoo_once(self.db,self.path,'PDI','s',fetcher=lambda q:self.fail('must not fetch'))['status'],'backoff')

    def test_receipt_after_cutoff_saved_but_not_ingested(self):
        async def late(q):
            return json.dumps(yahoo()).encode(),dict(started_at='2026-11-13T21:59:40Z',observed_at='2026-11-13T22:00:01Z',status=200,headers={})
        result=N.yahoo_once(self.db,self.path,'GOF','s',fetcher=late)
        self.assertEqual(result['status'],'excluded')
        self.assertEqual(self.db.execute('SELECT count(*) FROM observations').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)

    def test_transport_exact_route_no_cookies_redirects_retry(self):
        calls=[]
        def mock(request):
            calls.append(request)
            self.assertEqual(str(request.url).split('?')[0],N.YAHOO)
            self.assertNotIn('cookie',request.headers)
            self.assertNotIn('authorization',request.headers)
            self.assertEqual(request.url.params['newsCount'],'20')
            return httpx.Response(302,headers={'Location':'https://other.invalid/'})
        body,meta=asyncio.run(N.fetch_yahoo('GOF',httpx.MockTransport(mock)))
        self.assertEqual(meta['status'],302)
        self.assertEqual(len(calls),1)

    def test_transport_body_cap(self):
        with self.assertRaises(ValueError):
            asyncio.run(N.fetch_yahoo('GOF',httpx.MockTransport(lambda r:httpx.Response(200,content=b'x'*(N.MAX_BODY+1)))))

    def test_transport_deadline_covers_slow_stream(self):
        class Slow(httpx.AsyncByteStream):
            async def __aiter__(self):
                await asyncio.sleep(.1)
                yield b'{}'
        real=asyncio.timeout
        with patch.object(N.asyncio,'timeout',lambda _:real(.01)):
            with self.assertRaises(TimeoutError):
                asyncio.run(N.fetch_yahoo('GOF',httpx.MockTransport(lambda r:httpx.Response(200,stream=Slow()))))


if __name__=='__main__':unittest.main()
