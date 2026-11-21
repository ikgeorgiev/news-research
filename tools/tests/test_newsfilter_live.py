import copy
from datetime import timedelta
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import newsfilter_live as N


def event(key='one',at=None):
    at=at or N.live.utc()
    return dict(version=1,capture_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',endpoint=N.ENDPOINT,status=200,
        receipt_kind='passive_browser_copy',browser_request_observed_at=N.live.stamp(at-timedelta(seconds=1)),
        browser_body_observed_at=N.live.stamp(at),transport='xhr',headers={},
        scope=dict(type='filterArticles',queryString='symbols:OXLC',size=50,**{'from':0}),
        payload={'from':0,'size':50,'total':{'value':1,'relation':'eq'},'articles':[
            dict(id=key,title='Example &amp; distribution',sourceUrl='https://www.businesswire.com/news/home/20261109999999/en/',
                url='https://newsfilter.io/articles/local',publishedAt=N.live.stamp(at-timedelta(seconds=30)),
                symbols=['OXLC','UNVERIFIED'],source={'id':'businesswire','name':'BusinessWire'})]})


class NewsfilterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name);self.db=N.live.open_store(self.path)
        self.no_network=patch('socket.create_connection',side_effect=AssertionError('No source request'));self.no_network.start()
    def tearDown(self):
        self.db.close();self.tmp.cleanup();self.no_network.stop()
    def test_original_identity_only_from_link_never_source_or_tags(self):
        e=event();r=N.parse_newsfilter(e)[0][0]
        self.assertEqual(r['wire_identity'],'bw:20261109999999:en');self.assertEqual(r['validated_fund_tickers'],[])
        self.assertFalse(r['body_available']);self.assertEqual(r['title'],'Example & distribution')
        e['payload']['articles'][0]['sourceUrl']=e['payload']['articles'][0]['url']
        self.assertIsNone(N.parse_newsfilter(e)[0][0]['wire_identity'])
    def test_credentials_and_extra_response_fields_rejected_before_archive(self):
        for change in [lambda e:e.update(token='never-save'),lambda e:e['headers'].update(authorization='never-save'),
                       lambda e:e['scope'].update(token='never-save'),lambda e:e['payload']['articles'][0].update(private='never-save'),
                       lambda e:e['payload']['articles'][0]['source'].update(token='never-save')]:
            e=event();change(e)
            with self.assertRaises(ValueError):N.newsfilter_event(self.db,self.path,e,'s',True)
        self.assertEqual(list((self.path/'responses').iterdir()),[])
    def test_auth_actions_pagination_and_wrong_endpoint_rejected(self):
        for change in [lambda e:e['scope'].update(type='login'),lambda e:e['scope'].update({'from':50}),
                       lambda e:e.update(endpoint='https://api.newsfilter.io/public/actions')]:
            e=event();change(e)
            with self.assertRaises(ValueError):N.checked_event(e)
    def test_future_or_invalid_label_remains_explicit(self):
        e=event();e['payload']['articles'][0]['publishedAt']=N.live.stamp(N.live.utc()+timedelta(days=40))
        self.assertEqual(N.parse_newsfilter(e)[0][0]['publication_label_issue'],'future_label')
        e['payload']['articles'][0]['publishedAt']='yesterday'
        self.assertEqual(N.parse_newsfilter(e)[0][0]['publication_label_issue'],'missing_or_invalid_label')
    def test_future_label_never_newly_observed(self):
        now=N.live.utc();N.live.ingest(self.db,1,'s','q',[],N.live.stamp(now),'session',180)
        e=event(at=now+timedelta(seconds=1));e['payload']['articles'][0]['publishedAt']=N.live.stamp(now+timedelta(seconds=60))
        rows=N.parse_newsfilter(e)[0]
        self.assertEqual(N.live.ingest(self.db,2,'s','q',rows,N.live.stamp(now+timedelta(seconds=1)),'session',180),{'future_publication_label':1})
    def test_retained_capture_is_history_not_speed(self):
        e=event(at=N.live.utc()-timedelta(days=1))
        self.assertEqual(N.newsfilter_event(self.db,self.path,e,'s',True,True)['classifications'],{'compatibility_history':1})
        with self.assertRaises(ValueError):N.newsfilter_event(self.db,self.path,event(),'s',False,True)
    def test_stale_live_or_reversed_clocks_rejected(self):
        for e in [event(at=N.live.utc()-timedelta(minutes=5)),event()]:
            if e['browser_body_observed_at']>N.live.stamp(N.live.utc()-timedelta(minutes=1)):
                e['browser_request_observed_at']=N.live.stamp(N.live.utc()+timedelta(minutes=1))
            with self.assertRaises(ValueError):N.newsfilter_event(self.db,self.path,e,'s')
    def test_refusal_hold_survives_new_session(self):
        e=event();e['status']=403;e.pop('payload')
        self.assertEqual(N.newsfilter_event(self.db,self.path,e,'a',True)['status'],'native_error')
        self.db.close();self.db=N.live.open_store(self.path)
        self.assertEqual(N.newsfilter_event(self.db,self.path,event(),'b',True)['status'],'held')
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)
    def test_429_never_retried_and_headers_retained(self):
        e=event();e['status']=429;e['headers']={'retry-after':'3600'};e.pop('payload')
        N.newsfilter_event(self.db,self.path,e,'s',True)
        meta=json.loads(self.db.execute('SELECT meta FROM responses').fetchone()[0]);self.assertEqual(meta['headers']['retry-after'],'3600')
        self.assertEqual(N.newsfilter_event(self.db,self.path,event(),'s',True)['status'],'held')
    def test_schema_failure_is_sticky_not_empty_success(self):
        e=event();e['payload']['from']=50
        self.assertEqual(N.newsfilter_event(self.db,self.path,e,'s',True)['status'],'native_error')
        self.assertEqual(N.live.get(self.db,N.SOURCE+':hold')['reason'],'news_schema_failure')
    def test_capture_replay_and_duplicate_rows(self):
        e=event();e['payload']['articles']*=2
        r=N.newsfilter_event(self.db,self.path,e,'s',True)
        self.assertEqual(r['classifications'],{'compatibility_history':1,'duplicate_in_response':1})
        self.assertEqual(N.newsfilter_event(self.db,self.path,e,'s',True)['status'],'duplicate_capture')
    def test_navigation_and_filter_changes_reset_baseline(self):
        now=N.live.utc();e=event(at=now);N.newsfilter_event(self.db,self.path,e,'s')
        e=event('two',now+timedelta(seconds=1));e['capture_id']='bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
        self.assertEqual(N.newsfilter_event(self.db,self.path,e,'s')['classifications'],{'baseline':1})
        self.assertIsNone(self.db.execute('SELECT previous_sample FROM observations ORDER BY response_id DESC LIMIT 1').fetchone()[0])
    def test_scope_changes_when_query_or_size_changes(self):
        e=event();a=N.scope_of(e)[1];e['scope']['queryString']='symbols:GOF';self.assertNotEqual(a,N.scope_of(e)[1])
    def test_actual_captured_shape_with_timezone_without_colon(self):
        e=event();e['payload']['articles'][0]['publishedAt']='2026-11-05T05:00:00-0800'
        self.assertEqual(N.parse_newsfilter(e)[0][0]['published_at_label'],'2026-11-05T13:00:00+00:00')
    def test_native_completion_kept_separate_from_later_body_copy(self):
        e=event();e['browser_response_completed_at']=N.live.stamp(N.live.dt(e['browser_body_observed_at'])-timedelta(milliseconds=500))
        N.newsfilter_event(self.db,self.path,e,'s',True)
        meta=json.loads(self.db.execute('SELECT meta FROM responses').fetchone()[0])
        self.assertEqual(meta['browser_response_completed_at'],e['browser_response_completed_at'])
        self.assertEqual(meta['observed_at'],e['browser_body_observed_at'])
        self.assertLess(meta['browser_response_completed_at'],meta['observed_at'])
    def test_impossible_native_completion_rejected_before_archive(self):
        for offset in [-2,1]:
            e=event();e['browser_response_completed_at']=N.live.stamp(N.live.dt(e['browser_body_observed_at'])+timedelta(seconds=offset))
            with self.assertRaises(ValueError):N.newsfilter_event(self.db,self.path,e,'s',True)
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],0)


if __name__=='__main__':unittest.main()
