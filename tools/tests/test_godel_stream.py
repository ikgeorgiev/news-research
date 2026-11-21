import copy
from datetime import timedelta
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('news_live_stream_test',Path(__file__).resolve().parents[1]/'news_live.py')
N=importlib.util.module_from_spec(spec);spec.loader.exec_module(N)


def event():
    now=N.utc()
    return dict(version=2,endpoint='wss://terminal-api.godelterminal.com/events',receipt_kind='passive_browser_stream_batch',
        capture_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',browser_body_observed_at=N.stamp(now),dropped_frames=0,
        records=[dict(socket_id='bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',subscription='sub-4',destination='/user/queue/filterResponse/1174',
            message_id='native',scope={'sources':['wire'],'languages':['en'],'symbols':['GOF']},
            subscription_observed_at=N.stamp(now-timedelta(minutes=10)),browser_message_observed_at=N.stamp(now-timedelta(seconds=2)),
            browser_body_observed_at=N.stamp(now-timedelta(seconds=1)),native_frame_bytes=500,
            article=dict(id='one',title='Example distribution',publicationTime=str(int((now-timedelta(seconds=5)).timestamp()*1000)),
                         receivedTime=N.stamp(now-timedelta(seconds=4)),securityIds=['GOF',''],originalUrl=None,providerName='wire'))])


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name);self.db=N.open_store(self.path)
    def tearDown(self):
        self.db.close();self.tmp.cleanup()
    def test_independent_messages_no_absence_and_no_ticker_validation(self):
        e=event();second=copy.deepcopy(e['records'][0]);second['article']['id']='two';e['records'].append(second)
        result=N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(result['classifications'],{'stream_baseline':1,'stream_newly_observed':1})
        rows=self.db.execute('SELECT * FROM observations').fetchall()
        self.assertTrue(all(x['previous_sample'] is None for x in rows))
        self.assertTrue(all('"validated_fund_tickers": []' in x['data'] for x in rows))
        self.assertEqual(N.godel_event(self.db,self.path,e,'session')['status'],'duplicate_capture')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM observations').fetchone()[0],2)
    def test_source_holds_still_block_stream(self):
        for status in [401,403,404,429]:
            with self.db:N.put(self.db,'godel-browser:hold',{'status':status,'reason':'native_refusal_manual_review'})
            self.assertEqual(N.godel_event(self.db,self.path,event(),'session')['status'],'held')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM responses').fetchone()[0],0)
    def test_no_credentials_or_non_news_fields_admitted(self):
        for mutate in [lambda e:e.update(endpoint=e['endpoint']+'?jwt=private'),
                       lambda e:e['records'][0].update(destination='/user/queue/notifications'),
                       lambda e:e['records'][0]['article'].update(password='private')]:
            e=event();mutate(e)
            with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM responses').fetchone()[0],0)
    def test_entire_batch_validated_before_writing(self):
        e=event();bad=copy.deepcopy(e['records'][0]);bad['scope']['symbols']=None;e['records'].append(bad)
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM responses').fetchone()[0],0)
    def test_reload_socket_and_scope_changes_start_baselines(self):
        e=event();N.godel_event(self.db,self.path,e,'session')
        changed=copy.deepcopy(e);changed['capture_id']='cccccccc-cccc-cccc-cccc-cccccccccccc';changed['records'][0]['article']['id']='two'
        self.assertEqual(N.godel_event(self.db,self.path,changed,'session')['classifications'],{'stream_baseline':1})
        changed=event();changed['records'][0]['scope']['symbols']=['PDI'];changed['records'][0]['article']['id']='three'
        self.assertEqual(N.godel_event(self.db,self.path,changed,'session')['classifications'],{'stream_baseline':1})
    def test_status_includes_stream_but_never_proves_continuous_coverage(self):
        e=event();N.godel_event(self.db,self.path,e,'session')
        status=N.bridge_coverage.status(self.db,'godel-browser','session',capture_id=e['capture_id'])
        self.assertEqual(status['native_status'],'fresh');self.assertFalse(status['continuous_coverage_verified'])
        self.assertEqual(status['fresh_filter_eligible_count'],1)
    def test_stale_or_overlarge_batch_rejected(self):
        e=event();e['browser_body_observed_at']=N.stamp(N.utc()-timedelta(minutes=3))
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        e=event();e['records']*=101
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
    def test_stream_drop_diagnostic_retained_without_fake_news(self):
        e=event();e.update(records=[],dropped_frames=3)
        result=N.godel_event(self.db,self.path,e,'session');self.assertEqual(result['articles'],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM observations').fetchone()[0],0)
        self.assertIn('"dropped_frames": 3',self.db.execute('SELECT meta FROM responses').fetchone()[0])

    def test_ordered_delayed_copy_is_review_only_and_next_fresh_copy_is_baseline(self):
        first=event();N.godel_event(self.db,self.path,first,'session')
        prior=tuple(self.db.execute('SELECT * FROM streams').fetchone())
        late=event();late['records'][0]['article']['id']='delayed'
        late['records'][0]['browser_message_observed_at']=N.stamp(N.utc()-timedelta(seconds=57.405))
        late['records'][0]['browser_body_observed_at']=late['records'][0]['browser_message_observed_at']
        result=N.godel_event(self.db,self.path,late,'session')
        self.assertEqual(result['status'],'ok');self.assertTrue(result['review_only']);self.assertEqual(result['articles'],0)
        self.assertEqual(result['delayed_records'],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM observations').fetchone()[0],1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM items WHERE local_id='delayed'").fetchone()[0],0)
        self.assertEqual(tuple(self.db.execute('SELECT * FROM streams').fetchone()),prior)
        self.assertEqual(self.db.execute("SELECT count(*) FROM responses WHERE error='delayed_browser_copy_review_only'").fetchone()[0],1)
        self.assertIsNone(N.active_source_hold(self.db,'godel-browser'))
        fresh=event();fresh['records'][0]['article']['id']='fresh'
        self.assertEqual(N.godel_event(self.db,self.path,fresh,'session')['classifications'],{'stream_baseline':1})
        fresh=event();fresh['records'][0]['article']['id']='later'
        self.assertEqual(N.godel_event(self.db,self.path,fresh,'session')['classifications'],{'stream_newly_observed':1})
        self.assertTrue(all(r[0] is None for r in self.db.execute('SELECT previous_sample FROM observations')))

    def test_delay_exception_never_accepts_unordered_clocks_or_bad_schema(self):
        for change in (
            lambda r:r.update(browser_body_observed_at=N.stamp(N.utc()-timedelta(seconds=10))),
            lambda r:r['article'].update(title=''),
            lambda r:r.update(scope={'sources':None,'languages':[]})):
            e=event();change(e['records'][0])
            with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(self.db.execute('SELECT count(*) FROM responses').fetchone()[0],0)

    def test_worker_delivery_keeps_copy_clock_and_initial_history_separate(self):
        e=event();e.update(version=3,endpoint='https://app.godelterminal.com/',receipt_kind='passive_browser_worker_batch')
        r=e['records'][0];r['worker_id']=r.pop('socket_id');r['selected_article_bytes']=r.pop('native_frame_bytes');r.pop('message_id');r.update(worker_initial=True,worker_delivery_mode='EMIT:BUFFERED')
        result=N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(result['classifications'],{'stream_baseline':1})
        e=copy.deepcopy(e);e['records'][0]['article']['id']='cached-initial'
        self.assertEqual(N.godel_event(self.db,self.path,e,'session')['classifications'],{'stream_baseline':1})
        e=copy.deepcopy(e);e['records'][0]['article']['id']='new';e['records'][0]['worker_initial']=False
        self.assertEqual(N.godel_event(self.db,self.path,e,'session')['classifications'],{'stream_newly_observed':1})
        rows=self.db.execute('SELECT * FROM observations').fetchall()
        self.assertTrue(all(x['previous_sample'] is None for x in rows))
        self.assertIn('passive_browser_worker_copy',rows[-1]['data'])
        status=N.bridge_coverage.status(self.db,'godel-browser','session',capture_id=e['capture_id'])
        self.assertEqual(status['fresh_filter_eligible_count'],1)

    def test_worker_rejects_private_fields_and_unknown_delivery_mode(self):
        e=event();e.update(version=3,endpoint='https://app.godelterminal.com/',receipt_kind='passive_browser_worker_batch')
        r=e['records'][0];r['worker_id']=r.pop('socket_id');r['selected_article_bytes']=r.pop('native_frame_bytes');r.pop('message_id');r.update(worker_initial=False,worker_delivery_mode='DEBUG:METRICS:RESPONSE')
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        r['worker_delivery_mode']='EMIT:BUFFERED';r['token']='private'
        with self.assertRaises(ValueError):N.godel_event(self.db,self.path,e,'session')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM responses').fetchone()[0],0)

if __name__=='__main__':unittest.main()
