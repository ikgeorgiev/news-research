from datetime import timedelta
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import news_browser_report as report
import newsfilter_live as nf
from test_newsfilter_live import event


class BrowserReportTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.store=Path(self.temp.name)
        self.db=nf.live.open_store(self.store);self.now=nf.live.dt('2026-11-09T08:10:00Z')
    def tearDown(self):self.db.close();self.temp.cleanup()
    def save(self,key,offset,session='s',compatibility=False):
        at=self.now+timedelta(seconds=offset)
        with patch.object(nf.live,'utc',return_value=at):
            return nf.newsfilter_event(self.db,self.store,event(key,at),session,compatibility)

    def test_gap_restart_and_edges_are_censored_and_overlapping_store_not_doubled(self):
        self.save('one',0);self.save('two',60);self.save('three',241);self.save('four',242,'new')
        self.save('five',302,'new')
        result=report.summarize([self.store,self.store],'2026-11-09',self.now+timedelta(seconds=362))
        x=result['sources']['newsfilter-browser']
        self.assertEqual(x['successful_native_copies'],5)
        self.assertEqual(x['sampled_interval_seconds'],120)
        self.assertEqual(x['pre_first_sample_seconds'],600)
        self.assertEqual(x['after_last_sample_seconds'],60)
        self.assertEqual(x['unobserved_or_censored_seconds'],842)
        self.assertEqual(result['simultaneously_sampled_seconds'],0)
        self.assertIsNone(x['within_60_fraction']);self.assertFalse(x['complete_cef_coverage_proven'])

    def test_compatibility_history_is_not_a_live_sampling_interval(self):
        self.save('one',0,compatibility=True);self.save('two',60,compatibility=True)
        r=report.summarize([self.store],'2026-11-09',self.now+timedelta(seconds=90))['sources']['newsfilter-browser']
        self.assertEqual(r['successful_native_copies'],0);self.assertEqual(r['sampled_interval_seconds'],0)
        self.assertEqual(len(r['excluded_responses']),2)

    def test_report_does_not_mutate_the_store_or_claim_zero_releases(self):
        self.save('one',0)
        before=(self.store/'live.sqlite').read_bytes()
        r=report.summarize([self.store],'2026-11-09',self.now)
        self.assertEqual(before,(self.store/'live.sqlite').read_bytes())
        self.assertIsNone(r['sources']['newsfilter-browser']['validated_release_count'])
        self.assertEqual(r['source_requests'],0)

    def test_interval_union_and_missing_store(self):
        start=self.now
        self.assertEqual(report.seconds([(start,start+timedelta(seconds=60)),(start+timedelta(seconds=30),start+timedelta(seconds=90))]),90)
        r=report.summarize([self.store/'absent'],'2026-11-09',self.now)
        self.assertEqual(r['stores'][0]['status'],'not_initialized')
        self.assertEqual(r['sources']['godel-browser']['unobserved_or_censored_seconds'],600)

    def test_transport_recovery_diagnostics_are_read_only_and_do_not_create_coverage(self):
        event={'id':'gap-1','kind':'capture_recovered','at':self.now.isoformat(),'capture_id':None,'gap_ms':360000}
        with self.db:self.db.execute('INSERT INTO bridge_diagnostics VALUES(?,?,?,?)',('s','gap-1',self.now.isoformat(),json.dumps(event)))
        before=(self.store/'live.sqlite').read_bytes()
        r=report.summarize([self.store],'2026-11-09',self.now+timedelta(seconds=60))
        self.assertEqual(r['stores'][0]['transport_diagnostics'][0]['data'],event)
        self.assertEqual(r['sources']['godel-browser']['successful_native_copies'],0)
        self.assertEqual(r['sources']['godel-browser']['sampled_interval_seconds'],0)
        self.assertEqual(before,(self.store/'live.sqlite').read_bytes())
        with self.db:self.db.execute('DROP TABLE bridge_diagnostics')
        self.assertEqual(report.summarize([self.store],'2026-11-09',self.now)['stores'][0]['transport_diagnostics'],[])

    def test_push_copies_are_counted_without_fabricating_sampling_intervals(self):
        from test_godel_stream import event as stream_event
        e=stream_event();e['browser_body_observed_at']=self.now.isoformat()
        row=e['records'][0];row.update(subscription_observed_at=(self.now-timedelta(minutes=1)).isoformat(),browser_message_observed_at=self.now.isoformat(),browser_body_observed_at=self.now.isoformat())
        with patch.object(nf.live,'utc',return_value=self.now):
            nf.live.godel_event(self.db,self.store,e,'s')
        r=report.summarize([self.store],'2026-11-09',self.now+timedelta(seconds=90))['sources']['godel-browser']
        self.assertEqual(r['successful_native_copies'],1)
        self.assertEqual(r['transport_counts'],{'passive_browser_stream_copy':1})
        self.assertEqual(r['local_batch_counts'],{'passive_browser_stream_batch':1})
        self.assertEqual(r['sampled_interval_seconds'],0)
        self.assertEqual(r['scopes'][0]['filter_eligibility']['eligible_count'],1)

    def test_compatibility_push_copy_is_not_live_evidence(self):
        from test_godel_stream import event as stream_event
        e=stream_event();e['browser_body_observed_at']=self.now.isoformat()
        row=e['records'][0];row.update(subscription_observed_at=(self.now-timedelta(minutes=1)).isoformat(),browser_message_observed_at=self.now.isoformat(),browser_body_observed_at=self.now.isoformat())
        with patch.object(nf.live,'utc',return_value=self.now):
            nf.live.godel_event(self.db,self.store,e,'fixture',True)
        r=report.summarize([self.store],'2026-11-09',self.now+timedelta(seconds=90))['sources']['godel-browser']
        self.assertEqual(r['successful_native_copies'],0)
        self.assertEqual(r['sampled_interval_seconds'],0)
        self.assertEqual(r['local_batch_counts'],{})

    def test_late_copies_stay_out_of_speed_and_route_availability_counts(self):
        from test_godel_stream import event as stream_event
        e=stream_event();e['browser_body_observed_at']=self.now.isoformat()
        row=e['records'][0];row.update(subscription_observed_at=(self.now-timedelta(minutes=2)).isoformat(),
            browser_message_observed_at=(self.now-timedelta(seconds=57.405)).isoformat(),
            browser_body_observed_at=(self.now-timedelta(seconds=57.405)).isoformat())
        with patch.object(nf.live,'utc',return_value=self.now):
            result=nf.live.godel_event(self.db,self.store,e,'s')
        self.assertTrue(result['review_only'])
        before=(self.store/'live.sqlite').read_bytes()
        r=report.summarize([self.store],'2026-11-09',self.now+timedelta(seconds=60))['sources']['godel-browser']
        self.assertEqual(r['successful_native_copies'],0);self.assertEqual(r['sampled_interval_seconds'],0)
        self.assertIsNone(r['within_60_fraction']);self.assertIsNone(r['validated_release_count'])
        self.assertEqual(r['excluded_responses'][0]['reason'],'delayed_browser_copy_review_only')
        self.assertEqual(before,(self.store/'live.sqlite').read_bytes())


if __name__=='__main__':unittest.main()
