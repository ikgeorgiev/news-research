"""Day-session gates, finite-page continuity and current-universe diagnostics."""
from datetime import datetime, timedelta, timezone
import json
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import news_live as live
import newsfilter_live as nf
import bridge_coverage as coverage
from test_news_live import godel
from test_newsfilter_live import event


class DayPolicyTests(unittest.TestCase):
    def test_each_authorized_day_ends_at_22_utc(self):
        for day in range(9,14):
            now=datetime(2026,11,day,8,tzinfo=timezone.utc)
            end,mode=live.bridge_deadline(now,until_close=True)
            self.assertEqual(end,now.replace(hour=22));self.assertEqual(mode,'until_close')
            self.assertEqual((end-now).total_seconds(),14*3600)

    def test_preopen_midnight_weekend_and_other_dates_rejected(self):
        for label in ('2026-11-09T07:59:59Z','2026-11-09T22:00:00Z','2026-11-08T12:00:00Z','2026-11-14T08:00:00Z','2026-11-16T08:00:00Z'):
            with self.subTest(label=label),self.assertRaises(ValueError):
                live.bridge_deadline(live.dt(label),until_close=True)

    def test_day_mode_cannot_use_compatibility_escape(self):
        with self.assertRaises(ValueError):live.bridge_deadline(live.dt('2026-11-09T09:00:00Z'),compatibility=True,until_close=True)

    def test_short_mode_and_cutoff_remain_bounded(self):
        now=live.dt('2026-11-09T21:55:00Z')
        self.assertEqual(live.bridge_deadline(now)[0],now.replace(hour=22,minute=0))
        self.assertEqual(live.bridge_deadline(now,compatibility=True)[0],now+timedelta(minutes=20))
        for value in (0,61):
            with self.assertRaises(ValueError):live.bridge_deadline(now,minutes=value)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)
        self.db=live.open_store(self.path);self.now=live.utc()
    def tearDown(self):self.db.close();self.temp.cleanup()

    def test_universe_is_current_367_with_fingerprint(self):
        u=coverage.universe();self.assertEqual(u['count'],367);self.assertEqual(len(u['csv_sha256']),64)

    def test_explicit_all_funds_and_single_fund_are_distinct(self):
        u=coverage.universe()
        for symbols,count in ((['OXLC'],1),(u['symbols'],367),([],367),(['GOOG'],0)):
            s=coverage.filter_scope('godel-browser',{'symbols':symbols},u)
            self.assertEqual(s['eligible_count'],count)
        unknown=coverage.filter_scope('godel-browser',{'symbols':['OXLC US']},u)
        self.assertEqual(unknown['mode'],'unverified_expression')

    def test_missing_symbol_filter_does_not_imply_all_symbols(self):
        u=coverage.universe()
        result=coverage.filter_scope('godel-browser',{'sources':['native'],'languages':['en']},u)
        self.assertEqual(result['mode'],'symbols_omitted_unverified')
        self.assertEqual(result['eligible_count'],0)
        self.assertEqual(result['restrictions']['sources'],['native'])
        self.assertEqual(coverage.filter_scope('godel-browser',{'symbols':None},u)['eligible_count'],0)

    def test_newsfilter_full_query_under_selected_field_cap(self):
        u=coverage.universe();query='symbols:('+ ' OR '.join('"'+x+'"' for x in u['symbols'])+')'
        self.assertLess(len(query),8192)
        self.assertEqual(coverage.filter_scope(nf.SOURCE,{'queryString':query},u)['eligible_count'],367)
        for query in ('NOT source.id:"sec-api"','*:*'):
            self.assertEqual(coverage.filter_scope(nf.SOURCE,{'queryString':query},u)['eligible_count'],367)
        for query in ('NOT symbols:"OXLC"','symbols:"OXLC" AND title:"never"','symbols:"OXLC" OR NOT symbols:"GOF"'):
            self.assertEqual(coverage.filter_scope(nf.SOURCE,{'queryString':query},u)['mode'],'unverified_expression')

    def test_native_oxlc_text_query_is_only_one_fund(self):
        scope=coverage.filter_scope(nf.SOURCE,{'queryString':'title:"oxlc" OR description:"oxlc" OR symbols:"oxlc"'},coverage.universe())
        self.assertEqual(scope['eligible_symbols'],['OXLC'])

    def test_full_page_turnover_loses_fine_previous_sample(self):
        e=event('first',self.now);e['scope']['size']=1;e['payload']['size']=1
        nf.newsfilter_event(self.db,self.path,e,'s')
        e=event('second',self.now+timedelta(seconds=30));e['scope']['size']=1;e['payload']['size']=1
        with patch.object(live,'utc',return_value=self.now+timedelta(seconds=30)):
            r=nf.newsfilter_event(self.db,self.path,e,'s')
        self.assertEqual(r['assessment']['continuity']['reason'],'full_page_without_overlap')
        self.assertEqual(r['classifications'],{'baseline':1})
        self.assertIsNone(self.db.execute('SELECT previous_sample FROM observations ORDER BY response_id DESC LIMIT 1').fetchone()[0])

    def test_three_minute_gap_and_restart_are_explicit(self):
        nf.newsfilter_event(self.db,self.path,event('one',self.now),'s')
        later=self.now+timedelta(seconds=181)
        with patch.object(live,'utc',return_value=later):r=nf.newsfilter_event(self.db,self.path,event('two',later),'s')
        self.assertEqual(r['assessment']['continuity']['reason'],'native_update_gap')
        with patch.object(live,'utc',return_value=later+timedelta(seconds=1)):
            r=nf.newsfilter_event(self.db,self.path,event('three',later+timedelta(seconds=1)),'new_receiver')
        self.assertEqual(r['assessment']['continuity']['reason'],'new_capture_or_receiver')

    def test_repeated_native_updates_can_be_fresh_without_proving_coverage(self):
        nf.newsfilter_event(self.db,self.path,event('one',self.now),'s')
        later=self.now+timedelta(seconds=60)
        with patch.object(live,'utc',return_value=later):r=nf.newsfilter_event(self.db,self.path,event('two',later),'s')
        self.assertEqual(r['assessment']['continuity']['reason'],'repeated_native_sample')
        self.assertEqual(r['classifications'],{'newly_observed':1})
        current=coverage.status(self.db,nf.SOURCE,'s',later)
        self.assertEqual(current['native_status'],'fresh');self.assertFalse(current['continuous_coverage_verified'])
        stale=coverage.status(self.db,nf.SOURCE,'s',later+timedelta(seconds=181))
        self.assertEqual(stale['native_status'],'stale');self.assertEqual(stale['fresh_filter_eligible_count'],0)
        self.assertEqual(coverage.status(self.db,nf.SOURCE,'other',later)['native_status'],'no_native_updates')
        self.assertEqual(coverage.status(self.db,nf.SOURCE,'s',later,capture_id='new')['native_status'],'no_native_updates')

    def test_godel_accepts_367_filter_symbols_without_validating_article_tags(self):
        e=godel();e['scope']['symbols']=coverage.universe()['symbols']
        rows,_=live.parse_godel(e);self.assertEqual(rows[0]['validated_fund_tickers'],[])
        r=live.godel_event(self.db,self.path,e,'s')
        self.assertEqual(r['assessment']['filter_scope']['eligible_count'],367)

    def test_godel_schema_failure_is_durable_but_bad_clock_is_not_source_refusal(self):
        e=godel();e['payload']={}
        self.assertEqual(live.godel_event(self.db,self.path,e,'s')['status'],'native_error')
        self.assertEqual(live.get(self.db,'godel-browser:hold')['reason'],'godel_schema_failure')
        self.assertEqual(live.godel_event(self.db,self.path,godel(),'s')['status'],'held')

    def test_explicit_local_bug_review_preserves_old_hold_and_never_releases_later_failure(self):
        e=godel();e['scope']=None
        failed=live.godel_event(self.db,self.path,e,'s')
        hold=live.get(self.db,'godel-browser:hold')
        sha=self.db.execute('SELECT sha256 FROM responses WHERE id=?',(failed['response_id'],)).fetchone()[0]
        review={'resolution':'local_request_scope_copy_repaired','hold_sha256':hashlib.sha256(json.dumps(hold,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'response_sha256':sha}
        with self.db:live.put(self.db,'godel-browser:hold_review',review)
        self.assertIsNone(live.active_source_hold(self.db,'godel-browser'))
        self.assertEqual(live.get(self.db,'godel-browser:hold'),hold)
        self.assertEqual(live.godel_event(self.db,self.path,godel('new'),'new-baseline')['classifications'],{'baseline':1})
        later=godel('new-failure');later['payload']={}
        self.assertEqual(live.godel_event(self.db,self.path,later,'s')['status'],'native_error')
        self.assertIsNotNone(live.active_source_hold(self.db,'godel-browser'))

    def test_review_cannot_release_access_refusal_or_other_source(self):
        for source in ('godel-browser','newsfilter-browser'):
            for status in (401,403,404,429):
                hold={'status':status,'reason':'godel_schema_failure','response_id':1}
                for resolution in ('local_request_scope_copy_repaired','local_related_symbol_bound_repaired'):
                    review={'resolution':resolution,'hold_sha256':hashlib.sha256(json.dumps(hold,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'response_sha256':'irrelevant'}
                    with self.db:
                        live.put(self.db,source+':hold',hold);live.put(self.db,source+':hold_review',review)
                    self.assertEqual(live.active_source_hold(self.db,source),hold)


if __name__=='__main__':unittest.main()
