import importlib.util
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

SPEC=importlib.util.spec_from_file_location('news_compare',Path(__file__).resolve().parents[1]/'news_compare.py')
M=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(M)
URL='https://finance.yahoo.com/markets/stocks/articles/fund-announces-130000000.html'
WIRE='https://www.globenewswire.com/news-release/2026/11/05/3395763/0/en/example.html'
def case(**changes):
    return dict(article_id=1,title='Fund Announces Distribution',canonical_url=URL+'?.tsrc=rss',
        publisher_label_utc='2026-11-05T13:00:00Z',wire_identity=None,**changes)
def row(**changes):
    data=dict(local_id='yahoo-id',title='Fund Announces Distribution',link=URL,
        published_at_label='2026-11-05T13:00:00+00:00',related_tickers=['FALSE'],**changes)
    return dict(source='yahoo-search',local_id=data['local_id'],data=data,response_id=1,
        observed='2026-11-07T21:00:00Z',local_received='2026-11-07T21:00:00.1Z',
        classification='compatibility_history',scope='GOF',meta={},store='test.sqlite',
        path='response.json',sha256='test-hash',previous_sample=None)

class CompareTests(unittest.TestCase):
    def test_known_tracking_alias_crossreferences_without_inventing_wire(self):
        result=M.cross_reference(row(),[case()],[])
        self.assertEqual(result['status'],'same_archived_url')
        self.assertIsNone(result['source_wire_identity'])
        self.assertFalse(result['archived_identity_inherited'])
        self.assertEqual(result['validated_fund_tickers'],[])

    def test_other_queries_and_other_hosts_are_not_stripped(self):
        for value in [URL+'?id=2',URL+'?.tsrc=rss&id=2',WIRE+'?.tsrc=rss']:
            self.assertEqual(M.clean_url(value),value)
        self.assertIsNone(M.clean_url('https://user:pass@finance.yahoo.com/story'))

    def test_same_headline_on_different_date_does_not_join(self):
        r=row();r['data']['published_at_label']='2026-11-06T13:00:00Z'
        self.assertEqual(M.cross_reference(r,[case()],[])['status'],'identity_conflict_review')

    def test_unknown_date_remains_review_only_even_with_same_url(self):
        r=row();r['data']['published_at_label']=None
        self.assertEqual(M.cross_reference(r,[case()],[])['status'],'identity_conflict_review')

    def test_title_date_without_link_is_only_candidate(self):
        r=row();r['data']['link']=None
        result=M.cross_reference(r,[case()],[])
        self.assertEqual(result['status'],'title_date_candidate');self.assertIsNone(result['speed_result'])

    def test_local_raw_association_matches_existing_article_not_original_wire(self):
        c=case();c['canonical_url']=WIRE;c['wire_identity']='gnw:3395763:en'
        raw=dict(raw_id=9,article_id=1,raw_guid='yahoo-id',raw_link=URL+'?.tsrc=rss',raw_title=c['title'],raw_pub_date=c['publisher_label_utc'])
        result=M.cross_reference(row(),[c],[raw])
        self.assertEqual(result['status'],'stored_yahoo_association');self.assertIsNone(result['source_wire_identity'])
        self.assertEqual(result['stored_article_ids'],[1])

    def test_conflicting_raw_title_cannot_establish_association(self):
        c=case();c['canonical_url']=WIRE
        raw=dict(raw_id=9,article_id=1,raw_guid='yahoo-id',raw_link=URL,raw_title='Unrelated',raw_pub_date=c['publisher_label_utc'])
        result=M.cross_reference(row(),[c],[raw]);self.assertEqual(result['status'],'title_date_candidate')
        self.assertEqual(result['stored_article_ids'],[])

    def test_stored_association_disagreeing_with_archived_url_requires_review(self):
        saved = case()
        raw = dict(raw_id=9, article_id=2, raw_guid='yahoo-id', raw_link=URL,
                   raw_title=saved['title'], raw_pub_date=saved['publisher_label_utc'])
        result = M.cross_reference(row(), [saved], [raw])
        self.assertEqual(result['status'], 'identity_conflict_review')
        self.assertEqual(result['conflicts'][0]['reason'], 'conflicting_stored_article_association')
        self.assertEqual(result['stored_article_ids'], [2])

    def test_ambiguous_stored_ids_are_visible_even_outside_archive(self):
        saved = case()
        raw = dict(raw_id=9, article_id=1, raw_guid='yahoo-id', raw_link=URL,
                   raw_title=saved['title'], raw_pub_date=saved['publisher_label_utc'])
        for cases in ([saved], []):
            result = M.cross_reference(row(), cases, [raw, {**raw, 'raw_id': 10, 'article_id': 2}])
            self.assertEqual(result['status'], 'ambiguous_review')
            self.assertEqual(result['stored_article_ids'], [1, 2])

    def test_duplicate_title_date_candidates_are_ambiguous(self):
        c1=case();c1['canonical_url']=WIRE
        c2=dict(c1,article_id=2)
        self.assertEqual(M.cross_reference(row(),[c1,c2],[])['status'],'ambiguous_review')

    def test_different_wire_ids_cannot_merge_on_headline(self):
        c=case();c['wire_identity']='gnw:1234:en'
        r=row();r['data']['link']=WIRE
        self.assertEqual(M.cross_reference(r,[c],[])['status'],'identity_conflict_review')

    def test_original_wire_still_requires_matching_title_and_date(self):
        c=case();c['wire_identity']='gnw:3395763:en'
        r=row();r['data']['link']=WIRE
        self.assertEqual(M.cross_reference(r,[c],[])['status'],'original_wire_url')
        r['data']['title']='Different release'
        self.assertEqual(M.cross_reference(r,[c],[])['status'],'identity_conflict_review')

    def test_repeated_observations_are_not_unique_releases_or_speed_samples(self):
        r=row();result=M.compare([r,dict(r,response_id=2)],[case()])
        self.assertEqual(result['observations'],2);self.assertEqual(result['unique_source_ids'],1)
        self.assertEqual(result['distinct_item_versions'],1);self.assertEqual(result['speed_samples'],0)
        self.assertEqual(result['coverage_misses_asserted'],0);self.assertEqual(result['extra_tickers_asserted'],0)

    def test_changed_title_keeps_both_versions(self):
        r=row();changed={**r,'data':{**r['data'],'title':'Changed title'}}
        result=M.compare([r,changed],[case()]);self.assertEqual(result['distinct_item_versions'],2)
        self.assertEqual(result['unique_source_ids'],1)

    def test_transport_metadata_changes_do_not_create_new_article_versions(self):
        first = {**row(), 'source': 'godel-browser'}
        second = {**first, 'observed': '2026-11-07T21:00:01Z', 'response_id': 2}
        first['data'] = {**first['data'], 'browser_message_observed_at': first['observed'],
                         'receipt_kind': 'passive_browser_stream_copy', 'message_to_batch_seconds': 2}
        second['data'] = {**second['data'], 'browser_message_observed_at': second['observed'],
                          'receipt_kind': 'passive_browser_worker_copy', 'message_to_batch_seconds': 1,
                          'worker_initial': False, 'worker_delivery_mode': 'EMIT:BUFFERED'}
        report = M.compare([first, second], [case()])
        self.assertEqual(report['distinct_item_versions'], 1)
        item = report['items'][0]
        self.assertEqual(item['observations'], 2)
        self.assertEqual([e['observation_metadata']['receipt_kind'] for e in item['evidence']],
                         ['passive_browser_stream_copy', 'passive_browser_worker_copy'])

    def test_legacy_numeric_prn_requires_matching_original_url(self):
        c=case();c['wire_identity']='302922395'
        self.assertIsNone(M.case_wire(c))
        c['canonical_url']='https://www.prnewswire.com/news-releases/example-302922395.html'
        self.assertEqual(M.case_wire(c),'prn:302922395')
        c['wire_identity']='302922396';self.assertIsNone(M.case_wire(c))

    def test_existing_editorial_is_not_claimed_as_new_release(self):
        raw=dict(raw_id=9,article_id=99,raw_guid='yahoo-id',raw_link=URL,raw_title=row()['data']['title'],raw_pub_date='2026-11-05T13:00:00Z')
        result=M.cross_reference(row(),[],[raw])
        self.assertEqual(result['status'],'stored_article_outside_case_archive')
        self.assertEqual(result['stored_article_ids'],[99])

    def test_conflicting_archive_identity_is_flagged(self):
        c=case();c['canonical_url']=WIRE;c['wire_identity']='gnw:1234:en'
        r=row();r['data']['link']=WIRE
        result=M.cross_reference(r,[c],[])
        self.assertEqual(result['status'],'identity_conflict_review')
        self.assertEqual(result['conflicts'][0]['reason'],'conflicting_archive_identity')

    def test_invalid_window_rejected(self):
        for since,until in [('2026-11-07','2026-11-08'),('2026-11-08T00:00:00Z','2026-11-07T00:00:00Z'),('2026-11-01T00:00:00Z','2026-11-10T00:00:00Z')]:
            with self.assertRaises(ValueError):M.window(since,until)

    def test_wrong_store_identity_read_only_and_missing_store_not_created(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'live.sqlite'
            with self.assertRaises(sqlite3.OperationalError):M.read_live(temp,'2026-11-07T00:00:00Z','2026-11-08T00:00:00Z')
            self.assertFalse(path.exists())
            with closing(sqlite3.connect(path)) as db:db.execute('pragma application_id=12')
            before=path.read_bytes()
            with self.assertRaises(ValueError):M.read_live(temp,'2026-11-07T00:00:00Z','2026-11-08T00:00:00Z')
            self.assertEqual(path.read_bytes(),before)

if __name__=='__main__':unittest.main()
