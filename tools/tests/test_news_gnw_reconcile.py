"""Offline decision and read-only store regressions; no source access."""
import base64
import copy
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('gnw_reconcile', ROOT/'tools/news_gnw_reconcile.py')
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)

TITLE = 'Example Closed-End Fund Declares a Distribution'
STAMP = '2026-11-11T16:37:05+00:00'
URL = 'https://markets.chroniclejournal.com/thepilotnews/article/gnwcq-2026-11-11-example-fund'
ORIGINAL = 'https://www.globenewswire.com/news-release/2026/11/11/1234567/0/en/example-fund.html'
QUOTE = ('NEW YORK, Nov. 11, 2026 (GLOBE NEWSWIRE) -- Example Closed-End Fund '
         '(NYSE: BCV) declared a distribution of $0.50 per share payable in December.')


def fixture(link=False):
    # Synthetic copy for mechanical positive tests. This is NOT a live FC copy
    # of the saved Bancroft release and must never count as observed coverage.
    metadata = dict(**{'@type': 'NewsArticle'}, mainEntityOfPage={'@id': URL},
                    headline=TITLE, datePublished=STAMP)
    html = ('<html><script type="application/ld+json">' + json.dumps(metadata) + '</script>'
            '<div itemprop="articleBody"><p>' + QUOTE + '</p>'
            '<table><tr><th>Distribution</th><td>$0.50</td></tr></table>'
            + ('<p><a href="' + ORIGINAL + '">Original release</a></p>' if link else '')
            + '</div></html>').encode()
    review = g.parse_body(html, article_url=URL, expected_title=TITLE)
    original = dict(wire_identity='gnw:1234567:en', title=TITLE, original_url=ORIGINAL,
                    published=STAMP, article_id=77, primary_body_fund_tickers=['BCV'],
                    body_exchange_evidence=QUOTE, body_text=review['body_text'],
                    blocks=copy.deepcopy(review['reader']['blocks']), archive_sha256='a'*64)
    return html, review, original


class OfflineTestCase(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.network.start()
        self.addCleanup(self.network.stop)


class ReconciliationTests(OfflineTestCase):
    def test_literal_original_and_full_evidence_corroborate_without_publication(self):
        _, review, original = fixture(True)
        result = g.reconcile_body(review, [original])
        self.assertEqual(result['status'], 'literal_original_link_corroborated')
        self.assertTrue(result['original_identity_validated'])
        self.assertEqual(result['validated_fund_tickers'], ['BCV'])
        self.assertFalse(result['production_eligible'])
        self.assertFalse(result['latency_eligible'])
        self.assertFalse(result['duplicate_alert_permitted'])

    def test_full_body_without_literal_link_still_needs_identity_review(self):
        _, review, original = fixture()
        result = g.reconcile_body(review, [original])
        self.assertEqual(result['status'], 'exact_body_association_requires_identity_review')
        self.assertFalse(result['original_identity_validated'])
        self.assertEqual(result['validated_fund_tickers'], [])

    def test_title_match_alone_is_not_identity(self):
        _, review, original = fixture(True)
        original['body_text'] = original['body_text'].replace('$0.50', '$0.60')
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'body_or_table_conflict')

    def test_same_body_different_second_label_is_review(self):
        _, review, original = fixture(True)
        original['published'] = '2026-11-11T16:37:06+00:00'
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'publication_label_conflict')

    def test_feed_minute_label_cannot_replace_body_second_label(self):
        _, review, original = fixture(True)
        original['published'] = '2026-11-11T16:37:00+00:00'
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'publication_label_conflict')

    def test_exact_text_different_table_structure_is_conflict(self):
        _, review, original = fixture(True)
        original['blocks'][1]['rows'][0][1]['colspan'] = 2
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'body_or_table_conflict')

    def test_two_original_ids_with_identical_text_remain_ambiguous(self):
        _, review, original = fixture(True)
        another = dict(original, wire_identity='gnw:7654321:en', article_id=78)
        self.assertEqual(g.reconcile_body(review, [original, another])['status'], 'ambiguous_original_identity')

    def test_conflicting_literal_link_cannot_be_overridden_by_title(self):
        _, review, original = fixture(True)
        review['original_link_candidates'][0]['candidate_wire_identity'] = 'gnw:7654321:en'
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'conflicting_literal_original_links')

    def test_unrelated_body_is_unresolved_not_declared_non_cef(self):
        _, review, original = fixture()
        original['title'] += ' corrected'
        result = g.reconcile_body(review, [original])
        self.assertEqual(result['status'], 'no_reviewed_original_title_match')
        self.assertNotIn('non_cef', result)

    def test_body_digest_corruption_fails(self):
        _, review, original = fixture()
        review['body_text'] += ' changed'
        with self.assertRaisesRegex(ValueError, 'digest'):
            g.reconcile_body(review, [original])

    def test_reader_text_corruption_fails(self):
        _, review, original = fixture()
        review['reader']['blocks'][0]['text'] += ' changed'
        with self.assertRaisesRegex(ValueError, 'structure'):
            g.reconcile_body(review, [original])

    def test_stored_parser_rejection_is_never_promoted(self):
        _, review, original = fixture(True)
        review['body_parse_valid'] = False
        self.assertEqual(g.reconcile_body(review, [original])['status'], 'stored_parser_rejection')


class StoredEvidenceTests(OfflineTestCase):
    def setUp(self):
        super().setUp()
        self.temp = tempfile.TemporaryDirectory(prefix='gnw-reconciliation-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.html, self.review, self.original = fixture(True)
        self.review.update(body_parse_valid=True, appearance_classification='recent_label_identity_pending')
        self.now = datetime(2026, 11, 11, 16, 38, tzinfo=timezone.utc).timestamp()
        self.dbpath = self.root/'fc.sqlite'
        with closing(sqlite3.connect(self.dbpath)) as db, db:
            db.executescript('''
            CREATE TABLE fc_candidate(url,title,route,observed,last_absent,reason,status,attempts,body_received,error,request_pending);
            CREATE TABLE fc_seen(route,url,title,first_seen,initial);
            CREATE TABLE fc_review_body(url,received,html_sha256,body_sha256,html,review_json);
            CREATE TABLE fc_route(name,disabled);
            CREATE TABLE fc_control(key,value);
            CREATE TABLE fc_listing_diagnostic(route,started_at,finished_at,http_status);
            ''')
            db.execute('INSERT INTO fc_candidate VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (URL,TITLE,g.ROUTE,self.now,None,'fund_title:BCV','identity_review',1,self.now+1,None,0))
            db.execute('INSERT INTO fc_seen VALUES(?,?,?,?,?)', (g.ROUTE,URL,TITLE,self.now,0))
            db.execute('INSERT INTO fc_review_body VALUES(?,?,?,?,?,?)',
                       (URL,self.now+1,g.digest(self.html),self.review['body_sha256'],self.html,json.dumps(self.review)))
            db.execute('INSERT INTO fc_route VALUES(?,?)',('financialcontent-bw-index','404'))
            db.execute('INSERT INTO fc_control VALUES(?,?)',('disabled','403'))
        self.since, self.until = '2026-11-11T08:00:00Z', '2026-11-11T17:00:00Z'
        self.archives = self.root/'archives'
        self.archives.mkdir()
        self.doc = dict(self.review['reader'], article_id=77, wire_key='gnw:1234567:en',
                        original_url=ORIGINAL, source_url=ORIGINAL, body_kind='publisher_body',
                        identity_validation='canonical_title_date', body_published_at=STAMP)
        archive = self.archives/'77.json'
        archive.write_text(json.dumps(self.doc),encoding='utf-8')
        self.case = dict(classification='genuine_new_release', original_identity_title_date_consistent=True,
                         article_id=77, publisher_body_second_label=STAMP, original_url=ORIGINAL,
                         original_wire_identity='gnw:1234567:en',date='2026-11-11',title=TITLE,
                         archive={'sha256':g.digest(archive.read_bytes())},
                         primary_body_fund_tickers=['BCV'],body_exchange_evidence=QUOTE)
        self.cases = self.root/'cases.json'
        self.cases.write_text(json.dumps({'cases':[self.case]}),encoding='utf-8')
        self.registry = self.root/'registry.csv'
        self.registry.write_text('ticker,active\nBCV,TRUE\nGAMI,FALSE\n',encoding='utf-8')

    def snapshot(self):
        return g.snapshot_store(self.dbpath,self.since,self.until)

    def test_read_only_snapshot_preserves_every_database_byte_and_hold(self):
        before=self.dbpath.read_bytes()
        snapshot=self.snapshot()
        self.assertEqual(self.dbpath.read_bytes(),before)
        self.assertEqual(snapshot['controls'],[{'key':'disabled','value':'403'}])
        self.assertEqual(snapshot['routes'],[{'name':'financialcontent-bw-index','disabled':'404'}])
        self.assertEqual(snapshot['mode'],'ro')
        self.assertTrue(snapshot['query_only'])

    def test_missing_store_is_not_created(self):
        path=self.root/'missing.sqlite'
        with self.assertRaises(sqlite3.OperationalError):
            g.snapshot_store(path,self.since,self.until)
        self.assertFalse(path.exists())

    def test_row_limit_fails_instead_of_claiming_complete_coverage(self):
        with patch.object(g,'MAX_BODIES',0):
            with self.assertRaisesRegex(ValueError,'Row limit'):
                self.snapshot()

    def test_source_body_corruption_fails_before_output(self):
        with closing(sqlite3.connect(self.dbpath)) as db, db:
            db.execute("UPDATE fc_review_body SET html_sha256='bad'")
        with self.assertRaisesRegex(ValueError,'HTML digest'):
            self.snapshot()

    def test_frozen_payload_corruption_fails(self):
        snapshot=self.snapshot()
        snapshot['bodies'][0]['html_base64']=base64.b64encode(b'changed').decode()
        with self.assertRaisesRegex(ValueError,'HTML digest'):
            g.reconcile_snapshot(snapshot,[self.original])

    def test_held_candidate_retains_hold_even_if_parser_would_succeed(self):
        snapshot=self.snapshot()
        snapshot['bodies'][0]['status']='invalid'
        decision=g.reconcile_snapshot(snapshot,[self.original])['body_decisions'][0]
        self.assertEqual(decision['status'],'retained_candidate_hold_or_rejection')
        self.assertEqual(decision['stored_status'],'invalid')

    def test_censored_absence_and_history_are_not_latency(self):
        snapshot=self.snapshot()
        old=json.loads(snapshot['bodies'][0]['review_json'])
        old['appearance_classification']='historical_appearance'
        snapshot['bodies'][0]['review_json']=json.dumps(old)
        result=g.reconcile_snapshot(snapshot,[self.original])
        self.assertIsNone(result['within60_fraction'])
        self.assertIsNone(result['coverage_miss_count'])
        decision=result['body_decisions'][0]
        self.assertTrue(decision['historical_or_startup'])
        self.assertIsNone(decision['observation']['last_absent'])
        self.assertFalse(decision['latency_eligible'])

    def test_baseline_is_distinct_from_a_new_body_candidate(self):
        snapshot=self.snapshot()
        snapshot['sightings'][0]['initial']=1
        snapshot['candidates']=[]
        report=g.reconcile_snapshot(snapshot,[self.original])
        match=report['reviewed_discovery_checks'][0]['matches'][0]
        self.assertTrue(match['baseline'])
        self.assertFalse(match['selected'])
        self.assertFalse(report['reviewed_discovery_checks'][0]['original_identity_validated_by_listing'])

    def test_only_title_on_wrong_date_does_not_associate(self):
        snapshot=self.snapshot()
        snapshot['sightings'][0]['url']=URL.replace('2026-11-11','2026-11-10')
        result=g.reconcile_snapshot(snapshot,[self.original])
        self.assertEqual(result['counts']['original_titles_observed'],0)
        self.assertIsNone(result['coverage_miss_count'])

    def test_reviewed_native_archive_and_fund_evidence_validate(self):
        values,_=g.read_originals([self.cases],self.archives,{'BCV'})
        self.assertEqual(len(values),1)
        self.assertEqual(values[0]['primary_body_fund_tickers'],['BCV'])

    def test_original_archive_digest_change_fails(self):
        (self.archives/'77.json').write_text('{}',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'digest mismatch'):
            g.read_originals([self.cases],self.archives,{'BCV'})

    def test_quoted_body_evidence_cannot_be_invented(self):
        self.case['body_exchange_evidence']=QUOTE.replace('$0.50','$0.60')
        with self.assertRaisesRegex(ValueError,'fund evidence'):
            g._original(self.case,self.archives,{'BCV'})

    def test_query_or_provider_ticker_cannot_replace_primary_evidence(self):
        self.case['primary_body_fund_tickers']=['GAMI']
        with self.assertRaisesRegex(ValueError,'fund evidence'):
            g._original(self.case,self.archives,{'BCV','GAMI'})

    def test_inactive_or_absent_registry_ticker_cannot_pass(self):
        with self.assertRaisesRegex(ValueError,'fund evidence'):
            g._original(self.case,self.archives,set())

    def test_spoofed_original_hostname_cannot_pass(self):
        self.case['original_url']=ORIGINAL.replace('globenewswire.com','globenewswire.com.example.org')
        with self.assertRaisesRegex(ValueError,'URL and wire'):
            g._original(self.case,self.archives,{'BCV'})

    def test_overlapping_case_files_deduplicate_by_original_identity(self):
        originals,_=g.read_originals([self.cases,self.cases],self.archives,{'BCV'})
        self.assertEqual(len(originals),1)

    def test_duplicate_snapshot_identity_fails(self):
        snapshot=self.snapshot()
        snapshot['sightings']*=2
        with self.assertRaisesRegex(ValueError,'Duplicate snapshot'):
            g.reconcile_snapshot(snapshot,[self.original])

    def test_post_cutoff_or_nonfinite_evidence_is_rejected(self):
        for value in [float('nan'),float('inf'),True,datetime(2026,11,11,17,tzinfo=timezone.utc).timestamp()]:
            snapshot=self.snapshot()
            snapshot['sightings'][0]['first_seen']=value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError,'observation window'):
                g.reconcile_snapshot(snapshot,[self.original])

    def test_unrelated_route_in_saved_snapshot_is_rejected(self):
        snapshot=self.snapshot()
        snapshot['sightings'][0]['route']='yahoo'
        with self.assertRaisesRegex(ValueError,'Unexpected route'):
            g.reconcile_snapshot(snapshot,[self.original])

    def test_selection_replay_uses_fresh_names_without_assigning_funds(self):
        self.registry.write_text('ticker,active,fund_name,sponsor\nBCV,TRUE,Example Closed-End Fund,Example\n',encoding='utf-8')
        context,_=g.read_registry(self.registry)
        report=g.reconcile_snapshot(self.snapshot(),[self.original],context)
        replay=report['reviewed_discovery_checks'][0]['current_title_selection_replay']
        self.assertIsNotNone(replay['reason'])
        self.assertFalse(report['reviewed_discovery_checks'][0]['original_identity_validated_by_listing'])

    def test_duplicate_registry_symbol_is_not_silently_collapsed(self):
        self.registry.write_text('ticker,active\nBCV,TRUE\nBCV,TRUE\n',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'Duplicate active'):
            g.read_registry(self.registry)

    def test_seven_day_bound_and_order(self):
        for end in ['2026-11-10T08:00:00Z','2026-11-19T08:00:00Z','2026-11-11T08:00:00Z']:
            with self.subTest(end=end), self.assertRaises(ValueError):
                g.window(self.since,end)

    def test_cli_is_offline_and_frozen_replay_agrees(self):
        output=self.root/'result.json'
        common=['--cases',str(self.cases),'--archives',str(self.archives),'--registry',str(self.registry)]
        original_bytes=self.dbpath.read_bytes()
        first=g.main(['--store',str(self.dbpath),'--since',self.since,'--until',self.until,
                      '--output',str(output),*common])
        second=g.main(['--snapshot',str(output),'--output',str(self.root/'replayed.json'),*common])
        self.assertEqual(first['report'],second['report'])
        self.assertEqual(self.dbpath.read_bytes(),original_bytes)
        self.assertEqual(first['report']['production_articles_written'],0)
        with self.assertRaisesRegex(ValueError,'overwrite'):
            g.main(['--snapshot',str(output),'--output',str(output),*common])


if __name__=='__main__':
    unittest.main()
