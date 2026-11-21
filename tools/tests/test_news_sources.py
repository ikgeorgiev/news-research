"""All source intake tests are offline. Originals and failures remain immutable."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('news_sources', Path(__file__).resolve().parents[1] / 'news_sources.py')
N = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(N)
WIRE = 'https://www.globenewswire.com/news-release/2026/11/09/3400000/0/en/Example.html'
TITLE = 'Example Fund Announces Distribution'
TIME = '2026-11-09T14:00:20Z'


def rss(link=WIRE, title=TITLE, published='Mon, 09 Nov 2026 14:00:00 GMT'):
    return f'<rss><channel><title>GNW</title><item><title>{title}</title><link>{link}</link><guid>local-guid</guid><pubDate>{published}</pubDate></item></channel></rss>'.encode()


def yahoo():
    return {'data': {'lightyearList': {'main': {'edges': [{'node': {'asset': {
        '__typename': 'Story', 'id': 'local-yahoo', 'title': TITLE,
        'contentAttributes': {'canonicalUrl': 'https://finance.yahoo.com/news/local.html',
            'clickthroughUrl': WIRE, 'publishTime': '2026-11-09T14:00:00Z', 'displayTime': TIME},
        'finance': {'stockTickers': [{'symbol': 'FALSE'}]}}}}],
        'pageInfo': {'hasNextPage': False, 'endCursor': None}}}}}


class IntakeTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'create_connection', side_effect=AssertionError('No network'))
        self.network.start()
        self.sock = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.sock.start()
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.store = self.directory / 'review.sqlite'
        N.open_store(self.store, create=True).close()
        self.cases = self.directory / 'cases.json'
        self.cases.write_text(json.dumps({'cases': [dict(article_id=1, wire_identity='gnw:3400000:en',
            title=TITLE, date='2026-11-09', canonical_url=WIRE)]}), encoding='utf8')
        self.index = 0

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()
        self.sock.stop()

    def capture(self, body=None, route='guggenheim-gnw', **overrides):
        self.index += 1
        filename = f'body{self.index}.bin'
        body = rss() if body is None else body
        (self.directory / filename).write_bytes(body)
        manifest = dict(version=1, route=route, request_url=N.ROUTES[route]['urls'][0],
            payload_file=filename, payload_sha256=N.digest(body), http_status=200,
            received_at=TIME, receipt_kind='retained_response', classification='historical')
        manifest.update(overrides)
        path = self.directory / f'manifest{self.index}.json'
        path.write_text(json.dumps(manifest), encoding='utf8')
        return path

    def test_original_wire_and_title_match_without_ticker_assignment(self):
        N.ingest(self.store, self.capture())
        r = N.report(self.store, self.cases)
        self.assertEqual(r['records'][0]['match'], 'original_link_and_title_match')
        self.assertEqual(r['records'][0]['source_record']['validated_fund_tickers'], [])
        self.assertEqual(r['routes']['guggenheim-gnw']['eligible_independent_receipts'], 0)

    def test_yahoo_clickthrough_original_is_not_lost_behind_canonical(self):
        N.ingest(self.store, self.capture(json.dumps(yahoo()).encode(), route='yahoo-direct', requested_symbol='EXAMPLE'))
        r = N.report(self.store, self.cases)
        self.assertEqual(r['records'][0]['match'], 'original_link_and_title_match')
        self.assertEqual(r['records'][0]['source_record']['related_tickers'], [{'symbol': 'FALSE'}])
        self.assertEqual(r['routes']['yahoo-direct']['access_status'], 'held_prior_direct_yahoo_429')

    def test_duplicate_import_idempotent(self):
        p = self.capture()
        self.assertTrue(N.ingest(self.store, p)['inserted'])
        self.assertFalse(N.ingest(self.store, p)['inserted'])
        self.assertEqual(N.report(self.store, self.cases)['observations'], 1)

    def test_distinct_receipts_preserve_first_and_count_repeat(self):
        N.ingest(self.store, self.capture(received_at='2026-11-09T14:02:00Z'))
        N.ingest(self.store, self.capture(received_at=TIME))
        r = N.report(self.store, self.cases)['routes']['guggenheim-gnw']
        self.assertEqual(r['observations'], 2)
        self.assertEqual(r['repeated_observations'], 1)
        self.assertEqual(r['first_observations'][0]['observed_at'], '2026-11-09T14:00:20+00:00')

    def test_baseline_cannot_erase_new_first_receipt(self):
        N.ingest(self.store, self.capture(classification='baseline'))
        N.ingest(self.store, self.capture(receipt_kind='network_response', classification='new',
            request_started_at='2026-11-09T14:01:19Z', received_at='2026-11-09T14:01:20Z', clock_id='pc1'))
        r = N.report(self.store, self.cases)['routes']['guggenheim-gnw']
        self.assertEqual(len(r['first_observations']), 2)
        self.assertEqual(r['eligible_independent_receipts'], 1)

    def test_different_clocks_keep_separate_receipts(self):
        for clock in ['pc1', 'pc2']:
            N.ingest(self.store, self.capture(receipt_kind='network_response', classification='new',
                request_started_at='2026-11-09T14:00:19Z', clock_id=clock))
        self.assertEqual(len(N.report(self.store, self.cases)['routes']['guggenheim-gnw']['first_observations']), 2)

    def test_local_rss_guid_is_not_original_wire(self):
        body = rss('https://lasvegas.newsnetmedia.com/story/123/example/')
        N.ingest(self.store, self.capture(body, route='newsnet-gnw'))
        row = N.report(self.store, self.cases)['records'][0]
        self.assertEqual(row['match'], 'title_date_candidate_identity_unverified')
        self.assertIsNone(row['source_record']['wire_identity'])

    def test_xai_body_and_old_wire_tracking_remain_review_evidence(self):
        issuer_url = 'https://xainvestments.com/news-distributions/example/'
        body = rss(issuer_url).replace(b'<rss>', b'<rss xmlns:content="http://purl.org/rss/1.0/modules/content/">')
        body = body.replace(b'</item>', b'<content:encoded><![CDATA[<p>NYSE: MCN</p>'
            b'<a href="https://cts.businesswire.com/ct/CT?newsitemid=20240717309487">Issuer</a>'
            b']]></content:encoded></item>')
        N.ingest(self.store, self.capture(body, route='xai-distributions', classification='baseline'))
        report = N.report(self.store, self.cases)
        row = report['records'][0]['source_record']
        self.assertTrue(row['body_present'])
        self.assertIsNotNone(row['body_sha256'])
        self.assertIsNone(row['wire_identity'])
        self.assertEqual(row['validated_fund_tickers'], [])
        self.assertFalse(row['body_used_for_ticker_validation'])
        self.assertEqual(report['routes']['xai-distributions']['eligible_independent_receipts'], 0)

    def test_xai_only_accepts_its_published_feed_url(self):
        manifest = self.capture(rss(), route='xai-distributions',
                                request_url='https://xainvestments.com/guessed-feed/')
        with self.assertRaises(ValueError):
            N.ingest(self.store, manifest)

    def test_body_tracking_pixel_does_not_validate_wire(self):
        body = rss('https://lasvegas.newsnetmedia.com/story/123/example/').replace(b'</item>',
            b'<description>https://www.globenewswire.com/Tracker?data=3400000</description></item>')
        self.assertIsNone(N.parse_rss(body)[0]['wire_identity'])

    def test_mismatched_original_title_kept_for_review(self):
        N.ingest(self.store, self.capture(rss(title='Wrong title')))
        self.assertEqual(N.report(self.store, self.cases)['records'][0]['match'], 'conflicting_title_for_original_link')

    def test_ambiguous_reviewed_case_does_not_pass(self):
        data = json.loads(self.cases.read_text()); data['cases'].append(dict(data['cases'][0], article_id=2))
        self.cases.write_text(json.dumps(data))
        N.ingest(self.store, self.capture())
        self.assertEqual(N.report(self.store, self.cases)['records'][0]['match'], 'ambiguous_reviewed_cases')

    def test_fc_gnw_slug_is_local_identity_date_only(self):
        b = b'<div class="newsrpm-headlines1"><div class="headline"><div class="title"><a href="/thepilotnews/article/gnwcq-2026-11-9-example">Example Fund Announces Distribution</a></div></div></div>'
        N.ingest(self.store, self.capture(b, route='financialcontent-gnw'))
        row = N.report(self.store, self.cases)['records'][0]
        self.assertEqual(row['source_record']['publication_precision'], 'date_only')
        self.assertEqual(row['match'], 'title_date_candidate_identity_unverified')

    def test_fc_outside_widget_or_foreign_host_not_a_listing(self):
        for b in [b'<a href="/thepilotnews/article/gnwcq-2026-11-9-example">X</a>',
                  b'<div class="newsrpm-headlines1"><div class="headline"><div class="title"><a href="https://evil.test/thepilotnews/article/gnwcq-2026-11-9-example">X</a></div></div></div>']:
            with self.subTest(body=b), self.assertRaises(ValueError):
                N.parse_fc(b)

    def test_godel_manual_time_is_never_network_arrival(self):
        body = json.dumps({'version': 1, 'entries': [{'original_source_link': WIRE,
            'title': TITLE, 'observed_at': TIME, 'displayed_time_local': '2026-11-09 16:00:00'}]}).encode()
        N.ingest(self.store, self.capture(body, route='godel-terminal', receipt_kind='manual_capture', classification='new'))
        row = N.report(self.store, self.cases)['records'][0]
        self.assertEqual(row['match'], 'original_link_and_title_match')
        self.assertFalse(row['independent_receipt_eligible'])
        self.assertIsNone(row['source_record']['publication_label'])

    def test_godel_network_claim_rejected(self):
        with self.assertRaises(ValueError):
            N.load_capture(self.capture(b'{}', route='godel-terminal', receipt_kind='network_response'))

    def test_godel_old_capture_preserves_missing_title(self):
        body = json.dumps({'method': 'Manual anonymous public UI review. Historical rows; no prospective latency observation.',
            'cases': [{'original_source_link': WIRE, 'godel_ui_observed_at_utc': TIME}]}).encode()
        N.ingest(self.store, self.capture(body, route='godel-terminal', receipt_kind='manual_capture'))
        row = N.report(self.store, self.cases)['records'][0]
        self.assertEqual(row['match'], 'original_link_only_title_missing')
        self.assertEqual(row['source_record']['title'], '')

    def test_refusal_remains_after_success(self):
        N.ingest(self.store, self.capture(b'', route='yahoo-direct', http_status=429))
        N.ingest(self.store, self.capture(json.dumps(yahoo()).encode(), route='yahoo-direct'))
        r = N.report(self.store, self.cases)
        self.assertEqual(r['routes']['yahoo-direct']['access_status'], 'held_observed_http_429')
        self.assertEqual(r['captures'], 2)

    def test_non_200_never_parses_challenge_as_news(self):
        for status in [301, 401, 403, 404, 429, 500]:
            with self.subTest(status=status):
                m, rows, issues, _ = N.load_capture(self.capture(b'not rss', http_status=status))
                self.assertEqual(rows, [])
                self.assertTrue(issues)

    def test_malformed_rss_does_not_modify_store(self):
        before = self.store.read_bytes()
        with self.assertRaises(Exception):
            N.ingest(self.store, self.capture(b'<rss>'))
        self.assertEqual(before, self.store.read_bytes())

    def test_xml_entities_rejected(self):
        with self.assertRaises(ValueError):
            N.parse_rss(b'<!DOCTYPE x [<!ENTITY a "x">]><rss/>')

    def test_empty_valid_rss_is_explicit_observation_not_coverage_claim(self):
        N.ingest(self.store, self.capture(b'<rss><channel><title>GNW</title></channel></rss>'))
        r = N.report(self.store, self.cases)
        self.assertEqual((r['captures'], r['observations']), (1, 0))
        self.assertEqual(r['routes']['guggenheim-gnw']['unobserved_in_supplied_material'], [1])

    def test_tampered_payload_rejected(self):
        p = self.capture(); data = json.loads(p.read_text())
        (self.directory / data['payload_file']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'digest'):
            N.ingest(self.store, p)

    def test_payload_traversal_rejected(self):
        with self.assertRaisesRegex(ValueError, 'inside'):
            N.load_capture(self.capture(payload_file='../outside.json'))

    def test_absolute_payload_rejected(self):
        with self.assertRaises(ValueError):
            N.load_capture(self.capture(payload_file=str(self.cases)))

    def test_unsupported_endpoint_rejected(self):
        with self.assertRaisesRegex(ValueError, 'endpoint'):
            N.load_capture(self.capture(request_url='https://www.globenewswire.com/rss/list'))

    def test_missing_timezone_and_bad_network_order_rejected(self):
        for overrides in [dict(received_at='2026-11-09T14:00:00'),
                          dict(receipt_kind='network_response', request_started_at='2026-11-09T14:00:21Z', clock_id='pc'),
                          dict(receipt_kind='network_response', request_started_at='2026-11-09T14:00:19Z')]:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                N.load_capture(self.capture(**overrides))

    def test_unknown_json_schema_rejected(self):
        with self.assertRaises(ValueError):
            N.load_capture(self.capture(b'{}', route='yahoo-direct'))

    def test_partial_yahoo_never_eligible(self):
        data = yahoo(); data['errors'] = [{'message': 'unavailable'}]
        N.ingest(self.store, self.capture(json.dumps(data).encode(), route='yahoo-direct',
            receipt_kind='network_response', classification='new', clock_id='pc',
            request_started_at='2026-11-09T14:00:19Z'))
        self.assertFalse(N.report(self.store, self.cases)['records'][0]['independent_receipt_eligible'])

    def test_cached_yahoo_never_eligible(self):
        N.ingest(self.store, self.capture(json.dumps(yahoo()).encode(), route='yahoo-direct', receipt_kind='cached', classification='new'))
        self.assertFalse(N.report(self.store, self.cases)['records'][0]['independent_receipt_eligible'])

    def test_synthetic_receipt_never_eligible(self):
        N.ingest(self.store, self.capture(receipt_kind='network_response', classification='new',
            clock_id='pc', request_started_at='2026-11-09T14:00:19Z', synthetic_fixture=True))
        row=N.report(self.store, self.cases)['records'][0]
        self.assertFalse(row['independent_receipt_eligible'])
        self.assertTrue(row['synthetic_fixture'])

    def test_conflicting_yahoo_original_links_held(self):
        data=yahoo()
        data['data']['lightyearList']['main']['edges'][0]['node']['asset']['contentAttributes']['canonicalUrl']=WIRE.replace('3400000','3400001')
        N.ingest(self.store,self.capture(json.dumps(data).encode(),route='yahoo-direct'))
        row=N.report(self.store,self.cases)['records'][0]['source_record']
        self.assertIsNone(row['wire_identity'])
        self.assertIn('conflicting_original_urls',row['issues'])

    def test_utf16_xml_not_accepted_as_unchecked_entity_path(self):
        with self.assertRaises((UnicodeError,ValueError)):
            N.parse_rss('<!DOCTYPE x [<!ENTITY a "x">]><rss/>'.encode('utf16'))

    def test_more_than_250_rows_rejected(self):
        node = b'<item>' + rss().split(b'<item>')[1].split(b'</item>')[0] + b'</item>'
        body = b'<rss><channel><title>GNW</title>' + node * 251 + b'</channel></rss>'
        with self.assertRaises(ValueError):
            N.load_capture(self.capture(body))

    def test_existing_arbitrary_database_not_modified(self):
        p = self.directory / 'other.sqlite'
        db = sqlite3.connect(p); db.execute('CREATE TABLE important (x)'); db.commit(); db.close()
        before = p.read_bytes()
        with self.assertRaises(ValueError):
            N.open_store(p)
        self.assertEqual(before, p.read_bytes())
        with self.assertRaises(FileExistsError):
            N.open_store(p, create=True)

    def test_no_original_url_credentials_or_subdomain_spoof(self):
        for u in ['https://www.globenewswire.com.evil.test/news-release/2026/11/09/3400000/0/en/X.html',
                  'https://u:p@www.globenewswire.com/news-release/2026/11/09/3400000/0/en/X.html']:
            self.assertIsNone(N.wire_url(u))


if __name__ == '__main__':
    unittest.main()
