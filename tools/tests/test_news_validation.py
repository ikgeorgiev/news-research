import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.news_validation import archive_summary, delivery_report, main, read_json, reader_report, reviewed_case_wire, wire_identity


START = '2026-11-09T15:00:00Z'
END = '2026-11-09T15:20:00Z'
URL = 'https://www.businesswire.com/news/home/20261109123456/en/Fund-Distribution'


def delivery_row(aid, *, classification='automatic_new', waiting=False, duration=75):
    return dict(article_id=aid, provider='FinancialContent', title='Fund distribution', url=URL,
                publication_label='2026-11-09T14:59:00Z', server_first_seen_label=START,
                server_alert_label=START, classification=classification, received_at=START,
                request_id=aid, awaiting_user=waiting,
                viewport_at=START if duration is not None else None, receipt_to_viewport_ms=duration)


def delivery_document(rows=None):
    rows = rows if rows is not None else [delivery_row(1)]
    events = []
    for row in rows:
        events.append(dict(type='response_received', at=START, elapsed_ms=0,
                           request_id=row['request_id'], detail='queued' if row['awaiting_user'] else
                           'automatic' if row['classification'] == 'automatic_new' else 'initial'))
        if row['viewport_at']:
            events.append(dict(type='viewport_after_two_frames', at=START,
                               elapsed_ms=row['receipt_to_viewport_ms'], request_id=row['request_id'],
                               detail=str(row['article_id'])))
    return dict(version=1, type='cef_local_delivery_check', generated_at=END,
                session=dict(active=False, startedAt=START, expiresAt=END, stoppedReason='manual', dropped=0),
                max_wall_vs_monotonic_drift_ms=0, clock_warning=False, articles=rows, events=events)


def article(aid=1):
    return dict(id=aid, title='Fund distribution', canonical_url=URL,
                provider_name='Business Wire', published_at=START, summary='Short excerpt')


def archive(aid=1):
    return dict(version=1, article_id=aid, wire_key='bw:20261109123456:en', title='Fund distribution',
                original_url=URL, source_url=URL, published_at=START,
                blocks=[dict(type='paragraph', text='Complete retained test paragraph.'),
                        dict(type='table', caption='', rows=[[dict(text='AWF', header=False, colspan=1, rowspan=1),
                                                            dict(text='$0.10', header=False, colspan=1, rowspan=1)]])])


class OfflineReports(unittest.TestCase):
    def setUp(self):
        self.network = patch('socket.socket.connect', side_effect=AssertionError('Offline reports cannot connect'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_delivery_denominators_keep_unobserved_and_history_separate(self):
        rows = [delivery_row(1, duration=50), delivery_row(2, duration=61_000), delivery_row(3, duration=None),
                delivery_row(4, waiting=True, duration=100_000), delivery_row(5, classification='baseline'),
                delivery_row(6, classification='manual_or_history')]
        report = delivery_report(delivery_document(rows))
        measured = report['totals']['measured_receipt_to_viewport']
        self.assertEqual(measured['within_60_seconds'], dict(numerator=1, denominator=2, fraction=.5))
        self.assertEqual(report['totals']['eligible_automatic'], 3)
        self.assertEqual(report['totals']['viewport_unobserved'], 1)
        self.assertEqual([r['article_id'] for r in report['outliers_over_60_seconds']], [2])
        self.assertEqual(measured['median_ms'], 30_525)

    def test_zero_samples_are_unknown_not_zero_latency(self):
        report = delivery_report(delivery_document([delivery_row(1, duration=None)]))
        stats = report['totals']['measured_receipt_to_viewport']
        self.assertIsNone(stats['minimum_ms'])
        self.assertIsNone(stats['within_60_seconds']['fraction'])

    def test_synthetic_fixture_stays_labelled(self):
        doc = delivery_document()
        doc['synthetic_fixture'] = True
        self.assertTrue(delivery_report(doc)['synthetic_fixture'])

    def test_clock_jump_keeps_monotonic_duration_without_wall_subtraction(self):
        doc = delivery_document()
        doc['articles'][0]['viewport_at'] = '2026-11-09T14:50:00Z'
        doc['articles'][0]['publication_label'] = '2030-01-01T00:00:00Z'
        doc['clock_warning'] = True
        doc['max_wall_vs_monotonic_drift_ms'] = 600_000
        report = delivery_report(doc)
        self.assertEqual(report['totals']['measured_receipt_to_viewport']['median_ms'], 75)
        self.assertTrue(report['session']['local_clock_warning'])
        self.assertNotIn('label_to_viewport', report)

    def test_duplicate_rows_cannot_double_sample_size(self):
        row = delivery_row(1)
        report = delivery_report(delivery_document([row, copy.deepcopy(row)]))
        self.assertEqual(len(report['invalid_rows']), 2)
        self.assertEqual(report['totals']['eligible_automatic'], 0)

    def test_invalid_durations_are_excluded(self):
        for value in [-1, 1_200_001, 10**1000, float('inf'), float('nan'), True, '50']:
            with self.subTest(value=value):
                doc = delivery_document()
                doc['articles'][0]['receipt_to_viewport_ms'] = value
                report = delivery_report(doc)
                self.assertEqual(report['invalid_rows'][0]['reason'], 'invalid_viewport_duration')
                self.assertEqual(report['totals']['eligible_automatic'], 0)

    def test_nested_wrong_types_do_not_crash_reports(self):
        doc = delivery_document()
        doc['articles'][0]['classification'] = []
        self.assertEqual(delivery_report(doc)['invalid_rows'][0]['reason'], 'invalid_classification')
        for block in [dict(type=[]), dict(type='heading', level=[], text='Headline')]:
            body = archive()
            body['blocks'] = [block]
            with self.assertRaises(ValueError):
                archive_summary(body, article())

    def test_boolean_version_or_article_id_is_not_integer_identity(self):
        doc = delivery_document()
        doc['version'] = True
        with self.assertRaises(ValueError):
            delivery_report(doc)
        for field in ['version', 'article_id']:
            body = archive()
            body[field] = True
            with self.assertRaises(ValueError):
                archive_summary(body, article())

    def test_missing_viewport_with_duration_is_invalid_not_fast(self):
        doc = delivery_document()
        doc['articles'][0]['viewport_at'] = None
        self.assertEqual(delivery_report(doc)['invalid_rows'][0]['reason'], 'duration_without_viewport')

    def test_queued_response_cannot_be_relabelled_automatic(self):
        doc = delivery_document([delivery_row(1, waiting=True)])
        doc['articles'][0]['awaiting_user'] = False
        self.assertEqual(delivery_report(doc)['invalid_rows'][0]['reason'], 'queued_response_requires_user_action')

    def test_missing_receipt_event_cannot_create_automatic_sample(self):
        doc = delivery_document()
        doc['events'] = [e for e in doc['events'] if e['type'] != 'response_received']
        self.assertEqual(delivery_report(doc)['invalid_rows'][0]['reason'], 'automatic_response_event_missing')

    def test_dropped_events_do_not_override_retained_manual_response(self):
        doc = delivery_document()
        doc['session']['dropped'] = 1
        doc['events'][0]['detail'] = 'manual'
        report = delivery_report(doc)
        self.assertEqual(report['invalid_rows'][0]['reason'], 'response_kind_mismatch')
        self.assertEqual(report['totals']['measured_receipt_to_viewport']['n'], 0)

    def test_missing_viewport_field_does_not_abort_remaining_rows(self):
        doc = delivery_document([delivery_row(1, duration=None), delivery_row(2)])
        del doc['articles'][0]['viewport_at']
        report = delivery_report(doc)
        self.assertEqual(report['invalid_rows'][0]['reason'], 'missing_viewport_field')
        self.assertEqual(report['totals']['measured_receipt_to_viewport']['n'], 1)

    def test_truncated_event_stream_stays_explicit(self):
        doc = delivery_document()
        doc['session'].update(active=True, dropped=8)
        doc['events'] = []
        report = delivery_report(doc)
        self.assertEqual(report['session']['dropped_records'], 8)
        self.assertTrue(report['session']['active_at_export'])
        self.assertEqual(report['totals']['measured_receipt_to_viewport']['n'], 1)

    def test_invalid_session_or_event_fails_cleanly(self):
        for mutation in [lambda d: d.update(version=2),
                         lambda d: d['session'].update(expiresAt='2026-11-10T15:20:00Z'),
                         lambda d: d['session'].update(dropped=-1),
                         lambda d: d['events'][0].update(request_id=[]),
                         lambda d: d.update(articles=[delivery_row(1)] * 401)]:
            doc = delivery_document()
            mutation(doc)
            with self.assertRaises(ValueError):
                delivery_report(doc)

    def test_provider_and_utc_date_groups_stay_distinct(self):
        rows = [delivery_row(1), delivery_row(2)]
        rows[1].update(provider='GlobeNewswire', received_at='2026-11-10T01:00:00+02:00')
        report = delivery_report(delivery_document(rows))
        self.assertEqual(len(report['by_date_and_provider']), 2)
        self.assertEqual({r['date_utc'] for r in report['by_date_and_provider']}, {'2026-11-09'})

    def test_archive_counts_and_hashes_do_not_change_files(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / '1.json'
            path.write_text(json.dumps(archive()), encoding='utf8')
            original = path.read_bytes()
            report = reader_report({'articles': [article(), article(2)]}, folder)
            self.assertEqual(report['totals']['validated_archive_fraction']['fraction'], .5)
            self.assertEqual(report['totals']['tables'], 1)
            self.assertEqual(report['totals']['nonempty_table_cells'], 2)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(len(list(Path(folder).iterdir())), 1)

    def test_manual_case_is_preserved_and_not_made_live(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / '1.json').write_text(json.dumps(archive()), encoding='utf8')
            cases = dict(cases=[dict(article_id=1, wire_identity='bw:20261109123456:en', mode='manual', original_366_cohort=False),
                               dict(article_id=2, wire_identity='bw:20261109123457:en', mode='automatic')])
            report = reader_report({'items': [article()]}, folder, cases)
            self.assertEqual(report['rows'][0]['case_mode'], 'manual')
            self.assertFalse(report['rows'][0]['original_366_cohort'])
            self.assertEqual(report['reviewed_cases_absent_from_snapshot'], [2])

    def test_wrong_title_or_wire_is_rejected(self):
        for field, value, expected in [('title', 'Another fund', 'title_mismatch'),
                                       ('article_id', 2, 'article_identity_mismatch'),
                                       ('wire_key', 'bw:20261109999999:en', 'original_wire_identity_mismatch')]:
            body = archive()
            body[field] = value
            with self.assertRaisesRegex(ValueError, expected):
                archive_summary(body, article())
        with self.assertRaisesRegex(ValueError, 'snapshot_wire_identity_mismatch'):
            archive_summary(archive(), article(), 'bw:20261109999999:en')

    def test_legacy_prn_number_requires_matching_original_publisher_url(self):
        case = dict(wire_identity='302926053', canonical_url='https://www.prnewswire.com/news-releases/invesco-funds-302926053.html')
        self.assertEqual(reviewed_case_wire(case), 'prn:302926053')
        self.assertEqual(case['wire_identity'], '302926053')
        for url in ['https://finance.yahoo.com/news/local-302926053.html',
                    'https://www.prnewswire.com/news-releases/another-302926054.html', None]:
            with self.assertRaisesRegex(ValueError, 'unverified_bare_case_identity'):
                reviewed_case_wire({**case, 'canonical_url': url})

    def test_reviewed_canonical_conflict_is_not_hidden(self):
        with self.assertRaisesRegex(ValueError, 'reviewed_case_identity_conflict'):
            reviewed_case_wire(dict(wire_identity='bw:20261109999999:en', canonical_url=URL))

    def test_dates_require_zones_and_must_match_snapshot(self):
        for value in ['2026-11-09', '2026-11-09T15:00:00', '2026-11-08T15:00:00Z']:
            body = archive()
            body['published_at'] = value
            with self.assertRaises(ValueError):
                archive_summary(body, article())

    def test_unsafe_and_vendor_local_urls_are_not_wire_identity(self):
        for url in ['https://businesswire.com.evil.test/news/home/20261109123456/en/',
                    'https://user@businesswire.com/news/home/20261109123456/en/',
                    'https://businesswire.com:8443/news/home/20261109123456/en/',
                    'https://wallstreet-online.de/nachricht/123456', 'javascript:alert(1)']:
            self.assertIsNone(wire_identity(url))
        self.assertEqual(wire_identity(URL.replace('https:', 'http:')), 'bw:20261109123456:en')

    def test_body_shape_and_table_bounds_are_checked(self):
        for mutation in [lambda d: d.update(blocks=[]),
                         lambda d: d['blocks'][1]['rows'][0][0].update(colspan=41),
                         lambda d: d['blocks'].append(dict(type='script', text='run me')),
                         lambda d: d.update(blocks=[dict(type='paragraph', text='')]),
                         lambda d: d.update(blocks=[dict(type='paragraph', text='x')] * 2001)]:
            body = archive()
            mutation(body)
            with self.assertRaises(ValueError):
                archive_summary(body, article())

    def test_invalid_archive_does_not_hide_other_valid_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / '1.json').write_text('{broken', encoding='utf8')
            (Path(folder) / '2.json').write_text(json.dumps(archive(2)), encoding='utf8')
            report = reader_report({'articles': [article(), article(2)]}, folder)
            self.assertEqual(report['totals']['statuses'], {'archive_invalid': 1, 'archive_validated': 1})

    def test_snapshot_duplicates_and_non_string_summary_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            for rows in [[article(), article()], [{**article(), 'summary': []}], [{**article(), 'provider_name': {}}]]:
                with self.assertRaises(ValueError):
                    reader_report({'articles': rows}, folder)

    def test_symlink_cannot_read_archive_outside_directory(self):
        with tempfile.TemporaryDirectory() as folder, tempfile.TemporaryDirectory() as external:
            target = Path(external) / 'private.json'
            target.write_text(json.dumps(archive()), encoding='utf8')
            try:
                (Path(folder) / '1.json').symlink_to(target)
            except OSError:
                self.skipTest('File symlink privilege unavailable')
            report = reader_report({'articles': [article()]}, folder)
            self.assertEqual(report['rows'][0]['reason'], 'archive_path_outside_directory')

    def test_byte_limit_and_nonfinite_json(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'input.json'
            path.write_bytes(b'12345')
            with self.assertRaisesRegex(ValueError, 'exceeds'):
                read_json(path, 4)
            path.write_bytes(b'{"value":NaN}')
            with self.assertRaisesRegex(ValueError, 'Invalid JSON'):
                read_json(path)

    def test_cli_writes_new_output_and_never_overwrites_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            source, output = Path(folder) / 'input.json', Path(folder) / 'output.json'
            source.write_text(json.dumps(delivery_document()), encoding='utf8')
            before = source.read_bytes()
            with patch('sys.stdout', new=io.StringIO()):
                self.assertEqual(main(['delivery', str(source), str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertEqual(result['network_requests'], 0)
            self.assertEqual(result['database_connections'], 0)
            written = output.read_bytes()
            with patch('sys.stderr', new=io.StringIO()), self.assertRaises(SystemExit) as failure:
                main(['delivery', str(source), str(output)])
            self.assertEqual(failure.exception.code, 2)
            self.assertEqual(output.read_bytes(), written)
            self.assertEqual(source.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
