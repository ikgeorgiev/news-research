"""Synthetic adapter regressions; no public requests or production imports."""
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('news_syndicated_body', Path(__file__).resolve().parents[1] / 'news_syndicated_body.py')
N = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(N)
URL = 'https://markets.chroniclejournal.com/thepilotnews/article/gnwcq-2026-11-10-example-fund-distribution'
TITLE = 'Example Fund Declares Distribution'
WIRE = 'https://www.globenewswire.com/news-release/2026/11/10/9999999/0/en/Example-Fund-Declares-Distribution.html'
BODY = ('<p>Example Fund (Nasdaq-FUND) announces a distribution for its common shares. '
        'The record and payment dates appear below; shareholders should consult the full release.</p>'
        '<table><caption>Distribution</caption><tr><th>Class</th><th>Amount</th></tr>'
        '<tr><td>Common</td><td>$0.15</td></tr></table><p>Contact the issuer for further information.</p>')


def document(*, body=BODY, title=TITLE, published='2026-11-10T14:00:00Z', metadata_url=URL, extra=''):
    metadata = {'@type': 'NewsArticle', 'mainEntityOfPage': {'@id': metadata_url},
                'headline': title, 'datePublished': published}
    return (f'<html><script type="application/ld+json">{json.dumps(metadata)}</script>'
            f'{extra}<div itemprop="articleBody">{body}</div></html>').encode()


class BodyTests(unittest.TestCase):
    def setUp(self):
        self.net = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network allowed'))
        self.net.start()
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)

    def tearDown(self):
        self.net.stop()
        self.temp.cleanup()

    def parse(self, content=None, **kwargs):
        return N.parse_body(document() if content is None else content,
            article_url=kwargs.get('article_url', URL), expected_title=kwargs.get('expected_title', TITLE))

    def capture(self, content=None, **overrides):
        content = document() if content is None else content
        (self.directory / 'body.html').write_bytes(content)
        m = dict(version=1, route='financialcontent-gnw-body', request_url=URL,
            payload_file='body.html', payload_sha256=hashlib.sha256(content).hexdigest(),
            http_status=200, classification='historical', receipt_kind='retained_response',
            received_at='2026-11-10T14:01:00Z', expected_title=TITLE)
        m.update(overrides)
        path = self.directory / 'manifest.json'
        path.write_text(json.dumps(m), encoding='utf-8')
        return path

    def test_realistic_gnw_shape_preserves_body_tables_without_inventing_identity(self):
        r = self.parse()
        tables = [b for b in r['reader']['blocks'] if b['type'] == 'table']
        self.assertEqual(tables[0]['rows'][1][1]['text'], '$0.15')
        self.assertIsNone(r['wire_identity'])
        self.assertIsNone(r['reader']['wire_key'])
        self.assertFalse(r['production_eligible'])
        self.assertFalse(r['latency_eligible'])
        self.assertEqual(r['network_requests'], 0)

    def test_exchange_label_comes_from_body_and_is_not_a_ticker_assignment(self):
        r = self.parse()
        self.assertEqual(r['primary_body_exchange_labels'][0]['symbol_label'], 'FUND')
        e = r['primary_body_exchange_labels'][0]
        self.assertEqual(r['body_text'][e['body_start']:e['body_end']], 'Nasdaq-FUND')
        self.assertEqual(r['validated_fund_tickers'], [])

    def test_title_or_sidebar_ticker_is_not_body_evidence(self):
        r = self.parse(document(body=BODY.replace('(Nasdaq-FUND)', ''), extra='<aside>NYSE: EDF</aside>'))
        self.assertEqual(r['primary_body_exchange_labels'], [])

    def test_tracking_pixel_attachment_and_tracker_do_not_establish_identity(self):
        extra = ('<a href="https://www.globenewswire.com/Tracker?data=9999999">issuer</a>'
            '<a href="https://www.globenewswire.com/NewsRoom/AttachmentNg/123">photo</a>'
            '<img src="https://www.globenewswire.com/newsroom/ti?nf=9999999">')
        self.assertEqual(self.parse(document(body=BODY + extra))['original_link_candidates'], [])

    def test_literal_current_original_link_is_retained_only_as_candidate(self):
        r = self.parse(document(body=BODY + f'<a href="{WIRE}">Original release</a>'))
        self.assertEqual(r['original_link_candidates'][0]['candidate_wire_identity'], 'gnw:9999999:en')
        self.assertIsNone(r['wire_identity'])
        self.assertFalse(r['production_eligible'])

    def test_related_old_original_link_is_not_current_identity(self):
        old = WIRE.replace('/2026/11/10/', '/2026/11/09/')
        self.assertEqual(self.parse(document(body=BODY + f'<a href="{old}">Related</a>'))['original_link_candidates'], [])

    def test_conflicting_original_links_remain_ambiguous(self):
        r = self.parse(document(body=BODY + f'<a href="{WIRE}">A</a><a href="{WIRE.replace("9999999", "8888888")}">B</a>'))
        self.assertIn('conflicting_original_wire_links', r['issues'])
        self.assertIsNone(r['original_url'])

    def test_original_link_outside_body_is_not_evidence(self):
        self.assertEqual(self.parse(document(extra=f'<aside><a href="{WIRE}">Related</a></aside>'))['original_link_candidates'], [])

    def test_wrong_headline_fails(self):
        with self.assertRaisesRegex(ValueError, 'headline mismatch'):
            self.parse(document(title='Different Fund Distribution'))

    def test_wrong_metadata_page_fails(self):
        with self.assertRaisesRegex(ValueError, 'metadata'):
            self.parse(document(metadata_url=URL + '-different'))

    def test_timezone_missing_fails(self):
        with self.assertRaisesRegex(ValueError, 'timezone'):
            self.parse(document(published='2026-11-10T14:00:00'))

    def test_url_date_conflict_fails(self):
        with self.assertRaisesRegex(ValueError, 'date conflicts'):
            self.parse(document(published='2026-11-09T14:00:00Z'))

    def test_ambiguous_metadata_fails(self):
        text = document().decode()
        script = text[text.index('<script'):text.index('</script>') + 9]
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            self.parse(document(extra=script))

    def test_multiple_bodies_fail(self):
        with self.assertRaisesRegex(ValueError, 'ambiguous article body'):
            self.parse(document(extra='<div itemprop="articleBody">Second</div>'))

    def test_too_short_body_fails(self):
        with self.assertRaisesRegex(ValueError, 'too short'):
            self.parse(document(body='<p>FUND</p>'))

    def test_oversized_body_fails_before_parse(self):
        with self.assertRaisesRegex(ValueError, 'one MiB'):
            self.parse(b' ' * (N.MAX_BODY + 1))

    def test_utf8_and_html_entities_preserve_text(self):
        r = self.parse(document(body=BODY + '<p>Issuer’s café &amp; fund.</p>'))
        self.assertIn('Issuer’s café & fund.', r['body_text'])

    def test_inert_reader_excludes_scripts_and_embeds(self):
        r = self.parse(document(body=BODY + '<script>steal()</script><iframe>external</iframe>'))
        self.assertNotIn('steal', r['body_text'])
        self.assertNotIn('external', r['body_text'])
        self.assertNotIn('script', json.dumps(r['reader']))

    def test_nested_tables_are_review_failure_not_flattened(self):
        with self.assertRaisesRegex(ValueError, 'Nested tables'):
            self.parse(document(body=BODY + '<table><tr><td><table><tr><td>X</td></tr></table></td></tr></table>'))

    def test_same_host_cannot_infer_other_provider_or_path(self):
        for url in [URL.replace('gnwcq-', 'bizwire-'), URL.replace('markets.', 'other.'), URL + '?nocache=1', URL + '#x', URL.replace('https:', 'http:')]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.parse(article_url=url)

    def test_historical_manifest_is_preserved_and_never_latency_eligible(self):
        r = N.review_capture(self.capture())
        self.assertEqual(r['observation']['classification'], 'historical')
        self.assertFalse(r['latency_eligible'])

    def test_new_label_cannot_bypass_identity_review(self):
        r = N.review_capture(self.capture(classification='new'))
        self.assertFalse(r['production_eligible'])
        self.assertFalse(r['latency_eligible'])

    def test_manifest_rejects_digest_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            N.review_capture(self.capture(payload_sha256='0' * 64))

    def test_manifest_rejects_path_escape(self):
        with self.assertRaisesRegex(ValueError, 'inside'):
            N.review_capture(self.capture(payload_file='../body.html'))

    def test_manifest_rejects_refusals_without_reading_or_retry(self):
        for status in [401, 403, 404, 429, 302, True]:
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, 'retained HTTP 200'):
                N.review_capture(self.capture(http_status=status))

    def test_network_receipt_requires_ordered_times_and_clock(self):
        for overrides in [{}, {'request_started_at': '2026-11-10T14:02:00Z', 'clock_id': 'pc1'},
                          {'request_started_at': '2026-11-10T14:00:59Z'}]:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                N.review_capture(self.capture(receipt_kind='network_response', **overrides))

    def test_valid_network_receipt_is_separate_from_publication(self):
        r = N.review_capture(self.capture(receipt_kind='network_response',
            request_started_at='2026-11-10T14:00:59Z', clock_id='pc1'))
        self.assertNotEqual(r['observation']['received_at'], r['publication_label'])
        self.assertFalse(r['latency_eligible'])

    def test_output_does_not_overwrite_original_or_existing_review(self):
        manifest = self.capture()
        with patch.object(N.sys, 'argv', ['news_syndicated_body', str(manifest), str(manifest)]):
            before = manifest.read_bytes()
            with self.assertRaises(FileExistsError):
                N.main()
            self.assertEqual(manifest.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
