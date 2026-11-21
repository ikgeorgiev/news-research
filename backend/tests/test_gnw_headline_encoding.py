"""Strict, reversible correction of observed JSON-LD headline mojibake."""
import hashlib
import json
from pathlib import Path

import pytest
from lxml import html

from app.financialcontent.gnw_body import parse_body

SAVED = Path(__file__).parent / 'fixtures/correctness/gnw-sun-cruiser.html'
CONTENT = SAVED.read_bytes()
URL = 'https://markets.chroniclejournal.com/thepilotnews/article/gnwcq-2026-11-10-sun-cruiser-launches-the-cruisin-home-fund-to-help-cover-drinkers-holiday-airfare'
TITLE = "SUN CRUISER LAUNCHES THE \u2018CRUISIN' HOME FUND\u2019 TO HELP COVER DRINKERS' HOLIDAY AIRFARE"


def parse(content=CONTENT, title=TITLE):
    return parse_body(content, article_url=URL, expected_title=title)


def mutate(*, metadata=None, heading=None):
    doc = html.fromstring(CONTENT.decode('utf-8'))
    script = next(s for s in doc.xpath('//script[@type="application/ld+json"]')
                  if json.loads(s.text or '{}').get('@type') == 'NewsArticle')
    data = json.loads(script.text)
    if metadata: metadata(data)
    script.text = json.dumps(data, ensure_ascii=False)
    if heading: heading(doc)
    return html.tostring(doc, encoding='utf-8')


def test_saved_misencoded_headline_reads_without_inventing_identity_or_assignments():
    result = parse()
    assert result['title'] == TITLE and result['reader']['title'] == TITLE
    assert result['source_html_sha256'] == hashlib.sha256(CONTENT).hexdigest()
    assert result['headline_validation']['method'] == 'reversible_utf8_from_latin1_with_article_heading'
    assert result['headline_validation']['metadata_headline'] != TITLE
    assert result['wire_identity'] is None and result['original_url'] is None
    assert result['validated_fund_tickers'] == []
    assert [e['symbol_label'] for e in result['primary_body_exchange_labels']] == ['SAM']
    assert all(not e['assignment_validated'] for e in result['primary_body_exchange_labels'])
    assert result['production_eligible'] is False and result['latency_eligible'] is False
    assert result['network_requests'] == result['production_articles_written'] == 0


def test_exact_unicode_headline_preserves_normal_path():
    result = parse(mutate(metadata=lambda m: m.update(headline=TITLE)))
    assert result['headline_validation']['method'] == 'exact'
    assert result['title'] == TITLE


@pytest.mark.parametrize('change', ['different_title', 'lossy', 'double_encoding', 'cp1252',
    'missing_heading', 'wrong_heading', 'duplicate_heading', 'heading_outside_article',
    'body_outside_article', 'wrong_article_slug', 'date_conflict', 'wrong_metadata_page'])
def test_encoding_repair_cannot_relax_identity_checks(change):
    def metadata(m):
        if change == 'different_title': m['headline'] = m['headline'].replace('AIRFARE', 'TICKETS')
        elif change == 'lossy': m['headline'] = TITLE.replace('\u2018', '?')
        elif change == 'double_encoding': m['headline'] = m['headline'].encode('utf-8').decode('latin1')
        elif change == 'cp1252': m['headline'] = TITLE.encode('utf-8').decode('cp1252')
        elif change == 'date_conflict': m['datePublished'] = '2026-11-09T14:00:00Z'
        elif change == 'wrong_metadata_page': m['mainEntityOfPage']['@id'] = URL + '-other'
    def heading(doc):
        article = doc.xpath('//article[@data-slug]')[0]
        h = article.xpath('./h1')[0]
        if change == 'missing_heading': article.remove(h)
        elif change == 'wrong_heading': h.text = 'Other title'
        elif change == 'duplicate_heading': article.append(html.fragment_fromstring('<h1>Other title</h1>'))
        elif change == 'heading_outside_article': doc.xpath('//body')[0].append(h)
        elif change == 'body_outside_article': doc.xpath('//body')[0].append(doc.xpath('//*[@itemprop="articleBody"]')[0])
        elif change == 'wrong_article_slug': article.set('data-slug', 'other-release')
    with pytest.raises(ValueError): parse(mutate(metadata=metadata, heading=heading))


def test_correcting_headline_does_not_change_body_text_tables_or_raw_bytes():
    repaired = parse()
    normal = parse(mutate(metadata=lambda m: m.update(headline=TITLE)))
    assert repaired['body_text'] == normal['body_text']
    assert repaired['body_sha256'] == normal['body_sha256']
    assert repaired['reader']['blocks'] == normal['reader']['blocks']
    assert SAVED.read_bytes() == CONTENT


# Reuse the existing isolated worker/clock fixtures; all HTTP is MockTransport.
from tests.test_gnw_review_queue import clock, store, worker


def test_worker_archives_corrected_body_as_review_only_and_never_replays_it(worker, store, clock):
    from datetime import datetime, timezone
    import httpx
    from app.financialcontent.listings import GNW_ROUTE
    from app.financialcontent.store import Store
    from tests.test_gnw_review_queue import article, front, run_response
    clock[0] = datetime(2026, 11, 10, 14, 1, tzinfo=timezone.utc).timestamp()
    item = {'url': URL, 'title': TITLE}
    assert front(store, [article(0)], clock[0] - 60) == 0
    assert front(store, [item, article(0)], clock[0]) == 1
    claimed = store.claim_body(clock[0], allowed_routes=[GNW_ROUTE])
    assert run_response(worker, claimed, httpx.Response(200, content=CONTENT)) == [URL]
    with store.connect() as db:
        row = dict(db.execute('SELECT * FROM fc_candidate WHERE url=?', (URL,)).fetchone())
        archive = dict(db.execute('SELECT * FROM fc_review_body WHERE url=?', (URL,)).fetchone())
    assert row['status'] == 'identity_review' and row['entry'] is None and row['attempts'] == 1
    assert archive['html'] == CONTENT
    review = json.loads(archive['review_json'])
    assert review['headline_validation']['method'] == 'reversible_utf8_from_latin1_with_article_heading'
    assert review['validated_fund_tickers'] == [] and not review['production_eligible']
    reopened = Store(store.path); reopened.recover_owned_work()
    assert reopened.claim_body(clock[0] + 120, allowed_routes=[GNW_ROUTE]) is None
