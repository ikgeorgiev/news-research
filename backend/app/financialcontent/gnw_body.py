"""Bounded FC/GNW reader for identity-pending review; never emits a feed entry."""
from __future__ import annotations
import asyncio
from datetime import date, datetime, timezone
import hashlib
from html import unescape
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
from lxml import html
from app.financialcontent.structured_body import DROP, build_reader_document
from app.financialcontent.financialcontent_adapter import ArticleUnavailable

MAX_BODY = 1024 * 1024
MAX_MANIFEST = 64 * 1024
REQUEST_SECONDS = 15
ARTICLE_PATH = re.compile(r'/thepilotnews/article/gnwcq-(\d{4})-(\d{1,2})-(\d{1,2})-[^/]+')
WIRE_PATH = re.compile(r'/news-release/(\d{4})/(\d{2})/(\d{2})/(\d+)/\d+/en/[^/]+\.html', re.I)
EXCHANGE_LABEL = re.compile(r'\b(NASDAQ|NYSE(?:\s+AMERICAN)?|NYSEAMERICAN)\s*[:\-]\s*([A-Z][A-Z0-9.\-]{0,9})\b', re.I)


def tidy(value):
    return ' '.join(unescape(value).split()) if isinstance(value, str) else ''


def aware_time(value):
    if not isinstance(value, str) or len(value) > 80:
        raise ValueError('Explicit timestamp with timezone required')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('Invalid timestamp') from exc
    if result.tzinfo is None:
        raise ValueError('Explicit timezone required')
    return result.astimezone(timezone.utc)


def read_bounded(path, cap):
    with Path(path).open('rb') as stream:
        data = stream.read(cap + 1)
    if len(data) > cap:
        raise ValueError('Saved input exceeds its byte limit')
    return data


def article_date(url):
    if not isinstance(url, str) or len(url) > 4096:
        raise ValueError('Unexpected FC GNW URL')
    parts = urlsplit(url)
    match = ARTICLE_PATH.fullmatch(parts.path)
    if (parts.scheme != 'https' or parts.netloc != 'markets.chroniclejournal.com'
            or parts.query or parts.fragment or not match):
        raise ValueError('Unexpected FC GNW URL')
    return date(*map(int, match.groups()))


def _original_link_candidate(value, published):
    """Only literal body links qualify; tracking/media IDs never become wire IDs."""
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        parts = urlsplit(value)
        match = WIRE_PATH.fullmatch(parts.path)
        if (parts.scheme != 'https' or parts.netloc not in {'www.globenewswire.com', 'globenewswire.com'}
                or parts.query or parts.fragment or not match):
            return None
        linked_date = date(*map(int, match.groups()[:3]))
    except ValueError:
        return None
    # A related historical story cannot identify the current release.
    if linked_date != published.date():
        return None
    return {'url': 'https://www.globenewswire.com' + parts.path,
            'candidate_wire_identity': f'gnw:{match[4]}:en'}


def _headline_validation(metadata_headline, expected, body, article_url):
    """Accept only an exact title or the observed, corroborated encoding error."""
    title = tidy(metadata_headline)
    evidence = {'method': 'exact', 'metadata_headline': metadata_headline}
    if title == expected:
        return title, evidence
    # One strict reversal only: no lossy replacement, fuzzy matching, CP1252
    # fallback, repeated decoding or normalization of the article body.
    try:
        repaired = metadata_headline.encode('latin1').decode('utf-8')
    except (AttributeError, UnicodeError):
        raise ValueError('Discovery and body headline mismatch') from None
    if repaired.encode('utf-8').decode('latin1') != metadata_headline or tidy(repaired) != expected:
        raise ValueError('Discovery and body headline mismatch')
    owners = body.xpath('ancestor::article[1]')
    if len(owners) != 1 or owners[0].get('data-slug') != urlsplit(article_url).path.rsplit('/', 1)[-1]:
        raise ValueError('Encoding correction requires matching article scope')
    headings = owners[0].xpath('.//h1')
    if (len(headings) != 1 or headings[0].get('itemprop') != 'name'
            or tidy(' '.join(headings[0].itertext())) != expected):
        raise ValueError('Encoding correction requires an unambiguous matching article heading')
    evidence.update(method='reversible_utf8_from_latin1_with_article_heading',
                    corrected_headline=repaired, article_heading=expected)
    return expected, evidence


def parse_body(content, *, article_url, expected_title):
    path_date = article_date(article_url)
    if len(content) > MAX_BODY:
        raise ValueError('Body exceeds one MiB')
    expected = tidy(expected_title)
    if not expected or len(expected) > 2000:
        raise ValueError('A bounded exact discovery headline is required')
    tree = html.fromstring(content.decode('utf-8'), parser=html.HTMLParser(no_network=True))
    scripts = tree.xpath('//script[@type="application/ld+json"]')
    if len(scripts) > 20:
        raise ValueError('Excessive JSON-LD metadata')
    documents = []
    for script in scripts:
        try:
            data = json.loads(script.text or '')
        except ValueError:
            continue
        # Only the observed schema is accepted; no guessed alternate shapes.
        if (isinstance(data, dict) and data.get('@type') == 'NewsArticle'
                and isinstance(data.get('mainEntityOfPage'), dict)
                and data['mainEntityOfPage'].get('@id') == article_url):
            documents.append(data)
    if len(documents) != 1:
        raise ValueError('Missing or ambiguous matching NewsArticle metadata')
    metadata = documents[0]
    published = aware_time(metadata.get('datePublished'))
    if path_date != published.date():
        raise ValueError('Article URL date conflicts with publication metadata')
    bodies = tree.xpath('//*[@itemprop="articleBody"]')
    if len(bodies) != 1:
        raise ValueError('Missing or ambiguous article body')
    body = bodies[0]
    title, headline_validation = _headline_validation(metadata.get('headline'), expected, body, article_url)
    if len(list(body.iter())) > 30000:
        raise ValueError('Excessive body structure')
    for node in list(body.iterdescendants()):
        if isinstance(node.tag, str) and node.tag.lower() in DROP:
            node.drop_tree()
    text = ' '.join(' '.join(body.itertext()).split())
    if len(text) < 100:
        raise ValueError('Article body is absent or too short')
    links = {}
    for href in body.xpath('.//a/@href'):
        candidate = _original_link_candidate(href, published)
        if candidate:
            links[candidate['url']] = candidate
    identities = {v['candidate_wire_identity'] for v in links.values()}
    issues = ['original_wire_identity_not_independently_reviewed', 'ticker_assignments_not_validated']
    if not identities:
        issues.append('no_current_original_wire_link')
    elif len(identities) > 1:
        issues.append('conflicting_original_wire_links')
    # Retain structure without inventing an original identity. This cannot pass
    # the production archive writer's original-wire/committed-article checks.
    entry = dict(id=None, title=title, link=article_url, published=published.isoformat(),
                 summary=text, source={'title': 'GlobeNewswire', 'href': None})
    reader = build_reader_document(content, entry)
    evidence = []
    for match in EXCHANGE_LABEL.finditer(text):
        evidence.append({'exchange_label': match[1], 'symbol_label': match[2].upper(),
                         'body_start': match.start(), 'body_end': match.end(),
                         'text': match[0], 'assignment_validated': False})
    return dict(version=1, route='financialcontent-gnw-body', status='review_only',
        title=title, headline_validation=headline_validation, source_url=article_url, publication_label=published.isoformat(),
        publication_precision='second_label_not_availability',
        original_url=None, wire_identity=None, original_link_candidates=list(links.values()),
        body_text=text, body_sha256=hashlib.sha256(text.encode()).hexdigest(),
        source_html_sha256=hashlib.sha256(content).hexdigest(),
        primary_body_exchange_labels=evidence, validated_fund_tickers=[], reader=reader,
        issues=issues, network_requests=0, production_articles_written=0,
        production_eligible=False, latency_eligible=False)



async def fetch_review_body(client, url):
    """One allowlisted request. Caller persists shared host and URL controls."""
    article_date(url)
    async def read():
        async with client.stream('GET', url, follow_redirects=False) as response:
            if response.status_code != 200:
                raise ArticleUnavailable(response.status_code, response.headers.get('Retry-After'))
            body = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                body.extend(chunk)
                if len(body) > MAX_BODY:
                    raise ValueError('Body exceeds one MiB')
            return bytes(body)
    return await asyncio.wait_for(read(), timeout=REQUEST_SECONDS)


def failed_review(content, article_url, title, error_kind):
    """Retain rejected bounded HTML without assigning extracted facts to it."""
    digest = hashlib.sha256(content).hexdigest()
    return {'version': 1, 'route': 'financialcontent-gnw-body', 'status': 'review_only',
        'source_url': article_url, 'title': tidy(title), 'source_html_sha256': digest,
        'body_parse_valid': False, 'body_text': '', 'body_sha256': hashlib.sha256(b'').hexdigest(),
        'wire_identity': None, 'original_url': None, 'validated_fund_tickers': [],
        'production_eligible': False, 'latency_eligible': False,
        'reader': {'source_html_sha256': digest, 'wire_key': None, 'blocks': []},
        'issues': ['body_parser_rejected:' + error_kind]}
