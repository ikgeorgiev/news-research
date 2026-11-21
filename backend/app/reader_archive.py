"""Archive already fetched publisher bodies after a validated article commits.

No HTTP is performed here. Publisher identity, title and publication metadata
must match the committed article. The first saved document remains immutable.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import unicodedata
from urllib.parse import urlsplit

from lxml import html
from lxml.etree import ParserError, XMLSyntaxError

from app.financialcontent.structured_body import (
    DROP, MAX_ARCHIVE_BYTES, build_reader_document, save_reader_document, text_of,
)
from app.release_matching import release_identity
from app.gnw_feed_discovery import advertised_issuer_feed

PREFIX = {'businesswire': 'bw', 'globenewswire': 'gnw', 'prnewswire': 'prn'}
HOST = {'businesswire': 'businesswire.com', 'globenewswire': 'globenewswire.com', 'prnewswire': 'prnewswire.com'}


def normalized(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split())


def precise_date(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (AttributeError, ValueError):
        return None


def wire_key(url, source_code):
    parsed = urlsplit(url)
    # Legacy wire RSS may label an original URL with HTTP. This is identity
    # comparison only; the actual fetch policy is unchanged and never called here.
    if parsed.scheme not in {'http', 'https'} or parsed.username or parsed.password or parsed.port:
        raise ValueError('Unsupported publisher URL')
    if parsed.hostname not in {HOST.get(source_code), 'www.'+HOST.get(source_code, '')}:
        raise ValueError('Publisher host differs')
    identity = release_identity(url)
    if not identity or identity[0] != source_code:
        raise ValueError('Original wire identity missing')
    return ':'.join([PREFIX[source_code], identity[1]] + ([identity[2]] if identity[2] else []))


def build_publisher_document(html_text, *, source_code, page_url, article_url, title, published_at):
    content = html_text.encode('utf-8')
    if len(content) > MAX_ARCHIVE_BYTES:
        raise ValueError('Publisher body exceeds one MiB')
    expected = wire_key(page_url, source_code)
    if wire_key(article_url, source_code) != expected:
        raise ValueError('Committed article identity differs')
    try:
        tree = html.fromstring(content, parser=html.HTMLParser(encoding='utf-8'))
    except (ParserError, XMLSyntaxError) as exc:
        raise ValueError('Publisher document cannot be parsed') from exc
    canonicals = tree.xpath('//link[contains(concat(" ",normalize-space(@rel)," ")," canonical ")]/@href')
    if len(canonicals) != 1 or wire_key(canonicals[0], source_code) != expected:
        raise ValueError('Publisher canonical identity differs')
    headings = [normalized(text_of(node)) for node in tree.xpath('//h1') if text_of(node)]
    metadata = [normalized(v) for v in tree.xpath('//meta[@property="og:title"]/@content')]
    expected_title = normalized(title)
    if not headings or any(v != expected_title for v in headings+metadata):
        raise ValueError('Publisher headline differs or is ambiguous')
    published = precise_date(published_at)
    labels = tree.xpath('//meta[@name="date" or @property="article:published_time" or @property="og:article:published_time"]/@content')
    dates = [precise_date(v) for v in labels]
    if not published or not dates or any(d is None or abs((d-published).total_seconds()) > 60 for d in dates):
        raise ValueError('Publisher date is missing, imprecise or differs')
    if source_code == 'prnewswire':
        bodies = tree.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," release-body ")]//*[contains(concat(" ",normalize-space(@class)," ")," release-content ")]')
    else:
        bodies = tree.xpath('//*[@itemprop="articleBody"]')
    if len(bodies) != 1:
        raise ValueError('Publisher body is missing or ambiguous')
    body = deepcopy(bodies[0])
    for node in list(body.iterdescendants()):
        if isinstance(node.tag, str) and node.tag.lower() in DROP:
            node.drop_tree()
    body.set('itemprop', 'articleBody')
    entry = dict(id=expected, title=title, link=page_url, source={'href':canonicals[0]},
                 published=published.isoformat(), summary=text_of(body))
    document = build_reader_document(html.tostring(body, encoding='utf-8'), entry)
    document.update(source_html_sha256=hashlib.sha256(content).hexdigest(),
                    body_kind='publisher_body', identity_validation='canonical_title_date',
                    body_published_at=dates[0].isoformat(), archived_at=datetime.now(timezone.utc).isoformat())
    if source_code == 'globenewswire':
        discovery = advertised_issuer_feed(tree, canonical_url=canonicals[0], title=title)
        proposed = {**document, 'publisher_feed_discovery': discovery}
        # Optional metadata cannot make an otherwise valid near-cap reader fail.
        # Leave room for the article_id added by the immutable archive writer.
        if len(json.dumps(proposed, ensure_ascii=False).encode('utf-8')) <= MAX_ARCHIVE_BYTES - 128:
            document['publisher_feed_discovery'] = discovery
    return document


def save_publisher_archive(directory, article_id, html_text, **metadata):
    document = build_publisher_document(html_text, **metadata)
    return save_reader_document(directory, article_id, document)
