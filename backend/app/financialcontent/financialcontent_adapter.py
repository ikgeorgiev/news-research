"""Bounded public-body reader with exact title and original-wire validation."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
import json
import re
from urllib.parse import urlsplit

MAX_BYTES = 1024 * 1024
WALL_TIMEOUT_SECONDS = 15
HOST = 'markets.chroniclejournal.com'


class InvalidArticle(ValueError):
    pass


class ArticleUnavailable(ValueError):
    def __init__(self, status, retry_after=None):
        self.status = status
        self.retry_after = retry_after
        self.disable_route = status in {301, 302, 303, 307, 308, 401, 403, 404}
        super().__init__(f'Article HTTP {status}; no automatic retry or redirect')


def validate_url(url):
    parts = urlsplit(url)
    if (parts.scheme != 'https' or parts.netloc != HOST or parts.query or parts.fragment
            or not re.fullmatch(r'/thepilotnews/article/bizwire-\d{4}-\d{1,2}-\d{1,2}-[^/]+', parts.path)):
        raise InvalidArticle('Unexpected public article URL')


class ArticleHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.body_depth = None
        self.body_count = 0
        self.body_parts = []
        self.wire_urls = set()
        self.jsonld = []
        self.script = None
        self.ignore = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'div':
            self.depth += 1
            if attrs.get('itemprop') == 'articleBody':
                self.body_count += 1
                self.body_depth = self.depth
        if tag in {'script', 'style'}:
            self.ignore = tag
        if tag == 'script' and attrs.get('type') == 'application/ld+json':
            self.script = ''
        if self.body_depth and tag == 'a':
            parts = urlsplit(attrs.get('href', ''))
            if (parts.scheme == 'https' and parts.netloc in {'businesswire.com', 'www.businesswire.com'}
                    and re.fullmatch(r'/news/home/\d+/en/?', parts.path)):
                self.wire_urls.add('https://www.businesswire.com' + parts.path.rstrip('/') + '/')

    def handle_data(self, data):
        if self.script is not None:
            self.script += data
        if self.body_depth and not self.ignore:
            self.body_parts.append(data)

    def handle_endtag(self, tag):
        if tag == 'script' and self.script is not None:
            try:
                self.jsonld.append(json.loads(self.script))
            except ValueError:
                pass
            self.script = None
        if tag == self.ignore:
            self.ignore = None
        if tag == 'div':
            if self.body_depth == self.depth:
                self.body_depth = None
            self.depth = max(0, self.depth - 1)


def parse_article(content, *, article_url, expected_title, expected_wire_key=None):
    """Emit a normal feed entry with body evidence and the original wire identity."""
    validate_url(article_url)
    if len(content) > MAX_BYTES:
        raise InvalidArticle('Article exceeds one MiB')
    parser = ArticleHTML()
    parser.feed(content.decode('utf-8'))
    documents = [r for r in parser.jsonld if isinstance(r, dict)
                 and r.get('@type') == 'NewsArticle'
                 and isinstance(r.get('mainEntityOfPage'), dict)
                 and r['mainEntityOfPage'].get('@id') == article_url]
    if len(documents) != 1 or parser.body_count != 1 or len(parser.wire_urls) != 1:
        raise InvalidArticle('Missing or ambiguous article metadata/body/original wire identity')
    metadata = documents[0]
    title = ' '.join(unescape(metadata.get('headline', '')).split())
    listing_title = ' '.join(unescape(expected_title).split())
    if title != listing_title:
        # Some JSON-LD headlines contain UTF-8 bytes decoded as Latin-1.
        # Accept only an exact, reversible decoding to the observed listing.
        try:
            repaired = title.encode('latin-1').decode('utf-8')
        except UnicodeError:
            repaired = None
        if repaired != listing_title:
            raise InvalidArticle('Listing and article title mismatch')
        title = repaired
    try:
        published = datetime.fromisoformat(metadata['datePublished'].replace('Z', '+00:00'))
        if published.tzinfo is None:
            raise ValueError('Timezone missing')
    except (KeyError, ValueError, TypeError) as exc:
        raise InvalidArticle('Publication timestamp missing or ambiguous') from exc
    original_url = next(iter(parser.wire_urls))
    wire_id = re.search(r'/news/home/(\d+)/en/', original_url)[1]
    wire_key = f'bw:{wire_id}:en'
    if expected_wire_key is not None and wire_key != expected_wire_key:
        raise InvalidArticle('Unexpected original wire identity')
    body = ' '.join(' '.join(parser.body_parts).split())
    if len(body) < 100:
        raise InvalidArticle('Article body is absent or too short')
    return {
        'id': wire_key, 'title': title, 'link': article_url,
        'published': published.astimezone(timezone.utc).isoformat(),
        'summary': body, 'language': 'en',
        'source': {'title': 'Business Wire', 'href': original_url},
    }


async def fetch_article(client, url):
    """Single bounded request. Caller must persist refusal/backoff state."""
    validate_url(url)

    async def read():
        async with client.stream('GET', url, follow_redirects=False) as response:
            if response.status_code != 200:
                raise ArticleUnavailable(response.status_code, response.headers.get('Retry-After'))
            content = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                content.extend(chunk)
                if len(content) > MAX_BYTES:
                    raise InvalidArticle('Article exceeds one MiB')
            return bytes(content)

    return await asyncio.wait_for(read(), timeout=WALL_TIMEOUT_SECONDS)
