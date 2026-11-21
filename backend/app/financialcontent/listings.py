"""Published FinancialContent HTML listings; no article fetches or API discovery."""
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
import re
from urllib.parse import urljoin, urlsplit, urlunsplit

BASE = 'https://markets.chroniclejournal.com/thepilotnews'
REPUBLISHER_ROUTES = {
    'financialcontent-bw-index': BASE + '/news/provider/bizwire',
    'financialcontent-franklin-index': BASE + '/news/publisher/franklin%20resources,%20inc.%20and%20legg%20mason%20closed%20end%20funds',
}
GNW_ROUTE = 'financialcontent-gnw-index'
GNW_URL = BASE + '/news/provider/gnwcq'
PUBLISHED_ROUTES = {**REPUBLISHER_ROUTES, GNW_ROUTE: GNW_URL}


class ListingParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.widget = self.headline = self.title = None
        self.anchor = None
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = attrs.get('class', '').split()
        if tag == 'div':
            self.depth += 1
            if 'newsrpm-headlines1' in classes:
                self.widget = self.depth
            if self.widget and 'headline' in classes:
                self.headline = self.depth
            if self.headline and 'title' in classes:
                self.title = self.depth
        if tag == 'a' and self.title and attrs.get('href'):
            self.anchor = {'url': attrs['href'], 'title': ''}

    def handle_data(self, data):
        if self.anchor is not None:
            self.anchor['title'] += data

    def handle_endtag(self, tag):
        if tag == 'a' and self.anchor is not None:
            self.links.append(self.anchor)
            self.anchor = None
        if tag == 'div':
            for name in ('title', 'headline', 'widget'):
                if getattr(self, name) == self.depth:
                    setattr(self, name, None)
            self.depth = max(0, self.depth - 1)


def parse_republisher_index(content, route, article_key):
    provider = 'gnwcq' if route == GNW_ROUTE else 'bizwire'
    parser = ListingParser()
    parser.feed(content.decode('utf-8', errors='replace'))
    items = {}
    for link in parser.links:
        parts = urlsplit(urljoin(PUBLISHED_ROUTES[route], link['url']))
        if parts.scheme != 'https' or parts.netloc != 'markets.chroniclejournal.com':
            continue
        match = re.fullmatch(r'/thepilotnews/article/' + provider + r'-(\d{4})-(\d{1,2})-(\d{1,2})-[^/]+', parts.path)
        if not match:
            continue
        published = date(*(int(v) for v in match.groups())).isoformat()
        title = ' '.join(link['title'].split())
        if not title:
            raise ValueError('Republisher headline lacks a title')
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
        key = article_key(url)
        items.setdefault(key, {
            'key': key, 'url': url, 'title': title,
            'published_at': published, 'publication_precision': 'date_only',
            'publication_date_source': 'article_url_date',
            'stock_tickers': None, 'summary_html': '',
            'republisher_index_url': PUBLISHED_ROUTES[route],
            'original_wire_id': None,
        })
    if not items:
        raise ValueError('Republisher index has no recognized headlines; not a successful empty feed')
    return list(items.values())


def republisher_wire_candidates(records):
    """Candidate title/date joins, never a ticker validation or speed verdict."""
    def day(value):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
        except (ValueError, TypeError):
            return parsedate_to_datetime(value).date()

    def title(value):
        return re.sub(r'[^\w]+', ' ', unescape(value).casefold()).strip()

    fresh = [r for r in records if not r.get('initial_snapshot', False)]
    replicas = [r for r in fresh if r['route'] in REPUBLISHER_ROUTES or r['route']=='wallstreet-bw-rss']
    wires = [r for r in fresh if r['route'].startswith('bw-')]
    result = []
    for replica in replicas:
        for wire in wires:
            if title(replica['title']) != title(wire['title']):
                continue
            try:
                distance = abs((day(replica['published_at']) - day(wire['published_at'])).days)
            except (ValueError, TypeError, AttributeError):
                continue
            if distance <= 1:
                result.append({'republisher': replica, 'wire': wire,
                               'publication_date_distance_days': distance,
                               'status': 'candidate_requires_original_wire_id_content_and_current_ticker_verification',
                               'speed_winner': None})
    return result
