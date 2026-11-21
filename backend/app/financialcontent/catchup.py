"""Recognize the one published second page; never synthesize pagination URLs."""
from html.parser import HTMLParser
from urllib.parse import parse_qs, urljoin, urlsplit

from app.financialcontent.listings import REPUBLISHER_ROUTES

BASE = REPUBLISHER_ROUTES['financialcontent-bw-index']


def is_second_page(url):
    parts, base = urlsplit(url), urlsplit(BASE)
    return ((parts.scheme, parts.netloc, parts.path) == (base.scheme, base.netloc, base.path)
            and not parts.fragment
            and parse_qs(parts.query, keep_blank_values=True) == {'CurrentPage': ['1']})


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get('href')
        if tag == 'a' and href:
            self.urls.append(urljoin(BASE, href))


def published_next_page(previous, current, html):
    if not previous or not current or len(set(previous) & set(current)) > 5:
        return None
    parser = Links()
    parser.feed(html)
    return next((url for url in parser.urls if is_second_page(url)), None)
