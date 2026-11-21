"""Narrow, reviewed issuer-table identities; no requests or registry mutations."""
from collections import defaultdict
from datetime import date, datetime, timezone
import re
from urllib.parse import urlsplit

from lxml import etree, html


# These six same-symbol names were checked against two independent issuer
# releases (PRN 302926053 and 302896150) and the published Nasdaq directory.
# They are not general abbreviation substitutions or predecessor aliases.
_INVESCO_NAMES = {
    'VCV': ('Invesco CA Value', 'Invesco California Value Municipal Income Trust'),
    'VGM': ('Invesco Tr Inv Gr Muni', 'Invesco Trust for Investment Grade Municipals'),
    'VKI': ('Invesco Adv Muni Inc II', 'Invesco Advantage Municipal Income Trust II'),
    'VMO': ('Invesco Muni Opp', 'Invesco Municipal Opportunity Trust'),
    'VPV': ('Invesco PA Value', 'Invesco Pennsylvania Value Municipal Income Trust'),
    'VTN': ('Invesco Tr Inv Gr NY', 'Invesco Trust for Investment Grade New York Municipals'),
}
_TITLE = 'Invesco Closed-End Funds Declare Dividends'
_SPONSOR = 'Invesco Advisers, Inc'
_HEADER = ('Name of Closed-End Management Investment Company', 'Ticker',
           'Monthly Dividend Per Share', 'Change From Prior Distribution')

# Same surviving symbols, not aliases for the funds they acquired. Completed
# renames were reviewed in PRN 302793338 (MFM) and 302805462 (MMT); the exact
# ticker/exchange/name rows recur in retained PRN 302935591. These names are
# usable only in the issuer distribution table below, never as discovery hints
# or generic body/title keyword support.
_ABERDEEN_NAMES = {
    'MFM': ('MFS Municipal Income', 'Aberdeen Municipal Income Fund', date(2026, 6, 8)),
    'MMT': ('MFS Multimarket Income', 'Aberdeen Multi-Market Income Fund', date(2026, 6, 22)),
}
_ABERDEEN_TITLE = 'Certain Aberdeen Investments U.S. Closed-End Funds Declare Distribution Dates and Amounts'
_ABERDEEN_PATH = (r'/news-releases/certain-aberdeen-investments-us-closed-end-funds-'
                  r'declare-distribution-dates-and-amounts-\d{9}\.html')


def reviewed_table_names(symbol, registry_name, sponsor):
    """Qualify a full name only while the reviewed current registry pair matches."""
    renamed = _ABERDEEN_NAMES.get(symbol)
    if renamed and registry_name == renamed[0] and sponsor == 'MFS':
        return frozenset({renamed[1]})
    pair = _INVESCO_NAMES.get(symbol)
    if pair is None or registry_name != pair[0] or sponsor != _SPONSOR:
        return frozenset()
    return frozenset({pair[1]})


def _text(node):
    return ' '.join(' '.join(node.itertext()).split())


def reviewed_invesco_table_symbols(page, url, title, eligible_names):
    """Require original PRN identity and the exact fund/ticker table relationship."""
    if title != _TITLE or not eligible_names:
        return set()
    loc = urlsplit(url)
    if (loc.scheme != 'https' or loc.netloc != 'www.prnewswire.com' or loc.query or loc.fragment
            or not re.fullmatch(r'/news-releases/invesco-closed-end-funds-declare-dividends-\d{9}\.html', loc.path)):
        return set()
    try:
        doc = html.fromstring(page)
    except (etree.ParserError, ValueError, TypeError):
        return set()
    headings = doc.xpath('//h1')
    if (doc.xpath('//link[@rel="canonical"]/@href') != [url]
            or len(headings) != 1 or _text(headings[0]) != _TITLE):
        return set()

    row_names = defaultdict(set)
    for table in doc.xpath('//article//div[contains(concat(" ", normalize-space(@class), " "), " release-content ")]//table'):
        header_seen = False
        for row in table.xpath('./tr|./thead/tr|./tbody/tr'):
            cells = tuple(_text(cell) for cell in row.xpath('./td|./th'))
            if cells == _HEADER:
                header_seen = True
                continue
            if not header_seen:
                continue
            if len(cells) != 4 or not re.match(r'^\$\d', cells[2]):
                header_seen = False
                continue
            name, symbol = cells[:2]
            if symbol in eligible_names:
                row_names[symbol].add(name)

    # Conflicting duplicate rows cannot borrow a valid name from another row.
    return {symbol for symbol, names in row_names.items()
            if len(names) == 1 and names <= eligible_names[symbol]}


def reviewed_aberdeen_table_symbols(page, url, title, eligible_names):
    """Validate reviewed survivor names in an original issuer distribution row."""
    eligible = {symbol: names for symbol, names in eligible_names.items() if symbol in _ABERDEEN_NAMES}
    if title != _ABERDEEN_TITLE or not eligible:
        return set()
    loc = urlsplit(url)
    if (loc.scheme != 'https' or loc.netloc != 'www.prnewswire.com' or loc.query or loc.fragment
            or not re.fullmatch(_ABERDEEN_PATH, loc.path)):
        return set()
    try:
        doc = html.fromstring(page, parser=html.HTMLParser(no_network=True))
    except (etree.ParserError, ValueError, TypeError):
        return set()
    headings = doc.xpath('//h1')
    if (doc.xpath('//link[@rel="canonical"]/@href') != [url]
            or len(headings) != 1 or _text(headings[0]) != title):
        return set()
    labels = doc.xpath('//meta[@name="date" or @property="article:published_time" or @property="og:article:published_time"]/@content')
    try:
        dates = [datetime.fromisoformat(value.replace('Z', '+00:00')) for value in labels]
    except ValueError:
        return set()
    if not dates or any(value.tzinfo is None or value != dates[0] for value in dates):
        return set()
    eligible = {symbol: names for symbol, names in eligible.items()
                if dates[0].astimezone(timezone.utc).date() >= _ABERDEEN_NAMES[symbol][2]}
    bodies = doc.xpath('//article//*[contains(concat(" ", normalize-space(@class), " "), " release-body ")]'
                       '//*[contains(concat(" ", normalize-space(@class), " "), " release-content ")]')
    if len(bodies) != 1:
        return set()
    if headings[0].xpath('ancestor::article[1]') != bodies[0].xpath('ancestor::article[1]'):
        return set()
    rows = defaultdict(set)
    for table in bodies[0].xpath('.//table[not(ancestor::aside or ancestor::nav or ancestor::footer or ancestor::table)]'):
        header_seen = False
        for row in table.xpath('./tr|./thead/tr|./tbody/tr'):
            cells = tuple(_text(cell) for cell in row.xpath('./td|./th'))
            if cells == ('Ticker', 'Exchange', 'Fund', 'Amount'):
                header_seen = True
                continue
            if not header_seen or not cells:
                continue
            symbol = cells[0]
            if symbol in eligible:
                # Retain every occurrence, including malformed duplicates, so a
                # valid row cannot lend its name to a conflicting ticker row.
                rows[symbol].add(cells)
    accepted = set()
    for symbol, records in rows.items():
        if len(records) != 1:
            continue
        cells = next(iter(records))
        if (len(cells) == 4 and cells[1] == 'NYSE' and cells[2] in eligible[symbol]
                and cells[2] == _ABERDEEN_NAMES[symbol][1]
                and re.fullmatch(r'\$\s*\d+\.\d+', cells[3])):
            accepted.add(symbol)
    return accepted
