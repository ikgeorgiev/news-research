from __future__ import annotations

import re
import threading
import time
from collections import OrderedDict
from functools import lru_cache
from html import unescape
from urllib.parse import parse_qs, urlparse, urlunparse

import requests

from app.sources import (
    POLICY_GENERAL_ALLOWED,
    POLICY_SCOPED_CONTEXT_REQUIRED,
    SourcePageConfig,
    get_source_policy,
)


SOURCE_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; cef-news-feed/0.1; +local)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.1",
}
SOURCE_PAGE_CACHE_TTL_SECONDS = 6 * 60 * 60
SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS = 60
SOURCE_PAGE_CACHE_MAX_ITEMS = 1024

EXCHANGE_PATTERN = re.compile(
    r"\b(?:NYSE|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b"
)
PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")
TOKEN_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")
HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>", flags=re.IGNORECASE | re.DOTALL
)
HTML_NOISE_ELEMENT_PATTERN = re.compile(
    r"<(?:nav|aside|footer)\b[^>]*>.*?</(?:nav|aside|footer)>",
    flags=re.IGNORECASE | re.DOTALL,
)
HTML_SIDEBAR_DIV_OPEN_PATTERN = re.compile(
    r"<div\b[^>]*\bclass=\"[^\"]*sidebar[^\"]*\"[^>]*>",
    flags=re.IGNORECASE,
)
HTML_RELATED_NEWS_PATTERN = re.compile(
    r">\s*(?:More\s+News\s+From|Related\s+(?:News|Press\s+Releases|Articles))\b",
    flags=re.IGNORECASE,
)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
TABLE_CELL_SYMBOL_PATTERN = re.compile(
    r"<td[^>]*>(?:\s|<[^>]+>)*([A-Z][A-Z0-9\.\-]{0,9})(?:\s|<[^>]+>)*</td>",
    flags=re.IGNORECASE,
)
STOPWORDS = {
    "A",
    "AN",
    "AND",
    "ARE",
    "AS",
    "AT",
    "BY",
    "CEO",
    "ETF",
    "FOR",
    "FROM",
    "IN",
    "INC",
    "IS",
    "IT",
    "NAV",
    "NEW",
    "NOT",
    "OF",
    "ON",
    "OR",
    "Q",
    "THE",
    "TO",
    "US",
    "USA",
    "WITH",
}
AMBIGUOUS_TOKEN_SYMBOLS = {
    "FUND",
    "IDE",
}
MIN_PERSIST_CONFIDENCE = 0.65

_FUND_KEYWORD_STOPWORDS = frozenset(
    {
        "fund",
        "trust",
        "income",
        "bond",
        "municipal",
        "muni",
        "high",
        "yield",
        "total",
        "return",
        "global",
        "equity",
        "premium",
        "credit",
        "capital",
        "investment",
        "investments",
        "management",
        "advisors",
        "advisers",
        "asset",
        "securities",
        "opportunities",
        "opportunity",
        "strategic",
        "strategy",
        "strategies",
        "select",
        "quality",
        "real",
        "enhanced",
        "diversified",
        "dynamic",
        "value",
        "growth",
        "limited",
        "duration",
        "term",
        "assets",
        "corp",
        "corporation",
        "company",
        "inc",
        "llc",
        "llp",
        "the",
        "and",
        "of",
        "for",
        "new",
        "rate",
        "floating",
        "short",
        "first",
    }
)
_SHORT_SPONSOR_KEYWORD_STOPWORDS = frozenset(
    {
        "ag",
        "co",
        "ii",
        "iii",
        "inc",
        "llc",
        "llp",
        "lp",
        "ltd",
        "plc",
        "pte",
        "sa",
    }
)
_ALLOWED_TWO_CHAR_SPONSOR_KEYWORDS = frozenset({"c1"})

_source_page_cache: OrderedDict[str, tuple[float, str | None]] = OrderedDict()
_source_page_cache_lock = threading.Lock()


def _is_short_sponsor_keyword(
    raw_word: str, cleaned: str, *, is_leading_sponsor_word: bool
) -> bool:
    raw_clean = raw_word.strip(".,;()\"'")
    if len(cleaned) < 2 or len(cleaned) > 3:
        return False
    if cleaned in _FUND_KEYWORD_STOPWORDS:
        return False
    if cleaned in _SHORT_SPONSOR_KEYWORD_STOPWORDS:
        return False
    if not raw_clean.isalnum():
        return False
    if not any(char.isalpha() for char in raw_clean):
        return False
    if raw_clean.upper() != raw_clean:
        return False
    if len(cleaned) == 2:
        return cleaned in _ALLOWED_TWO_CHAR_SPONSOR_KEYWORDS
    return is_leading_sponsor_word


@lru_cache(maxsize=4096)
def _validation_keyword_pattern(keyword: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])"
    )


def _text_matches_validation_keywords(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    matched_keywords = [
        keyword
        for keyword in keywords
        if _validation_keyword_pattern(keyword).search(text_lower) is not None
    ]
    if not matched_keywords:
        return False
    if any(" " in keyword for keyword in matched_keywords):
        return True
    if any(len(keyword) <= 3 for keyword in matched_keywords):
        return True
    return len(set(matched_keywords)) >= 2


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, frozenset[str]]:
    result: dict[str, frozenset[str]] = {}
    for row in ticker_rows:
        symbol = row[1]
        symbol_lower = symbol.lower()
        fund_name = row[2] if len(row) > 2 else None
        sponsor = row[3] if len(row) > 3 else None
        keywords: set[str] = set()
        if fund_name:
            cleaned_fund_words: list[str] = []
            word_supports_phrase: list[bool] = []
            for idx, raw_word in enumerate(fund_name.split()):
                cleaned = raw_word.strip(".,;()\"'").lower()
                if cleaned == symbol_lower:
                    continue
                cleaned_fund_words.append(cleaned)
                is_short_keyword = _is_short_sponsor_keyword(
                    raw_word,
                    cleaned,
                    is_leading_sponsor_word=idx == 0,
                )
                is_distinctive_keyword = (
                    len(cleaned) >= 4
                    and cleaned not in _FUND_KEYWORD_STOPWORDS
                )
                if is_distinctive_keyword:
                    keywords.add(cleaned)
                elif is_short_keyword:
                    keywords.add(cleaned)
                word_supports_phrase.append(is_distinctive_keyword or is_short_keyword)
            for idx in range(len(cleaned_fund_words) - 1):
                if not (word_supports_phrase[idx] or word_supports_phrase[idx + 1]):
                    continue
                keywords.add(
                    f"{cleaned_fund_words[idx]} {cleaned_fund_words[idx + 1]}"
                )
        if sponsor:
            for idx, raw_word in enumerate(sponsor.split()):
                cleaned = raw_word.strip(".,;()\"'").lower()
                if cleaned == symbol_lower:
                    continue
                if _is_short_sponsor_keyword(
                    raw_word,
                    cleaned,
                    is_leading_sponsor_word=idx == 0,
                ):
                    keywords.add(cleaned)
        result[symbol.upper()] = frozenset(keywords)
    return result


def _parse_context_symbols(feed_url: str) -> list[str]:
    parsed = urlparse(feed_url)
    values = parse_qs(parsed.query).get("s", [])
    symbols: list[str] = []
    for value in values:
        symbols.extend(
            [token.strip().upper() for token in value.split(",") if token.strip()]
        )
    return symbols


def _canonical_article_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _is_source_article_url(url: str, hostname_suffix: str) -> bool:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not hostname:
        return False
    return hostname == hostname_suffix or hostname.endswith("." + hostname_suffix)


def _canonical_businesswire_article_url(url: str) -> str:
    return _canonical_article_url(url)


def _is_businesswire_article_url(url: str) -> bool:
    return _is_source_article_url(url, "businesswire.com")


def _cache_source_page(url: str, fetched_at: float, html_text: str | None) -> None:
    with _source_page_cache_lock:
        _source_page_cache[url] = (fetched_at, html_text)
        _source_page_cache.move_to_end(url)
        while len(_source_page_cache) > SOURCE_PAGE_CACHE_MAX_ITEMS:
            _source_page_cache.popitem(last=False)


def _fetch_source_page_html(
    url: str, timeout_seconds: int, config: SourcePageConfig
) -> str | None:
    fetch_url = _canonical_article_url(url)
    if not _is_source_article_url(fetch_url, config.hostname_suffix):
        return None

    now = time.time()
    with _source_page_cache_lock:
        cached = _source_page_cache.get(fetch_url)
        if cached is not None:
            fetched_at, cached_html = cached
            ttl_seconds = (
                SOURCE_PAGE_CACHE_TTL_SECONDS
                if cached_html is not None
                else SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS
            )
            if now - fetched_at <= ttl_seconds:
                _source_page_cache.move_to_end(fetch_url)
                return cached_html

    html_text: str | None = None
    try:
        response = requests.get(
            fetch_url,
            timeout=timeout_seconds,
            headers=SOURCE_PAGE_HEADERS,
        )
        if response.ok and response.text:
            html_text = response.text
    except (requests.RequestException, AttributeError):
        html_text = None

    _cache_source_page(fetch_url, time.time(), html_text)
    return html_text


def _extract_table_cell_symbols_from_html(
    html_text: str, known_symbols: set[str]
) -> set[str]:
    hits: set[str] = set()
    for raw_symbol in TABLE_CELL_SYMBOL_PATTERN.findall(html_text):
        symbol = raw_symbol.upper().strip()
        if symbol in STOPWORDS:
            continue
        if symbol in known_symbols:
            hits.add(symbol)
    return hits


def _html_to_plain_text(html_text: str) -> str:
    text = HTML_SCRIPT_STYLE_PATTERN.sub(" ", html_text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _strip_sidebar_divs(html_text: str) -> str:
    result: list[str] = []
    i = 0
    for match in HTML_SIDEBAR_DIV_OPEN_PATTERN.finditer(html_text):
        start = match.start()
        if start < i:
            continue
        result.append(html_text[i:start])
        depth = 1
        j = match.end()
        while j < len(html_text) and depth > 0:
            div_open = html_text.find("<div", j)
            div_close = html_text.find("</div>", j)
            if div_close == -1:
                j = len(html_text)
                break
            if div_open != -1 and div_open < div_close:
                depth += 1
                j = div_open + 4
            else:
                depth -= 1
                j = div_close + 6
        result.append(" ")
        i = j
    result.append(html_text[i:])
    return "".join(result)


def _strip_noise_elements(html_text: str) -> str:
    text = HTML_SCRIPT_STYLE_PATTERN.sub(" ", html_text)
    text = HTML_NOISE_ELEMENT_PATTERN.sub(" ", text)
    text = _strip_sidebar_divs(text)
    related = HTML_RELATED_NEWS_PATTERN.search(text)
    if related is not None:
        text = text[: related.start() + 1]
    return text


def _extract_source_fallback_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    timeout_seconds: int,
    config: SourcePageConfig,
    *,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> dict[str, tuple[str, float]]:
    html_text = _fetch_source_page_html(link, timeout_seconds, config)
    if not html_text:
        return {}

    body_html = _strip_noise_elements(html_text)
    plain_text = _html_to_plain_text(body_html)
    validation_text_lower = (
        " ".join([part for part in [title, summary, plain_text] if part]).lower()
        if symbol_keywords is not None
        else None
    )
    enriched_summary = " ".join(
        [part for part in [summary, plain_text] if part]
    ).strip()
    hits = _extract_entry_tickers(
        title,
        enriched_summary,
        link,
        feed_url,
        known_symbols,
        include_token=True,
        symbol_keywords=symbol_keywords,
    )
    hits = {
        symbol: hit
        for symbol, hit in hits.items()
        if hit[1] >= MIN_PERSIST_CONFIDENCE
    }

    for symbol in _extract_table_cell_symbols_from_html(body_html, known_symbols):
        confidence = 0.62
        if symbol_keywords is None:
            confidence = 0.84
        else:
            kws = symbol_keywords.get(symbol, frozenset())
            if not kws:
                confidence = 0.84
            elif (
                validation_text_lower is not None
                and _text_matches_validation_keywords(validation_text_lower, kws)
            ):
                confidence = 0.84
        existing = hits.get(symbol)
        if existing is None or confidence > existing[1]:
            hits[symbol] = (config.table_match_type, confidence)

    return hits


def _extract_entry_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    *,
    include_token: bool = True,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
) -> dict[str, tuple[str, float]]:
    hits: dict[str, tuple[str, float]] = {}

    def add(symbol: str, match_type: str, confidence: float) -> None:
        if symbol not in known_symbols:
            return
        existing = hits.get(symbol)
        if existing is None or confidence > existing[1]:
            hits[symbol] = (match_type, confidence)

    context_symbols = _parse_context_symbols(feed_url)
    if len(context_symbols) == 1:
        add(context_symbols[0], "context", 0.93)

    text = " ".join([title or "", summary or "", link or ""])
    text_lower = text.lower() if symbol_keywords is not None else None

    for symbol in EXCHANGE_PATTERN.findall(text):
        add(symbol.upper(), "exchange", 0.88)

    for symbol in PAREN_SYMBOL_PATTERN.findall(text):
        upper = symbol.upper()
        if upper in STOPWORDS:
            if (
                symbol_keywords is not None
                and text_lower is not None
                and upper in known_symbols
            ):
                kws = symbol_keywords.get(upper, frozenset())
                if kws and _text_matches_validation_keywords(text_lower, kws):
                    add(upper, "paren", 0.75)
            continue
        if symbol_keywords is not None:
            kws = symbol_keywords.get(upper, frozenset())
            if not kws:
                add(upper, "paren", 0.75)
                continue
            if (
                text_lower is not None
                and kws
                and _text_matches_validation_keywords(text_lower, kws)
            ):
                add(upper, "paren", 0.75)
            else:
                add(upper, "paren", 0.62)
            continue
        add(upper, "paren", 0.75)

    if include_token:
        for symbol in TOKEN_PATTERN.findall(text):
            upper = symbol.upper()
            if upper in STOPWORDS or upper in AMBIGUOUS_TOKEN_SYMBOLS:
                continue
            if symbol_keywords is not None and text_lower is not None:
                kws = symbol_keywords.get(upper, frozenset())
                if kws and _text_matches_validation_keywords(text_lower, kws):
                    add(upper, "validated_token", 0.68)
                    continue
            add(upper, "token", 0.62)

    return hits


def _max_ticker_confidence(hits: dict[str, tuple[str, float]]) -> float:
    if not hits:
        return 0.0
    return max(conf for _, conf in hits.values())


def _merge_ticker_hits(
    target: dict[str, tuple[str, float]],
    extra_hits: dict[str, tuple[str, float]],
) -> None:
    for symbol, (match_type, confidence) in extra_hits.items():
        existing = target.get(symbol)
        if existing is None or confidence > existing[1]:
            target[symbol] = (match_type, confidence)


def _verified_ticker_hits(
    source_code: str,
    hits: dict[str, tuple[str, float]],
) -> dict[str, tuple[str, float]]:
    policy = get_source_policy(source_code)
    return {
        symbol: hit
        for symbol, hit in hits.items()
        if hit[1] >= MIN_PERSIST_CONFIDENCE
        and (hit[0] != "context" or policy == POLICY_SCOPED_CONTEXT_REQUIRED)
    }


def _should_persist_entry(
    source_code: str, ticker_hits: dict[str, tuple[str, float]]
) -> bool:
    policy = get_source_policy(source_code)
    if policy == POLICY_GENERAL_ALLOWED:
        return True
    if not ticker_hits:
        return False
    return bool(_verified_ticker_hits(source_code, ticker_hits))
