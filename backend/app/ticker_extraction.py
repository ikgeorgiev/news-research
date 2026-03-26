from __future__ import annotations

import re
import threading
import time
from collections import OrderedDict
from urllib.parse import parse_qs, urlparse

from app.constants import (
    CONFIDENCE_CONTEXT,
    CONFIDENCE_EXCHANGE,
    CONFIDENCE_PAREN,
    CONFIDENCE_TABLE_VALIDATED,
    CONFIDENCE_UNVALIDATED,
    CONFIDENCE_VALIDATED_TOKEN,
    EXTRACTION_VERSION,
    MIN_PERSIST_CONFIDENCE,
    NO_KEYWORDS_CONFIDENCE,
)
from app.ticker_context import (
    _build_symbol_keywords as _build_symbol_keywords_impl,
    _text_matches_validation_keywords as _text_matches_validation_keywords_impl,
)
from app.sources import (
    POLICY_GENERAL_ALLOWED,
    POLICY_SCOPED_CONTEXT_REQUIRED,
    SourcePageConfig,
    _canonical_businesswire_article_url as _canonical_businesswire_article_url_impl,
    _fetch_source_page_html as _fetch_source_page_html_impl,
    _is_businesswire_article_url as _is_businesswire_article_url_impl,
    _is_source_article_url as _is_source_article_url_impl,
    get_source_policy,
)
from app.utils import extract_article_body, html_to_plain_text, strip_noise_elements


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

_source_page_cache: OrderedDict[str, tuple[float, str | None]] = OrderedDict()
_source_page_cache_lock = threading.Lock()


def _text_matches_validation_keywords(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    return _text_matches_validation_keywords_impl(text_lower, keywords)


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, frozenset[str]]:
    return _build_symbol_keywords_impl(ticker_rows)


def _parse_context_symbols(feed_url: str) -> list[str]:
    parsed = urlparse(feed_url)
    values = parse_qs(parsed.query).get("s", [])
    symbols: list[str] = []
    for value in values:
        symbols.extend(
            [token.strip().upper() for token in value.split(",") if token.strip()]
        )
    return symbols


def _is_source_article_url(url: str, hostname_suffix: str) -> bool:
    return _is_source_article_url_impl(url, hostname_suffix)


def _canonical_businesswire_article_url(url: str) -> str:
    return _canonical_businesswire_article_url_impl(url)


def _is_businesswire_article_url(url: str) -> bool:
    return _is_businesswire_article_url_impl(url)


def _fetch_source_page_html(
    url: str, timeout_seconds: int, config: SourcePageConfig
) -> str | None:
    return _fetch_source_page_html_impl(
        url,
        timeout_seconds,
        config,
        cache=_source_page_cache,
        cache_lock=_source_page_cache_lock,
        headers=SOURCE_PAGE_HEADERS,
        cache_ttl_seconds=SOURCE_PAGE_CACHE_TTL_SECONDS,
        failure_cache_ttl_seconds=SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS,
        cache_max_items=SOURCE_PAGE_CACHE_MAX_ITEMS,
        now_fn=time.time,
    )


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
    return html_to_plain_text(html_text)


def _strip_noise_elements(html_text: str) -> str:
    return strip_noise_elements(html_text)


def _extract_article_body(html_text: str) -> str | None:
    return extract_article_body(html_text)


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
) -> dict[str, tuple[str, float]] | None:
    html_text = _fetch_source_page_html(link, timeout_seconds, config)
    if not html_text:
        return None

    table_html = _strip_noise_elements(html_text)
    trafilatura_text = _extract_article_body(html_text)
    plain_text = (
        trafilatura_text
        if trafilatura_text is not None
        else _html_to_plain_text(table_html)
    )
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

    for symbol in _extract_table_cell_symbols_from_html(table_html, known_symbols):
        confidence = CONFIDENCE_UNVALIDATED
        if symbol_keywords is None:
            confidence = CONFIDENCE_TABLE_VALIDATED
        else:
            kws = symbol_keywords.get(symbol, frozenset())
            if not kws:
                confidence = NO_KEYWORDS_CONFIDENCE
            elif (
                validation_text_lower is not None
                and _text_matches_validation_keywords(validation_text_lower, kws)
            ):
                confidence = CONFIDENCE_TABLE_VALIDATED
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
        add(context_symbols[0], "context", CONFIDENCE_CONTEXT)

    text = " ".join([title or "", summary or "", link or ""])
    text_lower = text.lower() if symbol_keywords is not None else None

    for symbol in EXCHANGE_PATTERN.findall(text):
        add(symbol.upper(), "exchange", CONFIDENCE_EXCHANGE)

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
                    add(upper, "paren", CONFIDENCE_PAREN)
            continue
        if symbol_keywords is not None:
            kws = symbol_keywords.get(upper, frozenset())
            if not kws:
                add(upper, "paren", NO_KEYWORDS_CONFIDENCE)
                continue
            if (
                text_lower is not None
                and kws
                and _text_matches_validation_keywords(text_lower, kws)
            ):
                add(upper, "paren", CONFIDENCE_PAREN)
            else:
                add(upper, "paren", CONFIDENCE_UNVALIDATED)
            continue
        add(upper, "paren", CONFIDENCE_PAREN)

    if include_token:
        for symbol in TOKEN_PATTERN.findall(text):
            upper = symbol.upper()
            if upper in STOPWORDS or upper in AMBIGUOUS_TOKEN_SYMBOLS:
                continue
            if symbol_keywords is not None and text_lower is not None:
                kws = symbol_keywords.get(upper, frozenset())
                if kws and _text_matches_validation_keywords(text_lower, kws):
                    add(upper, "validated_token", CONFIDENCE_VALIDATED_TOKEN)
                    continue
            add(upper, "token", CONFIDENCE_UNVALIDATED)

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
