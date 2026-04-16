from __future__ import annotations

from dataclasses import dataclass
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
    _coerce_symbol_keyword_profile as _coerce_symbol_keyword_profile_impl,
    _matching_validation_keywords as _matching_validation_keywords_impl,
    SymbolKeywordProfile,
    _text_matches_validation_keywords as _text_matches_validation_keywords_impl,
)
from app.sources import (
    get_effective_source_policy,
    POLICY_GENERAL_ALLOWED,
    POLICY_SCOPED_CONTEXT_REQUIRED,
    SourcePageConfig,
    _canonical_source_article_url as _canonical_source_article_url_impl,
    _canonical_businesswire_article_url as _canonical_businesswire_article_url_impl,
    _fetch_source_page_html as _fetch_source_page_html_impl,
    _is_businesswire_article_url as _is_businesswire_article_url_impl,
    _is_source_article_url as _is_source_article_url_impl,
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
    r"\b(?:NYSE(?:\s+(?:American|Arca|MKT))?|NASDAQ|AMEX|OTC(?:QB|QX)?)\s*[:\-]\s*([A-Z]{1,5})\b"
)
PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")
TOKEN_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")
TABLE_CELL_SYMBOL_PATTERN = re.compile(
    r"<td[^>]*>(?:\s|<[^>]+>)*([A-Z][A-Z0-9\.\-]{0,9})(?:\s|<[^>]+>)*</td>",
    flags=re.IGNORECASE,
)
TIME_CONTEXT_BEFORE_PATTERN = re.compile(
    r"\b\d{1,2}(?::\d{2})\s*(?:a\.?m\.?|p\.?m\.?)?\s*(?:\(|\[)?\s*$"
    r"|"
    r"\b\d{1,2}\s*(?:a\.?m\.?|p\.?m\.?)\s*(?:\(|\[)?\s*$",
    re.IGNORECASE,
)
TIME_CONTEXT_AFTER_PATTERN = re.compile(
    r"(?i)^\s*(?:[|/,;\-]\s*)?\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)\b"
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
SYMBOL_CLASS_NORMAL = "normal"
SYMBOL_CLASS_TIMEZONE_OVERLAP = "timezone_overlap"
SYMBOL_CLASS_AMBIGUOUS_ACRONYM = "ambiguous_acronym"
SYMBOL_CLASS_COMMON_NOUN = "common_noun"

EVIDENCE_CHANNEL_CONTEXT = "context"
EVIDENCE_CHANNEL_EXCHANGE = "exchange"
EVIDENCE_CHANNEL_PAREN = "paren"
EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN = "title_summary_token"
EVIDENCE_CHANNEL_BODY_TOKEN = "body_token"
EVIDENCE_CHANNEL_SLUG_TOKEN = "slug_token"
EVIDENCE_CHANNEL_TABLE_SYMBOL = "table_symbol"

COMMON_NOUN_SYMBOLS = {"FUND"}
AMBIGUOUS_ACRONYM_SYMBOLS = {"IDE"}
TIMEZONE_ABBREVIATIONS = {
    "ACDT",
    "ACST",
    "AEDT",
    "AEST",
    "AKDT",
    "AKST",
    "AST",
    "BST",
    "CDT",
    "CEST",
    "CET",
    "CST",
    "EDT",
    "EEST",
    "EET",
    "EST",
    "GMT",
    "HKT",
    "IST",
    "JST",
    "MDT",
    "MST",
    "PDT",
    "PST",
    "SGT",
    "UTC",
}
FINANCE_INTENT_PATTERNS = tuple(
    re.compile(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])")
    for term in (
        "distribution",
        "dividend",
        "notice to shareholders",
        "nav",
        "tender offer",
        "rights offering",
        "monthly",
        "quarterly",
        "webcast",
        "conference call",
    )
)

_source_page_cache: OrderedDict[str, tuple[float, str | None]] = OrderedDict()
_source_page_cache_lock = threading.Lock()


def _text_matches_validation_keywords(
    text_lower: str, keywords: SymbolKeywordProfile | frozenset[str]
) -> bool:
    return _text_matches_validation_keywords_impl(text_lower, keywords)


def _matching_validation_keywords(
    text_lower: str, keywords: SymbolKeywordProfile | frozenset[str]
) -> frozenset[str]:
    return _matching_validation_keywords_impl(text_lower, keywords)


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, SymbolKeywordProfile]:
    return _build_symbol_keywords_impl(ticker_rows)


def _coerce_symbol_keyword_profile(
    keywords: SymbolKeywordProfile | frozenset[str] | None,
) -> SymbolKeywordProfile:
    return _coerce_symbol_keyword_profile_impl(keywords)


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    symbol: str
    channel: str
    timezone_context: bool = False
    table_context_text_lower: str = ""


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


def _source_page_fetch_requires_network(
    url: str,
    config: SourcePageConfig,
    *,
    now_fn=time.time,
) -> bool:
    fetch_url = _canonical_source_article_url_impl(url)
    if not _is_source_article_url(fetch_url, config.hostname_suffix):
        return False

    now = now_fn()
    with _source_page_cache_lock:
        cached = _source_page_cache.get(fetch_url)
        if cached is None:
            return True
        fetched_at, cached_html = cached
        ttl_seconds = (
            SOURCE_PAGE_CACHE_TTL_SECONDS
            if cached_html is not None
            else SOURCE_PAGE_FAILURE_CACHE_TTL_SECONDS
        )
        return now - fetched_at > ttl_seconds


def _extract_table_cell_symbols_from_html(
    html_text: str, known_symbols: set[str]
) -> set[str]:
    hits: set[str] = set()
    for symbol in _extract_table_symbol_contexts_from_html(html_text, known_symbols):
        hits.add(symbol)
    return hits


def _extract_table_symbol_contexts_from_html(
    html_text: str,
    known_symbols: set[str],
) -> dict[str, str]:
    table_pattern = re.compile(r"<table\b[^>]*>.*?</table>", flags=re.IGNORECASE | re.DOTALL)
    row_pattern = re.compile(r"<tr\b[^>]*>.*?</tr>", flags=re.IGNORECASE | re.DOTALL)
    cell_pattern = re.compile(
        r"<(td|th)\b[^>]*>(.*?)</\1>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    symbol_contexts: dict[str, list[str]] = {}

    for table_match in table_pattern.finditer(html_text):
        table_html = table_match.group(0)
        header_texts: list[str] = []
        for row_html in row_pattern.findall(table_html):
            cells = cell_pattern.findall(row_html)
            if not cells:
                continue
            row_texts: list[str] = []
            row_symbols: set[str] = set()
            row_has_header = False
            for tag_name, cell_html in cells:
                cell_text = _html_to_plain_text(cell_html).strip()
                if not cell_text:
                    continue
                row_texts.append(cell_text)
                if tag_name.lower() == "th":
                    row_has_header = True
                upper_text = cell_text.upper()
                if upper_text in known_symbols and upper_text not in STOPWORDS:
                    row_symbols.add(upper_text)
            if row_has_header:
                header_texts.extend(row_texts)
                continue
            if not row_symbols:
                continue
            context_parts = header_texts + row_texts
            context_text_lower = " ".join(context_parts).lower().strip()
            for symbol in row_symbols:
                symbol_contexts.setdefault(symbol, []).append(context_text_lower)

    return {
        symbol: " ".join(contexts).strip()
        for symbol, contexts in symbol_contexts.items()
    }


def _html_to_plain_text(html_text: str) -> str:
    return html_to_plain_text(html_text)


def _strip_noise_elements(html_text: str) -> str:
    return strip_noise_elements(html_text)


def _extract_article_body(html_text: str) -> str | None:
    return extract_article_body(html_text)


def _is_timezone_token_in_time_context(text: str, start: int, end: int) -> bool:
    symbol = text[start:end].upper()
    if symbol not in TIMEZONE_ABBREVIATIONS:
        return False

    before = text[max(0, start - 24) : start]
    after = text[end : min(len(text), end + 24)]
    return bool(
        TIME_CONTEXT_BEFORE_PATTERN.search(before)
        or TIME_CONTEXT_AFTER_PATTERN.search(after)
    )


def _symbol_class(symbol: str) -> str:
    if symbol in COMMON_NOUN_SYMBOLS:
        return SYMBOL_CLASS_COMMON_NOUN
    if symbol in AMBIGUOUS_ACRONYM_SYMBOLS:
        return SYMBOL_CLASS_AMBIGUOUS_ACRONYM
    if symbol in TIMEZONE_ABBREVIATIONS:
        return SYMBOL_CLASS_TIMEZONE_OVERLAP
    return SYMBOL_CLASS_NORMAL


def _is_finance_intent_text(text_lower: str) -> bool:
    return any(pattern.search(text_lower) for pattern in FINANCE_INTENT_PATTERNS)


def _standard_keyword_support(matched_keywords: frozenset[str]) -> bool:
    if not matched_keywords:
        return False
    if any(" " in keyword for keyword in matched_keywords):
        return True
    if any(len(keyword) <= 3 for keyword in matched_keywords):
        return True
    return len(set(matched_keywords)) >= 2


def _has_timezone_title_summary_support(
    profile: SymbolKeywordProfile,
    title_summary_matches: frozenset[str],
) -> bool:
    if not title_summary_matches:
        return False
    has_source_buckets = bool(profile.sponsor_terms or profile.sponsor_phrases)
    if has_source_buckets:
        sponsor_matches = title_summary_matches & (
            profile.sponsor_terms | profile.sponsor_phrases
        )
        if not sponsor_matches:
            return False
    if title_summary_matches & profile.timezone_safe_phrases:
        return True
    if has_source_buckets:
        extra_term_matches = (
            title_summary_matches & profile.timezone_safe_terms
        ) - (title_summary_matches & profile.sponsor_terms)
        return len(extra_term_matches) >= 2
    # Override profile: no sponsor/fund distinction available.
    # Require 3+ distinct term matches (matches old threshold).
    return len(title_summary_matches & profile.timezone_safe_terms) >= 3


def _has_ambiguous_acronym_title_summary_support(
    profile: SymbolKeywordProfile,
    title_summary_matches: frozenset[str],
    title_summary_text_lower: str,
) -> bool:
    if not title_summary_matches or not _is_finance_intent_text(title_summary_text_lower):
        return False
    has_source_buckets = bool(profile.sponsor_terms or profile.sponsor_phrases)
    if has_source_buckets:
        sponsor_matches = title_summary_matches & (
            profile.sponsor_terms | profile.sponsor_phrases
        )
        if not sponsor_matches:
            return False
    phrase_matches = title_summary_matches & profile.all_phrases
    if has_source_buckets:
        extra_terms = (title_summary_matches & profile.all_terms) - (
            title_summary_matches & profile.sponsor_terms
        )
        return bool(phrase_matches or extra_terms)
    # Override profile: no sponsor/fund distinction available.
    # Require phrase match or 2+ distinct term matches.
    return bool(phrase_matches) or len(title_summary_matches & profile.all_terms) >= 2


def _has_ambiguous_acronym_table_support(
    profile: SymbolKeywordProfile,
    title_summary_matches: frozenset[str],
    table_matches: frozenset[str],
) -> bool:
    if not title_summary_matches or not table_matches:
        return False

    has_source_buckets = bool(profile.sponsor_terms or profile.sponsor_phrases)
    if has_source_buckets:
        sponsor_matches = title_summary_matches & (
            profile.sponsor_terms | profile.sponsor_phrases
        )
        if not sponsor_matches:
            return False
        title_phrase_matches = title_summary_matches & profile.all_phrases
        title_extra_terms = (title_summary_matches & profile.all_terms) - (
            title_summary_matches & profile.sponsor_terms
        )
        if not (title_phrase_matches or title_extra_terms):
            return False
        table_phrase_matches = table_matches & profile.all_phrases
        table_extra_terms = (table_matches & profile.all_terms) - (
            table_matches & profile.sponsor_terms
        )
        return bool(table_phrase_matches or table_extra_terms)

    title_phrase_matches = title_summary_matches & profile.all_phrases
    title_term_matches = title_summary_matches & profile.all_terms
    if not (title_phrase_matches or len(title_term_matches) >= 2):
        return False
    table_phrase_matches = table_matches & profile.all_phrases
    table_term_matches = table_matches & profile.all_terms
    return bool(table_phrase_matches or table_term_matches)


def _combined_text_lower(
    title_summary_text_lower: str,
    body_text_lower: str | None,
) -> str:
    if not body_text_lower:
        return title_summary_text_lower
    if not title_summary_text_lower:
        return body_text_lower
    return f"{title_summary_text_lower} {body_text_lower}"


def _accept_candidate(
    candidate: CandidateEvidence,
    *,
    profile: SymbolKeywordProfile,
    title_summary_text_lower: str,
    body_text_lower: str | None,
    profiles_available: bool,
) -> tuple[str, float] | None:
    symbol_class = _symbol_class(candidate.symbol)
    title_summary_matches = _matching_validation_keywords(
        title_summary_text_lower, profile
    )
    body_matches = (
        _matching_validation_keywords(body_text_lower, profile)
        if body_text_lower
        else frozenset()
    )
    table_context_matches = (
        _matching_validation_keywords(candidate.table_context_text_lower, profile)
        if candidate.table_context_text_lower
        else frozenset()
    )
    combined_matches = title_summary_matches | body_matches
    combined_text = _combined_text_lower(title_summary_text_lower, body_text_lower)

    if candidate.channel == EVIDENCE_CHANNEL_CONTEXT:
        return ("context", CONFIDENCE_CONTEXT)
    if candidate.channel == EVIDENCE_CHANNEL_EXCHANGE:
        return ("exchange", CONFIDENCE_EXCHANGE)
    if candidate.channel == EVIDENCE_CHANNEL_PAREN:
        if candidate.symbol in STOPWORDS and not _standard_keyword_support(combined_matches):
            return None
        if not profiles_available:
            return ("paren", CONFIDENCE_PAREN)
        if not profile:
            return ("paren", NO_KEYWORDS_CONFIDENCE)
        if _standard_keyword_support(combined_matches):
            return ("paren", CONFIDENCE_PAREN)
        return ("paren", CONFIDENCE_UNVALIDATED)

    if candidate.channel == EVIDENCE_CHANNEL_TABLE_SYMBOL:
        if not profiles_available:
            return ("table_symbol", CONFIDENCE_TABLE_VALIDATED)
        if not profile:
            return ("table_symbol", NO_KEYWORDS_CONFIDENCE)
        if symbol_class == SYMBOL_CLASS_COMMON_NOUN:
            if _standard_keyword_support(title_summary_matches):
                return ("table_symbol", CONFIDENCE_TABLE_VALIDATED)
            return ("table_symbol", CONFIDENCE_UNVALIDATED)
        if symbol_class == SYMBOL_CLASS_AMBIGUOUS_ACRONYM:
            if _has_ambiguous_acronym_title_summary_support(
                profile, title_summary_matches, title_summary_text_lower
            ) or _has_ambiguous_acronym_table_support(
                profile, title_summary_matches, table_context_matches
            ):
                return ("table_symbol", CONFIDENCE_TABLE_VALIDATED)
            return ("table_symbol", CONFIDENCE_UNVALIDATED)
        if symbol_class == SYMBOL_CLASS_TIMEZONE_OVERLAP:
            if _has_timezone_title_summary_support(profile, title_summary_matches):
                return ("table_symbol", CONFIDENCE_TABLE_VALIDATED)
            return ("table_symbol", CONFIDENCE_UNVALIDATED)
        if _standard_keyword_support(combined_matches):
            return ("table_symbol", CONFIDENCE_TABLE_VALIDATED)
        return ("table_symbol", CONFIDENCE_UNVALIDATED)

    if candidate.symbol in STOPWORDS:
        return None
    if symbol_class == SYMBOL_CLASS_COMMON_NOUN:
        return None
    if candidate.channel == EVIDENCE_CHANNEL_SLUG_TOKEN:
        if symbol_class != SYMBOL_CLASS_NORMAL or candidate.timezone_context:
            return None
        return ("token", CONFIDENCE_UNVALIDATED)
    if symbol_class == SYMBOL_CLASS_AMBIGUOUS_ACRONYM:
        if candidate.channel != EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN:
            return None
        if _has_ambiguous_acronym_title_summary_support(
            profile, title_summary_matches, title_summary_text_lower
        ):
            return ("validated_token", CONFIDENCE_VALIDATED_TOKEN)
        return None
    if symbol_class == SYMBOL_CLASS_TIMEZONE_OVERLAP and candidate.timezone_context:
        if _has_timezone_title_summary_support(profile, title_summary_matches):
            return ("validated_token", CONFIDENCE_VALIDATED_TOKEN)
        return None
    if candidate.channel == EVIDENCE_CHANNEL_BODY_TOKEN:
        if title_summary_matches and _standard_keyword_support(combined_matches):
            return ("validated_token", CONFIDENCE_VALIDATED_TOKEN)
        return ("token", CONFIDENCE_UNVALIDATED)
    if body_text_lower and title_summary_matches and _standard_keyword_support(combined_matches):
        return ("validated_token", CONFIDENCE_VALIDATED_TOKEN)
    if _standard_keyword_support(title_summary_matches):
        return ("validated_token", CONFIDENCE_VALIDATED_TOKEN)
    return ("token", CONFIDENCE_UNVALIDATED)


def _extract_source_fallback_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    timeout_seconds: int,
    config: SourcePageConfig,
    *,
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None = None,
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
    title_summary_text = " ".join(
        [part for part in [title, summary] if part]
    ).strip()
    title_summary_text_lower = (
        title_summary_text.lower() if symbol_keywords is not None else ""
    )
    body_text_lower = plain_text.lower() if symbol_keywords is not None else None
    entry_hits = _extract_entry_tickers(
        title,
        summary,
        link,
        feed_url,
        known_symbols,
        include_token=True,
        symbol_keywords=symbol_keywords,
    )
    rescued_entry_hits = _collect_segment_hits(
        [
            (title, EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN),
            (summary, EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN),
            (link, EVIDENCE_CHANNEL_SLUG_TOKEN),
        ],
        known_symbols,
        symbol_keywords=symbol_keywords,
        title_summary_text_lower=title_summary_text_lower,
        body_text_lower=body_text_lower,
        include_token=True,
    )
    body_hits = _collect_segment_hits(
        [(plain_text, EVIDENCE_CHANNEL_BODY_TOKEN)],
        known_symbols,
        symbol_keywords=symbol_keywords,
        title_summary_text_lower=title_summary_text_lower,
        body_text_lower=body_text_lower,
        include_token=True,
    )
    hits = dict(entry_hits)
    _merge_ticker_hits(hits, rescued_entry_hits)
    _merge_ticker_hits(hits, body_hits)
    hits = {
        symbol: hit
        for symbol, hit in hits.items()
        if hit[1] >= MIN_PERSIST_CONFIDENCE
    }

    for symbol, table_context_text_lower in _extract_table_symbol_contexts_from_html(
        table_html, known_symbols
    ).items():
        profile = _coerce_symbol_keyword_profile(
            symbol_keywords.get(symbol) if symbol_keywords is not None else None
        )
        decision = _accept_candidate(
            CandidateEvidence(
                symbol=symbol,
                channel=EVIDENCE_CHANNEL_TABLE_SYMBOL,
                table_context_text_lower=table_context_text_lower,
            ),
            profile=profile,
            title_summary_text_lower=title_summary_text_lower,
            body_text_lower=body_text_lower,
            profiles_available=symbol_keywords is not None,
        )
        if decision is None:
            continue
        _, confidence = decision
        existing = hits.get(symbol)
        if existing is None or confidence > existing[1]:
            hits[symbol] = (config.table_match_type, confidence)

    return hits


def _collect_segment_hits(
    segments: list[tuple[str, str]],
    known_symbols: set[str],
    *,
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None = None,
    title_summary_text_lower: str = "",
    body_text_lower: str | None = None,
    include_token: bool = True,
) -> dict[str, tuple[str, float]]:
    hits: dict[str, tuple[str, float]] = {}

    def add(symbol: str, match_type: str, confidence: float) -> None:
        if symbol not in known_symbols:
            return
        existing = hits.get(symbol)
        if existing is None or confidence > existing[1]:
            hits[symbol] = (match_type, confidence)

    for segment, token_channel in segments:
        if not segment:
            continue
        for symbol in EXCHANGE_PATTERN.findall(segment):
            upper = symbol.upper()
            decision = _accept_candidate(
                CandidateEvidence(symbol=upper, channel=EVIDENCE_CHANNEL_EXCHANGE),
                profile=_coerce_symbol_keyword_profile(
                    symbol_keywords.get(upper) if symbol_keywords is not None else None
                ),
                title_summary_text_lower=title_summary_text_lower,
                body_text_lower=body_text_lower,
                profiles_available=symbol_keywords is not None,
            )
            if decision is not None:
                add(upper, *decision)

        for match in PAREN_SYMBOL_PATTERN.finditer(segment):
            upper = match.group(1).upper()
            if _is_timezone_token_in_time_context(segment, match.start(1), match.end(1)):
                continue
            decision = _accept_candidate(
                CandidateEvidence(symbol=upper, channel=EVIDENCE_CHANNEL_PAREN),
                profile=_coerce_symbol_keyword_profile(
                    symbol_keywords.get(upper) if symbol_keywords is not None else None
                ),
                title_summary_text_lower=title_summary_text_lower,
                body_text_lower=body_text_lower,
                profiles_available=symbol_keywords is not None,
            )
            if decision is not None:
                add(upper, *decision)

        if not include_token:
            continue

        for match in TOKEN_PATTERN.finditer(segment):
            upper = match.group(0).upper()
            decision = _accept_candidate(
                CandidateEvidence(
                    symbol=upper,
                    channel=token_channel,
                    timezone_context=_is_timezone_token_in_time_context(
                        segment, match.start(), match.end()
                    ),
                ),
                profile=_coerce_symbol_keyword_profile(
                    symbol_keywords.get(upper) if symbol_keywords is not None else None
                ),
                title_summary_text_lower=title_summary_text_lower,
                body_text_lower=body_text_lower,
                profiles_available=symbol_keywords is not None,
            )
            if decision is not None:
                add(upper, *decision)

    return hits


def _extract_entry_tickers(
    title: str,
    summary: str,
    link: str,
    feed_url: str,
    known_symbols: set[str],
    *,
    include_token: bool = True,
    symbol_keywords: dict[str, SymbolKeywordProfile | frozenset[str]] | None = None,
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

    title_summary_text_lower = (
        " ".join([part for part in [title, summary] if part]).lower()
        if symbol_keywords is not None
        else ""
    )
    segment_hits = _collect_segment_hits(
        [
            (title, EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN),
            (summary, EVIDENCE_CHANNEL_TITLE_SUMMARY_TOKEN),
            (link, EVIDENCE_CHANNEL_SLUG_TOKEN),
        ],
        known_symbols,
        symbol_keywords=symbol_keywords,
        title_summary_text_lower=title_summary_text_lower,
        body_text_lower=None,
        include_token=include_token,
    )
    _merge_ticker_hits(hits, segment_hits)
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
    policy = get_effective_source_policy(source_code)
    return {
        symbol: hit
        for symbol, hit in hits.items()
        if hit[1] >= MIN_PERSIST_CONFIDENCE
        and (hit[0] != "context" or policy == POLICY_SCOPED_CONTEXT_REQUIRED)
    }


def _should_persist_entry(
    source_code: str,
    ticker_hits: dict[str, tuple[str, float]],
    *,
    persistence_policy_override: str | None = None,
) -> bool:
    policy = get_effective_source_policy(
        source_code,
        persistence_policy_override=persistence_policy_override,
    )
    if policy == POLICY_GENERAL_ALLOWED:
        return True
    if not ticker_hits:
        return False
    return bool(_verified_ticker_hits(source_code, ticker_hits))
