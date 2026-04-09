from __future__ import annotations

from dataclasses import dataclass
import re
from functools import lru_cache

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Ticker


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

_GENERIC_PRNEWSWIRE_BODY_KEYWORDS = frozenset(
    {
        "asset",
        "assets",
        "bond",
        "bonds",
        "capital",
        "credit",
        "debt",
        "emerging",
        "equity",
        "fund",
        "global",
        "income",
        "market",
        "markets",
        "muni",
        "municipal",
        "opportunities",
        "opportunity",
        "premium",
        "return",
        "strategic",
        "strategy",
        "total",
        "trust",
        "yield",
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
    return re.compile(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])")


def _matching_validation_keywords(
    text_lower: str, keywords: frozenset[str]
) -> frozenset[str]:
    return frozenset(
        keyword
        for keyword in keywords
        if _validation_keyword_pattern(keyword).search(text_lower) is not None
    )


def _text_matches_validation_keywords(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    matched_keywords = _matching_validation_keywords(text_lower, keywords)
    if not matched_keywords:
        return False
    if any(" " in keyword for keyword in matched_keywords):
        return True
    if any(len(keyword) <= 3 for keyword in matched_keywords):
        return True
    return len(set(matched_keywords)) >= 2


def _has_phrase_or_short_validation_keyword_match(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    matched_keywords = _matching_validation_keywords(text_lower, keywords)
    if not matched_keywords:
        return False
    return any(" " in keyword or len(keyword) <= 3 for keyword in matched_keywords)


def _has_phrase_short_or_two_keyword_override_match(
    text_lower: str, keywords: frozenset[str]
) -> bool:
    matched_keywords = _matching_validation_keywords(text_lower, keywords)
    if not matched_keywords:
        return False
    if any(" " in keyword or len(keyword) <= 3 for keyword in matched_keywords):
        return True
    distinct_matches = set(matched_keywords)
    if len(distinct_matches) < 2:
        return False
    if len(keywords) == 2:
        return True
    return any(
        keyword not in _GENERIC_PRNEWSWIRE_BODY_KEYWORDS
        for keyword in distinct_matches
    )


def _normalize_validation_keywords(raw_value: str | None) -> frozenset[str]:
    if not raw_value:
        return frozenset()
    return frozenset(
        keyword
        for keyword in (part.strip().lower() for part in raw_value.split(","))
        if keyword
    )


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, frozenset[str]]:
    result: dict[str, frozenset[str]] = {}
    for row in ticker_rows:
        symbol = row[1]
        symbol_lower = symbol.lower()
        fund_name = row[2] if len(row) > 2 else None
        sponsor = row[3] if len(row) > 3 else None
        validation_kw_raw = row[4] if len(row) > 4 else None

        override_keywords = _normalize_validation_keywords(validation_kw_raw)
        if override_keywords:
            result[symbol.upper()] = override_keywords
            continue

        keywords: set[str] = set()
        cleaned_fund_words: list[str] = []
        if fund_name:
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
                    len(cleaned) >= 4 and cleaned not in _FUND_KEYWORD_STOPWORDS
                )
                if is_distinctive_keyword:
                    keywords.add(cleaned)
                elif is_short_keyword:
                    keywords.add(cleaned)
                word_supports_phrase.append(
                    is_distinctive_keyword or is_short_keyword
                )
            for idx in range(len(cleaned_fund_words) - 1):
                if not (word_supports_phrase[idx] or word_supports_phrase[idx + 1]):
                    continue
                keywords.add(
                    f"{cleaned_fund_words[idx]} {cleaned_fund_words[idx + 1]}"
                )
        if sponsor:
            sponsor_distinctive_words: list[str] = []
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
                elif (
                    len(cleaned) >= 4
                    and cleaned not in _FUND_KEYWORD_STOPWORDS
                ):
                    sponsor_distinctive_words.append(cleaned)

            sponsor_words = [
                word.strip(".,;()\"'").lower() for word in sponsor.split()
            ]
            sponsor_words = [word for word in sponsor_words if word]
            brand_words: list[str] = []
            for idx, sponsor_word in enumerate(sponsor_words):
                if idx < len(cleaned_fund_words) and cleaned_fund_words[idx] == sponsor_word:
                    brand_words.append(sponsor_word)
                else:
                    break
            if len(brand_words) >= 2:
                keywords.add(" ".join(brand_words))
            elif not keywords and len(sponsor_distinctive_words) == 2:
                keywords.add(" ".join(sponsor_distinctive_words))

        result[symbol.upper()] = frozenset(keywords)
    return result


@dataclass(frozen=True, slots=True)
class TickerContext:
    symbol_to_id: dict[str, int]
    id_to_symbol: dict[int, str]
    known_symbols: frozenset[str]
    symbol_keywords: dict[str, frozenset[str]]


def load_ticker_context(db: Session) -> TickerContext:
    ticker_rows = db.execute(
        select(
            Ticker.id,
            Ticker.symbol,
            Ticker.fund_name,
            Ticker.sponsor,
            Ticker.validation_keywords,
        ).where(Ticker.active.is_(True))
    ).all()
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    return TickerContext(
        symbol_to_id=symbol_to_id,
        id_to_symbol={row[0]: row[1].upper() for row in ticker_rows},
        known_symbols=frozenset(symbol_to_id.keys()),
        symbol_keywords=_build_symbol_keywords(ticker_rows),
    )
