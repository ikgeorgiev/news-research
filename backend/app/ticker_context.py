from __future__ import annotations

from dataclasses import dataclass
import re
from functools import lru_cache
from typing import AbstractSet, Iterable

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
_TIMEZONE_KEYWORD_EDGE_STOPWORDS = frozenset({"and", "of", "for", "the"})


@dataclass(frozen=True, slots=True, eq=False)
class SymbolKeywordProfile:
    fund_terms: frozenset[str] = frozenset()
    sponsor_terms: frozenset[str] = frozenset()
    fund_phrases: frozenset[str] = frozenset()
    sponsor_phrases: frozenset[str] = frozenset()
    fund_title_phrases: frozenset[str] = frozenset()
    all_terms: frozenset[str] = frozenset()
    all_phrases: frozenset[str] = frozenset()
    all_keywords: frozenset[str] = frozenset()
    timezone_safe_terms: frozenset[str] = frozenset()
    timezone_safe_phrases: frozenset[str] = frozenset()

    def __contains__(self, item: object) -> bool:
        return item in self.all_keywords

    def __iter__(self):
        return iter(self.all_keywords)

    def __len__(self) -> int:
        return len(self.all_keywords)

    def __bool__(self) -> bool:
        return bool(self.all_keywords)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SymbolKeywordProfile):
            return (
                self.fund_terms == other.fund_terms
                and self.sponsor_terms == other.sponsor_terms
                and self.fund_phrases == other.fund_phrases
                and self.sponsor_phrases == other.sponsor_phrases
                and self.fund_title_phrases == other.fund_title_phrases
                and self.all_terms == other.all_terms
                and self.all_phrases == other.all_phrases
                and self.all_keywords == other.all_keywords
                and self.timezone_safe_terms == other.timezone_safe_terms
                and self.timezone_safe_phrases == other.timezone_safe_phrases
            )
        if isinstance(other, (set, frozenset)):
            return self.all_keywords == frozenset(other)
        return NotImplemented


def _timezone_safe_phrases(
    phrases: Iterable[str],
) -> frozenset[str]:
    return frozenset(
        phrase
        for phrase in phrases
        if phrase
        and phrase.split()[0] not in _TIMEZONE_KEYWORD_EDGE_STOPWORDS
        and phrase.split()[-1] not in _TIMEZONE_KEYWORD_EDGE_STOPWORDS
    )


def _split_override_keywords(
    keywords: frozenset[str],
) -> tuple[frozenset[str], frozenset[str]]:
    terms = frozenset(keyword for keyword in keywords if " " not in keyword)
    phrases = frozenset(keyword for keyword in keywords if " " in keyword)
    return terms, phrases


def _profile_from_keyword_groups(
    *,
    fund_terms: AbstractSet[str] | None = None,
    sponsor_terms: AbstractSet[str] | None = None,
    fund_phrases: AbstractSet[str] | None = None,
    sponsor_phrases: AbstractSet[str] | None = None,
    fund_title_phrases: AbstractSet[str] | None = None,
    override_keywords: AbstractSet[str] | None = None,
) -> SymbolKeywordProfile:
    fund_title_phrases_frozen = frozenset(fund_title_phrases or ())
    if override_keywords is not None:
        override = frozenset(override_keywords)
        override_terms, override_phrases = _split_override_keywords(override)
        return SymbolKeywordProfile(
            fund_title_phrases=fund_title_phrases_frozen,
            all_terms=override_terms,
            all_phrases=override_phrases,
            all_keywords=override,
            timezone_safe_terms=override_terms,
            timezone_safe_phrases=_timezone_safe_phrases(override_phrases),
        )

    fund_terms_frozen = frozenset(fund_terms or ())
    sponsor_terms_frozen = frozenset(sponsor_terms or ())
    fund_phrases_frozen = frozenset(fund_phrases or ())
    sponsor_phrases_frozen = frozenset(sponsor_phrases or ())
    all_terms = fund_terms_frozen | sponsor_terms_frozen
    all_phrases = fund_phrases_frozen | sponsor_phrases_frozen
    return SymbolKeywordProfile(
        fund_terms=fund_terms_frozen,
        sponsor_terms=sponsor_terms_frozen,
        fund_phrases=fund_phrases_frozen,
        sponsor_phrases=sponsor_phrases_frozen,
        fund_title_phrases=fund_title_phrases_frozen,
        all_terms=all_terms,
        all_phrases=all_phrases,
        all_keywords=all_terms | all_phrases,
        timezone_safe_terms=all_terms,
        timezone_safe_phrases=_timezone_safe_phrases(all_phrases),
    )


def _coerce_symbol_keyword_profile(
    keywords: SymbolKeywordProfile | frozenset[str] | None,
) -> SymbolKeywordProfile:
    if isinstance(keywords, SymbolKeywordProfile):
        return keywords
    if keywords is None:
        return SymbolKeywordProfile()
    return _profile_from_keyword_groups(override_keywords=keywords)


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
    text_lower: str, keywords: SymbolKeywordProfile | frozenset[str]
) -> frozenset[str]:
    profile = _coerce_symbol_keyword_profile(keywords)
    return frozenset(
        keyword
        for keyword in profile.all_keywords
        if _validation_keyword_pattern(keyword).search(text_lower) is not None
    )


def _text_matches_validation_keywords(
    text_lower: str, keywords: SymbolKeywordProfile | frozenset[str]
) -> bool:
    matched_keywords = _matching_validation_keywords(text_lower, keywords)
    if not matched_keywords:
        return False
    if any(" " in keyword for keyword in matched_keywords):
        return True
    if any(len(keyword) <= 3 for keyword in matched_keywords):
        return True
    return len(set(matched_keywords)) >= 2


def _normalize_validation_keywords(raw_value: str | None) -> frozenset[str]:
    if not raw_value:
        return frozenset()
    return frozenset(
        keyword
        for keyword in (part.strip().lower() for part in raw_value.split(","))
        if keyword
    )


_FUND_TITLE_WORD_REPLACEMENTS = {
    "fd": "fund",
    "grd": "grade",
    "invstm": "investment",
    "opps": "opportunities",
    "opp": "opportunity",
    "tr": "trust",
}
_FUND_TITLE_CORPORATE_SUFFIXES = frozenset({"inc", "llc", "ltd", "plc"})
_FUND_TITLE_GENERIC_TRAILING_WORDS = frozenset({"fund", "trust"})


def _normalize_fund_title_phrase(raw_value: str | None) -> str:
    if not raw_value:
        return ""
    words = re.findall(r"[a-z0-9]+", raw_value.lower())
    expanded = [_FUND_TITLE_WORD_REPLACEMENTS.get(word, word) for word in words]
    return " ".join(expanded).strip()


def _fund_title_phrase_variants(fund_name: str | None) -> frozenset[str]:
    normalized = _normalize_fund_title_phrase(fund_name)
    if not normalized:
        return frozenset()

    variants = {normalized}
    words = normalized.split()
    while words and words[-1] in _FUND_TITLE_CORPORATE_SUFFIXES:
        words = words[:-1]
        if words:
            variants.add(" ".join(words))
    while words and words[-1] in _FUND_TITLE_GENERIC_TRAILING_WORDS:
        words = words[:-1]
        if words:
            variants.add(" ".join(words))

    return frozenset(
        phrase
        for phrase in variants
        if len(phrase.split()) >= 3
    )


def _build_symbol_keywords(
    ticker_rows: list,
) -> dict[str, SymbolKeywordProfile]:
    result: dict[str, SymbolKeywordProfile] = {}
    for row in ticker_rows:
        symbol = row[1]
        symbol_lower = symbol.lower()
        fund_name = row[2] if len(row) > 2 else None
        sponsor = row[3] if len(row) > 3 else None
        validation_kw_raw = row[4] if len(row) > 4 else None

        override_keywords = _normalize_validation_keywords(validation_kw_raw)
        if override_keywords:
            result[symbol.upper()] = _profile_from_keyword_groups(
                fund_title_phrases=_fund_title_phrase_variants(fund_name),
                override_keywords=override_keywords
            )
            continue

        fund_terms: set[str] = set()
        fund_phrases: set[str] = set()
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
                    fund_terms.add(cleaned)
                elif is_short_keyword:
                    fund_terms.add(cleaned)
                word_supports_phrase.append(
                    is_distinctive_keyword or is_short_keyword
                )
            for idx in range(len(cleaned_fund_words) - 1):
                if not (word_supports_phrase[idx] or word_supports_phrase[idx + 1]):
                    continue
                fund_phrases.add(
                    f"{cleaned_fund_words[idx]} {cleaned_fund_words[idx + 1]}"
                )
        sponsor_terms: set[str] = set()
        sponsor_phrases: set[str] = set()
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
                    sponsor_terms.add(cleaned)
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
            sponsor_terms.update(
                word
                for word in brand_words
                if len(word) >= 4 and word not in _FUND_KEYWORD_STOPWORDS
            )
            if len(brand_words) >= 2:
                sponsor_phrases.add(" ".join(brand_words))
            elif not sponsor_terms and not sponsor_phrases and len(sponsor_distinctive_words) == 2:
                sponsor_phrases.add(" ".join(sponsor_distinctive_words))

        result[symbol.upper()] = _profile_from_keyword_groups(
            fund_terms=fund_terms,
            sponsor_terms=sponsor_terms,
            fund_phrases=fund_phrases,
            sponsor_phrases=sponsor_phrases,
            fund_title_phrases=_fund_title_phrase_variants(fund_name),
        )
    return result


@dataclass(frozen=True, slots=True)
class TickerContext:
    symbol_to_id: dict[str, int]
    id_to_symbol: dict[int, str]
    known_symbols: frozenset[str]
    symbol_keywords: dict[str, SymbolKeywordProfile]


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
