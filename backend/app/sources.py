from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import threading
import time
from urllib.parse import quote_plus
from urllib.parse import urlparse, urlunparse

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import http_client
from app.config import Settings
from app.models import Source, Ticker


POLICY_GENERAL_ALLOWED = "general_allowed"
POLICY_VALIDATED_MAPPING_REQUIRED = "validated_mapping_required"
POLICY_SCOPED_CONTEXT_REQUIRED = "scoped_context_required"

SOURCE_POLICY: dict[str, str] = {
    "businesswire": POLICY_GENERAL_ALLOWED,
    "prnewswire": POLICY_VALIDATED_MAPPING_REQUIRED,
    "globenewswire": POLICY_VALIDATED_MAPPING_REQUIRED,
    "yahoo": POLICY_SCOPED_CONTEXT_REQUIRED,
}


@dataclass(slots=True, frozen=True)
class FeedDef:
    url: str
    persistence_policy_override: str | None = None
    article_source_name: str = ""


PRNEWSWIRE_FEEDS: list[str] = [
    "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mutual-funds-list.rss",
    "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
]

GLOBENEWSWIRE_FEEDS: list[str] = [
    # Trial configuration: use the single broad GlobeNewswire public-company feed
    # instead of the three narrower specialty-business subject feeds.
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
]

BUSINESSWIRE_FEEDS: list[FeedDef] = [
    FeedDef(
        url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
        article_source_name="Business Wire",
    ),
    FeedDef(
        url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override=POLICY_VALIDATED_MAPPING_REQUIRED,
        article_source_name="Business Wire",
    ),
]

FEED_POLICY_REGISTRY: dict[str, list[FeedDef]] = {
    "businesswire": BUSINESSWIRE_FEEDS,
}


@dataclass(slots=True)
class SourceFeed:
    code: str
    name: str
    base_url: str
    feeds: list[FeedDef]


@dataclass(slots=True, frozen=True)
class SourcePageConfig:
    source_code: str
    hostname_suffix: str
    table_match_type: str


PAGE_FETCH_CONFIGS: dict[str, SourcePageConfig] = {
    "businesswire": SourcePageConfig("businesswire", "businesswire.com", "bw_table"),
    "prnewswire": SourcePageConfig("prnewswire", "prnewswire.com", "prn_table"),
    "globenewswire": SourcePageConfig("globenewswire", "globenewswire.com", "gnw_table"),
}


def _canonical_source_article_url(url: str) -> str:
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
    return _canonical_source_article_url(url)


def _normalized_businesswire_fetch_url(url: str) -> str:
    parsed = urlparse(url)
    # Business Wire article slugs occasionally include percent-encoded
    # trademark/registered-mark characters from the feed copy
    # (for example `Liberty-All-Star%C2%AE-...`). Those variants can resolve to
    # a Business Wire error page, while the plain slug without the mark returns
    # the actual article. Normalize only the fetch URL so persistence keys stay
    # stable and existing rows do not split across old/new canonical forms.
    path = parsed.path
    for token in ("%C2%AE", "%c2%ae", "%E2%84%A2", "%e2%84%a2", "®", "™"):
        path = path.replace(token, "")
    # Business Wire RSS links are often `http://...`, but article pages redirect
    # to HTTPS. Page fetches deliberately do not follow redirects, so normalize
    # the fetch URL to HTTPS up front while leaving persisted article URLs alone.
    return urlunparse(("https", parsed.netloc, path, "", "", ""))


def _is_businesswire_article_url(url: str) -> bool:
    return _is_source_article_url(url, "businesswire.com")


def _cache_source_page(
    cache: OrderedDict[str, tuple[float, str | None]],
    cache_lock: threading.Lock,
    *,
    fetch_url: str,
    fetched_at: float,
    html_text: str | None,
    max_items: int,
) -> None:
    with cache_lock:
        cache[fetch_url] = (fetched_at, html_text)
        cache.move_to_end(fetch_url)
        while len(cache) > max_items:
            cache.popitem(last=False)


def _fetch_source_page_html(
    url: str,
    timeout_seconds: int,
    config: SourcePageConfig,
    *,
    cache: OrderedDict[str, tuple[float, str | None]],
    cache_lock: threading.Lock,
    headers: dict[str, str],
    cache_ttl_seconds: int,
    failure_cache_ttl_seconds: int,
    cache_max_items: int,
    now_fn=time.time,
) -> str | None:
    fetch_url = _canonical_source_article_url(url)
    if config.source_code == "businesswire":
        fetch_url = _normalized_businesswire_fetch_url(fetch_url)
    if not _is_source_article_url(fetch_url, config.hostname_suffix):
        return None

    now = now_fn()
    with cache_lock:
        cached = cache.get(fetch_url)
        if cached is not None:
            fetched_at, cached_html = cached
            ttl_seconds = (
                cache_ttl_seconds if cached_html is not None else failure_cache_ttl_seconds
            )
            if now - fetched_at <= ttl_seconds:
                cache.move_to_end(fetch_url)
                return cached_html

    html_text: str | None = None
    try:
        response = http_client.get_http_client().get(
            fetch_url,
            timeout=timeout_seconds,
            headers=headers,
            follow_redirects=False,
        )
        if response.is_success and response.text:
            html_text = response.text
    except (httpx.RequestError, httpx.HTTPStatusError):
        html_text = None

    _cache_source_page(
        cache,
        cache_lock,
        fetch_url=fetch_url,
        fetched_at=now_fn(),
        html_text=html_text,
        max_items=cache_max_items,
    )
    return html_text


def get_source_policy(source_code: str) -> str:
    return SOURCE_POLICY.get(source_code, POLICY_VALIDATED_MAPPING_REQUIRED)


def get_feed_policy_override(source_code: str, feed_url: str | None) -> str | None:
    if not feed_url:
        return None
    for feed in FEED_POLICY_REGISTRY.get(source_code, []):
        if feed.url == feed_url:
            return feed.persistence_policy_override
    return None


def get_effective_source_policy(
    source_code: str,
    *,
    feed_url: str | None = None,
    persistence_policy_override: str | None = None,
) -> str:
    return (
        persistence_policy_override
        or get_feed_policy_override(source_code, feed_url)
        or get_source_policy(source_code)
    )


def get_strict_feed_urls(source_code: str) -> list[str]:
    return [
        feed.url
        for feed in FEED_POLICY_REGISTRY.get(source_code, [])
        if get_effective_source_policy(
            source_code,
            feed_url=feed.url,
            persistence_policy_override=feed.persistence_policy_override,
        )
        != POLICY_GENERAL_ALLOWED
    ]


def get_active_symbols(db: Session) -> list[str]:
    rows = db.scalars(select(Ticker.symbol).where(Ticker.active.is_(True)).order_by(Ticker.symbol.asc())).all()
    return [symbol.upper() for symbol in rows]


def build_yahoo_feed_urls(symbols: list[str], chunk_size: int) -> list[str]:
    if not symbols:
        return []

    urls: list[str] = []
    for index in range(0, len(symbols), chunk_size):
        chunk = symbols[index : index + chunk_size]
        query = quote_plus(",".join(chunk), safe=",")
        urls.append(f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={query}&region=US&lang=en-US")
    return urls


def _build_feed_defs(
    urls: list[str],
    *,
    article_source_name: str,
    persistence_policy_override: str | None = None,
) -> list[FeedDef]:
    return [
        FeedDef(
            url=url,
            persistence_policy_override=persistence_policy_override,
            article_source_name=article_source_name,
        )
        for url in urls
    ]


def build_source_feeds(settings: Settings, db: Session) -> list[SourceFeed]:
    source_feeds: list[SourceFeed] = []

    if settings.source_enable_yahoo:
        symbols = get_active_symbols(db)
        source_feeds.append(
            SourceFeed(
                code="yahoo",
                name="Yahoo Finance",
                base_url="https://feeds.finance.yahoo.com",
                feeds=_build_feed_defs(
                    build_yahoo_feed_urls(symbols, settings.yahoo_chunk_size),
                    article_source_name="Yahoo Finance",
                ),
            )
        )

    if settings.source_enable_prn:
        source_feeds.append(
            SourceFeed(
                code="prnewswire",
                name="PR Newswire",
                base_url="https://www.prnewswire.com",
                feeds=_build_feed_defs(
                    PRNEWSWIRE_FEEDS,
                    article_source_name="PR Newswire",
                ),
            )
        )

    if settings.source_enable_gn:
        source_feeds.append(
            SourceFeed(
                code="globenewswire",
                name="GlobeNewswire",
                base_url="https://rss.globenewswire.com",
                feeds=_build_feed_defs(
                    GLOBENEWSWIRE_FEEDS,
                    article_source_name="GlobeNewswire",
                ),
            )
        )

    if settings.source_enable_bw:
        source_feeds.append(
            SourceFeed(
                code="businesswire",
                name="Business Wire",
                base_url="https://feed.businesswire.com",
                feeds=BUSINESSWIRE_FEEDS,
            )
        )

    return source_feeds


def seed_sources(db: Session, source_feeds: list[SourceFeed]) -> None:
    existing = {
        row.code: row
        for row in db.scalars(select(Source).where(Source.code.in_([src.code for src in source_feeds]))).all()
    }

    changed = False
    for source in source_feeds:
        current = existing.get(source.code)
        if current is None:
            db.add(
                Source(
                    code=source.code,
                    name=source.name,
                    base_url=source.base_url,
                    enabled=True,
                )
            )
            changed = True
        else:
            if current.name != source.name:
                current.name = source.name
                changed = True
            if current.base_url != source.base_url:
                current.base_url = source.base_url
                changed = True

    if changed:
        db.commit()
