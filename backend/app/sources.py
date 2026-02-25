from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote_plus

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Source, Ticker


PRNEWSWIRE_FEEDS: list[str] = [
    "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mutual-funds-list.rss",
    "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
]

GLOBENEWSWIRE_FEEDS: list[str] = [
    "https://rss.globenewswire.com/en/RssFeed/subjectcode/12-Dividend%20Reports%20And%20Estimates/feedTitle/Dividend%20Reports%20And%20Estimates",
    "https://rss.globenewswire.com/en/RssFeed/subjectcode/13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/Earnings%20Releases%20And%20Operating%20Results",
    "https://rss.globenewswire.com/en/RssFeed/subjectcode/27-Mergers%20And%20Acquisitions/feedTitle/Mergers%20And%20Acquisitions",
]

BUSINESSWIRE_FEEDS: list[str] = [
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
]


@dataclass(slots=True)
class SourceFeed:
    code: str
    name: str
    base_url: str
    feed_urls: list[str]


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


def build_source_feeds(settings: Settings, db: Session) -> list[SourceFeed]:
    source_feeds: list[SourceFeed] = []

    if settings.source_enable_yahoo:
        symbols = get_active_symbols(db)
        source_feeds.append(
            SourceFeed(
                code="yahoo",
                name="Yahoo Finance",
                base_url="https://feeds.finance.yahoo.com",
                feed_urls=build_yahoo_feed_urls(symbols, settings.yahoo_chunk_size),
            )
        )

    if settings.source_enable_prn:
        source_feeds.append(
            SourceFeed(
                code="prnewswire",
                name="PR Newswire",
                base_url="https://www.prnewswire.com",
                feed_urls=PRNEWSWIRE_FEEDS,
            )
        )

    if settings.source_enable_gn:
        source_feeds.append(
            SourceFeed(
                code="globenewswire",
                name="GlobeNewswire",
                base_url="https://rss.globenewswire.com",
                feed_urls=GLOBENEWSWIRE_FEEDS,
            )
        )

    if settings.source_enable_bw:
        source_feeds.append(
            SourceFeed(
                code="businesswire",
                name="Business Wire",
                base_url="https://feed.businesswire.com",
                feed_urls=BUSINESSWIRE_FEEDS,
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
            current.name = source.name
            current.base_url = source.base_url
            current.enabled = True
            changed = True

    if changed:
        db.commit()
