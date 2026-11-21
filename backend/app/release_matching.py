"""Conservative disambiguation of releases that share a headline."""
from __future__ import annotations

from collections.abc import Iterable
import re
import unicodedata
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Article, RawFeedItem, Source
from app.utils import clean_summary_text

WireIdentity = tuple[str, str, str]


def release_identity(url: str | None, guid: str | None = None,
                     source_code: str | None = None) -> WireIdentity | None:
    try:
        parsed = urlparse(url or "")
    except ValueError:
        return None
    if url and (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password):
        return None
    try:
        if parsed.port not in {None, 80, 443}:
            return None
    except ValueError:
        return None
    host = (parsed.hostname or "").lower()
    if host == "businesswire.com" or host.endswith(".businesswire.com"):
        match = re.match(r"/news/home/(\d+)/([a-z]{2})(?:/|$)", parsed.path, re.I)
        if match:
            return "businesswire", match[1], match[2].lower()
    if host == "prnewswire.com" or host.endswith(".prnewswire.com"):
        match = re.search(r"/news-releases/.+-(\d+)\.html$", parsed.path)
        if match:
            return "prnewswire", match[1], ""
    if host == "globenewswire.com" or host.endswith(".globenewswire.com"):
        match = re.search(r"/news-release/\d{4}/\d{2}/\d{2}/(\d+)/\d+/([a-z]{2})(?:/|$)", parsed.path, re.I)
        if match:
            return "globenewswire", match[1], match[2].lower()
    if source_code in {"businesswire", "financialcontent"}:
        match = re.fullmatch(r"bw:(\d+):([a-z]{2})", guid or "", re.I)
        if match:
            return "businesswire", match[1], match[2].lower()
        if source_code == "businesswire":
            match = re.fullmatch(r"(\d+)([a-z]{2})", guid or "", re.I)
            if match:
                return "businesswire", match[1], match[2].lower()
    return None


def _body_lead(summary: str | None) -> tuple[str, ...] | None:
    value = clean_summary_text(summary) or ""
    # Wire and Yahoo datelines differ even for the same syndicated body.
    value = re.sub(
        r"^[A-Z][A-Z .,']*(?:[A-Z][a-z]+ \d{1,2}, \d{4})?\s*--(?:\(BUSINESS WIRE\)--)?\s*",
        "", value,
    )
    words = re.findall(r"[a-z0-9]+", unicodedata.normalize("NFKC", value).casefold())
    return tuple(words[:24]) if len(words) >= 24 else None


def article_wire_identities(
    articles: list[Article], raw_evidence: Iterable[tuple[int, str | None, str | None, str]],
) -> dict[int, set[WireIdentity]]:
    identities: dict[int, set[WireIdentity]] = {article.id: set() for article in articles}
    for article in articles:
        key = release_identity(article.canonical_url)
        if key is not None:
            identities[article.id].add(key)
    for article_id, link, guid, source_code in raw_evidence:
        key = release_identity(link, guid, source_code)
        if key is not None:
            identities[article_id].add(key)
    return identities


def select_release_candidate(
    db: Session, candidates: list[Article], *,
    identity: WireIdentity | None, summary: str | None,
) -> Article | None:
    if not candidates:
        return None
    if identity is not None:
        rows = db.execute(
            select(RawFeedItem.article_id, RawFeedItem.raw_link,
                   RawFeedItem.raw_guid, Source.code)
            .join(Source, Source.id == RawFeedItem.source_id)
            .where(RawFeedItem.article_id.in_([article.id for article in candidates]))
        )
        identities = article_wire_identities(candidates, rows)
        exact = [a for a in candidates if identities[a.id] == {identity}]
        if exact:
            return min(exact, key=lambda a: a.id)
        # Distinct original wire IDs are distinct releases, even with one title.
        candidates = [a for a in candidates if not identities[a.id]]
    if len(candidates) == 1:
        return candidates[0]
    lead = _body_lead(summary)
    if lead is not None:
        matched = [a for a in candidates if _body_lead(a.summary) == lead]
        if len(matched) == 1:
            return matched[0]
    # Missing or ambiguous evidence must not attach a copy to the newest row.
    return None
