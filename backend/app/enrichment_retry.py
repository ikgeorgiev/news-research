"""Durable pending page work stored with the original raw feed item."""
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from app.models import RawFeedItem
from app.utils import parse_datetime, to_utc

RETRY_KEY = "_page_enrichment"
MAX_PENDING_PER_POLL = 3


def retry_is_due(retry: dict | None, now: datetime) -> bool:
    if retry and retry.get("control_state") == "held":
        return False
    due = parse_datetime(retry.get("next_retry_at")) if retry else None
    return due is None or to_utc(due) <= now


def pending_retry(previous: dict | None, *, attempted: bool, now: datetime, outcome=None) -> dict:
    if outcome is not None:
        attempted = outcome.attempted
    if previous and not attempted and outcome is None:
        return dict(previous)
    attempts = int((previous or {}).get("attempts", 0)) + int(attempted)
    delay = min(3600, 60 * 2 ** min(max(attempts - 1, 0), 6)) if attempted else 0
    result = {**(previous or {}), "attempts": attempts,
              "queued_at": (previous or {}).get("queued_at", now.isoformat()),
              "next_retry_at": (now + timedelta(seconds=max(delay,60 if outcome else 0))).isoformat()}
    if outcome is not None:
        result["page_outcome"] = outcome.kind
        if outcome.status is not None:result["last_http_status"] = outcome.status
        if outcome.kind in {"refused", "unknown_in_flight"}:
            result["control_state"] = "held"
            if (previous or {}).get("next_retry_at"):
                result.setdefault("previous_next_retry_at", previous["next_retry_at"])
            result["next_retry_at"] = None
        elif outcome.retry_at is not None:
            existing = parse_datetime(result["next_retry_at"])
            result["next_retry_at"] = max(to_utc(existing), to_utc(outcome.retry_at)).isoformat()
    return result


def due_enrichment_entries(db: Session, source_id: int, feed_url: str) -> list[dict]:
    # This is separate from the HTTP feed validators. A 304 cannot complete
    # unresolved page work, and a later RSS snapshot may no longer contain it.
    due = RawFeedItem.raw_payload_json[RETRY_KEY]["next_retry_at"].as_string()
    rows = db.scalars(select(RawFeedItem).where(
        RawFeedItem.source_id == source_id,
        RawFeedItem.feed_url == feed_url,
        RawFeedItem.article_id.is_(None),
        or_(RawFeedItem.raw_payload_json[RETRY_KEY]["control_state"].as_string().is_(None),
            RawFeedItem.raw_payload_json[RETRY_KEY]["control_state"].as_string() != "held"),
        due <= datetime.now(timezone.utc).isoformat(),
    ).order_by(due.asc(), RawFeedItem.id.asc()).limit(MAX_PENDING_PER_POLL)).all()
    entries = []
    for row in rows:
        payload = row.raw_payload_json or {}
        entries.append({
            "id": row.raw_guid, "title": row.raw_title, "link": row.raw_link,
            "published": to_utc(row.raw_pub_date).isoformat() if row.raw_pub_date else None,
            "summary": payload.get("summary"), "source": payload.get("source"),
            "language": payload.get("language"),
            "_enrichment_queued_at": payload[RETRY_KEY].get("queued_at"),
        })
    return entries
