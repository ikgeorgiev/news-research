from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Article, ArticleTicker, PushSubscription, RawFeedItem, Source, Ticker
from app.monitoring import record_push_delivery, record_push_delivery_duration, set_push_active_subscriptions
from app.utils import sha256_str

logger = logging.getLogger(__name__)

ALL_SCOPE_KEY = "all"
GENERAL_UNMAPPED_PROVIDER = "Business Wire"
MAX_ERROR_TEXT_LEN = 500

try:
    from pywebpush import WebPushException, webpush
except Exception:  # pragma: no cover - guarded at runtime
    WebPushException = Exception  # type: ignore[assignment]
    webpush = None


def push_runtime_enabled(settings: Settings) -> bool:
    if webpush is None:
        return False
    return bool(
        (settings.vapid_public_key or "").strip()
        and (settings.vapid_private_key or "").strip()
        and (settings.vapid_contact_email or "").strip()
    )


def hash_manage_token(token: str) -> str:
    return sha256_str(token)


def normalize_scopes(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = payload or {}
    include_all_news = bool(raw.get("include_all_news", True))
    watchlists: list[dict[str, Any]] = []
    raw_watchlists = raw.get("watchlists")
    if isinstance(raw_watchlists, list):
        seen_ids: set[str] = set()
        for item in raw_watchlists:
            if not isinstance(item, dict):
                continue
            watchlist_id = str(item.get("id", "")).strip()
            if not watchlist_id or watchlist_id in seen_ids:
                continue
            seen_ids.add(watchlist_id)

            tickers: list[str] = []
            raw_tickers = item.get("tickers")
            if isinstance(raw_tickers, list):
                seen_tickers: set[str] = set()
                for ticker in raw_tickers:
                    ticker_text = str(ticker).strip().upper()
                    if not ticker_text or ticker_text in seen_tickers:
                        continue
                    seen_tickers.add(ticker_text)
                    tickers.append(ticker_text)

            provider = str(item.get("provider", "")).strip() or None
            q = str(item.get("q", "")).strip() or None
            name = str(item.get("name", "")).strip() or None
            watchlists.append(
                {
                    "id": watchlist_id,
                    "name": name,
                    "tickers": tickers,
                    "provider": provider,
                    "q": q,
                }
            )
    return {
        "include_all_news": include_all_news,
        "watchlists": watchlists,
    }


def _iter_scope_queries(scopes: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    items: list[tuple[str, dict[str, Any]]] = []
    if scopes.get("include_all_news"):
        items.append(
            (
                ALL_SCOPE_KEY,
                {
                    "tickers": None,
                    "provider": None,
                    "q": None,
                    "include_unmapped_from_provider": GENERAL_UNMAPPED_PROVIDER,
                },
            )
        )

    for watchlist in scopes.get("watchlists", []):
        if not isinstance(watchlist, dict):
            continue
        watchlist_id = str(watchlist.get("id", "")).strip()
        if not watchlist_id:
            continue
        items.append(
            (
                f"watchlist:{watchlist_id}",
                {
                    "tickers": watchlist.get("tickers") or None,
                    "provider": watchlist.get("provider") or None,
                    "q": watchlist.get("q") or None,
                    "include_unmapped_from_provider": None,
                },
            )
        )
    return items


def _build_scope_query(
    db: Session,
    *,
    tickers: list[str] | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped_from_provider: str | None = None,
):
    query = select(Article)
    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )

    if tickers:
        ticker_match_exists = (
            select(1)
            .select_from(ArticleTicker)
            .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
            .where(
                and_(
                    ArticleTicker.article_id == Article.id,
                    Ticker.symbol.in_(tickers),
                )
            )
            .correlate(Article)
            .exists()
        )
        query = query.where(ticker_match_exists)
    elif include_unmapped_from_provider:
        include_name = include_unmapped_from_provider.strip()
        include_source_row = db.scalar(select(Source).where(func.lower(Source.name) == include_name.lower()))
        if include_source_row is None:
            query = query.where(mapped_exists)
        else:
            include_provider_exists = (
                select(1)
                .select_from(RawFeedItem)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        RawFeedItem.source_id == include_source_row.id,
                    )
                )
                .correlate(Article)
                .exists()
            )
            query = query.where(or_(mapped_exists, and_(~mapped_exists, include_provider_exists)))
    else:
        query = query.where(mapped_exists)

    if provider:
        provider_text = provider.strip()
        source_row = db.scalar(select(Source).where(func.lower(Source.name) == provider_text.lower()))
        if source_row is not None:
            canonical_source_id = (
                select(RawFeedItem.source_id)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        RawFeedItem.raw_link == Article.canonical_url,
                    )
                )
                .order_by(RawFeedItem.id.desc())
                .limit(1)
                .correlate(Article)
                .scalar_subquery()
            )
            latest_source_id = (
                select(RawFeedItem.source_id)
                .where(RawFeedItem.article_id == Article.id)
                .order_by(RawFeedItem.id.desc())
                .limit(1)
                .correlate(Article)
                .scalar_subquery()
            )
            query = query.where(func.coalesce(canonical_source_id, latest_source_id) == source_row.id)
        else:
            query = query.where(Article.provider_name.ilike(f"%{provider_text}%"))

    if q:
        query = query.where(Article.title.ilike(f"%{q.strip()}%"))

    return query


def _scope_max_article_id(db: Session, scope_params: dict[str, Any]) -> int:
    query = _build_scope_query(
        db,
        tickers=scope_params.get("tickers"),
        provider=scope_params.get("provider"),
        q=scope_params.get("q"),
        include_unmapped_from_provider=scope_params.get("include_unmapped_from_provider"),
    )
    value = db.scalar(query.with_only_columns(func.max(Article.id)).order_by(None))
    return int(value or 0)


def _scope_new_article_ids(
    db: Session,
    *,
    scope_params: dict[str, Any],
    after_id: int,
    limit: int,
) -> list[int]:
    query = _build_scope_query(
        db,
        tickers=scope_params.get("tickers"),
        provider=scope_params.get("provider"),
        q=scope_params.get("q"),
        include_unmapped_from_provider=scope_params.get("include_unmapped_from_provider"),
    )
    rows = db.execute(
        query.with_only_columns(Article.id)
        .where(Article.id > after_id)
        .order_by(Article.id.desc())
        .limit(limit)
    ).all()
    return [int(article_id) for (article_id,) in rows]


def _normalize_last_notified(payload: dict[str, Any] | None) -> dict[str, int]:
    result: dict[str, int] = {}
    if not isinstance(payload, dict):
        return result
    for key, value in payload.items():
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed > 0:
            result[str(key)] = parsed
    return result


def seed_last_notified_watermarks(
    db: Session,
    *,
    scopes: dict[str, Any],
    existing: dict[str, Any] | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    previous = _normalize_last_notified(existing)
    seeded: dict[str, int] = {}
    next_state: dict[str, int] = {}
    for scope_key, scope_params in _iter_scope_queries(scopes):
        prev = previous.get(scope_key)
        if isinstance(prev, int) and prev > 0:
            next_state[scope_key] = prev
            continue
        max_id = _scope_max_article_id(db, scope_params)
        next_state[scope_key] = max_id
        seeded[scope_key] = max_id
    return next_state, seeded


def _article_tickers_map(db: Session, article_ids: list[int]) -> dict[int, list[str]]:
    if not article_ids:
        return {}
    rows = db.execute(
        select(ArticleTicker.article_id, Ticker.symbol)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(ArticleTicker.article_id.in_(article_ids))
        .order_by(ArticleTicker.article_id.asc(), Ticker.symbol.asc())
    ).all()
    mapped: dict[int, list[str]] = {}
    for article_id, symbol in rows:
        mapped.setdefault(int(article_id), []).append(str(symbol))
    return mapped


def _build_payload(db: Session, *, article_ids: list[int], scope_keys: list[str]) -> dict[str, Any]:
    rows = db.scalars(
        select(Article).where(Article.id.in_(article_ids)).order_by(Article.id.desc())
    ).all()
    if not rows:
        return {}

    tickers_by_article = _article_tickers_map(db, [row.id for row in rows])
    all_tickers: list[str] = []
    seen_tickers: set[str] = set()
    for row in rows:
        for ticker in tickers_by_article.get(row.id, []):
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            all_tickers.append(ticker)

    newest = rows[0]
    count = len(rows)
    ticker_text = ", ".join(all_tickers[:5]) if all_tickers else "General"
    body = f"{ticker_text}{'...' if len(all_tickers) > 5 else ''}\n{newest.title}"
    return {
        "kind": "news_alert",
        "title": f"CEF News: {count} new article{'s' if count != 1 else ''}",
        "body": body,
        "url": "/",
        "article_ids": [row.id for row in rows],
        "scope_keys": scope_keys,
        "dedupe_key": f"push:{max(row.id for row in rows)}",
        "tag": "cef-news",
    }


def _build_vapid_sub_claim(raw: str) -> str:
    value = raw.strip()
    if not value:
        return value
    if value.startswith("mailto:") or value.startswith("https://") or value.startswith("http://"):
        return value
    if "@" in value:
        return f"mailto:{value}"
    return value


def _send_push_notification(
    settings: Settings,
    *,
    subscription: PushSubscription,
    payload: dict[str, Any],
) -> tuple[str, str | None]:
    if webpush is None:
        return "error", "pywebpush is not available"

    vapid_private_key = (settings.vapid_private_key or "").strip()
    vapid_contact = _build_vapid_sub_claim((settings.vapid_contact_email or "").strip())

    try:
        webpush(
            subscription_info={
                "endpoint": subscription.endpoint,
                "keys": {
                    "p256dh": subscription.key_p256dh,
                    "auth": subscription.key_auth,
                },
            },
            data=json.dumps(payload, separators=(",", ":")),
            vapid_private_key=vapid_private_key,
            vapid_claims={"sub": vapid_contact},
            timeout=settings.push_send_timeout_seconds,
        )
        return "success", None
    except WebPushException as exc:  # type: ignore[misc]
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        message = str(exc)
        if status_code in (404, 410):
            return "gone", message
        return "error", message
    except Exception as exc:  # pragma: no cover - defensive guard
        return "error", str(exc)


def _truncate_error(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) <= MAX_ERROR_TEXT_LEN:
        return text
    return text[:MAX_ERROR_TEXT_LEN]


def check_and_send_alerts(db: Session, settings: Settings) -> dict[str, int]:
    active_before = db.scalar(
        select(func.count()).select_from(select(PushSubscription.id).where(PushSubscription.active.is_(True)).subquery())
    )
    set_push_active_subscriptions(int(active_before or 0))

    if not push_runtime_enabled(settings):
        return {
            "scanned": 0,
            "sent": 0,
            "failed": 0,
            "deactivated": 0,
        }

    subscriptions = db.scalars(
        select(PushSubscription)
        .where(PushSubscription.active.is_(True))
        .order_by(PushSubscription.updated_at.asc(), PushSubscription.id.asc())
    ).all()

    scanned = 0
    sent = 0
    failed = 0
    deactivated = 0
    max_consecutive_failures = max(
        1,
        int(getattr(settings, "push_max_consecutive_failures", 20) or 20),
    )

    for subscription in subscriptions:
        scanned += 1
        try:
            scopes = normalize_scopes(subscription.alert_scopes_json)
            scope_queries = _iter_scope_queries(scopes)
            if not scope_queries:
                continue

            stable_watermarks, _seeded = seed_last_notified_watermarks(
                db,
                scopes=scopes,
                existing=subscription.last_notified_json,
            )
            advanced_watermarks = dict(stable_watermarks)
            article_ids: set[int] = set()
            touched_scope_keys: list[str] = []

            for scope_key, scope_params in scope_queries:
                previous = int(stable_watermarks.get(scope_key, 0) or 0)
                current_max = _scope_max_article_id(db, scope_params)
                if current_max <= previous:
                    advanced_watermarks[scope_key] = previous
                    continue

                fresh_ids = _scope_new_article_ids(
                    db,
                    scope_params=scope_params,
                    after_id=previous,
                    limit=settings.push_max_per_cycle,
                )
                if fresh_ids:
                    article_ids.update(fresh_ids)
                    touched_scope_keys.append(scope_key)
                advanced_watermarks[scope_key] = current_max

            subscription.alert_scopes_json = scopes
            # Keep stable watermark by default; only advance after successful push.
            subscription.last_notified_json = stable_watermarks

            if not article_ids:
                continue

            payload = _build_payload(
                db,
                article_ids=sorted(article_ids, reverse=True),
                scope_keys=sorted(set(touched_scope_keys)),
            )
            if not payload:
                continue

            started_at = time.perf_counter()
            status, error_text = _send_push_notification(
                settings,
                subscription=subscription,
                payload=payload,
            )
            record_push_delivery_duration(time.perf_counter() - started_at)

            now_utc = datetime.now(timezone.utc)
            if status == "success":
                sent += 1
                subscription.last_notified_json = advanced_watermarks
                subscription.failure_count = 0
                subscription.last_error = None
                subscription.last_success_at = now_utc
                db.execute(
                    update(Article)
                    .where(
                        Article.id.in_(list(article_ids)),
                        Article.first_alert_sent_at.is_(None),
                    )
                    .values(first_alert_sent_at=now_utc)
                )
                record_push_delivery("success")
            elif status == "gone":
                deactivated += 1
                subscription.active = False
                subscription.failure_count = int(subscription.failure_count or 0) + 1
                subscription.last_error = _truncate_error(error_text)
                record_push_delivery("gone")
            else:
                failed += 1
                subscription.failure_count = int(subscription.failure_count or 0) + 1
                subscription.last_error = _truncate_error(error_text)
                if int(subscription.failure_count or 0) >= max_consecutive_failures:
                    subscription.active = False
                    deactivated += 1
                    record_push_delivery("error_deactivated")
                else:
                    record_push_delivery("error")
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception("Push delivery loop failed for subscription id=%s", subscription.id)
            failed += 1
            subscription.failure_count = int(subscription.failure_count or 0) + 1
            subscription.last_error = _truncate_error(str(exc))
            if int(subscription.failure_count or 0) >= max_consecutive_failures:
                subscription.active = False
                deactivated += 1
                record_push_delivery("error_deactivated")
            else:
                record_push_delivery("error")

    db.commit()
    active_after = db.scalar(
        select(func.count()).select_from(select(PushSubscription.id).where(PushSubscription.active.is_(True)).subquery())
    )
    set_push_active_subscriptions(int(active_after or 0))

    return {
        "scanned": scanned,
        "sent": sent,
        "failed": failed,
        "deactivated": deactivated,
    }
