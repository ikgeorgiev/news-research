from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.article_filters import build_article_query
from app.config import Settings
from app.models import Article, ArticleTicker, PushSubscription, Ticker
from app.monitoring import record_push_delivery, record_push_delivery_duration, set_push_active_subscriptions
from app.query_utils import iter_chunks
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
    return build_article_query(
        db,
        tickers=tickers,
        provider=provider,
        q=q,
        include_unmapped_from_provider=include_unmapped_from_provider,
    )


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
    rows = []
    for chunk in iter_chunks(article_ids):
        rows.extend(
            db.execute(
                select(ArticleTicker.article_id, Ticker.symbol)
                .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
                .where(ArticleTicker.article_id.in_(chunk))
                .order_by(ArticleTicker.article_id.asc(), Ticker.symbol.asc())
            ).all()
        )
    mapped: dict[int, list[str]] = {}
    for article_id, symbol in rows:
        mapped.setdefault(int(article_id), []).append(str(symbol))
    return mapped


def _build_payload(db: Session, *, article_ids: list[int], scope_keys: list[str]) -> dict[str, Any]:
    rows: list[Article] = []
    for chunk in iter_chunks(article_ids):
        rows.extend(db.scalars(select(Article).where(Article.id.in_(chunk))).all())
    rows.sort(key=lambda row: row.id, reverse=True)
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
                db.commit()
                continue

            payload = _build_payload(
                db,
                article_ids=sorted(article_ids, reverse=True),
                scope_keys=sorted(set(touched_scope_keys)),
            )
            if not payload:
                db.commit()
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
                for chunk in iter_chunks(sorted(article_ids)):
                    db.execute(
                        update(Article)
                        .where(
                            Article.id.in_(chunk),
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
            db.commit()
        except Exception as exc:  # pragma: no cover - defensive guard
            db.rollback()
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
