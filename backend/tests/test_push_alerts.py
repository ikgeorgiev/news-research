from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.ingestion import run_ingestion_cycle
from app.routes.push import delete_push_subscription, get_push_vapid_key, upsert_push_subscription
from app.models import Article, ArticleTicker, PushSubscription, Ticker
from app.push_alerts import check_and_send_alerts
from app.schemas import (
    PushAlertScopes,
    PushDeleteRequest,
    PushSubscriptionKeys,
    PushSubscriptionPayload,
    PushUpsertRequest,
)
from app.sources import SourceFeed
from app.utils import sha256_str


def _seed_mapped_article(db: Session, *, slug: str) -> Article:
    ticker = db.scalar(select(Ticker).where(Ticker.symbol == "GOF"))
    if ticker is None:
        ticker = Ticker(symbol="GOF", active=True)
        db.add(ticker)
        db.commit()
        db.refresh(ticker)

    article = Article(
        canonical_url=f"https://example.com/{slug}",
        canonical_url_hash=sha256_str(f"https://example.com/{slug}"),
        title=f"Title {slug}",
        summary=f"Summary {slug}",
        published_at=datetime.now(timezone.utc),
        source_name="Test Source",
        provider_name="Test Provider",
        content_hash=sha256_str(f"content-{slug}"),
        title_normalized_hash=sha256_str(f"title-{slug}"),
        cluster_key=sha256_str(f"cluster-{slug}"),
    )
    db.add(article)
    db.commit()
    db.refresh(article)

    db.add(ArticleTicker(article_id=article.id, ticker_id=ticker.id))
    db.commit()
    db.refresh(article)
    return article


def _seed_article_with_ticker(
    db: Session,
    *,
    slug: str,
    symbol: str,
    active: bool,
) -> Article:
    ticker = db.scalar(select(Ticker).where(Ticker.symbol == symbol))
    if ticker is None:
        ticker = Ticker(symbol=symbol, active=active)
        db.add(ticker)
        db.commit()
        db.refresh(ticker)

    article = Article(
        canonical_url=f"https://example.com/{slug}",
        canonical_url_hash=sha256_str(f"https://example.com/{slug}"),
        title=f"Title {slug}",
        summary=f"Summary {slug}",
        published_at=datetime.now(timezone.utc),
        source_name="Test Source",
        provider_name="Test Provider",
        content_hash=sha256_str(f"content-{slug}"),
        title_normalized_hash=sha256_str(f"title-{slug}"),
        cluster_key=sha256_str(f"cluster-{slug}"),
    )
    db.add(article)
    db.commit()
    db.refresh(article)

    db.add(ArticleTicker(article_id=article.id, ticker_id=ticker.id))
    db.commit()
    db.refresh(article)
    return article


def _push_payload(*, endpoint: str, manage_token: str | None = None) -> PushUpsertRequest:
    return PushUpsertRequest(
        subscription=PushSubscriptionPayload(
            endpoint=endpoint,
            expiration_time=None,
            keys=PushSubscriptionKeys(
                p256dh="p256dh-key",
                auth="auth-key",
            ),
        ),
        scopes=PushAlertScopes(
            include_all_news=True,
            watchlists=[],
        ),
        manage_token=manage_token,
    )


def test_get_push_vapid_key_disabled_when_not_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.routes.push.push_runtime_enabled", lambda _settings: False)
    response = get_push_vapid_key()
    assert response.enabled is False
    assert response.public_key is None


def test_upsert_push_subscription_seeds_and_requires_manage_token(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    article = _seed_mapped_article(db, slug="seeded")

    monkeypatch.setattr("app.routes.push.push_runtime_enabled", lambda _settings: True)

    created = upsert_push_subscription(
        _push_payload(endpoint="https://push.example/sub-a"),
        db=db,
    )
    assert created.created is True
    assert created.manage_token is not None
    assert created.seeded_last_notified.get("all") == article.id

    with pytest.raises(HTTPException) as exc:
        upsert_push_subscription(
            _push_payload(endpoint="https://push.example/sub-a"),
            db=db,
        )
    assert exc.value.status_code == 401

    updated = upsert_push_subscription(
        _push_payload(
            endpoint="https://push.example/sub-a",
            manage_token=created.manage_token,
        ),
        db=db,
    )
    assert updated.created is False
    assert updated.id == created.id
    assert updated.manage_token == created.manage_token


def test_delete_push_subscription_requires_valid_manage_token(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    monkeypatch.setattr("app.routes.push.push_runtime_enabled", lambda _settings: True)

    created = upsert_push_subscription(
        _push_payload(endpoint="https://push.example/sub-delete"),
        db=db,
    )
    assert created.manage_token is not None

    with pytest.raises(HTTPException) as exc:
        delete_push_subscription(
            PushDeleteRequest(
                endpoint="https://push.example/sub-delete",
                manage_token="wrong-token",
            ),
            db=db,
        )
    assert exc.value.status_code == 401

    response = delete_push_subscription(
        PushDeleteRequest(
            endpoint="https://push.example/sub-delete",
            manage_token=created.manage_token,
        ),
        db=db,
    )
    assert response.deleted is True
    assert db.scalar(select(PushSubscription).where(PushSubscription.id == created.id)) is None


def test_check_and_send_alerts_deactivates_gone_subscription(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    first = _seed_mapped_article(db, slug="push-gone-1")
    second = _seed_mapped_article(db, slug="push-gone-2")

    sub = PushSubscription(
        endpoint="https://push.example/sub-gone",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    monkeypatch.setattr("app.push_alerts._send_push_notification", lambda *_args, **_kwargs: ("gone", "410 Gone"))

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=25,
        push_max_consecutive_failures=20,
    )
    stats = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)

    assert stats["scanned"] == 1
    assert stats["deactivated"] == 1
    assert sub.active is False
    assert sub.failure_count == 1
    assert second.id > first.id


def test_run_ingestion_cycle_continues_when_push_alerts_fail(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    settings_obj = SimpleNamespace(
        tickers_csv_path="data/cef_tickers.csv",
        source_enable_yahoo=False,
        source_enable_prn=False,
        source_enable_gn=False,
        source_enable_bw=True,
        yahoo_chunk_size=40,
        request_timeout_seconds=5,
        feed_fetch_max_attempts=1,
        feed_fetch_backoff_seconds=0.0,
        feed_fetch_backoff_jitter_seconds=0.0,
        feed_failure_backoff_base_seconds=30.0,
        feed_failure_backoff_max_seconds=600.0,
        raw_feed_retention_days=30,
        raw_feed_prune_batch_size=5000,
        raw_feed_prune_max_batches=1,
        raw_feed_prune_interval_seconds=3600,
        ingestion_stale_run_timeout_seconds=3600,
        ingestion_max_workers=1,
        ingestion_enable_conditional_get=False,
    )

    def fake_build_source_feeds(_settings, _db):
        return [
            SourceFeed(
                code="businesswire",
                name="Business Wire",
                base_url="https://feed.businesswire.com",
                feed_urls=["https://example.com/bw/1"],
            )
        ]

    def fake_load_tickers(_db: Session, _path: str):
        return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

    def fake_ingest_feed(*_args, **_kwargs):
        return {
            "source": "businesswire",
            "feed_url": "https://example.com/bw/1",
            "status": "success",
            "items_seen": 1,
            "items_inserted": 1,
            "error": None,
        }

    monkeypatch.setattr("app.ingestion.build_source_feeds", fake_build_source_feeds)
    monkeypatch.setattr("app.feed_runtime.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.ingestion.ingest_feed", fake_ingest_feed)
    monkeypatch.setattr("app.ingestion._should_run_raw_feed_prune", lambda _interval: False)
    monkeypatch.setattr(
        "app.ingestion.check_and_send_alerts",
        lambda _db, _settings: (_ for _ in ()).throw(RuntimeError("push failure")),
    )

    result = run_ingestion_cycle(db, settings_obj)

    assert result["total_items_inserted"] == 1
    assert result["failed_feeds"] == 0
    assert result["push_alerts"] == {
        "scanned": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
    }


def test_check_and_send_alerts_does_not_advance_watermark_on_transient_error(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    first = _seed_mapped_article(db, slug="retry-1")
    second = _seed_mapped_article(db, slug="retry-2")

    sub = PushSubscription(
        endpoint="https://push.example/sub-retry",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=25,
        push_max_consecutive_failures=20,
    )

    monkeypatch.setattr("app.push_alerts._send_push_notification", lambda *_args, **_kwargs: ("error", "timeout"))
    first_try = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)

    assert first_try["failed"] == 1
    assert int(sub.last_notified_json.get("all", 0) or 0) == first.id

    monkeypatch.setattr("app.push_alerts._send_push_notification", lambda *_args, **_kwargs: ("success", None))
    second_try = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)
    second_after = db.scalar(select(Article).where(Article.id == second.id))

    assert second_try["sent"] == 1
    assert int(sub.last_notified_json.get("all", 0) or 0) == second.id
    assert second_after is not None
    assert second_after.first_alert_sent_at is not None


def test_check_and_send_alerts_advances_each_scope_only_through_delivered_ids(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    seeded_articles = [_seed_mapped_article(db, slug=f"backlog-{idx}") for idx in range(6)]

    sub = PushSubscription(
        endpoint="https://push.example/sub-backlog",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={
            "include_all_news": True,
            "watchlists": [
                {
                    "id": "wl-1",
                    "name": "GOF",
                    "tickers": ["GOF"],
                }
            ],
        },
        last_notified_json={
            "all": seeded_articles[0].id,
            "watchlist:wl-1": seeded_articles[0].id,
        },
        manage_token_hash=sha256_str("token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    delivered_payloads: list[list[int]] = []

    def _send(*_args, **kwargs):
        delivered_payloads.append(list(kwargs["payload"]["article_ids"]))
        return ("success", None)

    monkeypatch.setattr("app.push_alerts._send_push_notification", _send)

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=3,
        push_max_consecutive_failures=20,
    )

    first_run = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)
    assert first_run["sent"] == 1
    assert delivered_payloads[0] == [seeded_articles[3].id, seeded_articles[2].id, seeded_articles[1].id]
    assert sub.last_notified_json == {
        "all": seeded_articles[3].id,
        "watchlist:wl-1": seeded_articles[3].id,
    }

    second_run = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)
    assert second_run["sent"] == 1
    assert delivered_payloads[1] == [seeded_articles[5].id, seeded_articles[4].id]
    assert sub.last_notified_json == {
        "all": seeded_articles[5].id,
        "watchlist:wl-1": seeded_articles[5].id,
    }


def test_check_and_send_alerts_excludes_articles_mapped_only_to_inactive_tickers(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    first = _seed_mapped_article(db, slug="active-baseline")
    _inactive_only = _seed_article_with_ticker(db, slug="inactive-only", symbol="AAA", active=False)

    sub = PushSubscription(
        endpoint="https://push.example/sub-inactive-only",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    send_calls = {"count": 0}

    def _send(*_args, **_kwargs):
        send_calls["count"] += 1
        return ("success", None)

    monkeypatch.setattr("app.push_alerts._send_push_notification", _send)

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=25,
        push_max_consecutive_failures=20,
    )

    stats = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)

    assert stats["sent"] == 0
    assert send_calls["count"] == 0
    assert sub.last_notified_json == {"all": first.id}


def test_check_and_send_alerts_deactivates_after_repeated_non_gone_errors(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    first = _seed_mapped_article(db, slug="repeat-error-1")
    _second = _seed_mapped_article(db, slug="repeat-error-2")

    sub = PushSubscription(
        endpoint="https://push.example/sub-repeat-error",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=25,
        push_max_consecutive_failures=2,
    )

    monkeypatch.setattr("app.push_alerts._send_push_notification", lambda *_args, **_kwargs: ("error", "timeout"))

    first_try = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)
    assert first_try["failed"] == 1
    assert first_try["deactivated"] == 0
    assert sub.active is True
    assert sub.failure_count == 1

    second_try = check_and_send_alerts(db, settings_obj)
    db.refresh(sub)
    assert second_try["failed"] == 1
    assert second_try["deactivated"] == 1
    assert sub.active is False
    assert sub.failure_count == 2


def test_check_and_send_alerts_persists_first_success_when_later_subscription_aborts(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    first = _seed_mapped_article(db, slug="persist-success-1")
    second = _seed_mapped_article(db, slug="persist-success-2")

    first_sub = PushSubscription(
        endpoint="https://push.example/sub-persist-1",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token-1"),
        active=True,
    )
    second_sub = PushSubscription(
        endpoint="https://push.example/sub-persist-2",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={"all": first.id},
        manage_token_hash=sha256_str("token-2"),
        active=True,
    )
    db.add_all([first_sub, second_sub])
    db.commit()

    settings_obj = SimpleNamespace(
        vapid_public_key="public",
        vapid_private_key="private",
        vapid_contact_email="alerts@example.com",
        push_send_timeout_seconds=10,
        push_max_per_cycle=25,
        push_max_consecutive_failures=20,
    )

    send_attempts = {"count": 0}

    def _send(*_args, **_kwargs):
        send_attempts["count"] += 1
        if send_attempts["count"] == 1:
            return ("success", None)
        raise KeyboardInterrupt("abort after first committed send")

    monkeypatch.setattr("app.push_alerts._send_push_notification", _send)

    with pytest.raises(KeyboardInterrupt):
        check_and_send_alerts(db, settings_obj)

    db.rollback()
    first_sub_after = db.scalar(select(PushSubscription).where(PushSubscription.id == first_sub.id))
    second_sub_after = db.scalar(select(PushSubscription).where(PushSubscription.id == second_sub.id))
    second_article = db.scalar(select(Article).where(Article.id == second.id))

    assert first_sub_after is not None
    assert second_sub_after is not None
    assert int(first_sub_after.last_notified_json.get("all", 0) or 0) == second.id
    assert int(second_sub_after.last_notified_json.get("all", 0) or 0) == first.id
    assert second_article is not None
    assert second_article.first_alert_sent_at is not None
