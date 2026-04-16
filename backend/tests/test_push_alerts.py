from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.ingestion import _run_push_alerts, run_ingestion_cycle
from app.routes.push import delete_push_subscription, get_push_vapid_key, upsert_push_subscription
from app.models import Article, ArticleTicker, PushSubscription, Ticker
from app.push_alerts import (
    PushAlertDispatcher,
    check_and_send_alerts,
    check_and_send_alerts_locked,
    seed_last_notified_watermarks,
    push_dispatcher_is_active,
)
from app.schemas import (
    PushAlertScopes,
    PushDeleteRequest,
    PushSubscriptionKeys,
    PushSubscriptionPayload,
    PushUpsertRequest,
)
from app.sources import FeedDef, SourceFeed
from app.utils import sha256_str
from tests.helpers import seed_article


class FakeSessionContext:
    def __enter__(self):
        return SimpleNamespace()

    def __exit__(self, exc_type, exc, tb):
        return False


def _seed_article_with_ticker(
    db: Session,
    *,
    slug: str,
    symbol: str = "GOF",
    active: bool = True,
) -> Article:
    ticker = db.scalar(select(Ticker).where(Ticker.symbol == symbol))
    if ticker is None:
        ticker = Ticker(symbol=symbol, active=active)
        db.add(ticker)
        db.commit()
        db.refresh(ticker)

    article = seed_article(db, slug=slug, published_at=datetime.now(timezone.utc))

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
    article = _seed_article_with_ticker(db, slug="seeded")

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


def test_delete_push_subscription_works_when_push_runtime_is_disabled(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    sub = PushSubscription(
        endpoint="https://push.example/sub-disabled-delete",
        key_p256dh="p256dh-key",
        key_auth="auth-key",
        expiration_time=None,
        alert_scopes_json={"include_all_news": True, "watchlists": []},
        last_notified_json={},
        manage_token_hash=sha256_str("disabled-token"),
        active=True,
    )
    db.add(sub)
    db.commit()

    monkeypatch.setattr("app.routes.push.push_runtime_enabled", lambda _settings: False)

    response = delete_push_subscription(
        PushDeleteRequest(
            endpoint="https://push.example/sub-disabled-delete",
            manage_token="disabled-token",
        ),
        db=db,
    )

    assert response.deleted is True
    assert db.scalar(select(PushSubscription).where(PushSubscription.id == sub.id)) is None


def test_upsert_push_subscription_recovers_from_unique_endpoint_conflict(
    monkeypatch: pytest.MonkeyPatch,
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine = create_engine(f"sqlite:///{(Path(tmp_dir) / 'push-subscriptions.db').as_posix()}")
        Base.metadata.create_all(bind=engine)
        session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
        db = session_factory()
        try:
            monkeypatch.setattr("app.routes.push.push_runtime_enabled", lambda _settings: True)

            original_commit = db.commit
            conflict_inserted = {"done": False}

            def commit_with_conflict():
                if not conflict_inserted["done"]:
                    conflict_inserted["done"] = True
                    conflict_db = session_factory()
                    conflict_db.add(
                        PushSubscription(
                            endpoint="https://push.example/sub-race",
                            key_p256dh="existing-p256dh",
                            key_auth="existing-auth",
                            expiration_time=None,
                            alert_scopes_json={"include_all_news": False, "watchlists": []},
                            last_notified_json={},
                            manage_token_hash=sha256_str("existing-token"),
                            active=True,
                        )
                    )
                    conflict_db.commit()
                    conflict_db.close()
                    raise IntegrityError("INSERT", {}, Exception("unique endpoint conflict"))
                return original_commit()

            monkeypatch.setattr(db, "commit", commit_with_conflict)

            response = upsert_push_subscription(
                _push_payload(endpoint="https://push.example/sub-race"),
                db=db,
            )

            subscription = db.scalar(
                select(PushSubscription).where(PushSubscription.endpoint == "https://push.example/sub-race")
            )

            assert response.created is False
            assert response.manage_token is None
            assert subscription is not None
            assert subscription.active is True
            assert subscription.key_p256dh == "existing-p256dh"
            assert subscription.key_auth == "existing-auth"
        finally:
            db.close()
            engine.dispose()


def test_check_and_send_alerts_deactivates_gone_subscription(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    first = _seed_article_with_ticker(db, slug="push-gone-1")
    second = _seed_article_with_ticker(db, slug="push-gone-2")

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


def test_check_and_send_alerts_processes_every_subscription_across_batches(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    first = _seed_article_with_ticker(db, slug="batch-seed-1")
    second = _seed_article_with_ticker(db, slug="batch-seed-2")

    subscriptions: list[PushSubscription] = []
    for idx in range(3):
        sub = PushSubscription(
            endpoint=f"https://push.example/sub-batch-{idx}",
            key_p256dh="p256dh-key",
            key_auth="auth-key",
            expiration_time=None,
            alert_scopes_json={"include_all_news": True, "watchlists": []},
            last_notified_json={"all": first.id},
            manage_token_hash=sha256_str(f"token-batch-{idx}"),
            active=True,
        )
        subscriptions.append(sub)
    db.add_all(subscriptions)
    db.commit()

    monkeypatch.setattr("app.push_alerts._SUBSCRIPTION_BATCH_SIZE", 1)
    send_order: list[int] = []

    def _send(*_args, **kwargs):
        send_order.append(int(kwargs["subscription"].id))
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

    assert stats["scanned"] == 3
    assert stats["sent"] == 3
    assert send_order == [sub.id for sub in subscriptions]
    for sub in subscriptions:
        db.refresh(sub)
        assert int(sub.last_notified_json.get("all", 0) or 0) == second.id


def test_check_and_send_alerts_continues_after_mid_scan_deactivation(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    first = _seed_article_with_ticker(db, slug="midscan-seed-1")
    second = _seed_article_with_ticker(db, slug="midscan-seed-2")

    subscriptions: list[PushSubscription] = []
    for idx in range(3):
        sub = PushSubscription(
            endpoint=f"https://push.example/sub-midscan-{idx}",
            key_p256dh="p256dh-key",
            key_auth="auth-key",
            expiration_time=None,
            alert_scopes_json={"include_all_news": True, "watchlists": []},
            last_notified_json={"all": first.id},
            manage_token_hash=sha256_str(f"token-midscan-{idx}"),
            active=True,
        )
        subscriptions.append(sub)
    db.add_all(subscriptions)
    db.commit()

    monkeypatch.setattr("app.push_alerts._SUBSCRIPTION_BATCH_SIZE", 1)
    send_order: list[int] = []

    def _send(*_args, **kwargs):
        subscription = kwargs["subscription"]
        send_order.append(int(subscription.id))
        if subscription.id == subscriptions[0].id:
            return ("gone", "410 Gone")
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

    assert stats["scanned"] == 3
    assert stats["sent"] == 2
    assert stats["deactivated"] == 1
    assert send_order == [sub.id for sub in subscriptions]

    db.refresh(subscriptions[0])
    assert subscriptions[0].active is False
    for sub in subscriptions[1:]:
        db.refresh(sub)
        assert sub.active is True
        assert int(sub.last_notified_json.get("all", 0) or 0) == second.id


def test_check_and_send_alerts_defers_new_higher_id_subscriptions_until_next_run(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    bind = db.get_bind()
    assert bind is not None
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=bind)

    first = _seed_article_with_ticker(db, slug="snapshot-seed-1")
    second = _seed_article_with_ticker(db, slug="snapshot-seed-2")

    subscriptions: list[PushSubscription] = []
    for idx in range(2):
        sub = PushSubscription(
            endpoint=f"https://push.example/sub-snapshot-{idx}",
            key_p256dh="p256dh-key",
            key_auth="auth-key",
            expiration_time=None,
            alert_scopes_json={"include_all_news": True, "watchlists": []},
            last_notified_json={"all": first.id},
            manage_token_hash=sha256_str(f"token-snapshot-{idx}"),
            active=True,
        )
        subscriptions.append(sub)
    db.add_all(subscriptions)
    db.commit()

    monkeypatch.setattr("app.push_alerts._SUBSCRIPTION_BATCH_SIZE", 1)
    send_order: list[int] = []
    inserted = {"done": False, "subscription_id": None}

    def _send(*_args, **kwargs):
        subscription = kwargs["subscription"]
        send_order.append(int(subscription.id))
        if not inserted["done"]:
            inserted["done"] = True
            with session_factory() as other_db:
                new_sub = PushSubscription(
                    endpoint="https://push.example/sub-snapshot-new",
                    key_p256dh="p256dh-key",
                    key_auth="auth-key",
                    expiration_time=None,
                    alert_scopes_json={"include_all_news": True, "watchlists": []},
                    last_notified_json={"all": first.id},
                    manage_token_hash=sha256_str("token-snapshot-new"),
                    active=True,
                )
                other_db.add(new_sub)
                other_db.commit()
                inserted["subscription_id"] = int(new_sub.id)
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

    first_run = check_and_send_alerts(db, settings_obj)

    assert first_run["scanned"] == 2
    assert first_run["sent"] == 2
    assert send_order == [sub.id for sub in subscriptions]
    assert inserted["subscription_id"] is not None

    inserted_sub = db.scalar(
        select(PushSubscription).where(PushSubscription.id == inserted["subscription_id"])
    )
    assert inserted_sub is not None
    assert int(inserted_sub.last_notified_json.get("all", 0) or 0) == first.id

    second_run = check_and_send_alerts(db, settings_obj)
    db.refresh(inserted_sub)

    assert second_run["scanned"] == 3
    assert second_run["sent"] == 1
    assert int(inserted_sub.last_notified_json.get("all", 0) or 0) == second.id


def test_seed_last_notified_watermarks_logs_and_reseeds_invalid_values(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    article = _seed_article_with_ticker(db, slug="reseed-invalid-watermark")
    warning_messages: list[str] = []

    def _capture_warning(message, *args):
        warning_messages.append(message % args)

    monkeypatch.setattr("app.push_alerts.logger.warning", _capture_warning)

    next_watermarks, seeded = seed_last_notified_watermarks(
        db,
        scopes={"include_all_news": True, "watchlists": []},
        existing={"all": "bad", "zero": 0, "negative": -1, "none": None, "obj": {"x": 1}},
        subscription_context="subscription_id=123",
    )

    assert next_watermarks == {"all": article.id}
    assert seeded == {"all": article.id}
    assert len(warning_messages) == 5
    assert all("subscription_id=123" in message for message in warning_messages)


def test_seed_last_notified_watermarks_does_not_warn_for_valid_values(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    db = db_session
    article = _seed_article_with_ticker(db, slug="valid-watermark")
    warning_messages: list[str] = []

    def _capture_warning(message, *args):
        warning_messages.append(message % args)

    monkeypatch.setattr("app.push_alerts.logger.warning", _capture_warning)

    next_watermarks, seeded = seed_last_notified_watermarks(
        db,
        scopes={"include_all_news": True, "watchlists": []},
        existing={"all": article.id},
        subscription_context="subscription_id=456",
    )

    assert next_watermarks == {"all": article.id}
    assert seeded == {}
    assert not warning_messages


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
                feeds=[FeedDef(url="https://example.com/bw/1", article_source_name="Business Wire")],
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
        "app.ingestion.check_and_send_alerts_locked",
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


def test_check_and_send_alerts_locked_skips_when_lock_is_held(monkeypatch: pytest.MonkeyPatch):
    class FakeResult:
        def __init__(self, value):
            self._value = value

        def scalar(self):
            return self._value

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _statement):
            return FakeResult(False)

        def commit(self):
            return None

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    fake_bind = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        engine=FakeEngine(),
    )
    fake_db = SimpleNamespace(get_bind=lambda: fake_bind)
    settings_obj = SimpleNamespace(push_dispatch_advisory_lock_key=123)

    monkeypatch.setattr(
        "app.push_alerts.check_and_send_alerts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected push run")),
    )

    assert check_and_send_alerts_locked(fake_db, settings_obj) is None


def test_run_push_alerts_skips_cycle_scan_when_dispatcher_is_active(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.ingestion.push_dispatcher_is_active", lambda: True)
    monkeypatch.setattr(
        "app.ingestion.check_and_send_alerts_locked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected cycle scan")),
    )

    stats = _run_push_alerts(
        SimpleNamespace(),
        SimpleNamespace(),
        total_inserted=1,
        notify_failed=False,
    )

    assert stats == {
        "scanned": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
    }


def test_run_push_alerts_preserves_cycle_scan_when_notify_failed(monkeypatch: pytest.MonkeyPatch):
    expected = {
        "scanned": 1,
        "sent": 1,
        "failed": 0,
        "deactivated": 0,
    }
    monkeypatch.setattr("app.ingestion.push_dispatcher_is_active", lambda: True)
    monkeypatch.setattr(
        "app.ingestion.check_and_send_alerts_locked",
        lambda *_args, **_kwargs: expected,
    )

    stats = _run_push_alerts(
        SimpleNamespace(),
        SimpleNamespace(),
        total_inserted=1,
        notify_failed=True,
    )

    assert stats == expected


def test_push_alert_dispatcher_becomes_active_only_when_listening():
    dispatcher = PushAlertDispatcher(
        "postgresql://cef:cef@localhost/test",
        SimpleNamespace(),
        lambda: (_ for _ in ()).throw(AssertionError("unused")),
    )

    assert dispatcher._is_listening is False
    assert dispatcher._enabled is True
    assert push_dispatcher_is_active() is False

    dispatcher._set_listening(True)
    assert dispatcher._is_listening is True
    assert dispatcher._enabled is True
    assert push_dispatcher_is_active() is True

    dispatcher._set_listening(False)
    assert dispatcher._is_listening is False
    assert push_dispatcher_is_active() is False


def test_push_alert_dispatcher_retries_after_lock_contention(monkeypatch: pytest.MonkeyPatch):
    settings_obj = SimpleNamespace()
    dispatcher = PushAlertDispatcher(
        "postgresql://cef:cef@localhost/test",
        settings_obj,
        lambda: FakeSessionContext(),
    )
    attempts: list[int] = []

    monkeypatch.setattr(dispatcher._shutdown, "wait", lambda timeout: False)

    def fake_locked(_db, _settings):
        attempts.append(1)
        if len(attempts) == 1:
            return None
        return {
            "scanned": 0,
            "sent": 0,
            "failed": 0,
            "deactivated": 0,
        }

    monkeypatch.setattr("app.push_alerts.check_and_send_alerts_locked", fake_locked)

    dispatcher._request_dispatch()
    dispatcher._run_pending_dispatches()

    assert len(attempts) == 2


def test_push_alert_dispatcher_requests_catchup_after_listen_established(monkeypatch: pytest.MonkeyPatch):
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement):
            assert statement == "LISTEN new_articles"

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def notifies(self, timeout):
            dispatcher._shutdown.set()
            return []

    settings_obj = SimpleNamespace()
    dispatcher = PushAlertDispatcher(
        "postgresql://cef:cef@localhost/test",
        settings_obj,
        lambda: (_ for _ in ()).throw(AssertionError("unused")),
    )
    requested = {"count": 0}

    monkeypatch.setattr("app.push_alerts.psycopg.connect", lambda *_args, **_kwargs: FakeConnection())
    monkeypatch.setattr(dispatcher, "_request_dispatch", lambda: requested.__setitem__("count", requested["count"] + 1))

    dispatcher._listen_loop()

    assert requested["count"] == 1
    assert dispatcher._is_listening is False


def test_push_alert_dispatcher_requeues_after_dispatch_failure(monkeypatch: pytest.MonkeyPatch):
    settings_obj = SimpleNamespace()
    dispatcher = PushAlertDispatcher(
        "postgresql://cef:cef@localhost/test",
        settings_obj,
        lambda: FakeSessionContext(),
    )
    attempts: list[int] = []

    monkeypatch.setattr(dispatcher._shutdown, "wait", lambda timeout: False)

    def fake_locked(_db, _settings):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient failure")
        return {
            "scanned": 0,
            "sent": 0,
            "failed": 0,
            "deactivated": 0,
        }

    monkeypatch.setattr("app.push_alerts.check_and_send_alerts_locked", fake_locked)

    dispatcher._request_dispatch()
    dispatcher._run_pending_dispatches()

    assert len(attempts) == 2


def test_check_and_send_alerts_does_not_advance_watermark_on_transient_error(db_session: Session, monkeypatch: pytest.MonkeyPatch):
    db = db_session
    first = _seed_article_with_ticker(db, slug="retry-1")
    second = _seed_article_with_ticker(db, slug="retry-2")

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
    seeded_articles = [_seed_article_with_ticker(db, slug=f"backlog-{idx}") for idx in range(6)]

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
    first = _seed_article_with_ticker(db, slug="active-baseline")
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
    first = _seed_article_with_ticker(db, slug="repeat-error-1")
    _second = _seed_article_with_ticker(db, slug="repeat-error-2")

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
    first = _seed_article_with_ticker(db, slug="persist-success-1")
    second = _seed_article_with_ticker(db, slug="persist-success-2")

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
