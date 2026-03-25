from __future__ import annotations

import secrets

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.models import PushSubscription
from app.push_alerts import hash_manage_token, normalize_scopes, push_runtime_enabled, seed_last_notified_watermarks
from app.schemas import (
    PushDeleteRequest,
    PushDeleteResponse,
    PushUpsertRequest,
    PushUpsertResponse,
    PushVapidKeyResponse,
)

push_router = APIRouter()


def _require_push_runtime_enabled() -> str:
    settings = get_settings()
    if not push_runtime_enabled(settings):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Push notifications are not configured",
        )
    return (settings.vapid_public_key or "").strip()


@push_router.get("/push/vapid-key", response_model=PushVapidKeyResponse)
def get_push_vapid_key():
    settings = get_settings()
    if not push_runtime_enabled(settings):
        return PushVapidKeyResponse(enabled=False, public_key=None)
    return PushVapidKeyResponse(
        enabled=True,
        public_key=(settings.vapid_public_key or "").strip() or None,
    )


@push_router.put("/push/subscription", response_model=PushUpsertResponse)
def upsert_push_subscription(payload: PushUpsertRequest, db: Session = Depends(get_db)):
    _require_push_runtime_enabled()

    endpoint = payload.subscription.endpoint.strip()
    if not endpoint:
        raise HTTPException(status_code=422, detail="Subscription endpoint is required")

    p256dh = payload.subscription.keys.p256dh.strip()
    auth = payload.subscription.keys.auth.strip()
    if not p256dh or not auth:
        raise HTTPException(status_code=422, detail="Subscription keys are required")

    submitted_manage_token = (payload.manage_token or "").strip() or None
    scopes = normalize_scopes(payload.scopes.model_dump())
    existing = db.scalar(select(PushSubscription).where(PushSubscription.endpoint == endpoint))

    created = existing is None
    manage_token = submitted_manage_token
    subscription = existing

    if subscription is None:
        manage_token = secrets.token_urlsafe(32)
        subscription = PushSubscription(
            endpoint=endpoint,
            key_p256dh=p256dh,
            key_auth=auth,
            expiration_time=payload.subscription.expiration_time,
            manage_token_hash=hash_manage_token(manage_token),
            active=True,
        )
        db.add(subscription)
    else:
        if not submitted_manage_token:
            raise HTTPException(status_code=401, detail="Manage token is required for this subscription")
        provided_hash = hash_manage_token(submitted_manage_token)
        if not secrets.compare_digest(provided_hash, subscription.manage_token_hash):
            raise HTTPException(status_code=401, detail="Invalid manage token")
        manage_token = submitted_manage_token

    next_watermarks, seeded = seed_last_notified_watermarks(
        db,
        scopes=scopes,
        existing=subscription.last_notified_json if not created else None,
    )

    subscription.endpoint = endpoint
    subscription.key_p256dh = p256dh
    subscription.key_auth = auth
    subscription.expiration_time = payload.subscription.expiration_time
    subscription.alert_scopes_json = scopes
    subscription.last_notified_json = next_watermarks
    subscription.active = True
    subscription.last_error = None

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        subscription = db.scalar(select(PushSubscription).where(PushSubscription.endpoint == endpoint))
        if subscription is None:
            raise

        created = False
        if submitted_manage_token is not None:
            provided_hash = hash_manage_token(submitted_manage_token)
            if not secrets.compare_digest(provided_hash, subscription.manage_token_hash):
                raise HTTPException(status_code=401, detail="Invalid manage token")
            manage_token = submitted_manage_token
            subscription.key_p256dh = p256dh
            subscription.key_auth = auth
            subscription.expiration_time = payload.subscription.expiration_time
            subscription.alert_scopes_json = scopes
            subscription.last_notified_json = next_watermarks
            subscription.active = True
            subscription.last_error = None
            db.commit()
        else:
            manage_token = None

    db.refresh(subscription)

    return PushUpsertResponse(
        id=subscription.id,
        active=subscription.active,
        created=created,
        manage_token=manage_token,
        seeded_last_notified=seeded,
    )


@push_router.delete("/push/subscription", response_model=PushDeleteResponse)
def delete_push_subscription(payload: PushDeleteRequest, db: Session = Depends(get_db)):
    endpoint = payload.endpoint.strip()
    manage_token = payload.manage_token.strip()
    if not endpoint:
        return PushDeleteResponse(deleted=False)

    subscription = db.scalar(select(PushSubscription).where(PushSubscription.endpoint == endpoint))
    if subscription is None:
        return PushDeleteResponse(deleted=False)

    provided_hash = hash_manage_token(manage_token)
    if not secrets.compare_digest(provided_hash, subscription.manage_token_hash):
        raise HTTPException(status_code=401, detail="Invalid manage token")

    db.delete(subscription)
    db.commit()
    return PushDeleteResponse(deleted=True)
