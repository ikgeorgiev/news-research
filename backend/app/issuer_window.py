"""Production issuer enrollment and request window, separate from the closed study."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def issuer_session(enrolled_at: datetime, now: datetime | None = None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or enrolled_at.tzinfo is None:
        raise ValueError('Issuer enrollment requires timezone-aware timestamps')
    now = now.astimezone(timezone.utc)
    local = now.astimezone(ZoneInfo('Europe/Kyiv'))
    opened = local.replace(hour=10, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    cutoff = (local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).astimezone(timezone.utc)
    active = (local.weekday() < 5 and now >= max(opened, enrolled_at)
              and now < cutoff - timedelta(seconds=30))
    return active, max(opened, enrolled_at), cutoff
