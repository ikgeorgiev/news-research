"""Summarize saved listing state without fetching, scheduling or clearing holds."""
from datetime import datetime, timezone

from app.financialcontent.listings import GNW_ROUTE

RESPONSE_HEADER_NAMES = ('content-type', 'cache-control', 'last-modified', 'retry-after')


def diagnostic_headers(headers):
    """Keep bounded response metadata; never cookies, locations or credentials."""
    return {name: str(headers[name])[:512] for name in RESPONSE_HEADER_NAMES if name in headers}


def diagnostic_error(error):
    if not error:
        return None
    value = str(error)
    return value if value.isascii() and value.isidentifier() and len(value) <= 128 else 'OtherError'


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


def _aggregate(rows, active):
    if not active or not rows:
        return 'inactive'
    statuses = {r['status'] for r in rows}
    if statuses == {'ok'}:
        return 'ok'
    if statuses <= {'ok', 'starting'}:
        return 'starting'
    if statuses == {'backoff'}:
        return 'backoff'
    if statuses <= {'held', 'host_held', 'request_outcome_unknown'}:
        return 'unavailable'
    return 'degraded'


def listing_health(inspection, enabled_routes, *, active, now):
    """Worker liveness, listing availability and review-only routes are distinct."""
    controls = inspection['controls']
    persisted = {r['name']: r for r in inspection['routes']}
    diagnostics = {r['route']: r for r in inspection.get('listing_diagnostics', [])}
    backoff = float(controls.get('backoff_until', 0))
    rows = []
    for name in sorted(set(persisted) | set(enabled_routes)):
        row = persisted.get(name, {})
        evidence = diagnostics.get(name)
        enabled = name in enabled_routes
        last_success = row.get('last_success')
        last_finished = evidence.get('finished_at') if evidence else None
        last_status = evidence.get('http_status') if evidence else None
        error = evidence.get('error_kind') if evidence else None
        pending = bool(row.get('request_pending'))
        recent = last_finished if last_finished is not None else last_success
        if row.get('disabled'):
            status = 'held'
        elif controls.get('disabled'):
            status = 'host_held'
        elif pending:
            status = 'request_outcome_unknown'
        elif not enabled or not active:
            status = 'inactive'
        elif backoff > now:
            status = 'backoff'
        elif error or (last_status is not None and last_status not in (200, 304)):
            status = 'failed'
        elif recent is None:
            status = 'starting'
        elif now-recent > 180:
            status = 'stale'
        elif last_success is None:
            status = 'starting'
        else:
            status = 'ok'
        hold_time = last_finished if row.get('disabled') == str(last_status) else None
        rows.append(dict(name=name, enabled=enabled, review_only=name == GNW_ROUTE, status=status,
            hold_reason=row.get('disabled'), host_hold_reason=controls.get('disabled'),
            request_pending=pending, last_success_at_utc=utc(last_success),
            last_attempt=evidence, hold_response_recorded_at_utc=utc(hold_time),
            legacy_hold_without_response_time=bool(row.get('disabled') and hold_time is None)))
    enabled = [r for r in rows if r['enabled']]
    production = [r for r in enabled if not r['review_only']]
    review = [r for r in enabled if r['review_only']]
    return dict(status=_aggregate(enabled, active), production_listing_status=_aggregate(production, active),
        review_listing_status=_aggregate(review, active),
        attention_required=any(r['status'] in {'held', 'host_held', 'request_outcome_unknown', 'failed', 'stale'} for r in enabled),
        checked_at_utc=utc(now), backoff_until_utc=utc(backoff) if backoff > now else None,
        enabled_route_count=len(enabled), held_route_count=sum(r['status'] in {'held', 'host_held'} for r in enabled),
        fresh_route_count=sum(r['status']=='ok' for r in enabled), routes=rows,
        freshness_is_not_complete_coverage=True)
