"""Offline assessment of the yfinance 1.8.0 GraphQL response contract.

No HTTP client, credentials, poller or production integration. Errors remain
explicit, and related-ticker labels never become validated fund assignments.
"""
from datetime import datetime
import json
from urllib.parse import urlsplit

SOURCE = 'https://github.com/ranaroussi/yfinance/blob/1.8.0/yfinance/base.py'
MAX_BYTES = 1024 * 1024


def iso(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.isoformat() if parsed.tzinfo is not None else None
    except ValueError:
        return None


def public_url(value):
    if not isinstance(value, str):
        return None
    try:
        parts = urlsplit(value)
        if parts.scheme == 'https' and parts.hostname and not parts.username and not parts.password:
            return value
    except ValueError:
        pass
    return None


def assess(status, body, *, received_at, requested_symbol, tab, network_response=True):
    if not iso(received_at) or tab not in {'news','all','press releases'}:
        raise ValueError('Explicit timezone and supported tab required')
    result = dict(status='invalid', received_at=received_at, requested_symbol=requested_symbol,
                  tab=tab, network_response=bool(network_response), items=[], issues=[],
                  absence_eligible=False, coverage_complete=None)
    if status != 200:
        result.update(status='http_error', http_status=status)
        return result
    if len(body) > MAX_BYTES:
        result['issues'].append('response_too_large')
        return result
    try:
        payload = json.loads(body)
        stream = payload['data']['lightyearList']['main']
        edges, info = stream['edges'], stream['pageInfo']
        if not isinstance(edges, list) or type(info.get('hasNextPage')) is not bool:
            raise ValueError('listing_schema')
    except (ValueError, TypeError, KeyError, AttributeError):
        result['issues'].append('missing_or_invalid_listing_schema')
        return result
    if payload.get('errors'):
        result['issues'].append('graphql_errors')
    seen = set()
    for edge in edges:
        try:
            asset = edge['node']['asset']
            kind = asset['__typename']
            if kind not in {'Story', 'Video', 'Outlink'}:
                raise ValueError('unavailable_asset')
            key, title = asset['id'], asset['title']
            if not isinstance(key, str) or not key or not isinstance(title, str) or not title.strip():
                raise ValueError('missing_identity')
            if key in seen:
                result['issues'].append('duplicate_asset_id')
                continue
            seen.add(key)
            attrs = asset.get('contentAttributes') or {}
            original = public_url(asset.get('url') if kind == 'Outlink' else attrs.get('canonicalUrl'))
            published = iso(attrs.get('publishTime'))
            display = iso(attrs.get('displayTime') or asset.get('displayTime'))
            if original is None:
                result['issues'].append('missing_article_url')
            if published is None:
                result['issues'].append('missing_publication_timestamp')
            result['items'].append(dict(
                id=key, title=title, kind=kind, published_at=published, display_at=display,
                url=original, clickthrough_url=public_url(attrs.get('clickthroughUrl')),
                provider=asset.get('provider') if kind == 'Outlink' else attrs.get('provider'),
                summary=attrs.get('summary'),
                related_tickers=(asset.get('finance') or {}).get('stockTickers') or [],
                validated_fund_tickers=[], original_wire_identity=None,
                speed_comparable=False,
            ))
        except (ValueError, KeyError, TypeError, AttributeError):
            result['issues'].append('unavailable_or_malformed_asset')
    result['status'] = 'partial' if result['issues'] else 'observed'
    result['has_next_page'] = info['hasNextPage']
    result['end_cursor'] = info.get('endCursor')
    # Even a valid finite response is a ranked ticker result, not complete wire coverage.
    # Cached responses and partial assets additionally cannot advance any absence bound.
    if not network_response:
        result['issues'].append('local_cache_not_a_new_observation')
        result['status'] = 'cached'
    return result
