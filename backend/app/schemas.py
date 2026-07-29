from __future__ import annotations

from datetime import datetime
from ipaddress import ip_address
from socket import inet_aton
from typing import Annotated
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator


class NewsItem(BaseModel):
    id: int
    read_key: str
    title: str
    url: str
    source: str
    provider: str
    summary: str | None
    published_at: datetime
    first_seen_at: datetime
    alert_sent_at: datetime | None
    tickers: list[str]
    dedupe_group: str


class NewsGlobalSummary(BaseModel):
    total: int
    tracked_ids: list[int]
    tracked_read_keys: list[str]
    tracked_limit: int


class NewsListResponse(BaseModel):
    items: list[NewsItem]
    next_cursor: str | None
    meta: dict[str, int | str | None]
    global_summary: NewsGlobalSummary | None = None

class NewsIdsResponse(BaseModel):
    ids: list[int]
    read_keys: list[str]
    next_cursor: str | None = None


class MarkAlertsSentRequest(BaseModel):
    article_ids: list[int] = Field(default_factory=list)


class MarkAlertsSentResponse(BaseModel):
    requested: int
    marked: int
    first_alert_sent_at: datetime | None = None


class TickerItem(BaseModel):
    symbol: str
    fund_name: str | None
    sponsor: str | None
    active: bool


class TickerListResponse(BaseModel):
    items: list[TickerItem]
    total: int


class IngestionRunItem(BaseModel):
    id: int
    source: str
    feed_url: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    items_seen: int
    items_inserted: int
    error_text: str | None


class IngestionRunResponse(BaseModel):
    items: list[IngestionRunItem]


class RunIngestionResponse(BaseModel):
    total_feeds: int
    total_items_seen: int
    total_items_inserted: int
    failed_feeds: int


class SourceRemapResponse(BaseModel):
    source_code: str
    processed: int
    articles_with_hits: int
    remapped_articles: int
    only_unmapped: bool


class DedupeResponse(BaseModel):
    scanned_articles: int
    duplicate_groups: int
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


class PurgeFalsePositiveResponse(BaseModel):
    dry_run: bool
    scanned_articles: int
    purged_articles: int
    deleted_article_tickers: int
    deleted_raw_feed_items: int


class ReloadTickersResponse(BaseModel):
    loaded: int
    created: int
    updated: int
    unchanged: int
    source_remaps: list[SourceRemapResponse] | None = None


class RevalidationResponse(BaseModel):
    scanned: int
    revalidated: int
    purged: int
    unchanged: int


class PushVapidKeyResponse(BaseModel):
    enabled: bool
    public_key: str | None = None


MAX_PUSH_ENDPOINT_LENGTH = 2048
MAX_PUSH_KEY_LENGTH = 512
MAX_PUSH_MANAGE_TOKEN_LENGTH = 256
MAX_PUSH_WATCHLISTS = 50
MAX_PUSH_TICKERS_PER_WATCHLIST = 500
MAX_PUSH_WATCHLIST_ID_LENGTH = 128
MAX_PUSH_WATCHLIST_NAME_LENGTH = 120
MAX_PUSH_PROVIDER_LENGTH = 120
MAX_PUSH_QUERY_LENGTH = 256
MAX_PUSH_TICKER_LENGTH = 16
MAX_PUSH_EXPIRATION_TIME = 2**63 - 1
PUSH_ENDPOINT_ALLOWED_HOSTS = frozenset(
    {
        # Current and legacy Chromium/FCM endpoints.
        "fcm.googleapis.com",
        "android.googleapis.com",
        # Firefox Autopush production endpoints.
        "updates.push.services.mozilla.com",
        "updates-push.services.mozaws.net",
        "push.services.mozilla.com",
        # Include the provider roots as well as their regional subdomains.
        "push.apple.com",
        "notify.windows.com",
    }
)
PUSH_ENDPOINT_ALLOWED_SUFFIXES = (
    ".push.apple.com",
    ".notify.windows.com",
)

PushEndpoint = Annotated[
    str,
    Field(min_length=1, max_length=MAX_PUSH_ENDPOINT_LENGTH),
]
PushKey = Annotated[
    str,
    Field(min_length=1, max_length=MAX_PUSH_KEY_LENGTH),
]
PushManageToken = Annotated[
    str,
    Field(min_length=1, max_length=MAX_PUSH_MANAGE_TOKEN_LENGTH),
]
PushWatchlistId = Annotated[
    str,
    Field(min_length=1, max_length=MAX_PUSH_WATCHLIST_ID_LENGTH),
]
PushTickerSymbol = Annotated[
    str,
    Field(min_length=1, max_length=MAX_PUSH_TICKER_LENGTH),
]


class PushSubscriptionKeys(BaseModel):
    p256dh: PushKey
    auth: PushKey


def validate_push_subscription_endpoint(value: str) -> str:
    endpoint = value.strip()
    try:
        parsed = urlsplit(endpoint)
        hostname = parsed.hostname
        # Accessing port performs urllib's numeric/range validation.
        _ = parsed.port
        username = parsed.username
        password = parsed.password
    except ValueError as exc:
        raise ValueError("Push subscription endpoint is not a valid URL") from exc

    if parsed.scheme.lower() != "https":
        raise ValueError("Push subscription endpoint must use HTTPS")
    if not hostname:
        raise ValueError("Push subscription endpoint must include a hostname")
    if username is not None or password is not None:
        raise ValueError("Push subscription endpoint must not include credentials")

    normalized_hostname = hostname.rstrip(".").lower()
    if not normalized_hostname or "%" in normalized_hostname:
        raise ValueError("Push subscription endpoint has an invalid hostname")
    if normalized_hostname == "localhost" or normalized_hostname.endswith(".localhost"):
        raise ValueError("Push subscription endpoint must not target localhost")

    # Browser push endpoints use a small set of public HTTPS services. Merely
    # rejecting private IP literals is insufficient because an attacker-owned
    # hostname can resolve to a private address when the notification is sent.
    try:
        address = ip_address(normalized_hostname)
    except ValueError:
        try:
            # inet_aton also recognizes legacy IPv4 spellings such as
            # 2130706433, 0177.0.0.1, and 127.1 that HTTP clients resolve
            # as 127.0.0.1.
            address = ip_address(inet_aton(normalized_hostname))
        except OSError:
            address = None

    if address is not None:
        if not address.is_global:
            raise ValueError(
                "Push subscription endpoint must not target a non-public IP address"
            )
        raise ValueError("Push subscription endpoint must use a supported push service")

    if (
        normalized_hostname not in PUSH_ENDPOINT_ALLOWED_HOSTS
        and not normalized_hostname.endswith(PUSH_ENDPOINT_ALLOWED_SUFFIXES)
    ):
        raise ValueError("Push subscription endpoint must use a supported push service")
    return endpoint


class PushSubscriptionPayload(BaseModel):
    endpoint: PushEndpoint
    expiration_time: int | None = Field(
        default=None,
        ge=0,
        le=MAX_PUSH_EXPIRATION_TIME,
    )
    keys: PushSubscriptionKeys

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        return validate_push_subscription_endpoint(value)


class PushWatchlistScope(BaseModel):
    id: PushWatchlistId
    name: str | None = Field(
        default=None,
        max_length=MAX_PUSH_WATCHLIST_NAME_LENGTH,
    )
    tickers: list[PushTickerSymbol] | None = Field(
        default=None,
        max_length=MAX_PUSH_TICKERS_PER_WATCHLIST,
    )
    provider: str | None = Field(
        default=None,
        max_length=MAX_PUSH_PROVIDER_LENGTH,
    )
    q: str | None = Field(
        default=None,
        max_length=MAX_PUSH_QUERY_LENGTH,
    )


class PushAlertScopes(BaseModel):
    include_all_news: bool = True
    watchlists: list[PushWatchlistScope] = Field(
        default_factory=list,
        max_length=MAX_PUSH_WATCHLISTS,
    )


class PushUpsertRequest(BaseModel):
    subscription: PushSubscriptionPayload
    scopes: PushAlertScopes
    manage_token: PushManageToken | None = None


class PushUpsertResponse(BaseModel):
    id: int
    active: bool
    created: bool
    manage_token: str | None = None
    seeded_last_notified: dict[str, int] = Field(default_factory=dict)


class PushDeleteRequest(BaseModel):
    # Deletion intentionally only bounds the endpoint. It does not re-run the
    # registration URL policy so administrators/users can remove legacy rows
    # that predate endpoint validation.
    endpoint: PushEndpoint
    manage_token: PushManageToken


class PushDeleteResponse(BaseModel):
    deleted: bool
