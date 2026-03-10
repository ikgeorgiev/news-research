from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class NewsItem(BaseModel):
    id: int
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
    tracked_limit: int


class NewsListResponse(BaseModel):
    items: list[NewsItem]
    next_cursor: str | None
    meta: dict[str, int | str | None]
    global_summary: NewsGlobalSummary | None = None

class NewsIdsResponse(BaseModel):
    ids: list[int]
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


class PushVapidKeyResponse(BaseModel):
    enabled: bool
    public_key: str | None = None


class PushSubscriptionKeys(BaseModel):
    p256dh: str
    auth: str


class PushSubscriptionPayload(BaseModel):
    endpoint: str
    expiration_time: int | None = None
    keys: PushSubscriptionKeys


class PushWatchlistScope(BaseModel):
    id: str
    name: str | None = None
    tickers: list[str] | None = None
    provider: str | None = None
    q: str | None = None


class PushAlertScopes(BaseModel):
    include_all_news: bool = True
    watchlists: list[PushWatchlistScope] = Field(default_factory=list)


class PushUpsertRequest(BaseModel):
    subscription: PushSubscriptionPayload
    scopes: PushAlertScopes
    manage_token: str | None = None


class PushUpsertResponse(BaseModel):
    id: int
    active: bool
    created: bool
    manage_token: str | None = None
    seeded_last_notified: dict[str, int] = Field(default_factory=dict)


class PushDeleteRequest(BaseModel):
    endpoint: str
    manage_token: str


class PushDeleteResponse(BaseModel):
    deleted: bool
