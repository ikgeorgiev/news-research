from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class NewsItem(BaseModel):
    id: int
    title: str
    url: str
    source: str
    provider: str
    summary: str | None
    published_at: datetime
    tickers: list[str]
    dedupe_group: str


class NewsListResponse(BaseModel):
    items: list[NewsItem]
    next_cursor: str | None
    meta: dict[str, int | str | None]


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
