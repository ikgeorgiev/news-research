from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


UTC_NOW = lambda: datetime.now(timezone.utc)


class Ticker(Base):
    __tablename__ = "tickers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    fund_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sponsor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW, onupdate=UTC_NOW)


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    base_url: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_url_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW, index=True)
    source_name: Mapped[str] = mapped_column(String(120), index=True)
    provider_name: Mapped[str] = mapped_column(String(120), index=True)
    content_hash: Mapped[str] = mapped_column(String(64), index=True)
    title_normalized_hash: Mapped[str] = mapped_column(String(64), index=True)
    cluster_key: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW, onupdate=UTC_NOW)

    tickers: Mapped[list[ArticleTicker]] = relationship(back_populates="article", cascade="all, delete-orphan")


class ArticleTicker(Base):
    __tablename__ = "article_tickers"
    __table_args__ = (UniqueConstraint("article_id", "ticker_id", name="uq_article_ticker"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    article_id: Mapped[int] = mapped_column(ForeignKey("articles.id", ondelete="CASCADE"), index=True)
    ticker_id: Mapped[int] = mapped_column(ForeignKey("tickers.id", ondelete="CASCADE"), index=True)
    match_type: Mapped[str] = mapped_column(String(32), default="token")
    confidence: Mapped[float] = mapped_column(Float, default=0.5)

    article: Mapped[Article] = relationship(back_populates="tickers")
    ticker: Mapped[Ticker] = relationship()


class RawFeedItem(Base):
    __tablename__ = "raw_feed_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)
    article_id: Mapped[int | None] = mapped_column(ForeignKey("articles.id", ondelete="SET NULL"), index=True)
    feed_url: Mapped[str] = mapped_column(Text, nullable=False)
    raw_guid: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_link: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_pub_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW, index=True)


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)
    feed_url: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=UTC_NOW)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(24), default="running", index=True)
    items_seen: Mapped[int] = mapped_column(Integer, default=0)
    items_inserted: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


Index("ix_articles_published_id", Article.published_at, Article.id)
Index("ix_runs_source_started", IngestionRun.source_id, IngestionRun.started_at)
