"""baseline schema

Revision ID: 20260319_0001
Revises:
Create Date: 2026-03-19 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260319_0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "articles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("canonical_url_hash", sa.String(length=64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_name", sa.String(length=120), nullable=False),
        sa.Column("provider_name", sa.String(length=120), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("title_normalized_hash", sa.String(length=64), nullable=False),
        sa.Column("cluster_key", sa.String(length=64), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_alert_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_articles_canonical_url_hash", "articles", ["canonical_url_hash"], unique=True)
    op.create_index("ix_articles_cluster_key", "articles", ["cluster_key"], unique=False)
    op.create_index("ix_articles_content_hash", "articles", ["content_hash"], unique=False)
    op.create_index("ix_articles_first_alert_sent_at", "articles", ["first_alert_sent_at"], unique=False)
    op.create_index("ix_articles_first_seen_at", "articles", ["first_seen_at"], unique=False)
    op.create_index("ix_articles_id", "articles", ["id"], unique=False)
    op.create_index("ix_articles_provider_name", "articles", ["provider_name"], unique=False)
    op.create_index("ix_articles_published_at", "articles", ["published_at"], unique=False)
    op.create_index("ix_articles_published_id", "articles", ["published_at", "id"], unique=False)
    op.create_index("ix_articles_source_name", "articles", ["source_name"], unique=False)
    op.create_index("ix_articles_title_normalized_hash", "articles", ["title_normalized_hash"], unique=False)

    op.create_table(
        "feed_poll_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("feed_url", sa.Text(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("backoff_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_feed_poll_state_backoff_until", "feed_poll_state", ["backoff_until"], unique=False)
    op.create_index("ix_feed_poll_state_feed_url", "feed_poll_state", ["feed_url"], unique=True)
    op.create_index("ix_feed_poll_state_id", "feed_poll_state", ["id"], unique=False)

    op.create_table(
        "push_subscriptions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("key_p256dh", sa.Text(), nullable=False),
        sa.Column("key_auth", sa.Text(), nullable=False),
        sa.Column("expiration_time", sa.BigInteger(), nullable=True),
        sa.Column("alert_scopes_json", sa.JSON(), nullable=False),
        sa.Column("last_notified_json", sa.JSON(), nullable=False),
        sa.Column("manage_token_hash", sa.String(length=64), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_push_subscriptions_active", "push_subscriptions", ["active"], unique=False)
    op.create_index("ix_push_subscriptions_active_updated", "push_subscriptions", ["active", "updated_at"], unique=False)
    op.create_index("ix_push_subscriptions_endpoint", "push_subscriptions", ["endpoint"], unique=True)
    op.create_index("ix_push_subscriptions_id", "push_subscriptions", ["id"], unique=False)
    op.create_index("ix_push_subscriptions_manage_token_hash", "push_subscriptions", ["manage_token_hash"], unique=False)

    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("base_url", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sources_code", "sources", ["code"], unique=True)
    op.create_index("ix_sources_enabled", "sources", ["enabled"], unique=False)
    op.create_index("ix_sources_id", "sources", ["id"], unique=False)

    op.create_table(
        "tickers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(length=16), nullable=False),
        sa.Column("fund_name", sa.String(length=255), nullable=True),
        sa.Column("sponsor", sa.String(length=255), nullable=True),
        sa.Column("validation_keywords", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_tickers_active", "tickers", ["active"], unique=False)
    op.create_index("ix_tickers_id", "tickers", ["id"], unique=False)
    op.create_index("ix_tickers_symbol", "tickers", ["symbol"], unique=True)

    op.create_table(
        "article_tickers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("article_id", sa.Integer(), nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("match_type", sa.String(length=32), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("extraction_version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.ForeignKeyConstraint(["article_id"], ["articles.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticker_id"], ["tickers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("article_id", "ticker_id", name="uq_article_ticker"),
    )
    op.create_index("ix_article_tickers_article_id", "article_tickers", ["article_id"], unique=False)
    op.create_index("ix_article_tickers_id", "article_tickers", ["id"], unique=False)
    op.create_index("ix_article_tickers_ticker_id", "article_tickers", ["ticker_id"], unique=False)

    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("feed_url", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("items_seen", sa.Integer(), nullable=False),
        sa.Column("items_inserted", sa.Integer(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ingestion_runs_id", "ingestion_runs", ["id"], unique=False)
    op.create_index("ix_ingestion_runs_source_id", "ingestion_runs", ["source_id"], unique=False)
    op.create_index("ix_ingestion_runs_status", "ingestion_runs", ["status"], unique=False)
    op.create_index("ix_runs_source_started", "ingestion_runs", ["source_id", "started_at"], unique=False)

    op.create_table(
        "raw_feed_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("article_id", sa.Integer(), nullable=True),
        sa.Column("feed_url", sa.Text(), nullable=False),
        sa.Column("raw_guid", sa.Text(), nullable=True),
        sa.Column("raw_title", sa.Text(), nullable=True),
        sa.Column("raw_link", sa.Text(), nullable=True),
        sa.Column("raw_pub_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_payload_json", sa.JSON(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["article_id"], ["articles.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_feed_items_article_id", "raw_feed_items", ["article_id"], unique=False)
    op.create_index("ix_raw_feed_items_fetched_at", "raw_feed_items", ["fetched_at"], unique=False)
    op.create_index("ix_raw_feed_items_id", "raw_feed_items", ["id"], unique=False)
    op.create_index("ix_raw_feed_items_source_guid", "raw_feed_items", ["source_id", "raw_guid"], unique=False)
    op.create_index("ix_raw_feed_items_source_id", "raw_feed_items", ["source_id"], unique=False)
    op.create_index("ix_raw_feed_items_source_link_pub_date", "raw_feed_items", ["source_id", "raw_link", "raw_pub_date"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_raw_feed_items_source_link_pub_date", table_name="raw_feed_items")
    op.drop_index("ix_raw_feed_items_source_id", table_name="raw_feed_items")
    op.drop_index("ix_raw_feed_items_source_guid", table_name="raw_feed_items")
    op.drop_index("ix_raw_feed_items_id", table_name="raw_feed_items")
    op.drop_index("ix_raw_feed_items_fetched_at", table_name="raw_feed_items")
    op.drop_index("ix_raw_feed_items_article_id", table_name="raw_feed_items")
    op.drop_table("raw_feed_items")

    op.drop_index("ix_runs_source_started", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_status", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_source_id", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_id", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")

    op.drop_index("ix_article_tickers_ticker_id", table_name="article_tickers")
    op.drop_index("ix_article_tickers_id", table_name="article_tickers")
    op.drop_index("ix_article_tickers_article_id", table_name="article_tickers")
    op.drop_table("article_tickers")

    op.drop_index("ix_tickers_symbol", table_name="tickers")
    op.drop_index("ix_tickers_id", table_name="tickers")
    op.drop_index("ix_tickers_active", table_name="tickers")
    op.drop_table("tickers")

    op.drop_index("ix_sources_id", table_name="sources")
    op.drop_index("ix_sources_enabled", table_name="sources")
    op.drop_index("ix_sources_code", table_name="sources")
    op.drop_table("sources")

    op.drop_index("ix_push_subscriptions_manage_token_hash", table_name="push_subscriptions")
    op.drop_index("ix_push_subscriptions_id", table_name="push_subscriptions")
    op.drop_index("ix_push_subscriptions_endpoint", table_name="push_subscriptions")
    op.drop_index("ix_push_subscriptions_active_updated", table_name="push_subscriptions")
    op.drop_index("ix_push_subscriptions_active", table_name="push_subscriptions")
    op.drop_table("push_subscriptions")

    op.drop_index("ix_feed_poll_state_id", table_name="feed_poll_state")
    op.drop_index("ix_feed_poll_state_feed_url", table_name="feed_poll_state")
    op.drop_index("ix_feed_poll_state_backoff_until", table_name="feed_poll_state")
    op.drop_table("feed_poll_state")

    op.drop_index("ix_articles_title_normalized_hash", table_name="articles")
    op.drop_index("ix_articles_source_name", table_name="articles")
    op.drop_index("ix_articles_published_id", table_name="articles")
    op.drop_index("ix_articles_published_at", table_name="articles")
    op.drop_index("ix_articles_provider_name", table_name="articles")
    op.drop_index("ix_articles_id", table_name="articles")
    op.drop_index("ix_articles_first_seen_at", table_name="articles")
    op.drop_index("ix_articles_first_alert_sent_at", table_name="articles")
    op.drop_index("ix_articles_content_hash", table_name="articles")
    op.drop_index("ix_articles_cluster_key", table_name="articles")
    op.drop_index("ix_articles_canonical_url_hash", table_name="articles")
    op.drop_table("articles")
