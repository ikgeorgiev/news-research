from functools import lru_cache
from datetime import datetime
from ipaddress import IPv4Network, IPv6Network, ip_network
from pathlib import Path

from pydantic import AwareDatetime, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_prefix: str = "/api/v1"

    # Local-first default. Docker compose overrides this with the container hostname (`db`).
    database_url: str = "postgresql+psycopg://cef:cef@localhost:5433/cef_news"
    database_pool_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="QueuePool size for non-SQLite databases.",
    )
    database_max_overflow: int = Field(
        default=10,
        ge=0,
        le=1000,
        description="Extra QueuePool connections allowed above the base pool size.",
    )
    database_pool_timeout: float = Field(
        default=30.0,
        ge=1.0,
        le=300.0,
        description="Seconds to wait for a pooled connection before timing out.",
    )

    yahoo_chunk_size: int = Field(default=40, ge=5, le=350)
    ingest_cooldown_seconds: float = Field(default=2.0, ge=0.0)

    scheduler_enabled: bool = True
    ingestion_independent_sources: bool = True
    source_plan_refresh_seconds: float = Field(default=30.0, ge=1.0)
    ingestion_maintenance_seconds: float = Field(default=60.0, ge=1.0)
    postcommit_enrichment_enabled: bool = True
    research_timing_log_path: str | None = None
    research_timing_log_until: datetime | None = None
    research_feed_receipts_enabled: bool = False
    research_feed_receipt_dates: str = ""
    research_feed_receipt_start_hour_utc: int = Field(default=8, ge=0, le=23)
    research_feed_receipt_end_hour_utc: int = Field(default=22, ge=1, le=24)
    news_validation_timing_enabled: bool = False
    news_validation_timing_log_path: str | None = None
    news_validation_timing_dates: str = ""
    news_validation_timing_start_hour_utc: int = Field(default=8, ge=0, le=23)
    news_validation_timing_end_hour_utc: int = Field(default=22, ge=1, le=24)
    admin_api_key: str | None = None
    ingestion_advisory_lock_key: int = 1_715_171_517
    migration_advisory_lock_key: int = 1_715_171_519
    ingestion_stale_run_timeout_seconds: int = Field(default=3600, ge=60, le=172800)
    ingestion_max_workers: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Max parallel workers for feed ingestion (Postgres only).",
    )
    ingestion_parallel_yahoo: bool = Field(
        default=False,
        description="Allow Yahoo feed chunks to run concurrently with each other.",
    )
    ingestion_enable_conditional_get: bool = Field(
        default=True,
        description="Enable RSS conditional GET (ETag/Last-Modified) to speed up no-op cycles.",
    )
    # Single attempt by design: the scheduler cooldown loop is the outer
    # retry. In-cycle retries compounded 20s timeouts into 50s+ stalls when
    # upstream was slow (observed on GlobeNewswire). Transient failures are
    # surfaced via ingestion_cycle_total{status="failed"} and recovered by
    # feed_failure_backoff_base_seconds on the next cycle.
    feed_fetch_max_attempts: int = Field(default=1, ge=1, le=10)
    feed_fetch_backoff_seconds: float = Field(default=0.25, ge=0.0, le=30.0)
    feed_fetch_backoff_jitter_seconds: float = Field(default=0.3, ge=0.0, le=10.0)
    feed_failure_backoff_base_seconds: float = Field(
        default=10.0,
        ge=0.0,
        le=3600.0,
        description="Base delay for per-feed exponential failure backoff.",
    )
    feed_failure_backoff_max_seconds: float = Field(
        default=600.0,
        ge=0.0,
        le=86400.0,
        description="Maximum delay for per-feed exponential failure backoff.",
    )
    raw_feed_retention_days: int = Field(default=30, ge=1, le=3650)
    raw_feed_prune_batch_size: int = Field(default=5000, ge=100, le=50000)
    raw_feed_prune_max_batches: int = Field(default=5, ge=1, le=100)
    raw_feed_prune_interval_seconds: int = Field(
        default=3600,
        ge=0,
        le=86400,
        description="How often to run raw_feed_items pruning (0 disables auto-prune).",
    )

    # Production FC is opt-in; importing reviewed controls is required before HTTP.
    source_enable_financialcontent: bool = False
    source_enable_free_discovery: bool = False
    free_discovery_state_path: str = str(Path(__file__).resolve().parents[2] / "data" / "free-discovery.sqlite")
    gnw_custom_feeds_path: str | None = None
    # Explicitly bounded review collection; None keeps this route disabled.
    # Missing original GNW identities never become production articles.
    financialcontent_gnw_review_until: AwareDatetime | None = None
    financialcontent_state_path: str = str(Path(__file__).resolve().parents[2] / "data" / "financialcontent.sqlite")
    article_body_archive_dir: str = str(Path(__file__).resolve().parents[2] / "data" / "article-bodies")

    source_enable_yahoo: bool = True
    source_enable_prn: bool = True
    source_enable_gn: bool = True
    # Set to the UTC enrollment time to enable fresh Guggenheim issuer releases.
    guggenheim_issuer_feed_start: AwareDatetime | None = None
    source_enable_bw: bool = True

    # Admin/maintenance body-fetch timeout for article page scrapes
    # (/admin/revalidate, /admin/purge). Live ingestion uses the shorter
    # ingest_source_page_timeout_seconds below so slow upstream article pages
    # do not stretch the scheduler cycle budget.
    request_timeout_seconds: int = Field(default=15, ge=2, le=120)
    feed_poll_timeout_seconds: int = Field(
        default=4,
        ge=1,
        le=120,
        description=(
            "Per-request timeout for RSS feed polling. Kept short so a slow "
            "feed cannot stall the ingestion cycle; body-fetch timeouts are "
            "controlled separately by request_timeout_seconds."
        ),
    )
    ingest_source_page_timeout_seconds: int = Field(
        default=4,
        ge=1,
        le=120,
        description=(
            "Per-request timeout for article-page fallback fetches during live "
            "ingestion. Kept short so a few slow article pages cannot dominate "
            "end-to-end cycle duration."
        ),
    )
    globenewswire_source_page_timeout_seconds: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Per-request timeout for GlobeNewswire source-page fallback HTML fetches.",
    )
    globenewswire_source_page_max_fetches_per_feed: int = Field(
        default=2,
        ge=0,
        le=50,
        description="Max uncached GlobeNewswire source-page fallback fetches per feed per cycle.",
    )
    sse_max_connections_per_ip: int = Field(default=5, ge=1, le=100)
    behind_proxy: bool = False
    trusted_proxy_ips: str = Field(
        default="",
        description=(
            "Comma-separated trusted proxy IPs or CIDRs used to validate "
            "forwarded headers."
        ),
    )
    push_send_timeout_seconds: int = Field(default=10, ge=1, le=120)
    push_max_per_cycle: int = Field(default=25, ge=1, le=500)
    push_max_consecutive_failures: int = Field(default=20, ge=1, le=1000)
    push_dispatch_advisory_lock_key: int = 1_715_171_518
    vapid_public_key: str | None = None
    vapid_private_key: str | None = None
    vapid_contact_email: str | None = None

    # Root-level default: <repo>/data/cef_tickers.csv
    tickers_csv_path: str = str(Path(__file__).resolve().parents[2] / "data" / "cef_tickers.csv")

    cors_origins: str = (
        "http://localhost:3000,http://127.0.0.1:3000,"
        "http://localhost:3005,http://127.0.0.1:3005"
    )

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def trusted_proxy_networks(self) -> tuple[IPv4Network | IPv6Network, ...]:
        return tuple(
            ip_network(value, strict=False)
            for value in (part.strip() for part in self.trusted_proxy_ips.split(","))
            if value
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
