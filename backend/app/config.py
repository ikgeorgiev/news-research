from functools import lru_cache
from pathlib import Path

from pydantic import Field
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

    yahoo_chunk_size: int = Field(default=40, ge=5, le=350)
    ingest_cooldown_seconds: float = Field(default=5.0, ge=0.0)

    scheduler_enabled: bool = True
    admin_api_key: str | None = None
    ingestion_advisory_lock_key: int = 1_715_171_517
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
    feed_fetch_max_attempts: int = Field(default=3, ge=1, le=10)
    feed_fetch_backoff_seconds: float = Field(default=1.0, ge=0.0, le=30.0)
    feed_fetch_backoff_jitter_seconds: float = Field(default=0.3, ge=0.0, le=10.0)
    feed_failure_backoff_base_seconds: float = Field(
        default=30.0,
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

    source_enable_yahoo: bool = True
    source_enable_prn: bool = True
    source_enable_gn: bool = True
    source_enable_bw: bool = True

    request_timeout_seconds: int = Field(default=20, ge=5, le=120)
    globenewswire_source_page_timeout_seconds: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Per-request timeout for GlobeNewswire source-page fallback HTML fetches.",
    )
    globenewswire_source_page_max_fetches_per_feed: int = Field(
        default=3,
        ge=0,
        le=50,
        description="Max uncached GlobeNewswire source-page fallback fetches per feed per cycle.",
    )
    sse_max_connections_per_ip: int = Field(default=5, ge=1, le=100)
    behind_proxy: bool = False
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
