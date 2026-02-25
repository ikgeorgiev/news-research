from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "CEF News Feed"
    app_env: str = "development"
    api_prefix: str = "/api/v1"

    backend_host: str = "0.0.0.0"
    backend_port: int = 8000

    database_url: str = "postgresql+psycopg://cef:cef@db:5432/cef_news"

    ingest_interval_seconds: int = Field(default=60, ge=30)
    yahoo_chunk_size: int = Field(default=40, ge=5, le=350)

    scheduler_enabled: bool = True

    source_enable_yahoo: bool = True
    source_enable_prn: bool = True
    source_enable_gn: bool = True
    source_enable_bw: bool = True

    request_timeout_seconds: int = Field(default=20, ge=5, le=120)

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
