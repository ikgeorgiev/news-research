from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session


def is_postgresql_url(database_url: str) -> bool:
    normalized = database_url.strip().lower()
    return normalized.startswith("postgresql://") or normalized.startswith("postgresql+psycopg://")


def to_psycopg_conninfo(database_url: str) -> str:
    prefix = "postgresql+psycopg://"
    if database_url.startswith(prefix):
        return "postgresql://" + database_url[len(prefix):]
    return database_url


def hash_hex_to_signed_bigint(value: str) -> int:
    raw = int(value[:16], 16)
    if raw >= 2**63:
        raw -= 2**64
    return raw


def acquire_article_dedupe_locks(db: Session, url_hash: str, title_hash: str) -> None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    for key in sorted({hash_hex_to_signed_bigint(url_hash), hash_hex_to_signed_bigint(title_hash)}):
        db.execute(select(func.pg_advisory_xact_lock(key)))
