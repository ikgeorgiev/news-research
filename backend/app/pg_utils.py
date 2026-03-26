from __future__ import annotations


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
