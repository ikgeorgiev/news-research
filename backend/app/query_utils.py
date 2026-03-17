from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar

from sqlalchemy import and_, select

from app.models import Article, ArticleTicker, Ticker


SQLITE_SAFE_IN_CHUNK_SIZE = 900

T = TypeVar("T")


def iter_chunks(
    values: Iterable[T],
    *,
    chunk_size: int = SQLITE_SAFE_IN_CHUNK_SIZE,
) -> Iterator[list[T]]:
    chunk: list[T] = []
    for value in values:
        chunk.append(value)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def escape_like_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def contains_literal_pattern(value: str) -> str:
    return f"%{escape_like_literal(value.strip())}%"


def prefix_literal_pattern(value: str) -> str:
    return f"{escape_like_literal(value.strip())}%"


def active_ticker_mapped_exists():
    """Correlated EXISTS: article has at least one active-ticker mapping."""
    return (
        select(1)
        .select_from(ArticleTicker)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(
            and_(
                ArticleTicker.article_id == Article.id,
                Ticker.active.is_(True),
            )
        )
        .correlate(Article)
        .exists()
    )


def any_ticker_mapped_exists():
    """Correlated EXISTS: article has at least one ticker mapping (any status)."""
    return (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
