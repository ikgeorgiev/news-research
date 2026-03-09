from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar


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
