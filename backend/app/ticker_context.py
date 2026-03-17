from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Ticker
from app.ticker_extraction import _build_symbol_keywords


@dataclass(frozen=True, slots=True)
class TickerContext:
    symbol_to_id: dict[str, int]
    id_to_symbol: dict[int, str]
    known_symbols: frozenset[str]
    symbol_keywords: dict[str, frozenset[str]]


def load_ticker_context(db: Session) -> TickerContext:
    ticker_rows = db.execute(
        select(
            Ticker.id,
            Ticker.symbol,
            Ticker.fund_name,
            Ticker.sponsor,
            Ticker.validation_keywords,
        ).where(Ticker.active.is_(True))
    ).all()
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    return TickerContext(
        symbol_to_id=symbol_to_id,
        id_to_symbol={row[0]: row[1].upper() for row in ticker_rows},
        known_symbols=frozenset(symbol_to_id.keys()),
        symbol_keywords=_build_symbol_keywords(ticker_rows),
    )
