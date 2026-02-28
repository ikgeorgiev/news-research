from __future__ import annotations

import csv
import threading
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Ticker


REQUIRED_COLUMNS = {"ticker"}
_ticker_reload_lock = threading.Lock()


def load_tickers_from_csv(db: Session, csv_path: str) -> dict[str, int]:
    with _ticker_reload_lock:
        path = Path(csv_path)
        if not path.exists():
            return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = {name.strip().lower() for name in (reader.fieldnames or [])}
            if not REQUIRED_COLUMNS.issubset(columns):
                raise ValueError(f"Missing required columns in {csv_path}: {REQUIRED_COLUMNS}")

            rows = list(reader)

        symbols = [row.get("ticker", "").strip().upper() for row in rows if row.get("ticker")]
        if not symbols:
            return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

        unique_symbols = sorted(set(symbols))
        existing = {
            ticker.symbol: ticker
            for ticker in db.scalars(select(Ticker).where(Ticker.symbol.in_(unique_symbols))).all()
        }

        created = 0
        updated = 0
        unchanged = 0

        for row in rows:
            symbol = row.get("ticker", "").strip().upper()
            if not symbol:
                continue

            fund_name = (row.get("fund_name") or "").strip() or None
            sponsor = (row.get("sponsor") or "").strip() or None
            active_raw = (row.get("active") or "true").strip().lower()
            active = active_raw not in {"0", "false", "no", "n"}

            item = existing.get(symbol)
            if item is None:
                candidate = Ticker(
                    symbol=symbol,
                    fund_name=fund_name,
                    sponsor=sponsor,
                    active=active,
                )
                try:
                    # Savepoint protects the batch when a concurrent writer inserted first.
                    with db.begin_nested():
                        db.add(candidate)
                        db.flush()
                    item = candidate
                    existing[symbol] = item
                    created += 1
                    continue
                except IntegrityError:
                    item = db.scalar(select(Ticker).where(Ticker.symbol == symbol))
                    if item is None:
                        raise
                    existing[symbol] = item

            changed = False
            if item.fund_name != fund_name:
                item.fund_name = fund_name
                changed = True
            if item.sponsor != sponsor:
                item.sponsor = sponsor
                changed = True
            if item.active != active:
                item.active = active
                changed = True

            if changed:
                updated += 1
            else:
                unchanged += 1

        db.commit()

        return {
            "loaded": len(rows),
            "created": created,
            "updated": updated,
            "unchanged": unchanged,
        }
