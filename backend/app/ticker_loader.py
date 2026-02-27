from __future__ import annotations

import csv
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Ticker


REQUIRED_COLUMNS = {"ticker"}


def load_tickers_from_csv(db: Session, csv_path: str) -> dict[str, int]:
    path = Path(csv_path)
    if not path.exists():
        return {"loaded": 0, "created": 0, "updated": 0}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = {name.strip().lower() for name in (reader.fieldnames or [])}
        if not REQUIRED_COLUMNS.issubset(columns):
            raise ValueError(f"Missing required columns in {csv_path}: {REQUIRED_COLUMNS}")

        rows = list(reader)

    symbols = [row.get("ticker", "").strip().upper() for row in rows if row.get("ticker")]
    if not symbols:
        return {"loaded": 0, "created": 0, "updated": 0}

    unique_symbols = sorted(set(symbols))
    existing = {
        ticker.symbol: ticker
        for ticker in db.scalars(select(Ticker).where(Ticker.symbol.in_(unique_symbols))).all()
    }

    created = 0
    updated = 0

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
            item = Ticker(
                symbol=symbol,
                fund_name=fund_name,
                sponsor=sponsor,
                active=active,
            )
            db.add(item)
            # Track newly created symbols so duplicates within the same CSV reuse this row.
            existing[symbol] = item
            created += 1
            continue

        item.fund_name = fund_name
        item.sponsor = sponsor
        item.active = active
        updated += 1

    db.commit()

    return {"loaded": len(rows), "created": created, "updated": updated}
