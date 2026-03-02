from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import list_tickers
from app.models import Ticker


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


def test_list_tickers_applies_pagination_and_reports_total():
    db = _make_db_session()
    db.add_all(
        [
            Ticker(symbol="AAA", active=True),
            Ticker(symbol="BBB", active=True),
            Ticker(symbol="CCC", active=True),
            Ticker(symbol="DDD", active=True),
            Ticker(symbol="ZZZ", active=False),
        ]
    )
    db.commit()

    response = list_tickers(q=None, limit=2, offset=1, db=db)

    assert response.total == 4
    assert [item.symbol for item in response.items] == ["BBB", "CCC"]
    db.close()


def test_list_tickers_filters_before_count_and_pagination():
    db = _make_db_session()
    db.add_all(
        [
            Ticker(symbol="AAA", active=True),
            Ticker(symbol="BBB", active=True),
            Ticker(symbol="CCC", active=True),
        ]
    )
    db.commit()

    response = list_tickers(q="BB", limit=10, offset=0, db=db)

    assert response.total == 1
    assert [item.symbol for item in response.items] == ["BBB"]
    db.close()
