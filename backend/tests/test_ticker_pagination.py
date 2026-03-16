from __future__ import annotations

from app.routes.news import list_tickers
from app.models import Ticker


def test_list_tickers_applies_pagination_and_reports_total(db_session):
    db_session.add_all(
        [
            Ticker(symbol="AAA", active=True),
            Ticker(symbol="BBB", active=True),
            Ticker(symbol="CCC", active=True),
            Ticker(symbol="DDD", active=True),
            Ticker(symbol="ZZZ", active=False),
        ]
    )
    db_session.commit()

    response = list_tickers(q=None, limit=2, offset=1, db=db_session)

    assert response.total == 4
    assert [item.symbol for item in response.items] == ["BBB", "CCC"]


def test_list_tickers_filters_before_count_and_pagination(db_session):
    db_session.add_all(
        [
            Ticker(symbol="ABB", active=True),
            Ticker(symbol="AAA", active=True),
            Ticker(symbol="BBB", active=True),
            Ticker(symbol="CCC", active=True),
        ]
    )
    db_session.commit()

    response = list_tickers(q="BB", limit=10, offset=0, db=db_session)

    assert response.total == 1
    assert [item.symbol for item in response.items] == ["BBB"]
