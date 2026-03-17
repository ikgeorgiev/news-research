from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from tests.helpers import FakeRssResponse


@pytest.fixture()
def db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


@pytest.fixture()
def stub_feed_io(monkeypatch):
    """Stub ``requests.get`` (feed-runtime + ticker-extraction) with
    :class:`FakeRssResponse` and return a callable to set feed entries.

    Usage::

        def test_something(db_session, stub_feed_io):
            stub_feed_io([entry_dict], "Business Wire")
            result = call_ingest(...)
    """
    fake = FakeRssResponse()
    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_a, **_kw: fake,
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_a, **_kw: fake,
    )

    def set_entries(entries, feed_title="Business Wire"):
        monkeypatch.setattr(
            "app.article_ingest.feedparser.parse",
            lambda _content: SimpleNamespace(
                feed={"title": feed_title}, entries=entries,
            ),
        )

    return set_entries
