from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from tests.helpers import FakeRssResponse


@pytest.fixture()
def scratch_dir() -> Iterator[Path]:
    """Provide an isolated temp directory without leaving files in the repository."""
    with TemporaryDirectory(prefix="cef-news-tests-") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture()
def db_session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@pytest.fixture()
def stub_feed_io(monkeypatch):
    """Stub shared HTTP client GET calls (feed-runtime + ticker-extraction) with
    :class:`FakeRssResponse` and return a callable to set feed entries.

    Usage::

        def test_something(db_session, stub_feed_io):
            stub_feed_io([entry_dict], "Business Wire")
            result = call_ingest(...)
    """
    fake = FakeRssResponse()
    class FakeClient:
        def get(self, *_args, **_kwargs):
            return fake

    monkeypatch.setattr(
        "app.http_client.get_http_client", lambda: FakeClient(),
    )
    monkeypatch.setattr(
        "app.http_client.get_feed_poll_client", lambda: FakeClient(),
    )
    monkeypatch.setattr(
        "app.http_client.reset_feed_poll_client", lambda _client=None: None,
    )
    monkeypatch.setattr(
        "app.http_client.retire_feed_poll_client", lambda _client=None: None,
    )

    def set_entries(entries, feed_title="Business Wire"):
        monkeypatch.setattr(
            "app.article_ingest.feedparser.parse",
            lambda _content: SimpleNamespace(
                feed={"title": feed_title}, entries=entries,
            ),
        )

    return set_entries
