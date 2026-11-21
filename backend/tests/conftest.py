from __future__ import annotations

from collections.abc import Iterator
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.database import Base
from app.models import SourcePageControl
from app.page_access import PageAccessStore, configure_page_access, reset_page_outcome
from tests.helpers import FakeRssResponse


@pytest.fixture(autouse=True)
def isolated_page_controls(monkeypatch):
    """Keep publisher access decisions and HTTP requests isolated in each test."""
    original_connect = socket.socket.connect

    def test_network_only(self, address):
        # Windows asyncio implements socketpair on an ephemeral loopback port.
        assert (
            address[0] in {'127.0.0.1', 'localhost'} and address[1] >= 49152
        ), 'Unexpected network connection'
        return original_connect(self, address)

    monkeypatch.setattr(socket.socket, 'connect', test_network_only)
    engine = create_engine('sqlite:///:memory:')
    SourcePageControl.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    configure_page_access(PageAccessStore(factory))
    reset_page_outcome()
    yield
    configure_page_access(None)
    reset_page_outcome()
    engine.dispose()


@pytest.fixture
def disposable_postgres_engine():
    """Isolate real row-lock and notification tests from all other tables."""
    database_url = os.getenv("CEF_DISPOSABLE_TEST_URL")
    if not database_url:
        pytest.skip("Set CEF_DISPOSABLE_TEST_URL to run disposable PostgreSQL tests")
    url = make_url(database_url)
    assert (url.drivername, url.host, url.database, url.username) == (
        "postgresql+psycopg", "127.0.0.1", "cef_study_disposable", "cef_test",
    )
    schema = "cef_test_" + uuid4().hex
    admin = create_engine(url)
    engine = create_engine(url, connect_args={
        "connect_timeout": 5,
        "options": f"-c search_path={schema} -c statement_timeout=5000 -c lock_timeout=4000",
    })
    try:
        with admin.begin() as conn:
            conn.execute(CreateSchema(schema))
        try:
            Base.metadata.create_all(engine)
            yield engine
        finally:
            engine.dispose()
            with admin.begin() as conn:
                conn.execute(DropSchema(schema, cascade=True))
    finally:
        admin.dispose()


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
