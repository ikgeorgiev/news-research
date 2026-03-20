from __future__ import annotations

import asyncio
import itertools
import logging
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _is_postgresql_url(database_url: str) -> bool:
    normalized = database_url.strip().lower()
    return normalized.startswith("postgresql://") or normalized.startswith("postgresql+psycopg://")


def _to_psycopg_conninfo(database_url: str) -> str:
    prefix = "postgresql+psycopg://"
    if database_url.startswith(prefix):
        return "postgresql://" + database_url[len(prefix) :]
    return database_url


class SSEBroadcaster:
    def __init__(self, database_url: str):
        self._conninfo = _to_psycopg_conninfo(database_url)
        self._enabled = _is_postgresql_url(database_url)
        self._healthy = False
        self._listeners: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]] = {}
        self._listener_ids = itertools.count(1)
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_available(self) -> bool:
        return self._enabled and self._healthy

    def start(self) -> None:
        if not self._enabled:
            logger.info("SSE broadcaster disabled: database is not PostgreSQL")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._shutdown.clear()
        self._thread = threading.Thread(
            target=self._listen_loop,
            daemon=True,
            name="sse-broadcaster",
        )
        self._thread.start()

    def stop(self) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._healthy = False

    def _listen_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                with psycopg.connect(self._conninfo, autocommit=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("LISTEN new_articles")
                    self._healthy = True
                    logger.info("SSE broadcaster listening on channel new_articles")
                    while not self._shutdown.is_set():
                        for notify in conn.notifies(timeout=1.0):
                            payload = {"count": int(notify.payload or 0)}
                            self._fan_out(payload)
            except Exception:
                self._healthy = False
                if self._shutdown.is_set():
                    break
                logger.exception("SSE broadcaster listener failed; retrying in 5 seconds")
                time.sleep(5)

    def _fan_out(self, payload: dict[str, Any]) -> None:
        with self._lock:
            listeners = list(self._listeners.values())
        for loop, listener_queue in listeners:
            try:
                loop.call_soon_threadsafe(_put_listener_payload, listener_queue, payload)
            except RuntimeError:
                continue

    @contextmanager
    def subscribe(self) -> Generator[asyncio.Queue[dict[str, Any]], None, None]:
        client_id = next(self._listener_ids)
        listener_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
        event_loop = asyncio.get_running_loop()
        with self._lock:
            self._listeners[client_id] = (event_loop, listener_queue)
        try:
            yield listener_queue
        finally:
            with self._lock:
                self._listeners.pop(client_id, None)


def notify_new_articles(db: Session, count: int) -> None:
    bind = db.get_bind()
    if bind is None:
        return
    dialect_name = getattr(bind.dialect, "name", None)
    if dialect_name != "postgresql":
        return

    engine: Engine
    if isinstance(bind, Engine):
        engine = bind
    else:
        engine = bind.engine

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(
            text("SELECT pg_notify('new_articles', :payload)"),
            {"payload": str(int(count))},
        )


def _put_listener_payload(
    listener_queue: asyncio.Queue[dict[str, Any]], payload: dict[str, Any]
) -> None:
    try:
        listener_queue.put_nowait(payload)
    except asyncio.QueueFull:
        return
