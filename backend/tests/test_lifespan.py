import asyncio
from contextlib import nullcontext
import logging
from unittest.mock import MagicMock

from fastapi import FastAPI
import pytest

from app import main
from app.config import Settings
from app.page_access import PageAccessStore, get_page_access


@pytest.mark.parametrize("failure", [None, "bootstrap", "starting", "serving", "stopping"])
def test_lifespan_unwinds_owned_resources(monkeypatch, failure):
    app = FastAPI()
    settings = Settings(_env_file=None, database_url="sqlite:///:memory:", scheduler_enabled=True,
                        postcommit_enrichment_enabled=True)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "assert_schema_at_head", lambda _: None)
    monkeypatch.setattr(main, "get_session_factory", MagicMock())
    sync = MagicMock(return_value={"ticker_sync": {}, "stale_runs_fixed": 0})
    if failure == "bootstrap":
        sync.side_effect = RuntimeError(failure)
    monkeypatch.setattr(main, "sync_runtime_state", sync)

    handler = logging.NullHandler()
    close_handler = MagicMock(wraps=handler.close)
    monkeypatch.setattr(handler, "close", close_handler)

    def attach_handler(_):
        logging.getLogger().addHandler(handler)
        return handler

    monkeypatch.setattr(main, "configure_research_timing", attach_handler)

    stopped = []
    store = MagicMock()
    store.close.side_effect = lambda: stopped.append("page access")
    monkeypatch.setattr(PageAccessStore, "for_database", lambda _: store)
    services = {}
    for name in ("IngestionScheduler", "ArticleEnrichmentWorker", "FinancialContentWorker",
                 "FreeDiscoveryWorker", "SSEBroadcaster", "PushAlertDispatcher"):
        worker = MagicMock()
        stop = worker.shutdown if name == "IngestionScheduler" else worker.stop
        stop.side_effect = lambda name=name: stopped.append(name)
        monkeypatch.setattr(main, name, MagicMock(return_value=worker))
        services[name] = worker
    if failure == "starting":
        services["FreeDiscoveryWorker"].start.side_effect = RuntimeError(failure)
    if failure == "stopping":
        def fail_stop():
            stopped.append("FreeDiscoveryWorker")
            raise RuntimeError(failure)
        services["FreeDiscoveryWorker"].stop.side_effect = fail_stop

    async def run():
        async with main.lifespan(app):
            assert get_page_access() is store
            assert main.get_scheduler() is services["IngestionScheduler"]
            assert app.state.sse_broadcaster is services["SSEBroadcaster"]
            if failure == "serving":
                raise RuntimeError(failure)

    try:
        outcome = pytest.raises(RuntimeError, match=failure) if failure else nullcontext()
        with outcome:
            asyncio.run(run())
        assert handler not in logging.getLogger().handlers
        close_handler.assert_called_once_with()
        if failure != "bootstrap":
            assert main.get_scheduler() is None
            assert get_page_access() is None
            assert stopped[-1] == "page access"
            started = {name for name, worker in services.items() if worker.start.called}
            assert sorted(stopped[:-1]) == sorted(started)
            assert stopped.index("IngestionScheduler") < stopped.index("ArticleEnrichmentWorker")
            assert stopped.index("ArticleEnrichmentWorker") < stopped.index("PushAlertDispatcher")
            assert not hasattr(app.state, "sse_broadcaster")
    finally:
        # Keep other tests isolated if an assertion catches a leaked handler.
        logging.getLogger().removeHandler(handler)
        main.set_scheduler(None)
