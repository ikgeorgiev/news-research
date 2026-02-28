from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import admin_reload_tickers, admin_remap_businesswire


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


def test_admin_reload_tickers_returns_remap_payload(monkeypatch):
    db = _make_db_session()
    calls: dict[str, int | bool] = {}

    def fake_load_tickers(_db: Session, _path: str) -> dict[str, int]:
        return {"loaded": 25, "created": 4, "updated": 5, "unchanged": 16}

    def fake_remap(_db: Session, _settings, *, limit: int, only_unmapped: bool):  # noqa: ANN001
        calls["limit"] = limit
        calls["only_unmapped"] = only_unmapped
        return {
            "processed": 20,
            "articles_with_hits": 7,
            "remapped_articles": 3,
            "only_unmapped": only_unmapped,
        }

    monkeypatch.setattr("app.main.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.main.remap_businesswire_articles", fake_remap)

    response = admin_reload_tickers(
        remap_unmapped_businesswire=True,
        remap_limit=321,
        db=db,
    )

    assert response.loaded == 25
    assert response.created == 4
    assert response.updated == 5
    assert response.unchanged == 16
    assert response.remap is not None
    assert response.remap.remapped_articles == 3
    assert calls["limit"] == 321
    assert calls["only_unmapped"] is True

    db.close()


def test_admin_reload_tickers_can_skip_remap(monkeypatch):
    db = _make_db_session()
    calls = {"remap_called": 0}

    def fake_load_tickers(_db: Session, _path: str) -> dict[str, int]:
        return {"loaded": 10, "created": 0, "updated": 0, "unchanged": 10}

    def fake_remap(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["remap_called"] += 1
        return {"processed": 0, "articles_with_hits": 0, "remapped_articles": 0, "only_unmapped": True}

    monkeypatch.setattr("app.main.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.main.remap_businesswire_articles", fake_remap)

    response = admin_reload_tickers(
        remap_unmapped_businesswire=False,
        remap_limit=500,
        db=db,
    )

    assert response.loaded == 10
    assert response.remap is None
    assert calls["remap_called"] == 0

    db.close()


def test_admin_remap_businesswire_response_contract(monkeypatch):
    db = _make_db_session()
    calls: dict[str, int | bool] = {}

    def fake_remap(_db: Session, _settings, *, limit: int, only_unmapped: bool):  # noqa: ANN001
        calls["limit"] = limit
        calls["only_unmapped"] = only_unmapped
        return {
            "processed": 11,
            "articles_with_hits": 5,
            "remapped_articles": 4,
            "only_unmapped": only_unmapped,
        }

    monkeypatch.setattr("app.main.remap_businesswire_articles", fake_remap)

    response = admin_remap_businesswire(limit=777, only_unmapped=False, db=db)

    assert response.processed == 11
    assert response.articles_with_hits == 5
    assert response.remapped_articles == 4
    assert response.only_unmapped is False
    assert calls["limit"] == 777
    assert calls["only_unmapped"] is False

    db.close()
