from __future__ import annotations

from sqlalchemy.orm import Session

from app.main import (
    admin_dedupe_businesswire_url_variants,
    admin_reload_tickers,
    admin_revalidate_articles,
)


def test_admin_reload_tickers_returns_source_remaps(monkeypatch, db_session):
    calls: dict[str, int | bool | list[str]] = {}

    def fake_load_tickers(_db: Session, _path: str) -> dict[str, int]:
        return {"loaded": 25, "created": 4, "updated": 5, "unchanged": 16}

    def fake_remap(
        _db: Session, _settings, *, source_code: str, limit: int, only_unmapped: bool,  # noqa: ANN001
    ):
        calls["limit"] = limit
        calls["only_unmapped"] = only_unmapped
        calls.setdefault("source_codes", []).append(source_code)

        if source_code == "businesswire":
            processed = 20
            articles_with_hits = 7
            remapped_articles = 3
        else:
            processed = 0
            articles_with_hits = 0
            remapped_articles = 0

        return {
            "source_code": source_code,
            "processed": processed,
            "articles_with_hits": articles_with_hits,
            "remapped_articles": remapped_articles,
            "only_unmapped": only_unmapped,
        }

    monkeypatch.setattr("app.main.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.main.remap_source_articles", fake_remap)

    response = admin_reload_tickers(
        remap_unmapped=True,
        remap_limit=321,
        db=db_session,
    )

    assert response.loaded == 25
    assert response.created == 4
    assert response.updated == 5
    assert response.unchanged == 16
    assert response.source_remaps is not None
    assert any(remap.source_code == "businesswire" for remap in response.source_remaps)
    bw_remap = next(remap for remap in response.source_remaps if remap.source_code == "businesswire")
    assert bw_remap.remapped_articles == 3
    assert calls["limit"] == 321
    assert calls["only_unmapped"] is True
    assert "businesswire" in calls["source_codes"]


def test_admin_reload_tickers_can_skip_remap(monkeypatch, db_session):
    calls = {"remap_called": 0}

    def fake_load_tickers(_db: Session, _path: str) -> dict[str, int]:
        return {"loaded": 10, "created": 0, "updated": 0, "unchanged": 10}

    def fake_remap(*_args, **_kwargs):  # noqa: ANN002,ANN003
        calls["remap_called"] += 1
        return {"processed": 0, "articles_with_hits": 0, "remapped_articles": 0, "only_unmapped": True}

    monkeypatch.setattr("app.main.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.main.remap_source_articles", fake_remap)

    response = admin_reload_tickers(
        remap_unmapped=False,
        remap_limit=500,
        db=db_session,
    )

    assert response.loaded == 10
    assert response.source_remaps is None
    assert calls["remap_called"] == 0


def test_admin_dedupe_businesswire_url_variants_response_contract(monkeypatch, db_session):
    def fake_dedupe(_db: Session):  # noqa: ANN001
        return {
            "scanned_articles": 42,
            "duplicate_groups": 6,
            "merged_articles": 7,
            "raw_items_relinked": 11,
            "ticker_rows_relinked": 3,
            "ticker_rows_updated": 1,
            "ticker_rows_deleted": 2,
        }

    monkeypatch.setattr("app.main.dedupe_businesswire_url_variants", fake_dedupe)

    response = admin_dedupe_businesswire_url_variants(db=db_session)

    assert response.scanned_articles == 42
    assert response.duplicate_groups == 6
    assert response.merged_articles == 7
    assert response.raw_items_relinked == 11
    assert response.ticker_rows_relinked == 3
    assert response.ticker_rows_updated == 1
    assert response.ticker_rows_deleted == 2



def test_admin_revalidate_articles_response_contract(monkeypatch, db_session):
    def fake_revalidate(_db: Session, *, limit: int, timeout_seconds: int):  # noqa: ANN001
        assert limit == 321
        assert timeout_seconds > 0
        return {"scanned": 12, "revalidated": 4, "purged": 3, "unchanged": 5}

    monkeypatch.setattr("app.main.revalidate_stale_article_tickers", fake_revalidate)

    response = admin_revalidate_articles(limit=321, db=db_session)

    assert response.scanned == 12
    assert response.revalidated == 4
    assert response.purged == 3
    assert response.unchanged == 5
