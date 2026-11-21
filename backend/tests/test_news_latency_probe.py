import asyncio
import json
from datetime import datetime, timezone

import httpx
import pytest

from scripts import news_latency_probe
from scripts.news_latency_probe import article_key, observe_items, parse_items, retry_delay, summary


def test_gnw_sitemap_and_rss_copies_match_without_merging_languages():
    url = "https://www.globenewswire.com/news-release/2026/10/26/12345/0/en/Original-Title.html"
    assert article_key(url) == article_key(url.replace("www.", "").replace("Original-Title", "Updated-Title"))
    assert article_key(url) != article_key(url.replace("/en/", "/fr/"))
    xml = f'''<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
      xmlns:n="http://www.google.com/schemas/sitemap-news/0.9"><url><loc>{url}</loc>
      <n:news><n:title>New release</n:title><n:publication_date>2026-10-26T10:00:00Z</n:publication_date>
      <n:stock_tickers>NYSE:ABC</n:stock_tickers></n:news></url></urlset>'''.encode()
    item = parse_items(xml)[0]
    assert item["key"] == "gnw:12345:en"
    assert item["stock_tickers"] == "NYSE:ABC"
    assert item["published_at"] == "2026-10-26T10:00:00Z"


def test_atom_is_recognized_and_html_error_is_not_a_successful_empty_feed():
    xml = b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry>
      <title>New release</title><link href="https://example.com/release"/>
      <updated>2026-10-26T10:00:00Z</updated></entry></feed>'''
    assert parse_items(xml)[0]["url"] == "https://example.com/release"
    with pytest.raises(ValueError, match="Expected RSS"):
        parse_items(b"<html><body>Access denied</body></html>")


def test_businesswire_feed_tracking_does_not_split_one_release():
    url = "https://www.businesswire.com/news/home/20261026012345/en/A-Release"
    assert article_key(url + "?feedref=one") == article_key(url + "?feedref=two")
    assert article_key(url) != article_key(url.replace("/en/", "/fr/"))


def test_baseline_items_are_not_latency_samples_and_new_items_keep_absence_bounds():
    a = {"key": "old", "url": "https://example.com/old"}
    b = {"key": "new", "url": "https://example.com/new"}
    states = {name: {"seen": {}, "last_success_at": None} for name in ("rss", "sitemap")}
    observe_items(states["rss"], [a], "2026-10-26T10:00:00Z")
    observe_items(states["sitemap"], [a], "2026-10-26T10:00:01Z")
    assert summary(states)["matched_new_items"] == []
    fresh = observe_items(states["rss"], [a, b], "2026-10-26T10:00:30Z")
    assert fresh[0]["last_absent_at"] == "2026-10-26T10:00:00Z"
    assert not fresh[0]["initial_snapshot"]
    observe_items(states["sitemap"], [a, b], "2026-10-26T10:00:31Z")
    assert [row["key"] for row in summary(states)["matched_new_items"]] == ["new"]
    assert observe_items(states["rss"], [a, b], "2026-10-26T10:01:00Z") == []


def test_retry_after_supports_seconds_and_http_dates():
    now = datetime(2026, 10, 26, 10, tzinfo=timezone.utc)
    assert retry_delay("120", now) == 120
    assert retry_delay("Mon, 26 Oct 2026 10:02:00 GMT", now) == 120
    assert retry_delay("bad", now) == 0


def test_oversized_feed_is_rejected_without_reading_article_pages(monkeypatch):
    monkeypatch.setattr(news_latency_probe, "MAX_BYTES", 16)
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=b"x" * 17)

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            with pytest.raises(news_latency_probe.ResponseTooLarge):
                await news_latency_probe.fetch(client, "https://example.com/feed", {})

    asyncio.run(exercise())
    assert requests == ["https://example.com/feed"]


def issuer_record(**overrides):
    return {"id": 9727, "status": "publish", "type": "news", "ticker": [66],
            "link": "https://tortoisecapital.com/news/tyg-release/",
            "title": {"rendered": "TYG distributions &amp; updates"},
            "content": {"rendered": "<p>Tortoise Energy Infrastructure (NYSE: TYG)</p>"},
            "date_gmt": "2026-08-20T15:00:00", "modified_gmt": "2026-10-26T16:00:00",
            **overrides}


def test_issuer_publication_not_modification_drives_date_and_stable_id():
    rows = [issuer_record(), issuer_record(id=9876, date_gmt="2026-10-20T15:00:00")]
    items = parse_items(json.dumps(rows).encode(), route="tortoise-tyg-json")
    assert items[0]["published_at"] == "2026-08-20T15:00:00+00:00"
    assert items[0]["key"] != items[1]["key"]
    assert items[0]["title"] == "TYG distributions & updates"
    assert "NYSE: TYG" in items[0]["summary_html"]
    assert items[0]["stock_tickers"] is None  # taxonomy is not validated evidence
    changed = parse_items(json.dumps([issuer_record(modified_gmt="2026-10-27T00:00:00")]).encode(), route="tortoise-tyg-json")
    state = {"seen": {}, "last_success_at": None}
    assert observe_items(state, items, "2026-10-27T10:00:00Z")[0]["initial_snapshot"]
    assert observe_items(state, changed, "2026-10-27T10:01:00Z") == []


@pytest.mark.parametrize("overrides", [{"ticker": [22]}, {"status": "draft"},
                                       {"date_gmt": None}, {"link": "https://unrelated.example/post"}])
def test_issuer_unexpected_data_is_not_counted_as_a_successful_snapshot(overrides):
    with pytest.raises(ValueError):
        parse_items(json.dumps([issuer_record(**overrides)]).encode(), route="tortoise-tyg-json")


def test_rss_preserves_summary_for_later_ticker_validation():
    body = b'<rss version="2.0"><channel><item><title>Distribution</title><link>https://example.com/a</link><description>NYSE: TYG</description></item></channel></rss>'
    assert "NYSE: TYG" in parse_items(body)[0]["summary_html"]
