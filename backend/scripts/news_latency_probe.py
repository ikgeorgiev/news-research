"""Bounded public-feed comparison; independent of the application DB and scheduler.

Run from backend: python -m scripts.news_latency_probe --duration-minutes 30
Results include initial snapshots, first sightings and last successful absence.
Publisher timestamps and cache headers are metadata, not measured delivery delay.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
from html import unescape
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit, urlunsplit
import xml.etree.ElementTree as ET

import feedparser
import httpx


ROUTES = {
    "gnw-rss": "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
    "gnw-google-sitemap": "https://globenewswire.com/NewsRoom/GoogleSitemap",
    "gnw-latest-sitemap": "https://sitemaps.globenewswire.com/news/en/latest.xml",
    "prn-financial-rss": "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "prn-mutual-funds-rss": "https://www.prnewswire.com/rss/financial-services-latest-news/mutual-funds-list.rss",
    "prn-dividends-rss": "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
    "bw-cef-rss": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
    "bw-finance-rss": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
    "bw-all-rss": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJcFVJYWQ==",
    "tortoise-tyg-json": "https://tortoisecapital.com/wp-json/wp/v2/news?ticker=66&_fields=id%2Cdate_gmt%2Cmodified_gmt%2Cstatus%2Ctype%2Clink%2Ctitle%2Ccontent%2Cticker",
}
DEFAULT_ROUTES = [
    "gnw-rss", "gnw-google-sitemap", "gnw-latest-sitemap", "prn-financial-rss",
    "bw-cef-rss", "bw-finance-rss",
]
HEADER_NAMES = ("date", "age", "cache-control", "etag", "last-modified", "cf-cache-status", "x-cache", "retry-after")
MAX_BYTES = 4 * 1024 * 1024


class ResponseTooLarge(ValueError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def article_key(url: str) -> str:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if host == "globenewswire.com":
        match = re.search(r"/news-release/\d{4}/\d{2}/\d{2}/(\d+)/\d+/([a-z-]+)/", parsed.path, re.I)
        if match:
            return f"gnw:{match[1]}:{match[2].lower()}"
    if host == "businesswire.com":
        match = re.search(r"/news/home/(\d+)/([a-z-]+)(?:/|$)", parsed.path, re.I)
        if match:
            return f"bw:{match[1]}:{match[2].lower()}"
    if host == "prnewswire.com":
        return urlunsplit((parsed.scheme.lower(), host, parsed.path.rstrip("/"), "", ""))
    return urlunsplit((parsed.scheme.lower(), host, parsed.path.rstrip("/"), parsed.query, ""))


def parse_issuer_items(content: bytes) -> list[dict]:
    """Public issuer JSON uses publication date, never the later edit date.

    Taxonomy membership is a discovery filter, not validated ticker evidence.
    Cross-provider title matches need separate review; IDs only join this issuer.
    """
    rows = json.loads(content)
    if not isinstance(rows, list):
        raise ValueError("Expected an issuer news collection")
    items = []
    for row in rows:
        if not isinstance(row, dict) or row.get("status") != "publish" or row.get("type") != "news":
            raise ValueError("Unexpected issuer item schema or publication status")
        if 66 not in row.get("ticker", []):
            raise ValueError("Issuer response did not respect the TYG taxonomy filter")
        post_id, link = row.get("id"), row.get("link", "")
        if not isinstance(post_id, int) or not link:
            raise ValueError("Issuer item lacks a stable ID or link")
        parts = urlsplit(link)
        if parts.scheme not in {"http", "https"} or parts.hostname not in {"tortoisecapital.com", "www.tortoisecapital.com"}:
            raise ValueError("Unexpected issuer article URL")
        published = row.get("date_gmt")
        if not published:
            raise ValueError("Issuer item lacks a publication timestamp")
        published_at = datetime.fromisoformat(published.replace("Z", "+00:00"))
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        items.append({
            "key": f"tortoise:{post_id}", "url": link,
            "title": unescape(row.get("title", {}).get("rendered", "")),
            "published_at": published_at.astimezone(timezone.utc).isoformat(),
            "modified_at": row.get("modified_gmt"), "stock_tickers": None,
            "issuer_taxonomy": row["ticker"],
            "summary_html": row.get("content", {}).get("rendered", ""),
        })
    return items


def parse_items(content: bytes, *, route: str | None = None) -> list[dict]:
    if route == "tortoise-tyg-json":
        return parse_issuer_items(content)
    root = ET.fromstring(content)
    root_name = root.tag.rsplit("}", 1)[-1].lower()
    if root_name == "urlset":
        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9", "n": "http://www.google.com/schemas/sitemap-news/0.9"}
        items = [{
            "url": node.findtext("s:loc", namespaces=ns),
            "title": node.findtext("n:news/n:title", namespaces=ns),
            "published_at": node.findtext("n:news/n:publication_date", namespaces=ns),
            "stock_tickers": node.findtext("n:news/n:stock_tickers", namespaces=ns),
        } for node in root.findall("s:url", ns)]
    elif root_name in {"rss", "feed", "rdf"}:
        parsed = feedparser.parse(content)
        items = [{
            "url": entry.get("link"), "title": entry.get("title"),
            "published_at": entry.get("published") or entry.get("updated"),
            "stock_tickers": None,
            "summary_html": entry.get("summary") or entry.get("description") or "",
        } for entry in parsed.entries]
    else:
        raise ValueError(f"Expected RSS, Atom or URL sitemap; received {root_name}")
    result = []
    for item in items:
        if item["url"] and urlsplit(item["url"]).scheme in {"http", "https"}:
            result.append({"key": article_key(item["url"]), **item})
    return result


def observe_items(state: dict, items: list[dict], received_at: str) -> list[dict]:
    baseline = state["last_success_at"] is None
    discoveries = []
    for item in items:
        if item["key"] not in state["seen"]:
            observation = {
                **item, "first_seen_at": received_at, "initial_snapshot": baseline,
                "last_absent_at": state["last_success_at"],
            }
            state["seen"][item["key"]] = observation
            discoveries.append(observation)
    state["last_success_at"] = received_at
    return discoveries


def retry_delay(value: str | None, now: datetime) -> float:
    if not value:
        return 0
    try:
        return max(0, float(value))
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0, (date - now).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0


async def fetch(client: httpx.AsyncClient, url: str, headers: dict) -> tuple[int, dict, bytes]:
    async with client.stream("GET", url, headers=headers) as response:
        data = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            data.extend(chunk)
            if len(data) > MAX_BYTES:
                raise ResponseTooLarge("Response exceeds research size limit")
        return response.status_code, {k: response.headers[k] for k in HEADER_NAMES if k in response.headers}, bytes(data)


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def summary(states: dict) -> dict:
    arrivals: dict[str, list[dict]] = {}
    for name, state in states.items():
        for observation in state["seen"].values():
            arrivals.setdefault(observation["key"], []).append({"route": name, **observation})
    comparable = [
        {"key": key, "observations": observed}
        for key, observed in arrivals.items()
        if len(observed) >= 2 and all(not row["initial_snapshot"] for row in observed)
    ]
    return {
        "routes": {
            name: {**{k: v for k, v in state.items() if k != "seen"},
                   "unique_items": len(state["seen"]),
                   "new_items": sum(not row["initial_snapshot"] for row in state["seen"].values())}
            for name, state in states.items()
        },
        "matched_new_items": comparable,
        "limitations": [
            "Initial snapshots are excluded from speed comparisons.",
            "Last absence to first sighting bounds this route's observed discovery, not original publication.",
            "Different route filters and rolling windows make item counts unsuitable as completeness rankings.",
            "Cached responses and publisher timestamps do not establish source update cadence.",
        ],
    }


async def run_probe(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    started_at = utc_now()
    deadline = time.monotonic() + args.duration_minutes * 60
    states = {name: {"url": ROUTES[name], "requests": 0, "successful_responses": 0,
                     "errors": 0, "disabled": False, "last_success_at": None, "seen": {}}
              for name in args.routes}

    def save_state(status: str) -> None:
        write_json(output / "state.json", {
            "pid": os.getpid(), "status": status, "started_at": started_at,
            "updated_at": utc_now(), "duration_minutes": args.duration_minutes,
            "interval_seconds": args.interval_seconds, **summary(states),
        })

    def emit(record: dict) -> None:
        with (output / "observations.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    async def collect(name: str, client: httpx.AsyncClient) -> None:
        state = states[name]
        validators: dict = {}
        failures = 0
        while time.monotonic() < deadline:
            request_started_at = utc_now()
            request_start = time.monotonic()
            state["requests"] += 1
            delay = args.interval_seconds
            record = {"route": name, "url": state["url"], "request_started_at": request_started_at}
            try:
                code, headers, body = await asyncio.wait_for(
                    fetch(client, state["url"], validators), timeout=15,
                )
                received = utc_now()
                record.update(status=code, headers=headers, response_received_at=received,
                              request_ms=round((time.monotonic() - request_start) * 1000, 3),
                              body_sha256=hashlib.sha256(body).hexdigest(), body_bytes=len(body))
                if code == 304 and state["last_success_at"] is not None:
                    state["last_success_at"] = received
                    state["successful_responses"] += 1
                    failures = 0
                    record["discoveries"] = []
                elif code == 200:
                    items = parse_items(body, route=name)
                    record["item_count"] = len(items)
                    record["discoveries"] = observe_items(state, items, received)
                    record["snapshot_keys"] = [item["key"] for item in items]
                    state["successful_responses"] += 1
                    failures = 0
                    validators = {}
                    if headers.get("etag"):
                        validators["If-None-Match"] = headers["etag"]
                    if headers.get("last-modified"):
                        validators["If-Modified-Since"] = headers["last-modified"]
                else:
                    if code in {401, 403, 404}:
                        state["disabled"] = True
                    delay = max(delay, retry_delay(headers.get("retry-after"), datetime.now(timezone.utc))
                                + (time.monotonic() - request_start))
                    raise ValueError(f"HTTP {code}")
            except Exception as exc:
                if isinstance(exc, ResponseTooLarge):
                    state["disabled"] = True
                failures += 1
                state["errors"] += 1
                record["error"] = f"{type(exc).__name__}: {exc}"
                delay = max(delay, min(600, args.interval_seconds * 2 ** min(failures, 5)))
            emit(record)
            save_state("running")
            if state["disabled"]:
                return
            # Fixed start-to-start cadence, with no burst to catch up after a slow response.
            remaining = deadline - time.monotonic()
            pause = max(0, delay - (time.monotonic() - request_start))
            if remaining > 0:
                await asyncio.sleep(min(pause, remaining))

    save_state("running")
    status = "failed"
    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True, http2=True,
            headers={"User-Agent": "CEFNewsLatencyResearch/0.1", "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, application/json"},
        ) as client:
            await asyncio.gather(*(collect(name, client) for name in args.routes))
        status = "completed"
    finally:
        save_state(status)
    print(json.dumps({"status": status, "output": str(output), "new_items": {k: sum(not i["initial_snapshot"] for i in v["seen"].values()) for k, v in states.items()}}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=float, default=30)
    parser.add_argument("--interval-seconds", type=float, default=30)
    parser.add_argument("--routes", nargs="+", choices=list(ROUTES), default=DEFAULT_ROUTES)
    parser.add_argument("--output", default=str(Path(__file__).resolve().parents[2] / ".tmp" / f"latency-probe-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"))
    args = parser.parse_args()
    if not 0 < args.duration_minutes <= 780 or args.interval_seconds < 10:
        parser.error("Duration must be 0–780 minutes; interval must be at least 10 seconds")
    asyncio.run(run_probe(args))


if __name__ == "__main__":
    main()
