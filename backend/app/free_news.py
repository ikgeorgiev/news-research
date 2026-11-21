"""Bounded free discovery backups. Candidates never create articles or alerts."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import urlsplit
from uuid import uuid4

import feedparser
import httpx

from app.financialcontent.runtime import singleton_lock, session_window
from app.release_matching import release_identity

logger = logging.getLogger(__name__)
MAX_BYTES = 1024 * 1024
MIN_INTERVAL = 300.0
ROUTES = {
    "pressreleasefeed": "https://pressreleasefeed.org/api/v1/releases?q=closed-end&per_page=100",
    "xai": "https://xainvestments.com/news-distributions/feed/",
}
HELD_GNW_FEED = "https://www.globenewswire.com/rssfeed/organization/MqpMjrUEB9C-kuKoReZgvA=="


def bounded_routes(routes, now):
    # The documented keyword query may time out across the whole archive.
    # A stable date window avoids that scan without per-request cache busting.
    result = dict(routes)
    since = (now.astimezone(timezone.utc).date()-timedelta(days=3)).isoformat()
    result['pressreleasefeed'] = ROUTES['pressreleasefeed']+'&since='+since
    return result


def load_routes(custom_path=None):
    routes = dict(ROUTES)
    if not custom_path:
        return routes
    path = Path(custom_path)
    if path.stat().st_size > 32768:
        raise ValueError("oversized_feed_configuration")
    config = json.loads(path.read_text(encoding="utf-8-sig"))
    if config.get("version") != 1 or not isinstance(config.get("feeds"), list) or len(config["feeds"]) > 8:
        raise ValueError("invalid_feed_configuration")
    for row in config["feeds"]:
        url = public_url(row.get("url"))
        parts = urlsplit(url)
        if (parts.netloc != "www.globenewswire.com" or not parts.path.lower().startswith("/rssfeed/")
                or parts.fragment or "%" in parts.path or ".." in parts.path
                or row.get("provenance") != "user_supplied_reader_feed"):
            raise ValueError("only_literal_user_supplied_gnw_rss")
        if parts.path.casefold() == urlsplit(HELD_GNW_FEED).path.casefold():
            raise ValueError("retained_oxford_hold")
        # Already enrolled ordinary feeds remain owned by the main collector.
        from app.sources import GLOBENEWSWIRE_FEEDS
        existing = [*GLOBENEWSWIRE_FEEDS,
                    "https://www.globenewswire.com/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=="]
        if any(url.casefold() == old.casefold() for old in [*existing, *routes.values()]):
            raise ValueError("duplicate_or_already_enrolled_feed")
        key = "gnw_custom_" + hashlib.sha256(url.encode()).hexdigest()[:16]
        routes[key] = url
    return routes


def public_url(value):
    if not isinstance(value, str) or len(value) > 4096 or re.search(r"[\s\x00-\x1f]", value):
        raise ValueError("invalid_url")
    p = urlsplit(value)
    if p.scheme != "https" or not p.hostname or p.username or p.password or p.port not in (None, 443):
        raise ValueError("invalid_url")
    return value


def publication(value):
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 100:
        raise ValueError("invalid_date")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(timezone.utc).isoformat() if dt.tzinfo is not None else None


def _text(value, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("invalid_text")
    return value.strip()


def parse_metadata(body):
    if len(body) > MAX_BYTES:
        raise ValueError("oversized_response")
    payload = json.loads(body)
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise ValueError("invalid_schema")
    data = payload["data"]
    if len(data) > 100:
        raise ValueError("too_many_items")
    rows, seen = [], set()
    for row in data:
        if not isinstance(row, dict):
            raise ValueError("invalid_item")
        item_id = _text(row.get("id"), 200)
        if item_id in seen:
            raise ValueError("duplicate_provider_id")
        seen.add(item_id)
        url = public_url(row.get("url"))
        identity = release_identity(url)
        publisher = row.get("publisher")
        label = publisher.get("name") if isinstance(publisher, dict) else None
        rows.append(dict(provider_id=item_id, title=_text(row.get("title"), 2000), url=url,
                         published_at=publication(row.get("published_at")),
                         publication_label=row.get("published_at"),
                         publisher_label=label[:200] if isinstance(label, str) else None,
                         wire_identity=list(identity) if identity else None,
                         identity_basis="original_url_only" if identity else "unresolved",
                         validated_tickers=[], review_only=True))
    return rows


def parse_issuer(body, *, gnw=False):
    if len(body) > MAX_BYTES or b"<!DOCTYPE" in body.upper() or b"<!ENTITY" in body.upper():
        raise ValueError("unsupported_xml")
    doc = feedparser.parse(body)
    if doc.bozo or not doc.feed.get("title") or len(doc.entries) > 100:
        raise ValueError("invalid_feed")
    rows, seen = [], set()
    for entry in doc.entries:
        url = public_url(entry.get("link"))
        identity = release_identity(url)
        if gnw:
            if identity is None or identity[0] != "globenewswire":
                raise ValueError("unexpected_original_link")
        elif urlsplit(url).hostname not in {"xainvestments.com", "www.xainvestments.com"}:
            raise ValueError("unexpected_issuer_link")
        item_id = _text(entry.get("id") or url, 4096)
        if item_id in seen:
            raise ValueError("duplicate_provider_id")
        seen.add(item_id)
        label = entry.get("published")
        try:
            dt = parsedate_to_datetime(label)
            published = dt.astimezone(timezone.utc).isoformat() if dt.tzinfo else None
        except (ValueError, TypeError, AttributeError):
            published = None
        rows.append(dict(provider_id=item_id, title=_text(entry.get("title"), 2000), url=url,
                         published_at=published, publication_label=label,
                         publisher_label="GlobeNewswire" if gnw else "XAI issuer",
                         wire_identity=list(identity) if identity else None,
                         identity_basis="original_url_only" if identity else "issuer_url_not_original_wire_identity",
                         validated_tickers=[], review_only=True))
    return rows


def next_delay(headers, now):
    ages = [int(v) for v in re.findall(r'(?:^|,)\s*(?:s-maxage|max-age)\s*=\s*"?(\d+)',
                                     headers.get("cache-control", ""), re.I)]
    delay = max([MIN_INTERVAL, *ages])
    retry = headers.get("retry-after")
    if retry:
        try:
            seconds = float(retry)
            if not (0 <= seconds < float("inf")):
                raise ValueError("nonfinite_retry_after")
        except ValueError:
            try:
                parsed = parsedate_to_datetime(retry)
                seconds = parsed.timestamp()-now if parsed.tzinfo else 0
            except (TypeError, ValueError, OverflowError):
                seconds = MIN_INTERVAL
        delay = max(delay, seconds)
    return delay


class DiscoveryStore:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS control(route TEXT PRIMARY KEY, host TEXT NOT NULL,
            hold TEXT, next_due REAL NOT NULL DEFAULT 0, pending REAL, session TEXT, baseline REAL);
          CREATE TABLE IF NOT EXISTS host_control(host TEXT PRIMARY KEY, hold TEXT, next_due REAL NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS response(id INTEGER PRIMARY KEY, route TEXT, started REAL,
            received REAL, status INTEGER, error TEXT, digest TEXT, item_count INTEGER, session TEXT, url TEXT);
          CREATE TABLE IF NOT EXISTS item(route TEXT, provider_id TEXT, digest TEXT,
            first_seen REAL, PRIMARY KEY(route, provider_id));
          CREATE TABLE IF NOT EXISTS observation(id INTEGER PRIMARY KEY, response_id INTEGER, route TEXT,
            provider_id TEXT, kind TEXT, data TEXT, FOREIGN KEY(response_id) REFERENCES response(id));
          CREATE TABLE IF NOT EXISTS candidate(signature TEXT PRIMARY KEY, first_seen REAL);
        """)
        if 'url' not in {row[1] for row in self.db.execute('PRAGMA table_info(response)')}:
            self.db.execute('ALTER TABLE response ADD COLUMN url TEXT')
            self.db.commit()

    def begin_session(self, route, session, url=None):
        host = urlsplit(url or ROUTES[route]).hostname
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO control(route,host) VALUES(?,?)", (route, host))
            self.db.execute("INSERT OR IGNORE INTO host_control(host) VALUES(?)", (host,))
            # An interrupted request is not silently replayed on restart.
            self.db.execute("UPDATE control SET hold=coalesce(hold,'uncertain_interrupted_request') WHERE route=? AND pending IS NOT NULL", (route,))
            self.db.execute("UPDATE control SET session=?,baseline=NULL WHERE route=? AND session IS NOT ?", (session, route, session))

    def claim(self, route, now):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute("SELECT c.*,h.hold host_hold,h.next_due host_due FROM control c JOIN host_control h USING(host) WHERE route=?", (route,)).fetchone()
            if not row or row['hold'] or row['host_hold'] or row['pending'] is not None or max(row['next_due'], row['host_due']) > now:
                return False
            self.db.execute("UPDATE control SET pending=?,next_due=? WHERE route=?", (now, now+MIN_INTERVAL, route))
            self.db.execute("UPDATE host_control SET next_due=? WHERE host=?", (now+MIN_INTERVAL, row['host']))
            return True

    def finish(self, route, now, status, headers, body, rows, error=None, *, url=None):
        delay = next_delay(headers, now)
        with self.db:
            control = self.db.execute("SELECT * FROM control WHERE route=?", (route,)).fetchone()
            if control is None or control['pending'] is None:
                raise ValueError("unclaimed_response")
            hold = (f"http_{status}" if (status and 300 <= status < 500 and status not in {408,429})
                    else "schema_failure" if error == "schema_failure" else None)
            cur = self.db.execute("INSERT INTO response(route,started,received,status,error,digest,item_count,session,url) VALUES(?,?,?,?,?,?,?,?,?)",
                (route, control['pending'], now, status, error, hashlib.sha256(body).hexdigest() if body else None, len(rows), control['session'],url))
            baseline = control['baseline']
            if status == 200 and error is None:
                for row in rows:
                    data = json.dumps(row, sort_keys=True, ensure_ascii=False)
                    digest = hashlib.sha256(data.encode()).hexdigest()
                    old = self.db.execute("SELECT digest FROM item WHERE route=? AND provider_id=?", (route,row['provider_id'])).fetchone()
                    signature = hashlib.sha256(json.dumps([row['wire_identity'] or row['url'], row['title'], row['published_at']], sort_keys=True).encode()).hexdigest()
                    prior_candidate = self.db.execute("SELECT 1 FROM candidate WHERE signature=?", (signature,)).fetchone()
                    kind = "baseline" if baseline is None else "updated" if old else "new"
                    if baseline is not None and old and old['digest'] == digest:
                        continue
                    if kind == "new":
                        pub = row['published_at']
                        if not pub:
                            kind = "unknown_date"
                        elif not baseline <= datetime.fromisoformat(pub).timestamp() <= now+300:
                            kind = "history"
                        elif prior_candidate:
                            kind = "matching_metadata_new_id"
                    self.db.execute("INSERT INTO observation(response_id,route,provider_id,kind,data) VALUES(?,?,?,?,?)",
                                    (cur.lastrowid,route,row['provider_id'],kind,data))
                    self.db.execute("INSERT INTO item VALUES(?,?,?,?) ON CONFLICT(route,provider_id) DO UPDATE SET digest=excluded.digest",
                                    (route,row['provider_id'],digest,now))
                    self.db.execute("INSERT OR IGNORE INTO candidate VALUES(?,?)", (signature, now))
                baseline = baseline if baseline is not None else now
            else:
                baseline = None  # Next successful full response establishes a fresh baseline.
            self.db.execute("UPDATE control SET hold=coalesce(hold,?),next_due=?,pending=NULL,baseline=? WHERE route=?",
                            (hold,now+delay,baseline,route))
            self.db.execute("UPDATE host_control SET hold=coalesce(hold,?),next_due=max(next_due,?) WHERE host=?",
                            (hold if status in {401,403,404} else None,now+delay,control['host']))

    def snapshot(self):
        routes=[dict(r) for r in self.db.execute(
            "SELECT c.route,c.hold,c.next_due,c.pending,c.baseline,h.hold host_hold,r.received last_response_at,r.status last_http_status,r.error last_error FROM control c JOIN host_control h USING(host) LEFT JOIN response r ON r.id=(SELECT max(id) FROM response WHERE route=c.route)")]
        available=sum(not r['hold'] and not r['host_hold'] and r['last_http_status']==200 and r['baseline'] is not None for r in routes)
        return dict(review_only=True, production_articles=0, scope="closed-end keyword metadata, XAI issuer, and explicitly supplied custom GNW feeds", routes=routes,
            discovery_status='available' if routes and available==len(routes) else 'partial' if available else 'unavailable',
            observations=[dict(r) for r in self.db.execute("SELECT route,kind,count(*) count FROM observation GROUP BY route,kind")])

    def close(self):
        self.db.close()


async def fetch_metadata(url, *, transport=None, approved_urls=()):
    if url not in ROUTES.values() and url not in approved_urls:
        raise ValueError("unreviewed_endpoint")
    # Fresh client per request means no response cookies survive into the next
    # request. Environment proxies and .netrc credentials are not inherited.
    async with asyncio.timeout(15), httpx.AsyncClient(timeout=15, follow_redirects=False,
            trust_env=False, transport=transport, headers={"User-Agent":"cef-news/1.0", "Accept":"application/json, application/rss+xml"}) as client:
        async with client.stream("GET", url) as response:
            headers = {key:response.headers[key] for key in ("cache-control","retry-after") if key in response.headers}
            if response.status_code != 200:
                return response.status_code, headers, b""
            content = bytearray()
            async for chunk in response.aiter_bytes(32768):
                if len(content)+len(chunk) > MAX_BYTES:
                    raise ValueError("oversized_response")
                content.extend(chunk)
            return response.status_code, headers, bytes(content)


class FreeDiscoveryWorker:
    def __init__(self, settings):
        self.settings = settings
        self.stop_event = threading.Event()
        self.thread = None
        self.state = {"status":"disabled", "review_only":True}

    def start(self):
        if not (self.settings.scheduler_enabled and self.settings.source_enable_free_discovery):
            return
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="free-news-discovery", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=20)

    def snapshot(self):
        return dict(self.state)

    def _run(self):
        lock = singleton_lock(str(self.settings.free_discovery_state_path)+".lock")
        if lock is None:
            self.state = {"status":"already_owned", "review_only":True}
            return
        store = None
        try:
            store = DiscoveryStore(self.settings.free_discovery_state_path)
            routes = load_routes(getattr(self.settings, "gnw_custom_feeds_path", None))
            session = uuid4().hex
            day = None
            while not self.stop_event.is_set():
                now = datetime.now(timezone.utc)
                active, current_day, cutoff = session_window(now)
                if active:
                    request_routes = bounded_routes(routes, now)
                    if current_day != day:
                        day = current_day
                        for route, url in routes.items():
                            store.begin_session(route, session+":"+day, url)
                    for route, url in request_routes.items():
                        if self.stop_event.is_set() or not session_window(datetime.now(timezone.utc))[0]:
                            break
                        if not store.claim(route, time.time()):
                            continue
                        status, headers, body, rows, error = None, {}, b"", [], None
                        try:
                            status, headers, body = asyncio.run(fetch_metadata(url, approved_urls=tuple(request_routes.values())))
                            if status == 200:
                                rows = parse_metadata(body) if route == "pressreleasefeed" else parse_issuer(body, gnw=route.startswith("gnw_custom_"))
                        except (ValueError, TypeError, KeyError, UnicodeError):
                            error = "schema_failure"
                        except Exception as exc:
                            error = type(exc).__name__
                        store.finish(route, time.time(), status, headers, body, rows, error, url=url)
                self.state = {**store.snapshot(), "status":"running" if active else "outside_hours", "next_cutoff":cutoff.isoformat()}
                self.stop_event.wait(1)
        except Exception as exc:
            logger.exception("Free discovery stopped; retained controls need inspection")
            self.state = {"status":"failed", "error":type(exc).__name__, "review_only":True}
        finally:
            if store:
                store.close()
            lock.close()
            if self.state.get("status") != "failed":
                self.state = {**self.state,"status":"stopped"}
