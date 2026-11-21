"""Application-owned FC discovery and body tasks with explicit opt-in and controls."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import threading
import time
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select, text

from app.financialcontent.delivery import deliver, IdentityConflict
from app.financialcontent.catchup import is_second_page
from app.financialcontent.financialcontent_adapter import (
    ArticleUnavailable, InvalidArticle, fetch_article, parse_article,
)
from app.financialcontent import gnw_body
from app.financialcontent.listings import REPUBLISHER_ROUTES, PUBLISHED_ROUTES, GNW_ROUTE, GNW_URL, parse_republisher_index
from app.financialcontent.store import Store
from app.financialcontent.health import listing_health
from app.financialcontent.structured_body import build_reader_document, write_reader_archive
from app.financialcontent.title_names import load_selection_context
from app.models import Source
from app.page_access import retry_deadline

logger = logging.getLogger(__name__)
MAX_LISTING_BYTES = 4 * 1024 * 1024
REQUEST_SECONDS = 15


def session_window(now):
    """Production default: weekdays 10:00-24:00 Kyiv, distinct from the closed study."""
    local = now.astimezone(ZoneInfo('Europe/Kyiv'))
    cutoff = (local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).astimezone(timezone.utc)
    active = local.weekday() < 5 and local.hour >= 10 and now < cutoff-timedelta(seconds=30)
    return active, local.date().isoformat(), cutoff


def singleton_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open('a+b')
    if stream.tell() == 0:
        stream.write(b'0')
        stream.flush()
    stream.seek(0)
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        stream.close()
        return None
    return stream


async def fetch_listing(client, url, headers, *, catchup=False):
    if not (is_second_page(url) if catchup else url in PUBLISHED_ROUTES.values()):
        raise ValueError('Only the reviewed published listings are allowed')

    async def read():
        async with client.stream('GET', url, headers=headers, follow_redirects=False) as response:
            status = response.status_code
            metadata = {k: v for k, v in response.headers.items()
                        if k in {'etag', 'last-modified', 'retry-after', 'content-type', 'cache-control'}}
            if status != 200:
                return status, metadata, b''
            content = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                content.extend(chunk)
                if len(content) > MAX_LISTING_BYTES:
                    raise ValueError('Listing exceeds four MiB')
            return status, metadata, bytes(content)

    return await asyncio.wait_for(read(), timeout=REQUEST_SECONDS)


def validate_recent(entry, now):
    published = datetime.fromisoformat(entry['published'])
    if published.tzinfo is None or not now-86400 <= published.timestamp() <= now+300:
        raise InvalidArticle('Publication metadata outside the recent-release window')


class FinancialContentWorker:
    def __init__(self, settings, factory):
        self.settings = settings
        self.factory = factory
        self.stop_event = threading.Event()
        self.thread = None
        self.store = None
        self.state = {'status': 'disabled', 'production': True}

    def start(self):
        if not (self.settings.scheduler_enabled and self.settings.source_enable_financialcontent):
            return
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._thread_main, name='financialcontent', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=35)
            if self.thread.is_alive():
                logger.error('FinancialContent worker did not finish its bounded shutdown')

    def snapshot(self):
        return dict(self.state)

    def enabled_routes(self, now=None):
        now = now or datetime.now(timezone.utc)
        routes = dict(REPUBLISHER_ROUTES)
        until = getattr(self.settings, 'financialcontent_gnw_review_until', None)
        if (isinstance(until, datetime) and until.tzinfo is not None
                and session_window(now)[0] and now < until-timedelta(seconds=30)):
            routes[GNW_ROUTE] = GNW_URL
        return routes

    def _thread_main(self):
        lock = singleton_lock(str(self.settings.financialcontent_state_path) + '.lock')
        if lock is None:
            self.state = {'status': 'already_owned', 'production': True}
            return
        try:
            self.store = Store(self.settings.financialcontent_state_path)
            if not self.store.inspect()['seed_reviewed']:
                self.state = {'status': 'needs_control_import', 'production': True}
                return
            self.store.recover_owned_work()
            asyncio.run(self._run())
        except Exception as exc:
            self.state = {'status': 'failed', 'error': type(exc).__name__, 'production': True}
            logger.exception('FinancialContent worker failed; retained state needs review')
        finally:
            lock.close()

    def _enabled_source(self):
        with self.factory() as db:
            if db.get_bind().dialect.name == 'postgresql':
                db.execute(text("SET LOCAL statement_timeout='5s'"))
            row = db.scalar(select(Source).where(Source.code == 'financialcontent'))
            return row is not None and row.enabled

    async def _run(self):
        self.source_enabled = False
        async with httpx.AsyncClient(timeout=15, follow_redirects=False,
                headers={'User-Agent': 'cef-news/1.0 (local research reader)', 'Accept': 'text/html'},
                limits=httpx.Limits(max_connections=2, max_keepalive_connections=2)) as client:
            await asyncio.gather(self._discovery_loop(client), self._delivery_loop(client))
        self.state = {**self.state, 'status': 'stopped'}

    async def _discovery_loop(self, client):
        refreshed = 0
        context = None
        while not self.stop_event.is_set():
            now = datetime.now(timezone.utc)
            active, day, cutoff = session_window(now)
            if active and time.time()-refreshed >= 30:
                self.source_enabled = self._enabled_source()
                with self.factory() as db:
                    if db.get_bind().dialect.name == 'postgresql':
                        db.execute(text("SET LOCAL statement_timeout='5s'"))
                    context = load_selection_context(db)
                refreshed = time.time()
            if active and self.source_enabled and context is not None:
                for route, url in self.enabled_routes(now).items():
                    if self.stop_event.is_set() or not session_window(datetime.now(timezone.utc))[0]:
                        break
                    interval = 30 if route == 'financialcontent-bw-index' else 60
                    await self.poll_listing(client, route, url, day, interval, context)
            status = 'running' if active and self.source_enabled else 'outside_hours' if not active else 'source_disabled'
            controls = self.store.inspect()
            if controls['controls'].get('disabled'):
                status = 'access_refused'
            elif any(r['request_pending'] for r in controls['routes']):
                status = 'listing_needs_review'
            self.state = {'status': status, 'production': True, 'day': day,
                          'updated_at': datetime.now(timezone.utc).isoformat(),
                          'next_cutoff': cutoff.isoformat(), 'queue': controls['counts'],
                          'catchup': controls['catchup'],
                          'gnw_review_enabled': GNW_ROUTE in self.enabled_routes(now),
                          'gnw_review_only': True,
                          'review_bodies': controls['review_bodies'],
                          'listing_health': listing_health(controls, self.enabled_routes(now),
                              active=active and self.source_enabled, now=time.time())}
            await asyncio.sleep(.5)

    async def poll_listing(self, client, route, url, day, interval, context):
        if route == GNW_ROUTE:
            if route not in self.enabled_routes() or url != GNW_URL:
                return
            interval = max(60, interval)
        now = time.time()
        headers = self.store.begin_listing(route, now=now, day=day, interval=interval)
        if headers is None:
            return
        status = None
        metadata = {}
        items = None
        error = None
        content = b''
        try:
            status, metadata, content = await fetch_listing(client, url, headers)
            if status == 200:
                items = parse_republisher_index(content, route, lambda value: value)
        except Exception as exc:
            error = type(exc).__name__
        received = time.time()
        retry_until = retry_deadline(metadata.get('retry-after'), datetime.fromtimestamp(received, timezone.utc)).timestamp() if status == 429 or metadata.get('retry-after') else 0
        self.store.finish_listing(route, status=status, now=received, day=day, items=items,
                                  context=context, headers=metadata, retry_until=retry_until, error=error,
                                  html=content.decode('utf-8', errors='replace') if content else None)
        if not self.stop_event.is_set() and session_window(datetime.now(timezone.utc))[0]:
            job = self.store.claim_catchup(received, time.time())
            if job:
                await self.poll_catchup(client, job)
        else:
            self.store.cancel_unstarted_catchup(received)

    async def poll_catchup(self, client, job):
        status, metadata, items, error = None, {}, None, None
        try:
            status, metadata, content = await fetch_listing(client, job['url'], {}, catchup=True)
            if status == 200:
                items = parse_republisher_index(content, 'financialcontent-bw-index', lambda value: value)
        except Exception as exc:
            error = type(exc).__name__
        received = time.time()
        retry_until = retry_deadline(metadata.get('retry-after'), datetime.fromtimestamp(received, timezone.utc)).timestamp() if status == 429 or metadata.get('retry-after') else 0
        self.store.finish_catchup(job, now=received, status=status, items=items, retry_until=retry_until, error=error)

    async def _delivery_loop(self, client):
        while not self.stop_event.is_set():
            active, _, _ = session_window(datetime.now(timezone.utc))
            if active and self.source_enabled:
                item = self.store.claim_body(time.time(), allowed_routes=self.enabled_routes())
                if item:
                    await self.process_body(client, item)
            await asyncio.sleep(.5)

    async def process_body(self, client, item):
        if item['route'] == GNW_ROUTE:
            await self.process_review_body(client, item)
            return
        try:
            if item['entry']:
                entry = json.loads(item['entry'])
                received = item['body_received']
            else:
                if not self.store.begin_body(item, time.time()):
                    return
                body = await fetch_article(client, item['url'])
                received = time.time()
                entry = parse_article(body, article_url=item['url'], expected_title=item['title'])
                validate_recent(entry, received)
                try:
                    entry['_reader_document'] = build_reader_document(body, entry)
                except Exception as exc:
                    entry['_reader_error'] = type(exc).__name__
                if not self.store.save_body(item, entry, received):
                    raise RuntimeError('Body lease changed')
            validate_recent(entry, time.time())
            # Stop requests early enough to finish one bounded DB transaction by cutoff.
            result = deliver(self.factory, entry,
                listing_seen_at=datetime.fromtimestamp(item['observed'], timezone.utc).isoformat(),
                body_received_at=datetime.fromtimestamp(received, timezone.utc).isoformat())
            # A display archive failure must never replay a committed delivery.
            self.store.finish_body(item, 'done' if result['article_id'] else 'unmapped', result=result)
            if result['article_id']:
                try:
                    status = write_reader_archive(self.settings.article_body_archive_dir, result['article_id'], entry)
                except Exception as exc:
                    status = 'error:' + type(exc).__name__
                with self.store.connect() as db:
                    db.execute('UPDATE fc_candidate SET archive_status=? WHERE url=?', (status, item['url']))
            logger.info('financialcontent_delivery %s', json.dumps(result, sort_keys=True))
        except ArticleUnavailable as exc:
            now = datetime.now(timezone.utc)
            retry_until = retry_deadline(exc.retry_after, now).timestamp() if exc.status == 429 or exc.retry_after else 0
            self.store.fail_body(item, now.timestamp(), status=exc.status, retry_until=retry_until, error=str(exc))
        except (InvalidArticle, UnicodeError) as exc:
            self.store.finish_body(item, 'invalid', error=str(exc))
        except IdentityConflict as exc:
            self.store.finish_body(item, 'identity_review', error=str(exc))
        except Exception as exc:
            self.store.fail_body(item, time.time(), error=type(exc).__name__)

    async def process_review_body(self, client, item):
        """GNW never enters the publication/dedup/notification path in this batch."""
        if GNW_ROUTE not in self.enabled_routes() or self.stop_event.is_set():
            self.store.finish_body(item, 'pending', due=time.time(), error='GNW review window closed')
            return
        if item['entry']:
            self.store.finish_body(item, 'identity_review', error='Unexpected feed entry on review-only route')
            return
        try:
            if not self.store.begin_body(item, time.time()):
                return
            request_started = time.time()
            body = await gnw_body.fetch_review_body(client, item['url'])
            received = time.time()
        except ArticleUnavailable as exc:
            now = datetime.now(timezone.utc)
            retry_until = retry_deadline(exc.retry_after, now).timestamp() if exc.status == 429 or exc.retry_after else 0
            self.store.fail_body(item, now.timestamp(), status=exc.status, retry_until=retry_until, error=str(exc))
            return
        except (ValueError, UnicodeError) as exc:
            self.store.finish_body(item, 'invalid', error=str(exc))
            return
        except Exception as exc:
            self.store.fail_body(item, time.time(), error=type(exc).__name__)
            return
        try:
            review = gnw_body.parse_body(body, article_url=item['url'], expected_title=item['title'])
            review['body_parse_valid'] = True
            published = datetime.fromisoformat(review['publication_label']).timestamp()
            review['appearance_classification'] = ('historical_appearance' if published < received-86400
                else 'future_label_review' if published > received+300 else 'recent_label_identity_pending')
        except Exception as exc:
            # Bad HTML and unexpected reader errors must stay local to this
            # candidate, not terminate the shared discovery/delivery worker.
            review = gnw_body.failed_review(body, item['url'], item['title'], type(exc).__name__)
        review['network_requests'] = 1
        review['receipt_kind'] = 'network_response'
        review['clock_id'] = 'backend_wall_utc'
        review['discovery'] = {'route': item['route'], 'first_observed_at': item['observed'],
            'last_absent_at': item['last_absent'], 'body_received_at': received,
            'body_request_started_at': request_started,
            'title_selection_reason': item['reason']}
        try:
            if not self.store.save_review_body(item, body, review, received):
                raise RuntimeError('Review lease changed')
        except Exception as exc:
            # A completed response whose archive outcome is uncertain must not
            # be re-fetched automatically. If it committed, this CAS is a no-op.
            self.store.finish_body(item, 'held_review_archive', error=type(exc).__name__)
