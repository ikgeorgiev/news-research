"""Use existing ticker and dedupe logic; send PG notification atomically on commit."""
from datetime import datetime, timedelta, timezone
import re
import time
from sqlalchemy import select, text
from app.article_ingest import _find_matching_article, _prepare_feed_entries, _process_single_entry
from app.constants import TITLE_DEDUP_WINDOW_HOURS
from app.models import IngestionRun, RawFeedItem, Source
from app.pg_utils import acquire_article_dedupe_locks
from app.raw_feed_items import _acquire_raw_item_locks
from app.ticker_context import load_ticker_context
from app.utils import normalize_title,sha256_str


class IdentityConflict(ValueError):
    pass


def guard_wire_identity(db,prepared,source_id):
    """Hold ambiguous same-title matches rather than merging different wire IDs."""
    match=re.fullmatch(r'bw:(\d+):en',prepared.raw_guid or '')
    if not match:raise IdentityConflict('Missing original wire identity')
    expected=match[1]
    _acquire_raw_item_locks(db,source_id=source_id,raw_guid=prepared.raw_guid,
                            raw_link=prepared.raw_link,published_at=prepared.raw_pub_date)
    url_hash=sha256_str(prepared.article_url)
    title_hash=sha256_str(normalize_title(prepared.title))
    acquire_article_dedupe_locks(db,url_hash,title_hash)
    existing=_find_matching_article(db,url_hash=url_hash,title_hash=title_hash,
        window_start=prepared.published_at-timedelta(hours=TITLE_DEDUP_WINDOW_HOURS),
        window_end=prepared.published_at+timedelta(hours=TITLE_DEDUP_WINDOW_HOURS),source_code='financialcontent').article
    if existing is None:return
    values=[existing.canonical_url]
    for raw in db.scalars(select(RawFeedItem).where(RawFeedItem.article_id==existing.id).limit(100)):
        values.extend([raw.raw_guid or '',raw.raw_link or ''])
    found=set()
    for value in values:
        key=re.fullmatch(r'bw:(\d+):en',value)
        url=re.search(r'businesswire\.com/news/home/(\d+)/en(?:/|$)',value)
        if key or url:found.add((key or url)[1])
    if found and found!={expected}:
        raise IdentityConflict('Conflicting original wire identity for same headline; retained for review')


def deliver(factory, entry, *, listing_seen_at, body_received_at):
    started=time.perf_counter()
    with factory() as db:
        if db.get_bind().dialect.name=='postgresql':
            db.execute(text("SET LOCAL statement_timeout='5s'"))
            db.execute(text("SET LOCAL lock_timeout='4s'"))
        source=db.scalar(select(Source).where(Source.code=='financialcontent'))
        if source is None:
            source=Source(code='financialcontent',name='FinancialContent',base_url='https://markets.chroniclejournal.com',enabled=True)
            db.add(source)
            db.flush()
        if not source.enabled:
            raise RuntimeError('FinancialContent source was disabled; refusing delivery')
        context=load_ticker_context(db)
        prepared=_prepare_feed_entries([entry],datetime.now(timezone.utc),source_code=source.code)
        if len(prepared)!=1:
            raise ValueError('Expected one validated article')
        source_id=source.id
        guard_wire_identity(db,prepared[0],source_id)
        run=IngestionRun(source_id=source_id,feed_url=entry['link'],status='running',items_seen=1)
        db.add(run)
        result=_process_single_entry(db,prepared=prepared[0],source=source,
            source_name='Business Wire',provider_name='FinancialContent',feed_url=entry['link'],
            known_symbols=context.known_symbols,symbol_to_id=context.symbol_to_id,symbol_keywords=context.symbol_keywords,
            timeout_seconds=4,page_config=None,persistence_policy_override='validated_mapping_required',
            recorded_guids=set(),recorded_pairs=set(),postcommit_enrichment_enabled=False)
        db.flush()
        raw=db.scalar(select(RawFeedItem).where(RawFeedItem.source_id==source_id,RawFeedItem.raw_guid==entry['id']))
        article_id=raw.article_id if raw else None
        notification_started=time.perf_counter()
        if result.created_article and db.get_bind().dialect.name=='postgresql':
            # PG delivers this only if the article transaction commits. A failure rolls both back.
            db.execute(text("SELECT pg_notify('new_articles', '1')"))
        notification_ms=(time.perf_counter()-notification_started)*1000
        run.status='success'
        run.items_inserted=int(result.created_article)
        run.finished_at=datetime.now(timezone.utc)
        commit_started=time.perf_counter()
        db.commit()
        committed=datetime.now(timezone.utc)
        return {'article_id':article_id,'created':result.created_article,'committed_at':committed.isoformat(),
                'listing_seen_at':listing_seen_at,'body_received_at':body_received_at,
                'processing_ms':(commit_started-started)*1000,'commit_ms':(time.perf_counter()-commit_started)*1000,
                'notification_enqueue_ms':notification_ms,'notification_atomic_with_commit':True,
                'screen_delivery':'unmeasured'}
