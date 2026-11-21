"""Durable page-request controls, independent of article/ingestion transactions."""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import math
import uuid
from urllib.parse import urlsplit

from sqlalchemy import select, text, create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models import SourcePageControl
from app.release_matching import release_identity


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def control_keys(url, source_code, hostname_suffix):
    parsed=urlsplit(url)
    host=(parsed.hostname or '').lower()
    if parsed.scheme not in {'http','https'} or parsed.username or parsed.password or parsed.port not in {None,80,443}:
        raise ValueError('Invalid page URL')
    if host!=hostname_suffix and not host.endswith('.'+hostname_suffix):
        raise ValueError('Wrong source host')
    identity=release_identity(url)
    page=':'.join(identity) if identity and identity[0]==source_code else hashlib.sha256((host+parsed.path).encode()).hexdigest()
    # Provider domain groups subdomains so another host cannot bypass a limit.
    return 'host:'+hostname_suffix, 'page:'+source_code+':'+page


def retry_deadline(value, now):
    seconds=60.0
    try:
        seconds=float(value)
        if not math.isfinite(seconds):raise ValueError()
    except (ValueError,TypeError):
        try:seconds=(utc(parsedate_to_datetime(value))-now).total_seconds()
        except (ValueError,TypeError,OverflowError,AttributeError):seconds=60.0
    seconds=max(60.0,seconds)
    try:return now+timedelta(seconds=seconds)
    except OverflowError:return datetime.max.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class PageOutcome:
    kind: str
    attempted: bool=False
    status: int|None=None
    retry_at: datetime|None=None
    detail: str|None=None


@dataclass(frozen=True)
class Permit:
    host_key: str
    page_key: str
    token: str


_last_outcome=ContextVar('source_page_outcome',default=None)
_store=None
def set_page_outcome(outcome):_last_outcome.set(outcome)
def get_page_outcome():return _last_outcome.get()
def reset_page_outcome():_last_outcome.set(None)
def configure_page_access(store):
    global _store
    _store=store
def get_page_access():return _store


class PageAccessStore:
    def __init__(self, factory):self.factory=factory

    @classmethod
    def for_database(cls, database_url):
        options={}
        if make_url(database_url).get_backend_name()=='postgresql':
            # Do not compete with transactions holding raw/article row locks.
            options=dict(pool_size=2,max_overflow=0,pool_timeout=1,
                         connect_args={'connect_timeout':3})
        engine=create_engine(database_url,pool_pre_ping=True,**options)
        store=cls(sessionmaker(bind=engine,autoflush=False))
        store.engine=engine
        return store

    def close(self):
        if hasattr(self,'engine'):self.engine.dispose()

    def _locked(self, db, keys):
        dialect=db.get_bind().dialect.name
        if dialect=='postgresql':
            db.execute(text("SET LOCAL statement_timeout='5s'"))
            db.execute(text("SET LOCAL lock_timeout='4s'"))
            insert=pg_insert
        elif dialect=='sqlite':
            # Unit tests only; deployed workers use PostgreSQL row locks.
            db.execute(text('BEGIN IMMEDIATE'))
            insert=sqlite_insert
        else:raise RuntimeError('Unsupported control database')
        for key in sorted(keys):
            db.execute(insert(SourcePageControl).values(key=key,network_attempts=0).on_conflict_do_nothing(index_elements=['key']))
        rows=db.scalars(select(SourcePageControl).where(SourcePageControl.key.in_(keys)).order_by(SourcePageControl.key).with_for_update()).all()
        return {row.key:row for row in rows}

    @staticmethod
    def _gate(rows, host_key, page_key, now):
        page=rows.get(page_key);host=rows.get(host_key)
        if page and page.refusal_status is not None:
            return PageOutcome('refused',status=page.refusal_status,detail=page.reason)
        if page and page.lease_token:
            expiry=utc(page.lease_until)
            return PageOutcome('in_flight' if expiry>now else 'unknown_in_flight',retry_at=expiry if expiry>now else None)
        deadlines=[utc(row.next_allowed_at) for row in [host,page] if row and row.next_allowed_at and utc(row.next_allowed_at)>now]
        if deadlines:return PageOutcome('backoff',retry_at=max(deadlines))
        return None

    def inspect(self, url, config, now):
        host,page=control_keys(url,config.source_code,config.hostname_suffix)
        with self.factory() as db:
            if db.get_bind().dialect.name=='postgresql':db.execute(text("SET LOCAL statement_timeout='5s'"))
            rows={r.key:r for r in db.scalars(select(SourcePageControl).where(SourcePageControl.key.in_([host,page])))}
            return self._gate(rows,host,page,now)

    def claim(self, url, config, now, timeout_seconds):
        host,page=control_keys(url,config.source_code,config.hostname_suffix)
        with self.factory.begin() as db:
            rows=self._locked(db,[host,page])
            gate=self._gate(rows,host,page,now)
            if gate:return gate
            token=uuid.uuid4().hex
            row=rows[page];row.lease_token=token
            row.lease_until=now+timedelta(seconds=max(60,timeout_seconds+30))
            row.network_attempts+=1;row.last_attempt_at=now
            return Permit(host,page,token)

    def finish(self, permit, *, status, retry_after=None, error=None, now, usable=False):
        with self.factory.begin() as db:
            rows=self._locked(db,[permit.host_key,permit.page_key]);page=rows[permit.page_key]
            if page.lease_token!=permit.token:
                return PageOutcome('unknown_in_flight',attempted=True,detail='Lease identity changed')
            page.lease_token=None;page.lease_until=None;page.last_status=status
            if status in {401,403,404,301,302,303,307,308}:
                page.refusal_status=status;page.refused_at=page.refused_at or now
                page.reason='http_refusal' if status in {401,403,404} else 'redirect_not_followed'
                return PageOutcome('refused',True,status,detail=page.reason)
            if status==429 or (status==503 and retry_after):
                deadline=retry_deadline(retry_after,now);host=rows[permit.host_key]
                if host.next_allowed_at:deadline=max(deadline,utc(host.next_allowed_at))
                host.next_allowed_at=deadline;host.last_status=status
                page.next_allowed_at=deadline
                return PageOutcome('backoff',True,status,deadline)
            if usable:
                # A concurrent success must never clear the provider's backoff.
                page.next_allowed_at=None
                return PageOutcome('body',True,status)
            page.next_allowed_at=now+timedelta(seconds=60)
            return PageOutcome('transient',True,status,utc(page.next_allowed_at),error)

    def seed_refusal(self, url, config, *, status, observed_at):
        """Import reviewed prior evidence without issuing a request or clearing state."""
        if status not in {401,403,404,301,302,303,307,308}:raise ValueError('Not refusal evidence')
        host,page=control_keys(url,config.source_code,config.hostname_suffix)
        with self.factory.begin() as db:
            row=self._locked(db,[host,page])[page]
            if row.refusal_status is None:
                row.refusal_status=status;row.refused_at=observed_at;row.reason='reviewed_prior_http_refusal'
