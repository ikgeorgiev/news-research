"""Optional, time- and size-bounded copies of existing ingestion diagnostics."""
from datetime import date, datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

from app.news_validation_timing import validation_timing_window

EVENTS = frozenset({"article_latency", "article_enrichment_latency", "source_page_latency", "feed_receipt"})
_receipt_handler = None


def utc_now():
    return datetime.now(timezone.utc)


class ResearchTimingHandler(RotatingFileHandler):
    def __init__(self, path: Path, until: datetime, window=None):
        self.until = until
        self.window = window
        super().__init__(path, maxBytes=8 * 1024 * 1024, backupCount=3,
                         encoding="utf-8", delay=True)
        self.setFormatter(logging.Formatter("%(message)s"))
        self.setLevel(logging.INFO)

    def emit(self, record):
        now = utc_now()
        if now >= self.until or (self.window is not None and not self.window.accepts(now)):
            if record.getMessage().startswith('feed_receipt '):
                record.research_write_failed = True
            return
        event, _, payload = record.getMessage().partition(" ")
        if event not in EVENTS:
            return
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return
        if not isinstance(data, dict):
            return
        # Copy the record: console handlers must retain their original text.
        copied = logging.makeLogRecord(record.__dict__)
        envelope = {"recorded_at": now.isoformat(), "process_id": record.process,
                    "event": event, "data": data}
        if self.window is not None:
            envelope.update(collection_id=self.window.collection_id,
                            configuration_fingerprint=self.window.fingerprint)
        copied.msg = json.dumps(envelope)
        copied.args = ()
        copied.research_origin_record = record
        super().emit(copied)

    def handleError(self, record):
        # StreamHandler normally swallows disk/encoding failures. Report that
        # outcome to this receipt's caller so it can discard interval state.
        if record.getMessage().startswith('{') and hasattr(record, 'research_origin_record'):
            origin = record.research_origin_record
            if origin.getMessage().startswith('feed_receipt '):
                origin.research_write_failed = True
                return
        super().handleError(record)

    def close(self):
        global _receipt_handler
        if _receipt_handler is self:
            _receipt_handler = None
        super().close()


def record_feed_receipt(**evidence):
    """Best-effort evidence only; diagnostic failures must not alter ingestion."""
    handler = _receipt_handler
    now = utc_now().astimezone(timezone.utc)
    if (handler is None or now >= handler.until
            or not handler.receipt_enabled or now.date() not in handler.receipt_dates
            or not handler.receipt_start_hour <= now.hour < handler.receipt_end_hour):
        return False
    try:
        if handler.window is not None:
            started, received = evidence.get('request_started_at'), evidence.get('received_at')
            if (not handler.window.accepts(started) or not handler.window.accepts(received)
                    or started.astimezone(timezone.utc).date() != received.astimezone(timezone.utc).date()
                    or received.astimezone(timezone.utc).date() != now.date()):
                handler.receipt_tracker.forget(evidence.get('feed_url', ''))
                return False
        data = handler.receipt_tracker.observe(**evidence)
        if data is None:
            return False
        payload = json.dumps(data, separators=(',', ':'), ensure_ascii=True)
        if len(payload.encode()) > 64 * 1024:
            handler.receipt_tracker.forget(evidence['feed_url'])
            return False
        record = logging.LogRecord('app.research_timing', logging.INFO, '', 0,
                                   'feed_receipt %s', (payload,), None)
        # Send directly to the existing bounded file handler, not console logs.
        handler.handle(record)
        if getattr(record, 'research_write_failed', False):
            handler.receipt_tracker.forget(evidence['feed_url'])
            return False
        return True
    except Exception:
        try:
            handler.receipt_tracker.forget(evidence.get('feed_url', ''))
        except Exception:
            pass
        return False


def configure_research_timing(settings):
    global _receipt_handler
    try:
        window = validation_timing_window(settings)
    except ValueError as exc:
        logging.getLogger(__name__).warning('Validation timing disabled: %s', exc)
        return None
    path, until = ((window.path, window.until) if window is not None else
                   (settings.research_timing_log_path, settings.research_timing_log_until))
    if not path or until is None:
        return None
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    if utc_now() >= until:
        return None
    target = Path(path).resolve()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        handler = ResearchTimingHandler(target, until, window)
    except OSError:
        logging.getLogger(__name__).warning('Timing diagnostics unavailable: log initialization failed')
        return None
    from app.feed_receipts import FeedReceiptTracker
    handler.receipt_tracker = FeedReceiptTracker()
    handler.receipt_enabled = bool(getattr(settings, 'research_feed_receipts_enabled', False))
    try:
        handler.receipt_dates = frozenset(date.fromisoformat(value.strip()) for value in
            getattr(settings, 'research_feed_receipt_dates', '').split(',') if value.strip())
    except (ValueError, AttributeError):
        handler.receipt_dates = frozenset()
    handler.receipt_start_hour = getattr(settings, 'research_feed_receipt_start_hour_utc', 8)
    handler.receipt_end_hour = getattr(settings, 'research_feed_receipt_end_hour_utc', 22)
    if window is not None:
        handler.receipt_enabled = True
        handler.receipt_dates = frozenset(window.dates)
        handler.receipt_start_hour = window.start_hour
        handler.receipt_end_hour = window.end_hour
    logging.getLogger().addHandler(handler)
    _receipt_handler = handler
    return handler


def research_timing_status():
    """Describe the loaded handler, never freshly re-read environment settings."""
    handler = _receipt_handler
    status = dict(process_id=os.getpid(), handler_attached=handler is not None,
                  recording_now=False, feed_receipts_enabled=False)
    if handler is None:
        return status
    now = utc_now()
    status.update(mode='weekday_validation' if handler.window is not None else 'legacy_study',
                  until=handler.until.isoformat(), feed_receipts_enabled=handler.receipt_enabled,
                  recording_now=now < handler.until and (handler.window is None or handler.window.accepts(now)))
    if handler.window is not None:
        status.update(**handler.window.description(), configuration_fingerprint=handler.window.fingerprint)
    return status
