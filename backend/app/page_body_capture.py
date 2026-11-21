"""An optional, entry-scoped handoff of already fetched HTML, with no I/O."""
from contextlib import contextmanager
from contextvars import ContextVar

_capture = ContextVar('publisher_body_capture', default=None)
MAX_BYTES = 1024 * 1024


@contextmanager
def capture_page_body(enabled=True):
    token = _capture.set({} if enabled else None)
    try:
        yield
    finally:
        _capture.reset(token)


def retain_page_body(url, source_code, content):
    target = _capture.get()
    if target is not None:
        target.clear()
        if isinstance(content, str) and len(content.encode('utf8')) <= MAX_BYTES:
            target.update(page_url=url, source_code=source_code, html_text=content)


def captured_page_body():
    value = _capture.get()
    return dict(value) if value else None
