from app.utils import canonicalize_url, clean_summary_text, decode_cursor, encode_cursor, normalize_title
from datetime import datetime, timezone


def test_canonicalize_url_removes_tracking_params():
    url = "https://finance.yahoo.com/news/example?utm_source=x&tsrc=rss&id=7"
    assert canonicalize_url(url) == "https://finance.yahoo.com/news/example?id=7"


def test_normalize_title_reduces_noise():
    assert normalize_title("  Hello,   World!  ") == "hello world"


def test_normalize_title_unescapes_html_entities():
    plain = "Cohen & Steers Infrastructure Fund, Inc. (UTF) Notification of Sources of Distribution Under Section 19(a)"
    escaped = "Cohen &amp; Steers Infrastructure Fund, Inc. (UTF) Notification of Sources of Distribution Under Section 19(a)"
    assert normalize_title(plain) == normalize_title(escaped)


def test_cursor_roundtrip():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    cursor = encode_cursor(now, 42)
    decoded = decode_cursor(cursor)
    assert decoded is not None
    dt, ident = decoded
    assert ident == 42
    assert dt == now


def test_clean_summary_text_removes_html_tags():
    raw = "<p>CHARLOTTE&nbsp;N.C.</p><div>Monthly <b>distribution</b></div>"
    assert clean_summary_text(raw) == "CHARLOTTE N.C. Monthly distribution"
