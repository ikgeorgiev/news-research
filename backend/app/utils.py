from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from dateutil import parser as dt_parser

TRACKING_PARAM_PREFIXES = (
    "utm_",
    "ga_",
)
TRACKING_PARAM_EXACT = {
    "tsrc",
    "cmpid",
    "ncid",
    "ocid",
}
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>",
    flags=re.IGNORECASE | re.DOTALL,
)
HTML_NOISE_ELEMENT_PATTERN = re.compile(
    r"<(?:nav|aside|footer)\b[^>]*>.*?</(?:nav|aside|footer)>",
    flags=re.IGNORECASE | re.DOTALL,
)
HTML_SIDEBAR_DIV_OPEN_PATTERN = re.compile(
    r"<div\b[^>]*\bclass=\"[^\"]*sidebar[^\"]*\"[^>]*>",
    flags=re.IGNORECASE,
)
HTML_RELATED_NEWS_PATTERN = re.compile(
    r">\s*(?:More\s+News\s+From|Related\s+(?:News|Press\s+Releases|Articles)|Also\s+from\s+this\s+source)\b",
    flags=re.IGNORECASE,
)

GENERAL_SOURCE_CODE = "businesswire"


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def sha256_str(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_title(value: str) -> str:
    # Decode HTML entities first so "&amp;" and "&" normalize to the same token stream.
    normalized = unescape((value or "")).strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def clean_summary_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = unescape(str(value))
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def html_to_plain_text(html_text: str) -> str:
    text = HTML_SCRIPT_STYLE_PATTERN.sub(" ", html_text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def strip_sidebar_divs(html_text: str) -> str:
    result: list[str] = []
    i = 0
    html_lower = html_text.lower()
    for match in HTML_SIDEBAR_DIV_OPEN_PATTERN.finditer(html_text):
        start = match.start()
        if start < i:
            continue
        result.append(html_text[i:start])
        depth = 1
        j = match.end()
        while j < len(html_text) and depth > 0:
            div_open = html_lower.find("<div", j)
            div_close = html_lower.find("</div>", j)
            if div_close == -1:
                j = len(html_text)
                break
            if div_open != -1 and div_open < div_close:
                depth += 1
                j = div_open + 4
            else:
                depth -= 1
                j = div_close + 6
        result.append(" ")
        i = j
    result.append(html_text[i:])
    return "".join(result)


def strip_noise_elements(html_text: str) -> str:
    text = HTML_SCRIPT_STYLE_PATTERN.sub(" ", html_text)
    text = HTML_NOISE_ELEMENT_PATTERN.sub(" ", text)
    text = strip_sidebar_divs(text)
    related = HTML_RELATED_NEWS_PATTERN.search(text)
    if related is not None:
        text = text[: related.start() + 1]
    return text


def extract_article_body(html_text: str) -> str | None:
    """Extract main article body with trafilatura when available."""
    try:
        import trafilatura

        result = trafilatura.extract(
            html_text,
            include_tables=True,
            include_comments=False,
            include_links=False,
            no_fallback=False,
        )
    except Exception:  # fault-isolation: broad catch intentional
        return None
    if result and len(result.strip()) > 50:
        return result
    return None


def canonicalize_url(url: str) -> str:
    if not url:
        return ""

    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    if netloc.endswith(":80"):
        netloc = netloc[:-3]
    if netloc.endswith(":443"):
        netloc = netloc[:-4]

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAM_EXACT:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        query_items.append((key, value))

    query = urlencode(sorted(query_items), doseq=True)
    path = parsed.path.rstrip("/") or "/"

    return urlunparse((scheme, netloc, path, "", query, ""))


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = parsedate_to_datetime(text)
        except Exception:  # fault-isolation: broad catch intentional
            try:
                dt = dt_parser.parse(text)
            except Exception:  # fault-isolation: broad catch intentional
                return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def encode_cursor(published_at: datetime, article_id: int) -> str:
    payload = {
        "published_at": published_at.isoformat(),
        "id": article_id,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def decode_cursor(cursor: str) -> tuple[datetime, int] | None:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("utf-8"))
        payload = json.loads(raw)
        dt = parse_datetime(payload["published_at"])
        article_id = int(payload["id"])
        if dt is None:
            return None
        return dt, article_id
    except Exception:
        return None


def to_json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_json_safe(val) for key, val in value.items()}
    return str(value)
