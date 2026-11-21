from collections import OrderedDict
from collections.abc import Callable
import hashlib
import sys
import threading


class ArticleBodyCache:
    """Bound successful HTML-to-body results, without caching ticker decisions."""

    def __init__(self, max_items: int = 128, max_bytes: int = 16 * 1024 * 1024):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self._items: OrderedDict[bytes, tuple[str, int]] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._bytes = 0

    def extract(self, html: str, extractor: Callable[[str], str | None]) -> str | None:
        # Retain only the digest and result, not a second cache of whole pages.
        key = hashlib.sha256(html.encode("utf-8", errors="surrogatepass")).digest()
        with self._lock:
            cached = self._items.get(key)
            if cached is not None:
                self._items.move_to_end(key)
                return cached[0]

        body = extractor(html)
        # Missing/failed extraction must remain retryable on the next call.
        if not body:
            return body
        size = sys.getsizeof(key) + sys.getsizeof(body)
        if self.max_items <= 0 or size > self.max_bytes:
            return body
        with self._lock:
            # Another worker may have finished the same page in the meantime.
            cached = self._items.get(key)
            if cached is not None:
                self._items.move_to_end(key)
                return cached[0]
            while self._items and (
                len(self._items) >= self.max_items or self._bytes + size > self.max_bytes
            ):
                _, (_, removed_size) = self._items.popitem(last=False)
                self._bytes -= removed_size
            self._items[key] = (body, size)
            self._bytes += size
        return body
