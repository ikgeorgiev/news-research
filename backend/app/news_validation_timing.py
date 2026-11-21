"""Explicit weekday windows for a new validation log, separate from old studies."""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re


@dataclass(frozen=True)
class ValidationTimingWindow:
    path: Path
    dates: tuple[date, ...]
    start_hour: int
    end_hour: int

    @property
    def until(self):
        return datetime.combine(self.dates[-1], time.min, timezone.utc) + timedelta(hours=self.end_hour)

    @property
    def collection_id(self):
        return f"news-validation-{self.dates[0]:%Y%m%d}-{self.dates[-1]:%Y%m%d}"

    def description(self):
        return dict(collection_id=self.collection_id, dates=[d.isoformat() for d in self.dates],
                    start_hour_utc=self.start_hour, end_hour_utc=self.end_hour,
                    until=self.until.isoformat())

    @property
    def fingerprint(self):
        spec = dict(version=1, path=str(self.path), **self.description())
        return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()

    def accepts(self, moment):
        if not isinstance(moment, datetime) or moment.tzinfo is None:
            return False
        utc = moment.astimezone(timezone.utc)
        return utc.date() in self.dates and self.start_hour <= utc.hour < self.end_hour


def validation_timing_window(settings):
    """Return a finite window or fail closed; errors never include a path or secret."""
    if not getattr(settings, 'news_validation_timing_enabled', False):
        return None
    raw = getattr(settings, 'news_validation_timing_dates', '')
    if not isinstance(raw, str):
        raise ValueError('invalid_dates')
    values = [value.strip() for value in raw.split(',')]
    if not 1 <= len(values) <= 10 or any(not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) for value in values):
        raise ValueError('invalid_dates')
    try:
        dates = tuple(sorted(date.fromisoformat(value) for value in values))
    except ValueError:
        raise ValueError('invalid_dates') from None
    if (len(set(dates)) != len(dates) or any(d.weekday() > 4 for d in dates)
            or (dates[-1] - dates[0]).days > 13):
        raise ValueError('invalid_dates')
    start = getattr(settings, 'news_validation_timing_start_hour_utc', 8)
    end = getattr(settings, 'news_validation_timing_end_hour_utc', 22)
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= 24:
        raise ValueError('invalid_hours')
    raw_path = getattr(settings, 'news_validation_timing_log_path', None)
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError('missing_path')
    try:
        target = Path(raw_path).resolve()
        legacy = getattr(settings, 'research_timing_log_path', None)
        if legacy:
            old = Path(legacy).resolve()
            if target == old or (target.parent == old.parent and target.name.startswith(old.name + '.')):
                raise ValueError('legacy_path_collision')
        if target.is_dir():
            raise ValueError('path_is_directory')
    except (OSError, RuntimeError):
        raise ValueError('invalid_path') from None
    return ValidationTimingWindow(target, dates, start, end)
