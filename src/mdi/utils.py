"""Helper utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Iterable
from urllib.parse import urlparse, urlunparse

from dateutil import parser as date_parser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandidateLink:
    """Discovered link metadata."""

    url: str
    title: str | None
    summary: str | None
    published_at: str | None


def canonicalize_url(url: str) -> str:
    """Normalize URL by dropping query/fragment and lowercasing scheme/host."""
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    scheme = parsed.scheme.lower()
    return urlunparse((scheme, netloc, parsed.path, "", "", ""))


def parse_date(value: str | None) -> datetime | None:
    """Parse a date string into datetime."""
    if not value:
        return None
    try:
        return date_parser.parse(value)
    except (ValueError, TypeError) as exc:
        logger.debug("Failed to parse date %s: %s", value, exc)
        return None


def within_window(date_value: datetime | None, start: datetime, end: datetime) -> bool:
    """Check if date is within window."""
    if date_value is None:
        return False
    return start <= date_value <= end


def filter_urls_by_patterns(urls: Iterable[str], allow_domains: list[str], deny_regex: list[str]) -> list[str]:
    """Filter URLs by domain allowlist and deny regex patterns."""
    import re

    filtered = []
    for url in urls:
        parsed = urlparse(url)
        if allow_domains and parsed.netloc not in allow_domains:
            continue
        if any(re.search(pattern, url) for pattern in deny_regex):
            continue
        filtered.append(url)
    return filtered
