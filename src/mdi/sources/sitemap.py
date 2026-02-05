"""Sitemap discovery for multiple sources."""
from __future__ import annotations

import gzip
import logging
from io import BytesIO
from typing import Iterable

from bs4 import BeautifulSoup

from mdi.fetch import fetch_url
from mdi.utils import CandidateLink, parse_date, within_window

logger = logging.getLogger(__name__)


def _parse_sitemap(xml_text: str) -> list[tuple[str, str | None]]:
    soup = BeautifulSoup(xml_text, "xml")
    urls = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if loc and loc.text:
            urls.append((loc.text.strip(), lastmod.text.strip() if lastmod else None))
    return urls


def _fetch_sitemap(session, url: str, timeout_s: int) -> list[tuple[str, str | None]]:
    fetched = fetch_url(session, url, timeout_s=timeout_s)
    if not fetched.text and not fetched.content:
        return []
    content = fetched.text or ""
    if url.endswith(".gz"):
        raw = gzip.decompress(fetched.content or b"").decode("utf-8", errors="ignore")
        content = raw
    return _parse_sitemap(content)


def discover_links(session, config: dict, start_date, end_date) -> list[CandidateLink]:
    """Discover links from sitemap, filtered by event window."""
    sitemap_url = config.get("sitemap_url")
    max_urls = int(config.get("max_urls_per_event", 200))
    timeout_s = config.get("timeout_s", 20)

    urls = _fetch_sitemap(session, sitemap_url, timeout_s=timeout_s)
    results: list[CandidateLink] = []
    for loc, lastmod in urls:
        if start_date and end_date:
            date_value = parse_date(lastmod)
            if date_value and not within_window(date_value, start_date, end_date):
                continue
        results.append(CandidateLink(url=loc, title=None, summary=None, published_at=lastmod))
        if len(results) >= max_urls:
            break
    return results
