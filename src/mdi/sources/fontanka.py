"""Fontanka tag-based discovery."""
from __future__ import annotations

import logging
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from mdi.fetch import fetch_url
from mdi.utils import CandidateLink

logger = logging.getLogger(__name__)


def discover_links(session, config: dict, start_date, end_date) -> list[CandidateLink]:
    """Discover links from Fontanka tag pages."""
    tag_url = config.get("tag_url")
    max_pages = int(config.get("max_pages", 3))
    results: list[CandidateLink] = []

    for page in range(1, max_pages + 1):
        page_url = tag_url if page == 1 else f"{tag_url}?page={page}"
        fetched = fetch_url(session, page_url, timeout_s=config.get("timeout_s", 20))
        if not fetched.text:
            logger.warning("Fontanka page empty: %s", page_url)
            continue
        soup = BeautifulSoup(fetched.text, "lxml")
        for link in soup.select("a[href]"):
            href = link.get("href")
            if not href:
                continue
            if "/" not in href:
                continue
            url = urljoin(page_url, href)
            results.append(CandidateLink(url=url, title=link.get_text(strip=True), summary=None, published_at=None))

    return results
