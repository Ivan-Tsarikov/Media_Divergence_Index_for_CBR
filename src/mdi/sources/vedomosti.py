"""Vedomosti archive discovery."""
from __future__ import annotations

import logging
import re
from datetime import timedelta

from bs4 import BeautifulSoup

from mdi.fetch import fetch_url
from mdi.utils import CandidateLink

logger = logging.getLogger(__name__)


def discover_links(session, config: dict, start_date, end_date) -> list[CandidateLink]:
    """Discover Vedomosti links from daily archives within event window."""
    archive_url = config.get("archive_url", "https://www.vedomosti.ru/archive/{yyyy}/{mm}/{dd}")
    allow_article_regex = re.compile(config.get("allow_article_regex", "/articles/\\d{4}/\\d{2}/\\d{2}/"))
    results: list[CandidateLink] = []

    if start_date is None or end_date is None:
        return results

    current = start_date
    while current <= end_date:
        url = archive_url.format(yyyy=current.year, mm=f"{current.month:02d}", dd=f"{current.day:02d}")
        fetched = fetch_url(session, url, timeout_s=config.get("timeout_s", 20))
        if not fetched.text:
            current += timedelta(days=1)
            continue
        soup = BeautifulSoup(fetched.text, "lxml")
        for link in soup.select("a[href]"):
            href = link.get("href")
            if not href:
                continue
            if not allow_article_regex.search(href):
                continue
            if href.startswith("//"):
                href = f"https:{href}"
            elif href.startswith("/"):
                href = f"https://www.vedomosti.ru{href}"
            results.append(CandidateLink(url=href, title=link.get_text(strip=True), summary=None, published_at=None))
        current += timedelta(days=1)

    return results
