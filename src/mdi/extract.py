"""HTML extraction utilities."""
from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from typing import Any

from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import trafilatura

logger = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    """Result of extracting text and metadata."""

    title: str | None
    published_at: str | None
    text: str | None
    parse_status: str


def _parse_json_ld(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "{}")
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list) and payload:
            return payload[0]
    return {}


def extract_article(html: str) -> ExtractResult:
    """Extract article text, title, and date from HTML."""
    if not html:
        return ExtractResult(title=None, published_at=None, text=None, parse_status="empty_html")

    downloaded = trafilatura.extract(html, include_comments=False, include_tables=False)
    text = downloaded.strip() if downloaded else None

    soup = BeautifulSoup(html, "lxml")
    title = None
    published_at = None

    if soup.title and soup.title.text:
        title = soup.title.text.strip()

    json_ld = _parse_json_ld(soup)
    if json_ld:
        title = json_ld.get("headline") or title
        published_at = json_ld.get("datePublished") or json_ld.get("dateCreated")

    meta_time = soup.find("meta", attrs={"property": "article:published_time"})
    if meta_time and meta_time.get("content"):
        published_at = meta_time["content"]

    if not published_at:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            published_at = time_tag["datetime"]

    if published_at:
        try:
            published_at = date_parser.parse(published_at).isoformat()
        except (ValueError, TypeError) as exc:
            logger.debug("Failed to parse date: %s", exc)

    parse_status = "ok" if text else "no_text"
    return ExtractResult(title=title, published_at=published_at, text=text, parse_status=parse_status)
