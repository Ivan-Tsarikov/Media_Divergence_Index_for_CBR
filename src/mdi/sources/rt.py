"""RT source discovery using JSON search endpoint."""
from __future__ import annotations

import logging
from typing import Iterable

from mdi.fetch import fetch_json
from mdi.utils import CandidateLink

logger = logging.getLogger(__name__)


def discover_links(session, config: dict, start_date, end_date) -> list[CandidateLink]:
    """Discover RT links using search API."""
    results: list[CandidateLink] = []
    next_page = None
    max_pages = int(config.get("max_pages", 5))

    for _ in range(max_pages):
        params = {
            "format": "json",
            "q": config.get("query", "ключевая ставка"),
            "pageSize": config.get("page_size", 50),
        }
        if next_page:
            params["nextPage"] = next_page
        if start_date is not None:
            params["df"] = start_date.strftime("%Y-%m-%d")
        if end_date is not None:
            params["dt"] = end_date.strftime("%Y-%m-%d")

        status, payload, error = fetch_json(
            session,
            config.get("search_url", "https://russian.rt.com/search"),
            params=params,
            timeout_s=config.get("timeout_s", 20),
        )
        if error or not payload:
            logger.warning("RT search error: %s", error)
            break

        docs = payload.get("docs", [])
        for doc in docs:
            url = doc.get("url")
            if not url:
                continue
            results.append(
                CandidateLink(
                    url=url,
                    title=doc.get("title"),
                    summary=doc.get("summary"),
                    published_at=doc.get("date"),
                )
            )
        next_page = payload.get("nextPage")
        if not next_page:
            break

    return results
