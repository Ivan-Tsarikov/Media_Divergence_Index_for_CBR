"""HTTP fetching utilities with retries, caching, and politeness."""
from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import requests
import requests_cache
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, urlunparse
from urllib import robotparser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchConfig:
    """Configuration for HTTP fetches."""

    user_agent: str
    timeout_s: int
    sleep_s: float
    retries_total: int
    retries_backoff: float
    retries_statuses: list[int]
    cache_enabled: bool
    cache_backend: str
    cache_expire_after_s: int


def build_session(config: FetchConfig) -> requests.Session:
    """Create a cached session with retries."""
    if config.cache_enabled:
        session = requests_cache.CachedSession(
            backend=config.cache_backend,
            expire_after=config.cache_expire_after_s,
        )
    else:
        session = requests.Session()

    retry = Retry(
        total=config.retries_total,
        backoff_factor=config.retries_backoff,
        status_forcelist=config.retries_statuses,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": config.user_agent})
    return session


@dataclass
class FetchResult:
    """Result of a fetch operation."""

    url: str
    status_code: int | None
    text: str | None
    content: bytes | None
    error: str | None


class RobotsCache:
    """Robots.txt cache for polite crawling."""

    def __init__(self) -> None:
        self._parsers: dict[str, robotparser.RobotFileParser] = {}

    def allowed(self, session: requests.Session, url: str, user_agent: str, timeout_s: int) -> bool:
        """Check robots.txt rules for a given URL."""
        parsed = urlparse(url)
        base = urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))
        parser = self._parsers.get(base)
        if parser is None:
            parser = robotparser.RobotFileParser()
            try:
                response = session.get(base, timeout=timeout_s)
                if response.ok:
                    parser.parse(response.text.splitlines())
            except requests.RequestException:
                parser = robotparser.RobotFileParser()
                parser.parse([])
            self._parsers[base] = parser
        return parser.can_fetch(user_agent, url)


def fetch_url(session: requests.Session, url: str, timeout_s: int) -> FetchResult:
    """Fetch a URL and return response text and status."""
    try:
        response = session.get(url, timeout=timeout_s)
        response.raise_for_status()
        return FetchResult(
            url=url,
            status_code=response.status_code,
            text=response.text,
            content=response.content,
            error=None,
        )
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        logger.warning("HTTP error for %s: %s", url, status)
        return FetchResult(
            url=url,
            status_code=status,
            text=exc.response.text if exc.response else None,
            content=exc.response.content if exc.response else None,
            error=str(exc),
        )
    except requests.RequestException as exc:
        logger.warning("Request error for %s: %s", url, exc)
        return FetchResult(url=url, status_code=None, text=None, content=None, error=str(exc))


def fetch_json(session: requests.Session, url: str, params: dict[str, Any], timeout_s: int) -> tuple[int | None, dict[str, Any] | None, str | None]:
    """Fetch JSON payload from a URL."""
    try:
        response = session.get(url, params=params, timeout=timeout_s)
        response.raise_for_status()
        return response.status_code, response.json(), None
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        logger.warning("HTTP error for %s: %s", url, status)
        return status, None, str(exc)
    except requests.RequestException as exc:
        logger.warning("Request error for %s: %s", url, exc)
        return None, None, str(exc)
