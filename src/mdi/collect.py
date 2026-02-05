"""CLI entrypoint for collecting key rate articles."""
from __future__ import annotations

import argparse
from dataclasses import dataclass, asdict
import logging
from pathlib import Path
import time
from typing import Iterable

import pandas as pd
import yaml

from mdi.events import load_events, iter_event_windows
from mdi.fetch import FetchConfig, RobotsCache, build_session, fetch_url
from mdi.extract import extract_article
from mdi.relevance import RelevanceConfig, is_relevant
from mdi.utils import CandidateLink, canonicalize_url, filter_urls_by_patterns
from mdi.sources import rt, fontanka, vedomosti, sitemap

logger = logging.getLogger(__name__)


@dataclass
class ArticleRecord:
    """Normalized article record."""

    source: str
    url: str
    canonical_url: str
    title: str | None
    published_at: str | None
    text: str | None
    summary: str | None
    event_date_time: str
    event_decision: str | None
    event_new_rate: float | None
    fetch_status: str
    parse_status: str
    relevance: bool


def load_config(path: str | Path) -> dict:
    """Load YAML configuration."""
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_fetch_config(config: dict) -> FetchConfig:
    """Build FetchConfig from YAML."""
    retries = config.get("retries", {})
    cache = config.get("cache", {})
    return FetchConfig(
        user_agent=config.get("user_agent", "mdi-collector/0.1"),
        timeout_s=int(config.get("request_timeout_s", 20)),
        sleep_s=float(config.get("sleep_s", 1.0)),
        retries_total=int(retries.get("total", 3)),
        retries_backoff=float(retries.get("backoff_factor", 0.5)),
        retries_statuses=list(retries.get("status_forcelist", [])),
        cache_enabled=bool(cache.get("enabled", True)),
        cache_backend=str(cache.get("backend", "sqlite")),
        cache_expire_after_s=int(cache.get("expire_after_s", 86400)),
    )


def discover_for_source(session, source: str, source_config: dict, start_date, end_date) -> list[CandidateLink]:
    """Dispatch discovery by source."""
    if source == "rt":
        return rt.discover_links(session, source_config, start_date, end_date)
    if source == "fontanka":
        return fontanka.discover_links(session, source_config, start_date, end_date)
    if source == "vedomosti":
        return vedomosti.discover_links(session, source_config, start_date, end_date)
    if source == "sitemap":
        return sitemap.discover_links(session, source_config, start_date, end_date)
    raise ValueError(f"Unknown source {source}")


def collect_articles(config_path: str, events_path: str, output_path: str) -> pd.DataFrame:
    """Run full collection pipeline."""
    config = load_config(config_path)
    fetch_config = build_fetch_config(config)
    session = build_session(fetch_config)
    robots = RobotsCache()

    events = load_events(events_path)
    windows = iter_event_windows(events, config["window_days"]["before"], config["window_days"]["after"])

    relevance_cfg = RelevanceConfig(**config["relevance"])

    records: list[ArticleRecord] = []
    seen = set()

    for event, start_date, end_date in windows:
        for source_name, source_config in config.get("sources", {}).items():
            if not source_config.get("enabled", False):
                continue

            if source_name == "sitemap":
                for sitemap_name, sitemap_cfg in source_config.get("sources", {}).items():
                    combined = dict(sitemap_cfg)
                    combined["max_urls_per_event"] = source_config.get("max_urls_per_event", 200)
                    combined["timeout_s"] = fetch_config.timeout_s
                    candidates = discover_for_source(session, "sitemap", combined, start_date, end_date)
                    records.extend(
                        _process_candidates(
                            session,
                            fetch_config,
                            relevance_cfg,
                            sitemap_name,
                            combined,
                            candidates,
                            event,
                            start_date,
                            end_date,
                            seen,
                            robots,
                        )
                    )
                continue

            source_config = dict(source_config)
            source_config["timeout_s"] = fetch_config.timeout_s
            candidates = discover_for_source(session, source_name, source_config, start_date, end_date)
            records.extend(
                _process_candidates(
                    session,
                    fetch_config,
                    relevance_cfg,
                    source_name,
                    source_config,
                    candidates,
                    event,
                    start_date,
                    end_date,
                    seen,
                    robots,
                )
            )

    df = pd.DataFrame([asdict(r) for r in records])
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix == ".csv":
        df.to_csv(output_path, index=False)
    elif output_path.suffix == ".jsonl":
        df.to_json(output_path, orient="records", lines=True, force_ascii=False)
    elif output_path.suffix == ".parquet":
        df.to_parquet(output_path, index=False)
    else:
        raise ValueError("Output must be .csv, .jsonl, or .parquet")

    return df


def _process_candidates(
    session,
    fetch_config: FetchConfig,
    relevance_cfg: RelevanceConfig,
    source_name: str,
    source_config: dict,
    candidates: Iterable[CandidateLink],
    event,
    start_date,
    end_date,
    seen: set,
    robots: RobotsCache,
) -> list[ArticleRecord]:
    records: list[ArticleRecord] = []
    allow_domains = source_config.get("allow_domains", [])
    deny_url_regex = source_config.get("deny_url_regex", [])
    urls = filter_urls_by_patterns([c.url for c in candidates], allow_domains, deny_url_regex)

    for candidate in candidates:
        if candidate.url not in urls:
            continue
        canonical = canonicalize_url(candidate.url)
        dedupe_key = (source_name, canonical)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        if not robots.allowed(session, candidate.url, fetch_config.user_agent, fetch_config.timeout_s):
            fetch_status = "robots_disallow"
            fetched = None
        else:
            fetched = fetch_url(session, candidate.url, timeout_s=fetch_config.timeout_s)
            time.sleep(fetch_config.sleep_s)
            fetch_status = "ok" if fetched and fetched.text else "error"
        extract = extract_article(fetched.text or "") if fetched and fetched.text else None

        if extract:
            title = extract.title or candidate.title
            text = extract.text
            published_at = extract.published_at or candidate.published_at
            parse_status = extract.parse_status
        else:
            title = candidate.title
            text = None
            published_at = candidate.published_at
            parse_status = "no_html" if fetch_status != "robots_disallow" else "robots_disallow"

        relevance = is_relevant(title, text, relevance_cfg) if text or title else False

        records.append(
            ArticleRecord(
                source=source_name,
                url=candidate.url,
                canonical_url=canonical,
                title=title,
                published_at=published_at,
                text=text,
                summary=candidate.summary,
                event_date_time=event.event_date_time.isoformat(),
                event_decision=event.decision,
                event_new_rate=event.new_rate,
                fetch_status=fetch_status,
                parse_status=parse_status,
                relevance=relevance,
            )
        )

    return records


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Collect CBR key rate articles")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--events", required=True, help="Path to events CSV/XLSX")
    parser.add_argument("--out", required=True, help="Output path (.csv/.jsonl/.parquet)")
    args = parser.parse_args()

    collect_articles(args.config, args.events, args.out)


if __name__ == "__main__":
    main()
