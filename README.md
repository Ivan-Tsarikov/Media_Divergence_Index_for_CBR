# Media Divergence Index for CBR (Phase 1)

Reproducible pipeline for collecting Russian-language news about the CBR key rate around decision dates.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Run the collector with the test events file:

```bash
python -m mdi.collect --config config.yaml --events events_test.csv --out data/out/articles.csv
```

## What the pipeline does

1. **Link discovery** per source (RT search API, Fontanka tag pages, Vedomosti daily archive, sitemaps).
2. **Fetch + extract** HTML (Trafilatura + metadata).
3. **Relevance filter**: AND of key rate regex + CBR mention near the lede + decision trigger.
4. **Window assignment**: event window [-1; +2] days around `event_date_time`.
5. **Output** to CSV/JSONL/Parquet with normalized fields.

## Configuration

Key settings live in `config.yaml`:

- `window_days`: event window before/after.
- `relevance`: regexes + lede length for strict filtering.
- `sources`: per-source discovery settings and URL allow/deny rules.
- `cache` and `retries`: polite crawling via caching and backoff.

## Output schema

`articles.csv` fields:

- `source`, `url`, `canonical_url`
- `title`, `published_at`, `text`, `summary`
- `event_date_time`, `event_decision`, `event_new_rate`
- `fetch_status`, `parse_status`, `relevance`

## Testing

Unit tests use local HTML fixtures only:

```bash
pytest
```

Integration (optional, not in CI):

```bash
python -m mdi.collect --config config.yaml --events events_test.csv --out data/out/articles.csv
```

## Notes & limitations

- Paywalled/blocked pages may return metadata only; the pipeline records `fetch_status` and `parse_status`.
- Robots.txt should be respected by keeping rate limiting and retries configured in `config.yaml`.
- Sitemaps are capped per event to avoid full-site downloads.

## Next phase

Phase 2 will add a baseline MDI computation (rule-based stance + divergence per event).
