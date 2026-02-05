# Media Divergence Index for CBR (Phase 1)

Reproducible pipeline for collecting Russian-language news about the CBR key rate around decision dates.

## Quick start

```bash
python -m pip install --upgrade pip setuptools wheel
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

On Windows PowerShell, activate with:

```powershell
.venv\Scripts\Activate.ps1
```

Then install the project from the repo root (note the dot after `-e`):

```powershell
pip install -e .
```

The pinned dependencies include wheels for Python 3.13 (including lxml), so using 3.13 avoids local builds.

Run the collector with the test events file:

```bash
python -m mdi.collect --config config.yaml --events events_test.csv --out data/out/articles.csv
```

If you see `ModuleNotFoundError: No module named 'mdi'`, make sure you ran `pip install -e .` (with the dot) in the project root with the venv activated. You can confirm the install with:

```powershell
python -m pip show mdi
python -c "import mdi; print(mdi.__file__)"
```

If `pip show` reports nothing, rerun the editable install from the repository root.

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