# AGENTS.md

## Code style
- Use Python 3.11+ syntax.
- Prefer dataclasses and type hints.
- Logging must use the standard `logging` module (no prints in library code).
- Keep network logic in `fetch.py`; sources should only build URLs and parse HTML/XML.
- All public functions should have docstrings.

## Safety & politeness
- Respect robots.txt where applicable.
- Always keep rate limiting and retries configurable via `config.yaml`.
- Never disable SSL verification.

## Extending sources
- Add new source modules under `src/mdi/sources/`.
- Each source must expose a `discover_links` function.
- Add URL allow/deny patterns for each source in `config.yaml`.
