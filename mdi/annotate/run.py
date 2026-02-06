from __future__ import annotations

import os
import argparse
import yaml
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from langchain_gigachat.chat_models import GigaChat

from .io import read_table, load_documents, read_existing_annotations, append_output_row, normalize_text
from .graph import make_graph
from .schema import OutputRow


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def init_llm(cfg: dict):
    gcfg = cfg["gigachat"]
    creds = os.getenv("GIGACHAT_CREDENTIALS") or gcfg.get("credentials")
    if not creds:
        raise RuntimeError("Set env var GIGACHAT_CREDENTIALS (or put credentials in configs/annotate.yaml)")

    # scope can matter a lot (PERS/B2B/CORP)
    scope = gcfg.get("scope")  # optional

    return GigaChat(
        credentials=creds,
        scope=scope,
        verify_ssl_certs=bool(gcfg.get("verify_ssl_certs", False)),
        model=gcfg.get("model", "GigaChat"),
        temperature=float(gcfg.get("temperature", 0.0)),
        timeout=int(gcfg.get("timeout_sec", 60)),
    )


def _sanitize_text_for_llm(text: str) -> str:
    """
    Reduce blacklist risk and noise:
    - cut "Читайте также" and similar tails
    - remove excessive URLs blocks
    - trim long whitespace runs
    """
    t = normalize_text(text)

    # cut common tails
    cut_markers = [
        "\nЧитайте также",
        "\nПодписывайтесь",
        "\nРеклама",
        "\nМатериал подготовлен",
        "\nИсточник:",
        "\nСмотрите также",
    ]
    for m in cut_markers:
        idx = t.lower().find(m.lower())
        if idx != -1 and idx > 200:
            t = t[:idx].strip()

    # remove huge URL lists
    lines = []
    url_run = 0
    for line in t.split("\n"):
        if "http://" in line or "https://" in line:
            url_run += 1
        else:
            url_run = 0
        # if we see 3+ consecutive URL lines, drop them
        if url_run >= 3:
            continue
        lines.append(line)

    t = "\n".join(lines).strip()

    # compress overly long spaces
    while "  " in t:
        t = t.replace("  ", " ")
    return t


def to_output_row(doc: dict, parsed: dict | None, annotator: str, attempts: int, error: str | None) -> dict:
    status = "ok" if parsed else "failed"

    if parsed:
        reasons = "; ".join(parsed.get("reasons", []) or [])
        evidence = " ||| ".join(parsed.get("evidence", []) or [])
        notes = parsed.get("notes")
        out = OutputRow(
            event_id=doc["event_id"],
            doc_id=doc["doc_id"],
            source_type=doc["source_type"],
            source_name=doc["source_name"],
            published_at=doc.get("published_at"),
            title=doc.get("title"),
            stance=parsed["stance"],
            strength=int(parsed["strength"]),
            reasons=reasons,
            mentions_key_rate=bool(parsed["mentions_key_rate"]),
            evidence=evidence,
            notes=notes,
            annotator=annotator,
            attempts=attempts,
            status=status,
            error=None,
        )
    else:
        out = OutputRow(
            event_id=doc["event_id"],
            doc_id=doc["doc_id"],
            source_type=doc["source_type"],
            source_name=doc["source_name"],
            published_at=doc.get("published_at"),
            title=doc.get("title"),
            stance="irrelevant",
            strength=0,
            reasons="",
            mentions_key_rate=False,
            evidence="",
            notes=None,
            annotator=annotator,
            attempts=attempts,
            status=status,
            error=error or "unknown error",
        )

    return out.model_dump()


def process_one(graph, doc: dict, max_retries: int, annotator: str) -> dict:
    state = {"row": doc, "attempt": 0, "max_retries": max_retries}
    result = graph.invoke(state)
    parsed = result.get("parsed")
    attempts = int(result.get("attempt", 0))
    error = (result.get("error") or "").strip() or None
    return to_output_row(doc, parsed, annotator=annotator, attempts=attempts, error=error)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/annotate.yaml")
    ap.add_argument("--input", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    cfg = load_config(args.config)

    input_path = args.input or cfg["io"]["input_path"]
    out_path = args.out or cfg["io"]["output_path"]

    # Read input table
    df = read_table(input_path)

    colmap = cfg["io"]["columns"]

    # Reduce blacklist + rate limit probability: give less text
    max_chars = int(cfg["annotation"].get("max_chars", 3500))

    docs = load_documents(df, colmap=colmap, max_chars=max_chars)

    # Add per-doc throttling and retry settings (passed through state row)
    sleep_sec = float(cfg.get("gigachat", {}).get("request_sleep_sec", 1.5))
    req_retries = int(cfg.get("gigachat", {}).get("request_retries", 4))
    backoff_base = float(cfg.get("gigachat", {}).get("backoff_base_sec", 5.0))

    for d in docs:
        d["_sleep_sec"] = sleep_sec
        d["_request_retries"] = req_retries
        d["_backoff_base_sec"] = backoff_base

        # sanitize text_focus further (it already contains title/lead/text)
        d["text_focus"] = _sanitize_text_for_llm(d["text_focus"])

    # Resume
    done = read_existing_annotations(out_path)
    docs = [d for d in docs if d["doc_id"] not in done]

    llm = init_llm(cfg)
    graph = make_graph(llm)

    max_retries = int(cfg["annotation"].get("max_retries", 3))

    # IMPORTANT: to avoid 429, default concurrency=1
    concurrency = int(cfg["annotation"].get("concurrency", 1))
    annotator = str(cfg["annotation"].get("annotator_name", "gigachat_llm"))

    if not docs:
        print("No new documents to annotate.")
        return

    # Process with safe exception handling: NEVER crash the whole run
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(process_one, graph, d, max_retries, annotator) for d in docs]
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Annotating"):
            try:
                row = fut.result()
            except Exception as e:
                # fallback: we don't know which doc, so we can't append a proper row
                # but at least we print and continue
                print(f"[ERROR] worker failed: {e}")
                continue
            append_output_row(out_path, row)

    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
