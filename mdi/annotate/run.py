from __future__ import annotations

import os
import argparse
import yaml
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from langchain_gigachat.chat_models import GigaChat  # official import :contentReference[oaicite:8]{index=8}

from .io import read_table, load_documents, read_existing_annotations, append_output_row
from .graph import make_graph
from .schema import OutputRow


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def init_llm(cfg: dict):
    provider = cfg.get("provider", "gigachat")
    if provider != "gigachat":
        raise ValueError("Only provider=gigachat is implemented in this MVP")

    gcfg = cfg["gigachat"]
    creds = os.getenv("GIGACHAT_CREDENTIALS") or gcfg.get("credentials")
    if not creds:
        raise RuntimeError("Set env var GIGACHAT_CREDENTIALS (or put credentials in configs/annotate.yaml)")

    return GigaChat(
        credentials=creds,
        verify_ssl_certs=bool(gcfg.get("verify_ssl_certs", False)),
        model=gcfg.get("model", "GigaChat"),
        temperature=float(gcfg.get("temperature", 0.0)),
        timeout=int(gcfg.get("timeout_sec", 60)),
    )


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
    error = result.get("error") or None
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

    df = read_table(input_path)

    colmap = cfg["io"]["columns"]
    max_chars = int(cfg["annotation"].get("max_chars", 8000))
    docs = load_documents(df, colmap=colmap, max_chars=max_chars)

    done = read_existing_annotations(out_path)
    docs = [d for d in docs if d["doc_id"] not in done]

    llm = init_llm(cfg)
    graph = make_graph(llm)

    max_retries = int(cfg["annotation"].get("max_retries", 2))
    concurrency = int(cfg["annotation"].get("concurrency", 2))
    annotator = str(cfg["annotation"].get("annotator_name", "gigachat_llm"))

    if not docs:
        print("No new documents to annotate.")
        return

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(process_one, graph, d, max_retries, annotator) for d in docs]
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Annotating"):
            row = fut.result()
            append_output_row(out_path, row)

    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
