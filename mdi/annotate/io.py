from __future__ import annotations

import os
import pandas as pd
from typing import Dict, Any, Optional


def read_table(path: str) -> pd.DataFrame:
    if path.lower().endswith(".xlsx") or path.lower().endswith(".xls"):
        return pd.read_excel(path)
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    raise ValueError(f"Unsupported input format: {path}")


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def normalize_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()


def build_text_focus(title: str, lead: str, text: str, max_chars: int) -> str:
    # Даем модели заголовок+лид и затем тело (обрезаем по max_chars)
    parts = []
    if title:
        parts.append(f"ЗАГОЛОВОК: {title}")
    if lead:
        parts.append(f"ЛИД: {lead}")
    if text:
        parts.append(f"ТЕКСТ:\n{text}")
    joined = "\n\n".join(parts).strip()
    if len(joined) > max_chars:
        joined = joined[:max_chars]
    return joined


def load_documents(df: pd.DataFrame, colmap: Dict[str, str], max_chars: int) -> list[dict[str, Any]]:
    required = ["event_id", "doc_id", "source_type", "source_name", "title", "lead", "text"]
    for k in required:
        if k not in colmap:
            raise ValueError(f"Missing column mapping for '{k}' in config")

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        event_id = str(r[colmap["event_id"]]).strip()
        doc_id = str(r[colmap["doc_id"]]).strip()

        source_type = str(r[colmap["source_type"]]).strip().lower()
        source_name = str(r[colmap["source_name"]]).strip()

        published_at = None
        if "published_at" in colmap and colmap["published_at"] in df.columns:
            val = r[colmap["published_at"]]
            if pd.notna(val):
                published_at = str(val)

        title = normalize_text(r.get(colmap["title"], ""))
        lead = normalize_text(r.get(colmap["lead"], ""))
        text = normalize_text(r.get(colmap["text"], ""))

        text_focus = build_text_focus(title, lead, text, max_chars=max_chars)

        rows.append({
            "event_id": event_id,
            "doc_id": doc_id,
            "source_type": source_type,
            "source_name": source_name,
            "published_at": published_at,
            "title": title,
            "lead": lead,
            "text_focus": text_focus,
        })
    return rows


def read_existing_annotations(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    df = pd.read_csv(path)
    if "doc_id" not in df.columns:
        return set()
    return set(df["doc_id"].astype(str).tolist())


def append_output_row(path: str, row: dict[str, Any]) -> None:
    ensure_dir(path)
    import pandas as pd

    out_df = pd.DataFrame([row])
    if os.path.exists(path):
        out_df.to_csv(path, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        out_df.to_csv(path, index=False, encoding="utf-8-sig")
