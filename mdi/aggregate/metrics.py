from __future__ import annotations

import numpy as np
import pandas as pd


def majority_stance(stances: pd.Series) -> str:
    """Return majority stance, or 'mixed' if tie, or 'na' if empty."""
    counts = stances.value_counts()
    if counts.empty:
        return "na"
    top = counts.iloc[0]
    if (counts == top).sum() > 1:
        return "mixed"
    return str(counts.index[0])


def avg_strength(series: pd.Series) -> float:
    if series is None or len(series) == 0:
        return np.nan
    return float(pd.to_numeric(series, errors="coerce").mean())


def top_reasons(reasons_series: pd.Series, k: int = 2) -> str:
    """
    reasons column is expected to be string like "inflation; expectations"
    or empty/NaN. Returns top-k joined by '; '.
    """
    items: list[str] = []
    for r in reasons_series.dropna().astype(str):
        parts = [p.strip() for p in r.replace(",", ";").split(";")]
        items += [p for p in parts if p]
    if not items:
        return ""
    vc = pd.Series(items).value_counts()
    return "; ".join(vc.index[:k].tolist())


def compute_divergence(official_stance: str, media_stance: str) -> float:
    """
    divergence_stance:
      - 1 if both are in {hawkish,dovish,neutral} and differ
      - 0 if both are in {hawkish,dovish,neutral} and same
      - NaN if media_stance is mixed/na/irrelevant or official missing
    """
    if official_stance is None or media_stance is None:
        return np.nan

    o = str(official_stance).lower()
    m = str(media_stance).lower()

    allowed = {"hawkish", "dovish", "neutral"}
    if o not in allowed:
        return np.nan
    if m not in allowed:
        return np.nan

    return float(int(o != m))
