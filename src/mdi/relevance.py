"""Relevance filtering for key rate articles."""
from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class RelevanceConfig:
    """Regex configuration for relevance filtering."""

    keyrate_regex: str
    cbr_regex: str
    decision_regex: str
    cbr_lede_chars: int


def is_relevant(title: str | None, text: str | None, config: RelevanceConfig) -> bool:
    """Return True if article matches key rate + CBR + decision criteria."""
    title = title or ""
    text = text or ""

    keyrate_re = re.compile(config.keyrate_regex, re.IGNORECASE)
    cbr_re = re.compile(config.cbr_regex, re.IGNORECASE)
    decision_re = re.compile(config.decision_regex, re.IGNORECASE)

    if not keyrate_re.search(title + " " + text):
        return False

    lede = (title + " " + text)[: config.cbr_lede_chars]
    if not cbr_re.search(lede):
        return False

    if not decision_re.search(title + " " + text):
        return False

    return True
