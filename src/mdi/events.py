"""Event table loader utilities."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class Event:
    """Event metadata for a CBR decision."""

    event_date_time: pd.Timestamp
    decision: str | None = None
    new_rate: float | None = None


def load_events(path: str | Path) -> list[Event]:
    """Load events from CSV or XLSX file."""
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported events file type: {path.suffix}")

    if "event_date_time" not in df.columns:
        raise ValueError("events file must contain event_date_time column")

    df["event_date_time"] = pd.to_datetime(df["event_date_time"], utc=False)

    events: list[Event] = []
    for _, row in df.iterrows():
        events.append(
            Event(
                event_date_time=row["event_date_time"],
                decision=row.get("decision"),
                new_rate=row.get("new_rate"),
            )
        )
    return events


def iter_event_windows(events: Iterable[Event], days_before: int, days_after: int) -> list[tuple[Event, pd.Timestamp, pd.Timestamp]]:
    """Return event windows as (event, start, end) tuples."""
    windows = []
    for event in events:
        start = event.event_date_time.normalize() - pd.Timedelta(days=days_before)
        end = event.event_date_time.normalize() + pd.Timedelta(days=days_after)
        windows.append((event, start, end))
    return windows
