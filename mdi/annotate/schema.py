from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


Stance = Literal["hawkish", "dovish", "neutral", "mixed", "irrelevant"]


class LLMAnnotation(BaseModel):
    """
    То, что должна вернуть LLM (и только это).
    event_id/doc_id/source_* мы добавим сами из входной таблицы.
    """
    stance: Stance = Field(..., description="Оценка тона/позиции относительно ужесточения ДКП.")
    strength: int = Field(..., ge=0, le=3, description="0..3 — сила выраженности позиции")
    reasons: List[str] = Field(default_factory=list, description="Короткие причины/маркеры")
    mentions_key_rate: bool = Field(..., description="Есть ли упоминание ключевой ставки/решения по ставке")
    evidence: List[str] = Field(default_factory=list, description="1-3 короткие цитаты/фрагменты из текста")
    notes: Optional[str] = Field(default=None, description="Опционально: сомнения/особые случаи")


class OutputRow(BaseModel):
    # метаданные из входа
    event_id: str
    doc_id: str
    source_type: str
    source_name: str
    published_at: Optional[str] = None
    title: Optional[str] = None

    # разметка
    stance: Stance
    strength: int
    reasons: str
    mentions_key_rate: bool
    evidence: str
    notes: Optional[str] = None

    # техполя
    annotator: str
    attempts: int
    status: Literal["ok", "failed"]
    error: Optional[str] = None
