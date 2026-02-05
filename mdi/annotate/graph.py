from __future__ import annotations

import json
from typing import Any, Dict, TypedDict, Optional

from langgraph.graph import StateGraph, START, END

from pydantic import ValidationError

from .schema import LLMAnnotation
from .prompts import build_prompt


class State(TypedDict, total=False):
    row: Dict[str, Any]
    prompt: str
    raw: str
    parsed: Dict[str, Any]
    error: str
    attempt: int
    max_retries: int


def _extract_json(text: str) -> str:
    if not text:
        return ""
    # простая эвристика: вырезаем от первой { до последней }
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text.strip()
    return text[start:end + 1].strip()


def make_graph(llm) -> Any:
    """
    llm: ChatModel с методом invoke(prompt) -> AIMessage/str
    """

    def annotate_node(state: State) -> State:
        row = state["row"]
        prompt = build_prompt(
            source_type=row["source_type"],
            source_name=row["source_name"],
            title=row.get("title"),
            lead=row.get("lead"),
            text=row["text_focus"],
        )
        res = llm.invoke(prompt)
        raw = getattr(res, "content", None)
        if raw is None:
            raw = str(res)
        return {"prompt": prompt, "raw": str(raw)}

    def validate_node(state: State) -> State:
        raw = state.get("raw", "")
        json_text = _extract_json(raw)
        try:
            obj = json.loads(json_text)
        except Exception as e:
            return {"error": f"json.loads failed: {e.__class__.__name__}: {e}"}

        try:
            ann = LLMAnnotation.model_validate(obj)
        except ValidationError as e:
            return {"error": f"schema validation failed: {e}"}

        return {"parsed": ann.model_dump(), "error": ""}

    def repair_node(state: State) -> State:
        attempt = int(state.get("attempt", 0)) + 1
        max_retries = int(state.get("max_retries", 2))

        err = state.get("error", "")
        raw = state.get("raw", "")

        repair_prompt = (
            "Ты исправляешь JSON.\n"
            "Верни ТОЛЬКО исправленный JSON-объект без Markdown и без текста вне JSON.\n"
            "Ошибки:\n"
            f"{err}\n\n"
            "Вот исходный ответ модели:\n"
            f"{raw}\n\n"
            "Исправь так, чтобы строго соответствовало схеме полей:\n"
            '{"stance":"hawkish|dovish|neutral|mixed|irrelevant","strength":0,"reasons":["..."],"mentions_key_rate":true,"evidence":["..."],"notes":"..."}'
        )

        res = llm.invoke(repair_prompt)
        fixed = getattr(res, "content", None)
        if fixed is None:
            fixed = str(res)

        return {
            "attempt": attempt,
            "max_retries": max_retries,
            "raw": str(fixed),
        }

    def should_repair(state: State) -> str:
        if state.get("parsed"):
            return "ok"
        attempt = int(state.get("attempt", 0))
        max_retries = int(state.get("max_retries", 2))
        if attempt < max_retries:
            return "repair"
        return "fail"

    g = StateGraph(State)
    g.add_node("annotate", annotate_node)
    g.add_node("validate", validate_node)
    g.add_node("repair", repair_node)

    g.add_edge(START, "annotate")
    g.add_edge("annotate", "validate")
    g.add_conditional_edges("validate", should_repair, {"ok": END, "repair": "repair", "fail": END})
    g.add_edge("repair", "validate")

    return g.compile()
