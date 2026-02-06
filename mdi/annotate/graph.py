from __future__ import annotations

import json
import time
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
    attempt: int          # repair-attempts (JSON fixing)
    max_retries: int


def _extract_json(text: str) -> str:
    """Try to cut out a JSON object from a model response."""
    if not text:
        return ""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text.strip()
    return text[start:end + 1].strip()


def _is_rate_limit_error(msg: str) -> bool:
    m = (msg or "").lower()
    return ("429" in m) or ("too many requests" in m) or ("rate limit" in m)


def _is_timeout_error(msg: str) -> bool:
    m = (msg or "").lower()
    return ("timeout" in m) or ("timed out" in m) or ("connecttimeout" in m)


def make_graph(llm) -> Any:
    """
    Graph: annotate -> validate -> (repair -> validate)* -> end
    We also handle network/rate-limit errors inside annotate node.
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

        # Throttle: small sleep before each request (helps avoid 429)
        sleep_sec = float(row.get("_sleep_sec", 0.0) or 0.0)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

        # Retry on 429 / transient errors
        max_req_retries = int(row.get("_request_retries", 4) or 4)
        base_backoff = float(row.get("_backoff_base_sec", 5.0) or 5.0)

        last_err: Optional[str] = None
        for i in range(max_req_retries):
            try:
                res = llm.invoke(prompt)
                raw = getattr(res, "content", None)
                if raw is None:
                    raw = str(res)
                return {"prompt": prompt, "raw": str(raw), "error": ""}
            except Exception as e:
                msg = str(e)
                last_err = msg

                # Rate limit -> backoff and retry
                if _is_rate_limit_error(msg):
                    time.sleep(base_backoff * (i + 1))
                    continue

                # Timeout sometimes transient -> short retry
                if _is_timeout_error(msg):
                    time.sleep(2.0 * (i + 1))
                    continue

                # Other errors: do not retry by default
                return {"prompt": prompt, "raw": "", "error": f"invoke failed: {msg}"}

        return {"prompt": prompt, "raw": "", "error": f"invoke failed after retries: {last_err}"}

    def validate_node(state: State) -> State:
        # If annotate already failed -> skip validation
        err = (state.get("error") or "").strip()
        if err:
            return {"parsed": None, "error": err}

        raw = state.get("raw", "")
        json_text = _extract_json(raw)

        try:
            obj = json.loads(json_text)
        except Exception as e:
            return {"parsed": None, "error": f"json.loads failed: {e.__class__.__name__}: {e}"}

        try:
            ann = LLMAnnotation.model_validate(obj)
        except ValidationError as e:
            return {"parsed": None, "error": f"schema validation failed: {e}"}

        return {"parsed": ann.model_dump(), "error": ""}

    def repair_node(state: State) -> State:
        attempt = int(state.get("attempt", 0)) + 1
        max_retries = int(state.get("max_retries", 2))

        err = state.get("error", "")
        raw = state.get("raw", "")

        repair_prompt = (
            "Ты исправляешь JSON-ответ.\n"
            "Верни ТОЛЬКО корректный JSON-объект без Markdown и без текста вне JSON.\n\n"
            "Ошибки:\n"
            f"{err}\n\n"
            "Исходный ответ модели:\n"
            f"{raw}\n\n"
            "Схема (строго):\n"
            '{"stance":"hawkish|dovish|neutral|mixed|irrelevant",'
            '"strength":0,'
            '"reasons":["..."],'
            '"mentions_key_rate":true,'
            '"evidence":["..."],'
            '"notes":"..."}'
        )

        try:
            res = llm.invoke(repair_prompt)
            fixed = getattr(res, "content", None)
            if fixed is None:
                fixed = str(res)
            return {
                "attempt": attempt,
                "max_retries": max_retries,
                "raw": str(fixed),
                "error": "",
            }
        except Exception as e:
            # even repair can hit 429; don't crash
            return {
                "attempt": attempt,
                "max_retries": max_retries,
                "raw": "",
                "error": f"repair invoke failed: {e}",
            }

    def route_after_validate(state: State) -> str:
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
    g.add_conditional_edges("validate", route_after_validate, {"ok": END, "repair": "repair", "fail": END})
    g.add_edge("repair", "validate")

    return g.compile()
