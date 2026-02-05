from __future__ import annotations

import json
from textwrap import dedent

# Мини-кодбук прямо в промпте (чтобы не зависеть от файлов)
CODEBOOK = dedent("""
Ты размечаешь тексты о денежно-кредитной политике (ДКП) и ключевой ставке.

Определи stance (одно значение):
- hawkish: риторика про ужесточение/повышение ставки/борьбу с инфляцией приоритетно, риск перегрева, необходимость жесткости.
- dovish: риторика про смягчение/снижение ставки/поддержку роста, акцент на риски для экономики, готовность смягчаться.
- neutral: описательно, без явного уклона.
- mixed: есть и "ястребиные", и "голубиные" маркеры без явного доминирования.
- irrelevant: текст не про ключевую ставку/решение/ДКП (например, общая экономика без ставки).

strength (0..3):
0 — уклон не выражен,
1 — слабый,
2 — средний,
3 — сильный.

mentions_key_rate:
true если прямо говорится про ключевую ставку/решение по ставке/изменение ставки/ДКП ЦБ,
иначе false.

reasons: 1-5 коротких маркеров (русским языком), например:
["говорит о необходимости удерживать высокую ставку", "акцент на рисках инфляции"]

evidence: 1-3 коротких фрагмента ИЗ ТЕКСТА (не выдумывай), до ~160 символов каждый.
""")


SYSTEM = dedent("""
Ты — строгий разметчик. Твоя задача: вернуть ТОЛЬКО JSON-объект строго по схеме.
Никакого Markdown, никаких пояснений, никакого текста вне JSON.
Без лишних ключей.
""")


def build_prompt(source_type: str, source_name: str, title: str | None, lead: str | None, text: str) -> str:
    schema_hint = {
        "stance": "hawkish|dovish|neutral|mixed|irrelevant",
        "strength": 0,
        "reasons": ["..."],
        "mentions_key_rate": True,
        "evidence": ["..."],
        "notes": "..."
    }

    payload = {
        "source_type": source_type,
        "source_name": source_name,
        "title": title or "",
        "lead": lead or "",
        "text": text,
    }

    return dedent(f"""
    {SYSTEM}

    {CODEBOOK}

    Верни JSON по этой схеме (пример структуры, не значения):
    {json.dumps(schema_hint, ensure_ascii=False)}

    Текст для разметки (JSON с полями source/title/lead/text):
    {json.dumps(payload, ensure_ascii=False)}
    """).strip()
