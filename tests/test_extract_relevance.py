from pathlib import Path

from mdi.extract import extract_article
from mdi.relevance import RelevanceConfig, is_relevant


def fixture_path(name: str) -> Path:
    return Path("tests/fixtures") / name


def test_extract_article_from_fixture():
    html = fixture_path("article.html").read_text(encoding="utf-8")
    result = extract_article(html)
    assert result.title
    assert result.text
    assert result.parse_status == "ok"


def test_relevance_matches_keyrate():
    html = fixture_path("article.html").read_text(encoding="utf-8")
    result = extract_article(html)
    cfg = RelevanceConfig(
        keyrate_regex=r"ключев\w*\s+ставк\w*",
        cbr_regex=r"(Банк\s+России|ЦБ|центробанк)",
        decision_regex=r"(совет\s+директор[а-я]*|решени|повысил|снизил|сохранил)",
        cbr_lede_chars=500,
    )
    assert is_relevant(result.title, result.text, cfg) is True


def test_relevance_rejects_rubric():
    html = fixture_path("rubric.html").read_text(encoding="utf-8")
    result = extract_article(html)
    cfg = RelevanceConfig(
        keyrate_regex=r"ключев\w*\s+ставк\w*",
        cbr_regex=r"(Банк\s+России|ЦБ|центробанк)",
        decision_regex=r"(совет\s+директор[а-я]*|решени|повысил|снизил|сохранил)",
        cbr_lede_chars=500,
    )
    assert is_relevant(result.title, result.text, cfg) is False


def test_extract_paywall_no_text():
    html = fixture_path("paywall.html").read_text(encoding="utf-8")
    result = extract_article(html)
    assert result.parse_status in {"no_text", "ok"}
