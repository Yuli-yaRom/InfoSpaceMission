"""Нормалізація пошукового запиту для ключа кешу Redis (п. 15).

"Mars, ROVER landing!" і "landing rover mars" -> однаковий ключ:
нижній регістр, без пунктуації, слова відсортовані, плюс відсортовані фільтри й пагінація.
"""
import hashlib
import re
from typing import Any, Dict

from domain.mission_dtos import MissionFilter

_WORD = re.compile(r"\w+", re.UNICODE)


def normalize_text(text: str) -> str:
    return " ".join(sorted(_WORD.findall(text.lower())))


def active_filters(flt: MissionFilter) -> Dict[str, Any]:
    """Лише задані фільтри (без пагінації) — для показу в списку останніх запитів."""
    data = flt.model_dump(exclude={"limit", "offset"})
    return {k: (v.lower() if isinstance(v, str) else v) for k, v in data.items() if v not in (None, "")}


def normalize_query(text: str, flt: MissionFilter) -> str:
    parts = [f"q={normalize_text(text)}"]
    parts += [f"{k}={v}" for k, v in sorted(active_filters(flt).items())]
    parts += [f"limit={flt.limit}", f"offset={flt.offset}"]
    return "|".join(parts)


def cache_key(text: str, flt: MissionFilter) -> str:
    digest = hashlib.sha1(normalize_query(text, flt).encode("utf-8")).hexdigest()
    return f"search:{digest}"
