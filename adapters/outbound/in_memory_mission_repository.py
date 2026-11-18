"""Адаптери в пам'яті — для тестів і запуску без Docker.
Реалізують ті самі порти, що й SqlMissionRepository / ElasticsearchMissionSearchAdapter /
Redis-адаптери, тому вибір робиться лише в app.py."""
import html
import re
from typing import AsyncIterator, Dict, List, Optional

from application.mission_ports import (
    MissionRepositoryPort, MissionSearchPort, RecentQueriesPort, RecommendationCachePort,
    SearchCachePort,
)
from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    MissionDTO, MissionFilter, RecentQueryDTO, SearchHitDTO, SearchPageDTO,
)


def _match(m: MissionDTO, f: MissionFilter) -> bool:
    return ((f.body is None or m.celestial_body_id.lower() == f.body.lower())
            and (f.type is None or m.type.lower() == f.type.lower())
            and (f.status is None or m.status.lower() == f.status.lower())
            and (f.year_from is None or m.launch_year >= f.year_from)
            and (f.year_to is None or m.launch_year <= f.year_to))


class InMemoryMissionRepository(MissionRepositoryPort):
    def __init__(self):
        self._items: Dict[str, MissionDTO] = {}

    def all(self) -> List[MissionDTO]:
        return list(self._items.values())

    async def save(self, mission: MissionDTO) -> None:
        self._items[mission.id] = mission

    async def save_many(self, missions: List[MissionDTO]) -> None:
        for m in missions:
            self._items[m.id] = m

    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]:
        return self._items.get(mission_id)

    async def find_by_filters(self, flt: MissionFilter) -> List[MissionDTO]:
        found = sorted((m for m in self._items.values() if _match(m, flt)),
                       key=lambda m: m.launch_year, reverse=True)
        return found[flt.offset: flt.offset + flt.limit]

    async def count(self, flt: Optional[MissionFilter] = None) -> int:
        if flt is None:
            return len(self._items)
        return sum(1 for m in self._items.values() if _match(m, flt))

    async def iter_batches(self, batch_size: int = 1000) -> AsyncIterator[List[MissionDTO]]:
        items = self.all()
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]


def _highlight(text: str, words: List[str]) -> Optional[str]:
    """Як ES з encoder: html — спершу екранування, потім <mark> навколо збігів."""
    if not text or not words:
        return None
    escaped = html.escape(text)
    pattern = re.compile("|".join(re.escape(html.escape(w)) for w in words), re.IGNORECASE)
    marked, n = pattern.subn(lambda m: f"<mark>{m.group(0)}</mark>", escaped)
    return marked if n else None


class InMemoryMissionSearch(MissionSearchPort):
    """Спрощена заміна Elasticsearch: пошук підрядка в назві/звіті/описі (без стемінгу)."""

    def __init__(self):
        self._items: Dict[str, MissionDTO] = {}

    async def index_many(self, missions: List[MissionDTO]) -> None:
        for m in missions:
            self._items[m.id] = m

    async def count(self) -> int:
        return len(self._items)

    async def search(self, text: str, flt: MissionFilter) -> SearchPageDTO:
        words = text.lower().split()
        found = [m for m in self._items.values() if _match(m, flt)
                 and all(w in f"{m.name} {m.report} {m.description}".lower() for w in words)]
        if not words:
            found.sort(key=lambda m: m.launch_year, reverse=True)
        hits = []
        for m in found[flt.offset: flt.offset + flt.limit]:
            hl = {f: [h] for f in ("name", "report", "description")
                  if (h := _highlight(getattr(m, f), words))}
            hits.append(SearchHitDTO(mission=m, highlights=hl))
        return SearchPageDTO(total=len(found), items=hits)

    async def find_similar(self, mission: MissionDTO, limit: int) -> List[MissionDTO]:
        def score(m: MissionDTO) -> float:
            return (2 * (m.celestial_body_id.lower() == mission.celestial_body_id.lower())
                    + 1.5 * (m.type.lower() == mission.type.lower())
                    + 0.5 * (m.agency_id.lower() == mission.agency_id.lower()))
        scored = [(score(m), m) for m in self._items.values() if m.id != mission.id]
        scored = [x for x in scored if x[0] > 0]
        scored.sort(key=lambda x: (-x[0], x[1].name))
        return [m for _, m in scored[:limit]]


class InMemoryRecommendationCache(RecommendationCachePort):
    def __init__(self):
        self._data: Dict[str, List[RecommendationDTO]] = {}

    async def get(self, mission_id: str) -> Optional[List[RecommendationDTO]]:
        return self._data.get(mission_id)

    async def put(self, mission_id: str, items: List[RecommendationDTO]) -> None:
        self._data[mission_id] = items


class InMemorySearchCache(SearchCachePort):
    """Без TTL: у пам'яті процесу кеш живе до перезапуску (TTL важливий лише для Redis)."""

    def __init__(self):
        self._data: Dict[str, str] = {}

    async def get(self, key: str) -> Optional[SearchPageDTO]:
        raw = self._data.get(key)
        return SearchPageDTO.model_validate_json(raw) if raw is not None else None

    async def put(self, key: str, page: SearchPageDTO, ttl_seconds: int) -> None:
        self._data[key] = page.model_dump_json()


class InMemoryRecentQueries(RecentQueriesPort):
    def __init__(self):
        self._items: List[RecentQueryDTO] = []

    async def push(self, entry: RecentQueryDTO, keep: int = 20) -> None:
        self._items.insert(0, entry)
        del self._items[keep:]

    async def list(self, limit: int = 20) -> List[RecentQueryDTO]:
        return self._items[:limit]
