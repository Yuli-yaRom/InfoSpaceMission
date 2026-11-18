"""Адаптери в пам'яті — для тестів і запуску без Docker.
Реалізують ті самі порти, що й SqlMissionRepository / ElasticsearchMissionSearchAdapter /
RedisRecommendationCacheAdapter, тому вибір робиться лише в app.py."""
from typing import Dict, List, Optional

from application.mission_ports import (
    MissionRepositoryPort, MissionSearchPort, RecommendationCachePort,
)
from domain.dtos import RecommendationDTO
from domain.mission_dtos import MissionDTO, MissionFilter, MissionPageDTO


def _match(m: MissionDTO, f: MissionFilter) -> bool:
    return ((f.body is None or m.celestial_body_id.lower() == f.body.lower())
            and (f.type is None or m.type.lower() == f.type.lower())
            and (f.status is None or m.status.lower() == f.status.lower())
            and (f.year_from is None or m.launch_year >= f.year_from)
            and (f.year_to is None or m.launch_year <= f.year_to))


class InMemoryMissionRepository(MissionRepositoryPort):
    def __init__(self):
        self._items: Dict[str, MissionDTO] = {}

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


class InMemoryMissionSearch(MissionSearchPort):
    """Спрощена заміна Elasticsearch: пошук підрядка в назві/описі."""

    def __init__(self):
        self._items: Dict[str, MissionDTO] = {}

    async def index_many(self, missions: List[MissionDTO]) -> None:
        for m in missions:
            self._items[m.id] = m

    async def search(self, text: str, flt: MissionFilter) -> MissionPageDTO:
        words = text.lower().split()
        found = [m for m in self._items.values() if _match(m, flt)
                 and all(w in f"{m.name} {m.description}".lower() for w in words)]
        return MissionPageDTO(total=len(found), items=found[flt.offset: flt.offset + flt.limit])

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
