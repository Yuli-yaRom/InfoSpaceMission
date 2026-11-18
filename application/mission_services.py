import logging
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from application.mission_ports import (
    GetRecommendationsUseCase, MissionCommandUseCase, MissionQueryUseCase,
    MissionRepositoryPort, MissionSearchPort, MissionSearchUseCase,
    RecentQueriesPort, RecommendationCachePort, SearchCachePort,
)
from application.search_normalization import active_filters, cache_key
from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    MissionCreateRequest, MissionDTO, MissionFilter, MissionPageDTO, RecentQueryDTO,
    SearchPageDTO, SearchResultDTO, StatsDTO,
)

log = logging.getLogger(__name__)


class MissionQueryService(MissionQueryUseCase):
    """Структуровані запити (фільтри, пагінація) — з SQL."""

    def __init__(self, repository: MissionRepositoryPort):
        self._repo = repository

    async def list_missions(self, flt: MissionFilter) -> MissionPageDTO:
        items = await self._repo.find_by_filters(flt)
        total = await self._repo.count(flt)
        return MissionPageDTO(total=total, items=items)

    async def get_mission(self, mission_id: str) -> Optional[MissionDTO]:
        return await self._repo.find_by_id(mission_id)

    async def get_stats(self) -> StatsDTO:
        return StatsDTO(total_documents=await self._repo.count())


class MissionCommandService(MissionCommandUseCase):
    """Запис: спочатку SQL (джерело істини), потім індексація в Elasticsearch."""

    def __init__(self, repository: MissionRepositoryPort, search: MissionSearchPort):
        self._repo = repository
        self._search = search

    async def create_mission(self, request: MissionCreateRequest) -> MissionDTO:
        return (await self.create_missions([request]))[0]

    async def create_missions(self, requests: List[MissionCreateRequest]) -> List[MissionDTO]:
        missions = [MissionDTO(id=str(uuid.uuid4()), **r.model_dump()) for r in requests]
        if not missions:
            return missions
        await self._repo.save_many(missions)
        try:
            await self._search.index_many(missions)
        except Exception:
            # SQL уже містить дані; пошуковий індекс можна переіндексувати пізніше
            log.exception("Не вдалося проіндексувати %d місій в Elasticsearch", len(missions))
        return missions


class MissionSearchService(MissionSearchUseCase):
    """Cache-aside (п. 15): Redis search:<hash> -> (промах) -> Elasticsearch -> Redis з TTL.
    Кожен запит також потрапляє в список останніх запитів (LPUSH + LTRIM)."""

    def __init__(self, search: MissionSearchPort, cache: SearchCachePort,
                 recent: RecentQueriesPort, ttl_seconds: int = 300, keep_recent: int = 20):
        self._search = search
        self._cache = cache
        self._recent = recent
        self._ttl = ttl_seconds
        self._keep = keep_recent

    async def search(self, text: str, flt: MissionFilter) -> SearchResultDTO:
        started = time.perf_counter()
        key = cache_key(text, flt)
        page: Optional[SearchPageDTO] = None
        try:
            page = await self._cache.get(key)
        except Exception:
            log.warning("Redis недоступний, шукаємо без кешу", exc_info=True)
        status = "HIT" if page is not None else "MISS"
        if page is None:
            page = await self._search.search(text, flt)
            try:
                await self._cache.put(key, page, self._ttl)
            except Exception:
                log.warning("Не вдалося записати результат пошуку в Redis", exc_info=True)
        took_ms = round((time.perf_counter() - started) * 1000, 2)

        entry = RecentQueryDTO(query=text.strip(), filters=active_filters(flt), total=page.total,
                               status="Знайдено" if page.total else "Пусто", cache=status,
                               at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        try:
            await self._recent.push(entry, self._keep)
        except Exception:
            log.warning("Не вдалося оновити список останніх запитів", exc_info=True)

        return SearchResultDTO(query=text, total=page.total, items=page.items, cache=status, took_ms=took_ms)

    async def recent(self, limit: int = 20) -> List[RecentQueryDTO]:
        try:
            return await self._recent.list(limit)
        except Exception:
            log.warning("Redis недоступний, список останніх запитів порожній", exc_info=True)
            return []


def _similarity_reason(src: MissionDTO, other: MissionDTO) -> str:
    reasons = []
    if src.celestial_body_id.lower() == other.celestial_body_id.lower():
        reasons.append(f"те саме тіло: {other.celestial_body_id}")
    if src.type.lower() == other.type.lower():
        reasons.append(f"той самий тип: {other.type}")
    if src.agency_id.lower() == other.agency_id.lower():
        reasons.append(f"те саме агентство: {other.agency_id}")
    return ", ".join(reasons) or "схожий опис"


class RecommendationService(GetRecommendationsUseCase):
    """Cache-aside: Redis -> (промах) -> Elasticsearch more_like_this -> Redis."""

    def __init__(self, repository: MissionRepositoryPort, search: MissionSearchPort,
                 cache: RecommendationCachePort):
        self._repo = repository
        self._search = search
        self._cache = cache

    async def recommend(self, mission_id: str, limit: int = 5) -> List[RecommendationDTO]:
        cached = await self._safe_cache_get(mission_id)
        if cached is not None and len(cached) >= limit:
            return cached[:limit]

        mission = await self._repo.find_by_id(mission_id)
        if mission is None:
            raise KeyError(mission_id)
        similar = await self._search.find_similar(mission, limit)
        items = [RecommendationDTO(mission_id=m.id, name=m.name,
                                   similarity_reason=_similarity_reason(mission, m))
                 for m in similar]
        await self._safe_cache_put(mission_id, items)
        return items

    async def _safe_cache_get(self, mission_id: str) -> Optional[List[RecommendationDTO]]:
        try:
            return await self._cache.get(mission_id)
        except Exception:
            log.warning("Redis недоступний, читаємо без кешу", exc_info=True)
            return None

    async def _safe_cache_put(self, mission_id: str, items: List[RecommendationDTO]) -> None:
        try:
            await self._cache.put(mission_id, items)
        except Exception:
            log.warning("Не вдалося записати рекомендації в Redis", exc_info=True)
