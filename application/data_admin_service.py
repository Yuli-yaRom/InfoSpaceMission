"""Часткове й повне видалення даних (з UI, через REST або з консолі: tools/data_admin.py).

* часткове — DELETE за фільтрами (тіло, тип, статус, роки) у PostgreSQL/Citus + delete_by_query
  в Elasticsearch. Місце на диску після DELETE не повертається одразу: рядки лише позначаються
  мертвими, їх прибирає VACUUM (звичайний — для повторного використання, FULL — повертає місце ОС);
* повне — TRUNCATE (місце звільняється одразу) + перестворення індексу Elasticsearch
  + очищення кешів Redis.
Після видалення кеш пошуку й рекомендацій скидається, інакше Redis ще 5 хв віддавав би видалені місії.
"""
import logging
import time
from typing import Optional

from application.mission_ports import (
    DataAdminUseCase, GeneratorControlPort, MissionRepositoryPort, MissionSearchPort,
    RecommendationCachePort, SearchCachePort,
)
from application.write_ledger import WriteLedger
from domain.mission_dtos import DeleteResultDTO, MissionFilter, VacuumResultDTO

log = logging.getLogger(__name__)


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 1)


def has_filter(flt: MissionFilter) -> bool:
    return any(v not in (None, "") for v in (flt.body, flt.type, flt.status, flt.year_from, flt.year_to))


class DataAdminService(DataAdminUseCase):
    def __init__(self, repo: MissionRepositoryPort, search: MissionSearchPort,
                 search_cache: SearchCachePort, rec_cache: RecommendationCachePort,
                 ledger: WriteLedger, generator: Optional[GeneratorControlPort] = None):
        self._repo = repo
        self._search = search
        self._search_cache = search_cache
        self._rec_cache = rec_cache
        self._ledger = ledger
        self._gen = generator

    async def _size(self) -> Optional[int]:
        try:
            return await self._repo.storage_bytes()
        except Exception:  # noqa: BLE001
            return None

    async def _clear_caches(self) -> int:
        n = 0
        for cache in (self._search_cache, self._rec_cache):
            try:
                n += await cache.clear()
            except Exception:  # noqa: BLE001 — Redis недоступний: дані вже видалені, кеш доживе TTL
                log.warning("Не вдалося очистити кеш %s", type(cache).__name__)
        return n

    async def preview(self, flt: MissionFilter) -> int:
        return await self._repo.count(flt)

    async def delete_one(self, mission_id: str) -> DeleteResultDTO:
        start = time.perf_counter()
        n = await self._repo.delete_ids([mission_id])
        self._ledger.add_deleted(n)
        if n:
            try:
                await self._search.delete_ids([mission_id])
            except Exception:  # noqa: BLE001
                log.warning("Не вдалося видалити %s з Elasticsearch", mission_id)
        return DeleteResultDTO(scope="id", deleted_rows=n, took_ms=_ms(start),
                               caches_cleared=await self._clear_caches() if n else 0)

    async def delete_by_filter(self, flt: MissionFilter) -> DeleteResultDTO:
        if not has_filter(flt):
            raise ValueError("Для часткового видалення потрібен хоча б один фільтр; "
                             "для повного — окрема команда")
        size_before = await self._size()
        start = time.perf_counter()
        n = await self._repo.delete_by_filter(flt)
        self._ledger.add_deleted(n)
        took = _ms(start)
        try:
            note = await self._search.delete_by_filter(flt)
        except Exception as e:  # noqa: BLE001
            note = f"Elasticsearch: не вдалося ({type(e).__name__}) — пошук може показувати видалені місії"
        log.info("Часткове видалення %s: %d рядків за %.0f мс", flt.model_dump(exclude_none=True), n, took)
        return DeleteResultDTO(scope="filter", deleted_rows=n, took_ms=took, search_note=note,
                               caches_cleared=await self._clear_caches(), size_before=size_before,
                               size_after=await self._size())

    async def delete_all(self) -> DeleteResultDTO:
        size_before = await self._size()
        before = self._ledger.total()
        start = time.perf_counter()
        if self._gen is not None:
            # TRUNCATE бере ексклюзивне блокування — чекаємо, поки генератор допише поточні пачки
            async with self._gen.quiesced():
                await self._repo.delete_all()
                self._ledger.rebase(0)
                self._gen.forget_ids()
        else:
            await self._repo.delete_all()
            self._ledger.rebase(0)
        took = _ms(start)
        try:
            await self._search.clear()
            note = "Elasticsearch: індекс перестворено"
        except Exception as e:  # noqa: BLE001
            note = f"Elasticsearch: не вдалося очистити ({type(e).__name__})"
        log.info("Повне видалення: ~%d рядків за %.0f мс", before, took)
        return DeleteResultDTO(scope="all", deleted_rows=before, took_ms=took, search_note=note,
                               caches_cleared=await self._clear_caches(), size_before=size_before,
                               size_after=await self._size())

    async def vacuum(self, full: bool) -> VacuumResultDTO:
        size_before = await self._size()
        start = time.perf_counter()
        await self._repo.vacuum(full=full)
        return VacuumResultDTO(took_ms=_ms(start), full=full, size_before=size_before,
                               size_after=await self._size())
