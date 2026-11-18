"""Composition root: тут адаптери під'єднуються до портів.

Сховища вибираються змінними оточення (див. docker-compose.yml):
  DATABASE_URL   -> PostgreSQL / координатор Citus (основне сховище)   інакше — у пам'яті
  CITUS_WORKERS  -> "host:port,..." воркерів Citus (п. 13–14)          інакше — імітація кластера
  ES_URL         -> Elasticsearch (пошук, схожі місії)                 інакше — у пам'яті
  REDIS_URL      -> Redis (кеш пошуку й рекомендацій, останні запити)  інакше — у пам'яті
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable, List, Optional

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from adapters.inbound.rest_api import (
    GeneratorControlRestController, MissionQueryRestController, MissionRestController,
    MissionSearchRestController, ShardingRestController,
)
from adapters.outbound.fake_generator import FakeGeneratorAdapter
from adapters.outbound.in_memory_mission_repository import (
    InMemoryMissionRepository, InMemoryMissionSearch, InMemoryRecentQueries,
    InMemoryRecommendationCache, InMemorySearchCache,
)
from adapters.outbound.seed_data import seed_requests
from application.mapreduce_service import MapReduceService, ShardingService
from application.mission_services import (
    MissionCommandService, MissionQueryService, MissionSearchService, RecommendationService,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", "300"))


async def _retry(name: str, fn: Callable, attempts: int = 30, delay: float = 2.0):
    """Контейнери БД стартують довше за застосунок — чекаємо їх готовності."""
    for i in range(1, attempts + 1):
        try:
            return await fn()
        except Exception as e:  # noqa: BLE001
            if i == attempts:
                raise
            log.info("%s ще не готовий (%s), спроба %d/%d", name, type(e).__name__, i, attempts)
            await asyncio.sleep(delay)


def create_app(run_generator: bool = True, database_url: Optional[str] = None,
               es_url: Optional[str] = None, redis_url: Optional[str] = None,
               citus_workers: Optional[str] = None) -> FastAPI:
    database_url = database_url or os.getenv("DATABASE_URL")
    es_url = es_url or os.getenv("ES_URL")
    redis_url = redis_url or os.getenv("REDIS_URL")
    citus_workers = citus_workers or os.getenv("CITUS_WORKERS")

    startup: List[Callable] = []
    shutdown: List[Callable] = []

    # ---- Основне сховище (+ шардінг) ----
    if database_url:
        from adapters.outbound.sql_mission_repository import SqlMissionRepository
        repo = SqlMissionRepository(database_url)
        startup.append(lambda: _retry("SQL", repo.init_schema))
        shutdown.append(repo.close)
    else:
        repo = InMemoryMissionRepository()

    if database_url and citus_workers:
        from adapters.outbound.citus_cluster import PgConn, parse_workers, setup_citus
        from adapters.outbound.citus_sharding_adapter import CitusShardAdapter
        coord = PgConn.from_sqlalchemy_url(database_url)
        workers = parse_workers(citus_workers)
        coord_host = os.getenv("CITUS_COORDINATOR_HOST", coord.host)
        startup.append(lambda: _retry("Citus", lambda: setup_citus(coord, coord_host, workers)))
        shards = CitusShardAdapter(coord)
        shutdown.append(shards.close)
    elif isinstance(repo, InMemoryMissionRepository):
        from adapters.outbound.in_memory_shards import InMemoryShardCluster
        shards = InMemoryShardCluster(repo)
    else:
        shards = None  # PostgreSQL без Citus: сторінка шардінгу недоступна

    # ---- Пошук ----
    if es_url:
        from adapters.outbound.elasticsearch_adapter import ElasticsearchMissionSearchAdapter
        search = ElasticsearchMissionSearchAdapter(es_url)
        startup.append(lambda: _retry("Elasticsearch", search.init_index))
        shutdown.append(search.close)
    else:
        search = InMemoryMissionSearch()

    # ---- Кеш ----
    if redis_url:
        from adapters.outbound.redis_adapter import (
            RedisRecentQueriesAdapter, RedisRecommendationCacheAdapter, RedisSearchCacheAdapter,
            make_client,
        )
        redis_client = make_client(redis_url)
        rec_cache = RedisRecommendationCacheAdapter(client=redis_client)
        search_cache = RedisSearchCacheAdapter(redis_client)
        recent = RedisRecentQueriesAdapter(redis_client)
        shutdown.append(rec_cache.close)
    else:
        rec_cache = InMemoryRecommendationCache()
        search_cache = InMemorySearchCache()
        recent = InMemoryRecentQueries()

    commands = MissionCommandService(repo, search)
    generator = FakeGeneratorAdapter(commands)

    async def reindex_if_needed() -> None:
        """Новий індекс (missions-v2) порожній, а SQL — ні: переносимо дані в Elasticsearch."""
        if await search.count() == 0 and await repo.count() > 0:
            n = 0
            async for batch in repo.iter_batches(1000):
                await search.index_many(batch)
                n += len(batch)
            log.info("Переіндексовано %d місій у пошуковий індекс", n)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        for step in startup:
            await step()
        if await repo.count() == 0:
            await commands.create_missions(seed_requests())
        else:
            await reindex_if_needed()
        task = asyncio.create_task(generator.run()) if run_generator else None
        yield
        if task:
            task.cancel()
        for step in shutdown:
            await step()

    app = FastAPI(title="InfoSpaceMission", lifespan=lifespan)
    app.include_router(MissionSearchRestController(
        MissionSearchService(search, search_cache, recent, ttl_seconds=SEARCH_CACHE_TTL),
        RecommendationService(repo, search, rec_cache)).router)
    if shards is not None:
        app.include_router(ShardingRestController(ShardingService(shards), MapReduceService(shards)).router)
    app.include_router(MissionQueryRestController(MissionQueryService(repo)).router)
    app.include_router(MissionRestController(commands).router)
    app.include_router(GeneratorControlRestController(generator).router)
    # Статика останньою, щоб не перекривала /api
    app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
