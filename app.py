"""Composition root: тут адаптери під'єднуються до портів.

Сховища вибираються змінними оточення (див. docker-compose.yml):
  DATABASE_URL  -> PostgreSQL  (основне сховище)       інакше — у пам'яті
  ES_URL        -> Elasticsearch (пошук, схожі місії)   інакше — у пам'яті
  REDIS_URL     -> Redis (кеш рекомендацій)             інакше — у пам'яті
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
    MissionSearchRestController,
)
from adapters.outbound.fake_generator import FakeGeneratorAdapter
from adapters.outbound.in_memory_mission_repository import (
    InMemoryMissionRepository, InMemoryMissionSearch, InMemoryRecommendationCache,
)
from adapters.outbound.seed_data import seed_requests
from application.mission_services import (
    MissionCommandService, MissionQueryService, MissionSearchService, RecommendationService,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")


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
               es_url: Optional[str] = None, redis_url: Optional[str] = None) -> FastAPI:
    database_url = database_url or os.getenv("DATABASE_URL")
    es_url = es_url or os.getenv("ES_URL")
    redis_url = redis_url or os.getenv("REDIS_URL")

    startup: List[Callable] = []
    shutdown: List[Callable] = []

    if database_url:
        from adapters.outbound.sql_mission_repository import SqlMissionRepository
        repo = SqlMissionRepository(database_url)
        startup.append(lambda: _retry("SQL", repo.init_schema))
        shutdown.append(repo.close)
    else:
        repo = InMemoryMissionRepository()

    if es_url:
        from adapters.outbound.elasticsearch_adapter import ElasticsearchMissionSearchAdapter
        search = ElasticsearchMissionSearchAdapter(es_url)
        startup.append(lambda: _retry("Elasticsearch", search.init_index))
        shutdown.append(search.close)
    else:
        search = InMemoryMissionSearch()

    if redis_url:
        from adapters.outbound.redis_adapter import RedisRecommendationCacheAdapter
        cache = RedisRecommendationCacheAdapter(redis_url)
        shutdown.append(cache.close)
    else:
        cache = InMemoryRecommendationCache()

    commands = MissionCommandService(repo, search)
    generator = FakeGeneratorAdapter(commands)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        for step in startup:
            await step()
        if await repo.count() == 0:
            await commands.create_missions(seed_requests())
        task = asyncio.create_task(generator.run()) if run_generator else None
        yield
        if task:
            task.cancel()
        for step in shutdown:
            await step()

    app = FastAPI(title="InfoSpaceMission", lifespan=lifespan)
    app.include_router(MissionSearchRestController(
        MissionSearchService(search), RecommendationService(repo, search, cache)).router)
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
