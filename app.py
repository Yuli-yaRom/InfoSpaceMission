"""Composition root: тут адаптери під'єднуються до портів.
Щоб перейти на реальну БД — замінити InMemoryMissionRepository на Mongo/SQL-адаптер."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from adapters.inbound.rest_api import (
    GeneratorControlRestController, MissionQueryRestController, MissionRestController,
)
from adapters.outbound.in_memory_mission_repository import (
    FakeGeneratorAdapter, InMemoryMissionRepository,
)
from application.mission_services import MissionCommandService, MissionQueryService


def create_app(run_generator: bool = True) -> FastAPI:
    repo = InMemoryMissionRepository()
    generator = FakeGeneratorAdapter(repo)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(generator.run()) if run_generator else None
        yield
        if task:
            task.cancel()

    app = FastAPI(title="InfoSpaceMission", lifespan=lifespan)
    app.include_router(MissionQueryRestController(MissionQueryService(repo)).router)
    app.include_router(MissionRestController(MissionCommandService(repo)).router)
    app.include_router(GeneratorControlRestController(generator).router)
    # Статика останньою, щоб не перекривала /api
    app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
