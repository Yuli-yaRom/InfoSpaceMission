"""REST-рівень пошуку/рекомендацій (in-memory адаптери) + кеш-логіка RecommendationService."""
import asyncio

import pytest
from fastapi.testclient import TestClient

from adapters.outbound.in_memory_mission_repository import (
    InMemoryMissionRepository, InMemoryMissionSearch, InMemoryRecommendationCache,
)
from app import create_app
from application.mission_services import MissionCommandService, RecommendationService
from domain.mission_dtos import MissionCreateRequest


@pytest.fixture
def client():
    with TestClient(create_app(run_generator=False)) as c:
        yield c


def _id_of(client, name):
    items = client.get("/api/missions", params={"limit": 100}).json()["items"]
    return next(m["id"] for m in items if m["name"] == name)


def test_search_by_text(client):
    data = client.get("/api/search", params={"q": "марсохід"}).json()
    names = {m["name"] for m in data["items"]}
    assert {"Curiosity", "Perseverance", "Opportunity", "Zhurong"} <= names


def test_search_with_filter(client):
    data = client.get("/api/search", params={"q": "Місяць", "type": "CREWED"}).json()
    assert {m["name"] for m in data["items"]} == {"Apollo 11", "Artemis III"}


def test_created_mission_is_searchable(client):
    client.post("/api/missions", json={"name": "Europa Clipper", "type": "PROBE", "agency_id": "NASA",
                                       "celestial_body_id": "europa", "launch_year": 2024,
                                       "description": "Дослідження крижаного супутника"})
    assert client.get("/api/search", params={"q": "крижаного"}).json()["total"] == 1


def test_recommendations(client):
    recs = client.get(f"/api/missions/{_id_of(client, 'Curiosity')}/recommendations",
                      params={"limit": 3}).json()
    assert len(recs) == 3
    assert all("mars" in r["similarity_reason"] for r in recs)
    assert "Curiosity" not in {r["name"] for r in recs}


def test_recommendations_missing_mission(client):
    assert client.get("/api/missions/nope/recommendations").status_code == 404


class _CountingSearch(InMemoryMissionSearch):
    def __init__(self):
        super().__init__()
        self.similar_calls = 0

    async def find_similar(self, mission, limit):
        self.similar_calls += 1
        return await super().find_similar(mission, limit)


class _BrokenCache(InMemoryRecommendationCache):
    async def get(self, mission_id):
        raise ConnectionError("redis down")

    async def put(self, mission_id, items):
        raise ConnectionError("redis down")


def _setup(cache):
    repo, search = InMemoryMissionRepository(), _CountingSearch()
    cmd = MissionCommandService(repo, search)
    reqs = [MissionCreateRequest(name=f"M{i}", type="ROVER", agency_id="NASA",
                                 celestial_body_id="mars", launch_year=2000 + i) for i in range(4)]
    missions = asyncio.run(cmd.create_missions(reqs))
    return RecommendationService(repo, search, cache), search, missions[0].id


def test_second_call_served_from_cache():
    svc, search, mid = _setup(InMemoryRecommendationCache())
    first = asyncio.run(svc.recommend(mid, 2))
    second = asyncio.run(svc.recommend(mid, 2))
    assert first == second and search.similar_calls == 1


def test_works_when_cache_is_down():
    svc, search, mid = _setup(_BrokenCache())
    assert len(asyncio.run(svc.recommend(mid, 2))) == 2
