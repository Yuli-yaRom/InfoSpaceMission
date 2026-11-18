"""Тести реальних адаптерів без Docker:
- SQL: справжній SQLAlchemy на SQLite (aiosqlite) — той самий код, що й для PostgreSQL;
- Elasticsearch / Redis: фейкові клієнти, перевіряємо запити та серіалізацію."""
import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.outbound.elasticsearch_adapter import ElasticsearchMissionSearchAdapter
from adapters.outbound.redis_adapter import RedisRecommendationCacheAdapter
from adapters.outbound.sql_mission_repository import SqlMissionRepository
from domain.dtos import RecommendationDTO
from domain.mission_dtos import MissionDTO, MissionFilter


def _m(name, body="mars", typ="ROVER", year=2000, status="ACTIVE"):
    return MissionDTO(id=str(uuid.uuid4()), name=name, type=typ, status=status, agency_id="NASA",
                      celestial_body_id=body, launch_year=year, description=f"{name} desc")


# ---------- SQL ----------
def test_sql_repository_roundtrip(tmp_path):
    async def scenario():
        repo = SqlMissionRepository(engine=create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db"))
        await repo.init_schema()
        ms = [_m("A", year=2001), _m("B", "moon", "LANDER", 1969), _m("C", year=2020, status="FAILED")]
        await repo.save_many(ms)
        await repo.save(_m("D", "Mars", year=1999))

        assert await repo.count() == 4
        assert (await repo.find_by_id(ms[1].id)).name == "B"
        assert await repo.find_by_id("missing") is None

        flt = MissionFilter(body="MARS")  # регістр не важливий
        assert await repo.count(flt) == 3
        assert [m.name for m in await repo.find_by_filters(flt)] == ["C", "A", "D"]  # рік ↓
        assert await repo.count(MissionFilter(type="rover", year_from=2000, year_to=2010)) == 1
        page = await repo.find_by_filters(MissionFilter(limit=2, offset=1))
        assert [m.name for m in page] == ["A", "D"]
        await repo.close()

    asyncio.run(scenario())


# ---------- Elasticsearch ----------
class _FakeIndices:
    def __init__(self):
        self.created = None

    async def exists(self, index):
        return False

    async def create(self, index, mappings, settings):
        self.created = (index, mappings, settings)


class _FakeES:
    def __init__(self, hits):
        self.indices = _FakeIndices()
        self.hits = hits
        self.last_search = None

    async def search(self, **kwargs):
        self.last_search = kwargs
        return {"hits": {"total": {"value": len(self.hits)}, "hits": self.hits}}


def _hit(m):
    return {"_id": m.id, "_source": m.model_dump(exclude={"id"})}


def test_es_search_builds_query_and_maps_hits():
    m = _m("Curiosity")
    es = _FakeES([_hit(m)])
    adapter = ElasticsearchMissionSearchAdapter(client=es)
    page = asyncio.run(adapter.search("rover", MissionFilter(body="Mars", year_from=2000, limit=5, offset=10)))

    assert page.total == 1 and page.items[0] == m
    q = es.last_search
    assert q["from_"] == 10 and q["size"] == 5
    assert q["query"]["bool"]["must"][0]["multi_match"]["query"] == "rover"
    assert {"term": {"celestial_body_id": "mars"}} in q["query"]["bool"]["filter"]
    assert {"range": {"launch_year": {"gte": 2000}}} in q["query"]["bool"]["filter"]


def test_es_empty_text_is_match_all():
    es = _FakeES([])
    asyncio.run(ElasticsearchMissionSearchAdapter(client=es).search("  ", MissionFilter()))
    assert es.last_search["query"]["bool"]["must"] == [{"match_all": {}}]


def test_es_find_similar_excludes_self():
    src, other = _m("A"), _m("B")
    es = _FakeES([_hit(other)])
    res = asyncio.run(ElasticsearchMissionSearchAdapter(client=es).find_similar(src, 3))
    assert res == [other]
    bool_q = es.last_search["query"]["bool"]
    assert bool_q["must_not"] == [{"ids": {"values": [src.id]}}]
    assert bool_q["should"][0]["more_like_this"]["like"][0]["_id"] == src.id


def test_es_init_index_creates_mapping():
    es = _FakeES([])
    asyncio.run(ElasticsearchMissionSearchAdapter(client=es, index="missions").init_index())
    index, mappings, _ = es.indices.created
    assert index == "missions" and mappings["properties"]["description"]["type"] == "text"


# ---------- Redis ----------
class _FakeRedis:
    def __init__(self):
        self.store, self.ttl = {}, {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key], self.ttl[key] = value, ex


def test_redis_cache_roundtrip_with_ttl():
    r = _FakeRedis()
    cache = RedisRecommendationCacheAdapter(client=r, ttl_seconds=60)
    items = [RecommendationDTO(mission_id="1", name="Луна", similarity_reason="те саме тіло: moon")]

    async def scenario():
        assert await cache.get("x") is None
        await cache.put("x", items)
        return await cache.get("x")

    assert asyncio.run(scenario()) == items
    assert r.ttl["recs:x"] == 60
    assert "Луна" in r.store["recs:x"]  # ensure_ascii=False — кирилиця читабельна в redis-cli
