"""П. 13–15: нормалізація ключа, кеш HIT/MISS, останні запити, підсвічування,
ключ шардування, розподіл, EXPLAIN і map-reduce (на in-memory імітації кластера)."""
import asyncio
import uuid
from collections import Counter

import pytest
from fastapi.testclient import TestClient

from adapters.outbound.elasticsearch_adapter import _MAPPING, ElasticsearchMissionSearchAdapter
from adapters.outbound.in_memory_mission_repository import (
    InMemoryMissionSearch, InMemoryRecentQueries, InMemorySearchCache,
)
from adapters.outbound.redis_adapter import RedisRecentQueriesAdapter, RedisSearchCacheAdapter
from adapters.outbound.in_memory_mission_repository import InMemoryMissionRepository
from adapters.outbound.in_memory_shards import InMemoryShardCluster
from app import create_app
from application.mapreduce_service import STOP_WORDS
from application.mission_services import MissionSearchService
from application.search_normalization import cache_key, normalize_query
from domain.mission_dtos import MissionDTO, MissionFilter, RecentQueryDTO, SearchPageDTO


@pytest.fixture
def client():
    with TestClient(create_app(run_generator=False)) as c:
        yield c


def _m(name, body="mars", report="", status="ACTIVE", year=2000):
    return MissionDTO(id=str(uuid.uuid4()), name=name, type="ROVER", status=status, agency_id="NASA",
                      celestial_body_id=body, launch_year=year, report=report)


# ---------- Нормалізація ключа ----------
def test_normalization_ignores_case_punctuation_and_word_order():
    f = MissionFilter(body="Mars")
    assert cache_key("Rover, LANDING!", f) == cache_key("landing   rover", MissionFilter(body="mars"))
    assert normalize_query("b a", MissionFilter()).startswith("q=a b|")


def test_normalization_distinguishes_filters_and_pages():
    base = cache_key("rover", MissionFilter())
    assert base != cache_key("rover", MissionFilter(body="moon"))
    assert base != cache_key("rover", MissionFilter(offset=10))
    assert base.startswith("search:") and len(base) == len("search:") + 40


# ---------- Cache-aside ----------
class _CountingSearch(InMemoryMissionSearch):
    calls = 0

    async def search(self, text, flt):
        self.calls += 1
        return await super().search(text, flt)


class _BrokenCache(InMemorySearchCache):
    async def get(self, key):
        raise ConnectionError("redis down")

    async def put(self, key, page, ttl_seconds):
        raise ConnectionError("redis down")


def _service(cache=None):
    search = _CountingSearch()
    asyncio.run(search.index_many([_m("Curiosity", report="The rover landed in Gale Crater."),
                                   _m("Luna 9", "moon", report="First soft landing.")]))
    return MissionSearchService(search, cache or InMemorySearchCache(), InMemoryRecentQueries()), search


def test_miss_then_hit():
    svc, search = _service()
    first = asyncio.run(svc.search("landed", MissionFilter()))
    second = asyncio.run(svc.search("LANDED!", MissionFilter()))
    assert (first.cache, second.cache) == ("MISS", "HIT")
    assert search.calls == 1 and first.total == second.total == 1
    assert first.took_ms >= 0 and second.took_ms >= 0


def test_search_works_when_redis_is_down():
    svc, _ = _service(_BrokenCache())
    res = asyncio.run(svc.search("landed", MissionFilter()))
    assert res.cache == "MISS" and res.total == 1


def test_recent_queries_status_and_trim():
    svc, _ = _service()
    asyncio.run(svc.search("landed", MissionFilter(body="mars")))
    asyncio.run(svc.search("qwertyuiop", MissionFilter()))
    recent = asyncio.run(svc.recent())
    assert [(r.query, r.status) for r in recent] == [("qwertyuiop", "Пусто"), ("landed", "Знайдено")]
    assert recent[1].filters == {"body": "mars"}

    store = InMemoryRecentQueries()
    for i in range(25):
        asyncio.run(store.push(RecentQueryDTO(query=str(i), total=0, status="Пусто", cache="MISS", at=""), 20))
    items = asyncio.run(store.list(50))
    assert len(items) == 20 and items[0].query == "24"


def test_in_memory_highlight_is_html_escaped():
    search = InMemoryMissionSearch()
    asyncio.run(search.index_many([_m("X", report="<b>rover</b> landed")]))
    hit = asyncio.run(search.search("landed", MissionFilter())).items[0]
    assert hit.highlights["report"] == ["&lt;b&gt;rover&lt;/b&gt; <mark>landed</mark>"]


# ---------- REST ----------
def test_search_endpoint_hit_miss_and_recent(client):
    r1 = client.get("/api/search", params={"q": "landed"}).json()
    r2 = client.get("/api/search", params={"q": "Landed"}).json()
    assert r1["cache"] == "MISS" and r2["cache"] == "HIT" and r1["total"] > 0
    assert any("<mark>" in f for h in r1["items"] for fr in h["highlights"].values() for f in fr)
    recent = client.get("/api/search/recent").json()
    assert recent[0]["cache"] == "HIT" and recent[0]["status"] == "Знайдено"


# ---------- Elasticsearch: маппінг і highlight ----------
class _FakeES:
    def __init__(self):
        self.last = None

    async def search(self, **kw):
        self.last = kw
        return {"hits": {"total": {"value": 0}, "hits": []}}


def test_es_mapping_uses_english_analyzer():
    assert _MAPPING["properties"]["report"] == {"type": "text", "analyzer": "english"}
    assert _MAPPING["properties"]["celestial_body_id"]["type"] == "keyword"


def test_es_search_requests_html_encoded_highlight():
    es = _FakeES()
    asyncio.run(ElasticsearchMissionSearchAdapter(client=es).search("landing", MissionFilter()))
    hl = es.last["highlight"]
    assert hl["encoder"] == "html" and hl["pre_tags"] == ["<mark>"] and "report" in hl["fields"]
    assert "report" in es.last["query"]["bool"]["must"][0]["multi_match"]["fields"]


# ---------- Redis-адаптери (фейковий клієнт) ----------
class _FakePipe:
    def __init__(self, r):
        self.r, self.ops = r, []

    def lpush(self, k, v):
        self.ops.append(("lpush", k, v))

    def ltrim(self, k, a, b):
        self.ops.append(("ltrim", k, a, b))

    async def execute(self):
        for op in self.ops:
            if op[0] == "lpush":
                self.r.lists.setdefault(op[1], []).insert(0, op[2])
            else:
                self.r.lists[op[1]] = self.r.lists[op[1]][op[2]:op[3] + 1]


class _FakeRedis:
    def __init__(self):
        self.kv, self.ttl, self.lists = {}, {}, {}

    async def get(self, k):
        return self.kv.get(k)

    async def set(self, k, v, ex=None):
        self.kv[k], self.ttl[k] = v, ex

    def pipeline(self):
        return _FakePipe(self)

    async def lrange(self, k, a, b):
        return self.lists.get(k, [])[a:b + 1]


def test_redis_search_cache_and_recent_list():
    r = _FakeRedis()
    cache, recent = RedisSearchCacheAdapter(r), RedisRecentQueriesAdapter(r)
    page = SearchPageDTO(total=0, items=[])

    async def scenario():
        await cache.put("search:abc", page, 300)
        got = await cache.get("search:abc")
        for i in range(3):
            await recent.push(RecentQueryDTO(query=str(i), total=0, status="Пусто", cache="MISS", at=""), keep=2)
        return got, await recent.list()

    got, items = asyncio.run(scenario())
    assert got == page and r.ttl["search:abc"] == 300
    assert [i.query for i in items] == ["2", "1"]


# ---------- Ключ шардування (id, хеш) ----------
def test_id_distribution_is_even_across_nodes():
    repo = InMemoryMissionRepository()
    asyncio.run(repo.save_many([_m(f"M{i}", ["mars", "moon", "venus"][i % 3]) for i in range(3000)]))
    d = asyncio.run(InMemoryShardCluster(repo).distribution())
    assert d.distribution_column == "id"
    shares = [n.rows / d.total_rows for n in d.nodes]
    assert all(0.28 < s < 0.39 for s in shares), shares  # ~1/3 на кожен вузол


# ---------- Шардінг і map-reduce (імітація кластера) ----------
def test_distribution_endpoint(client):
    d = client.get("/api/sharding/distribution").json()
    assert d["distribution_column"] == "id" and len(d["shards"]) == 12
    assert len(d["nodes"]) == 3 and d["total_rows"] == 11
    assert sum(n["rows"] for n in d["nodes"]) == 11


def test_explain_endpoint(client):
    plans = client.get("/api/sharding/explain", params={"body": "mars"}).json()
    by_label = {p["label"]: p["task_count"] for p in plans}
    assert by_label["Запит без фільтра"] == 12
    assert by_label["Запит за тілом (mars)"] == 12
    assert by_label["Точковий запит за id (ключ розподілу)"] == 1
    assert client.get("/api/sharding/explain", params={"body": "x'; drop"}).status_code == 422


def test_mapreduce_decades_matches_direct_aggregation(client):
    for i in range(30):
        client.post("/api/missions", json={"name": f"T{i}", "type": "ROVER", "agency_id": "ESA",
                                           "celestial_body_id": ["mars", "moon", "venus"][i % 3],
                                           "status": ["COMPLETED", "FAILED", "ACTIVE"][i % 3],
                                           "launch_year": 1960 + i * 2})
    res = client.get("/api/mapreduce/decades").json()
    assert res["match"] is True and len(res["per_node"]) == 3
    assert sum(n["rows"] for n in res["per_node"]) == 41
    assert sum(d["missions"] for d in res["result"]) == 41
    sixties = next(d for d in res["result"] if d["decade"] == 1960)
    assert sixties["success_rate"] == round(sixties["completed"] / (sixties["completed"] + sixties["failed"]), 4)
    assert all(n["map_ms"] >= 0 for n in res["per_node"]) and res["reduce_ms"] >= 0


def test_mapreduce_terms_top20_without_stop_words(client):
    res = client.get("/api/mapreduce/terms").json()
    assert res["match"] is True
    mars_terms = dict(res["result"]["mars"])
    assert mars_terms["rover"] >= 4  # "rover" є у звітах усіх марсоходів
    assert not STOP_WORDS & set(mars_terms) and len(res["result"]["mars"]) <= 20


def test_mapreduce_unknown_job(client):
    assert client.get("/api/mapreduce/nope").status_code == 404
