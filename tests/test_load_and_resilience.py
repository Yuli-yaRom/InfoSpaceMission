"""Навантаження, видалення, відмовостійкість — без Docker (адаптери в пам'яті, SQLite)."""
import asyncio
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.outbound.benchmark_store import JsonlBenchmarkStore
from adapters.outbound.fake_generator import FakeGeneratorAdapter
from adapters.outbound.in_memory_mission_repository import (
    InMemoryMissionRepository, InMemoryMissionSearch, InMemoryRecommendationCache, InMemorySearchCache,
)
from adapters.outbound.sql_mission_repository import SqlMissionRepository
from app import create_app
from application.benchmark_service import BenchmarkService
from application.data_admin_service import DataAdminService
from application.load_metrics import MetricsRecorder
from application.mission_services import MissionCommandService
from application.resilience_service import ResilienceService
from application.write_ledger import WriteLedger
from domain.mission_dtos import (
    BenchmarkRunDTO, BenchmarkStartRequest, GeneratorSettingsDTO, GeneratorSettingsPatch, MissionDTO,
    MissionFilter, SearchPageDTO,
)


def _m(name="X", body="mars", year=2000):
    return MissionDTO(id=str(uuid.uuid4()), name=name, type="ROVER", status="ACTIVE", agency_id="NASA",
                      celestial_body_id=body, launch_year=year, report="The rover landed.")


class FlakyRepo(InMemoryMissionRepository):
    """Імітація відмови вузла: поки down=True, запис падає. ack_lost — запис проходить,
    але відповідь «губиться» (виняток після коміту), як при обриві з'єднання."""

    def __init__(self):
        super().__init__()
        self.down = False
        self.ack_lost_once = False
        self.calls = 0

    async def insert_bulk(self, missions, idempotent=False):
        self.calls += 1
        if self.down:
            raise ConnectionError("connection to the remote node citus-worker-2:5432 failed")
        await super().insert_bulk(missions, idempotent)
        if self.ack_lost_once:
            self.ack_lost_once = False
            raise ConnectionError("server closed the connection unexpectedly")


def _generator(repo, **settings):
    ledger = WriteLedger()
    cmd = MissionCommandService(repo, InMemoryMissionSearch(), ledger)
    gen = FakeGeneratorAdapter(cmd, GeneratorSettingsDTO(**settings), buffer_limit=5_000, retry_max_s=0.05)
    gen.TICK = 0.02
    return gen, ledger


async def _run_for(gen, seconds):
    task = asyncio.create_task(gen.run())
    await asyncio.sleep(seconds)
    return task


# ---------- Генератор: відмова вузла не губить дані ----------
def test_outage_buffers_and_retries_without_loss():
    async def scenario():
        repo = FlakyRepo()
        gen, ledger = _generator(repo, docs_per_second=5000, batch_size=200, writers=3, index_search=False)
        events = []
        gen.on_event = lambda level, msg: events.append(level)
        task = await _run_for(gen, 0.3)
        repo.down = True
        await asyncio.sleep(0.4)
        during = gen.status()
        assert during.state == "retrying" and during.pending > 0 and during.failed_attempts > 0
        repo.down = False
        gen.update(GeneratorSettingsPatch(docs_per_second=0))   # зупиняємо джерело, чекаємо спорожнення буфера
        for _ in range(100):
            await asyncio.sleep(0.05)
            if gen.pending == 0:
                break
        task.cancel()
        st = gen.status()
        assert st.pending == 0 and st.state == "paused"
        assert st.generated == st.inserted == await repo.count() == ledger.total()
        assert events[0] == "warn" and "ok" in events

    asyncio.run(scenario())


def test_buffer_is_bounded_backpressure():
    async def scenario():
        repo = FlakyRepo()
        repo.down = True
        gen, _ = _generator(repo, docs_per_second=50_000, batch_size=500, writers=2, index_search=False)
        task = await _run_for(gen, 0.5)
        assert gen.pending <= gen.buffer_limit and gen.generated == gen.pending
        task.cancel()

    asyncio.run(scenario())


def test_retry_after_lost_ack_is_idempotent():
    async def scenario():
        repo = FlakyRepo()
        repo.ack_lost_once = True
        gen, _ = _generator(repo, max_speed=False, docs_per_second=1000, batch_size=100, writers=1,
                            index_search=False)
        task = await _run_for(gen, 0.3)
        gen.update(GeneratorSettingsPatch(docs_per_second=0))
        await asyncio.sleep(0.3)
        task.cancel()
        assert gen.failed_attempts == 1
        assert await repo.count() == gen.inserted == gen.generated   # без дублікатів і без втрат

    asyncio.run(scenario())


def test_max_speed_and_modifications():
    async def scenario():
        repo = FlakyRepo()
        gen, ledger = _generator(repo, max_speed=True, batch_size=500, writers=2,
                                 updates_per_second=500, deletes_per_second=200, index_search=True)
        gen.MOD_TICK = 0.05
        task = await _run_for(gen, 0.6)
        gen.update(GeneratorSettingsPatch(max_speed=False, docs_per_second=0, updates_per_second=0,
                                          deletes_per_second=0))
        await asyncio.sleep(0.2)
        task.cancel()
        st = gen.status()
        assert st.inserted > 1000 and st.updated > 0 and st.deleted > 0
        assert await repo.count() == st.inserted - st.deleted == ledger.total()

    asyncio.run(scenario())


def test_switching_from_max_speed_back_to_rate():
    """Регресія: у режимі «максимум» із дуже швидкою БД продюсер не помічав зміну налаштувань."""
    async def scenario():
        repo = InMemoryMissionRepository()
        gen, _ = _generator(repo, max_speed=True, batch_size=200, writers=2, index_search=False)
        task = await _run_for(gen, 0.3)
        gen.update(GeneratorSettingsPatch(max_speed=False, docs_per_second=0))
        await asyncio.sleep(0.2)
        before = gen.generated
        await asyncio.sleep(0.3)
        task.cancel()
        assert gen.generated == before and gen.status().state == "paused"

    asyncio.run(scenario())


# ---------- Метрики і заміри ----------
def test_metrics_and_benchmark_summary(tmp_path):
    async def scenario():
        repo = InMemoryMissionRepository()
        gen, ledger = _generator(repo, docs_per_second=0, batch_size=200, index_search=False)
        rec = MetricsRecorder(repo, gen, ledger, interval=0.1)
        store = JsonlBenchmarkStore(str(tmp_path / "b.jsonl"))
        bench = BenchmarkService(gen, rec, ledger, store, None, "test")
        gen_task = asyncio.create_task(gen.run())
        rec_task = asyncio.create_task(rec.run())
        run = await bench.start(BenchmarkStartRequest.model_construct(
            label="t", duration_s=1, max_speed=True, docs_per_second=1))
        assert run.status == "running" and gen.settings().max_speed
        await asyncio.sleep(1.4)
        gen_task.cancel()
        rec_task.cancel()
        done = (await bench.history())[0]
        assert done.status == "done" and done.inserted > 0 and done.avg_rate > 0
        assert done.series and done.p95_batch_ms is not None
        assert not gen.settings().max_speed            # попередні налаштування повернуто
        assert (await rec.stats()).total_documents == ledger.total() == await repo.count()
        assert "insert_rate" in rec.to_csv().splitlines()[0]

    asyncio.run(scenario())


def test_jsonl_store_roundtrip(tmp_path):
    async def scenario():
        store = JsonlBenchmarkStore(str(tmp_path / "x" / "b.jsonl"))
        run = BenchmarkRunDTO(id="a1", label="l", topology="1 вузол", nodes=1, started_at="2026-01-01T00:00:00",
                              duration_s=1, planned_s=1, status="done", settings=GeneratorSettingsDTO())
        await store.save(run)
        await store.save(run.model_copy(update={"id": "b2", "started_at": "2026-01-02T00:00:00"}))
        assert [r.id for r in await store.list()] == ["b2", "a1"]
        assert await store.delete("a1") and not await store.delete("a1")
        assert [r.id for r in await JsonlBenchmarkStore(str(tmp_path / "x" / "b.jsonl")).list()] == ["b2"]

    asyncio.run(scenario())


# ---------- Видалення ----------
def test_data_admin_partial_and_full_delete():
    async def scenario():
        repo, search = InMemoryMissionRepository(), InMemoryMissionSearch()
        cache, recs = InMemorySearchCache(), InMemoryRecommendationCache()
        ledger = WriteLedger()
        ms = [_m(body=b) for b in ["mars"] * 3 + ["moon"] * 2]
        await repo.save_many(ms)
        await search.index_many(ms)
        ledger.rebase(5)
        await cache.put("search:k", SearchPageDTO(total=0, items=[]), 300)
        admin = DataAdminService(repo, search, cache, recs, ledger)

        with pytest.raises(ValueError):
            await admin.delete_by_filter(MissionFilter())       # без фільтра — не можна
        assert await admin.preview(MissionFilter(body="MARS")) == 3
        res = await admin.delete_by_filter(MissionFilter(body="mars"))
        assert res.deleted_rows == 3 and res.caches_cleared == 1
        assert await repo.count() == 2 == await search.count() == ledger.total()

        res = await admin.delete_all()
        assert res.deleted_rows == 2 and await repo.count() == 0 == await search.count() == ledger.total()

    asyncio.run(scenario())


def test_sql_bulk_insert_update_delete(tmp_path):
    async def scenario():
        repo = SqlMissionRepository(engine=create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db"))
        await repo.init_schema()
        ms = [_m(body="mars", year=2001), _m(body="moon", year=1969), _m(body="mars", year=2020)]
        await repo.insert_bulk(ms)
        await repo.insert_bulk(ms + [_m()], idempotent=True)        # повтор пачки: без дублікатів
        assert await repo.count() == 4
        assert await repo.update_status([ms[0].id, "missing"], "FAILED") == 1
        assert (await repo.find_by_id(ms[0].id)).status == "FAILED"
        assert await repo.update_status_by_filter(MissionFilter(body="moon"), "COMPLETED") == 1
        assert await repo.delete_ids([ms[1].id]) == 1
        assert await repo.delete_by_filter(MissionFilter(body="mars", year_from=2010)) == 1
        assert await repo.count() == 2
        await repo.delete_all()
        assert await repo.count() == 0 and await repo.storage_bytes() is None
        await repo.close()

    asyncio.run(scenario())


# ---------- Відмовостійкість: знімок і перевірка ----------
def test_resilience_snapshot_and_check():
    async def scenario():
        repo = FlakyRepo()
        gen, ledger = _generator(repo, docs_per_second=3000, batch_size=100, writers=2, deletes_per_second=0,
                                 index_search=False)
        res = ResilienceService(repo, ledger, gen, None, None)
        task = await _run_for(gen, 0.2)
        snap = await res.snapshot()
        repo.down = True
        await asyncio.sleep(0.3)
        repo.down = False
        await asyncio.sleep(0.3)
        check = await res.check()
        task.cancel()
        assert check.ok and check.lost_rows == 0
        assert check.actual_rows == snap.rows + check.inserted_since
        assert check.generated_since == check.inserted_since + check.pending - snap.pending

    asyncio.run(scenario())


# ---------- REST ----------
@pytest.fixture
def client():
    with TestClient(create_app(run_generator=False)) as c:
        yield c


def test_rest_delete_endpoints(client):
    total = client.get("/api/stats").json()["total_documents"]
    mars = client.get("/api/data/preview", params={"body": "mars"}).json()["rows"]
    assert mars > 0
    assert client.delete("/api/data").status_code == 422                # потрібен фільтр
    r = client.delete("/api/data", params={"body": "mars"}).json()
    assert r["deleted_rows"] == mars
    assert client.get("/api/stats").json()["total_documents"] == total - mars
    one = client.get("/api/missions").json()["items"][0]["id"]
    assert client.delete(f"/api/missions/{one}").json()["deleted_rows"] == 1
    assert client.delete(f"/api/missions/{one}").status_code == 404
    assert client.delete("/api/data/all").status_code == 422             # без confirm=yes
    assert client.delete("/api/data/all", params={"confirm": "yes"}).status_code == 200
    assert client.get("/api/missions").json()["total"] == 0
    assert client.post("/api/data/vacuum").status_code == 200


def test_rest_resilience_and_metrics(client):
    nodes = client.get("/api/cluster/health").json()
    assert {n["role"] for n in nodes} == {"coordinator", "worker"} and all(n["up"] for n in nodes)
    assert client.post("/api/resilience/check").status_code == 409     # спершу знімок
    assert client.post("/api/resilience/snapshot").status_code == 200
    chk = client.post("/api/resilience/check").json()
    assert chk["ok"] and chk["lost_rows"] == 0
    assert client.get("/api/cluster/events").json()[0]["message"].startswith("Перевірка")
    assert client.post("/api/stats/recount").json()["total_documents"] == 11
    assert client.get("/api/metrics").status_code == 200
    assert client.get("/api/metrics.csv").text.startswith("t,at,insert_rate")
    assert client.get("/api/benchmarks").json() == []


# ---------- Виправлення після рев'ю ----------
def test_vacuum_has_no_120s_limit():
    """timeout=None в asyncpg = command_timeout пулу (120 с) — VACUUM на великій таблиці скасувався б."""
    from adapters.outbound import sql_mission_repository as sqlrepo

    class _Conn:
        async def execute(self, sql, timeout="unset"):
            self.sql, self.timeout = sql, timeout

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *a):
            return False

    class _Pool:
        def __init__(self):
            self.conn = _Conn()

        def acquire(self):
            return _Acquire(self.conn)

    repo = SqlMissionRepository("postgresql+asyncpg://u:p@localhost:1/db")   # без підключення
    repo._pg_pool = _Pool()
    asyncio.run(repo.vacuum(full=True))
    assert "FULL" in repo._pg_pool.conn.sql
    assert repo._pg_pool.conn.timeout == sqlrepo.VACUUM_TIMEOUT_S >= 3600


def test_benchmark_first_window_skips_warmup():
    from application.benchmark_service import windows
    # 100 с стабільно 10 000 р/с, але перші дві точки — розгін (1000, 5000)
    series = [[2, 1000], [4, 5000]] + [[t, 10000] for t in range(6, 101, 2)]
    w = windows(series, 100)
    assert w["warmup"] == 10 and w["first"] == 10000 and w["change"] == 0
    # реальне падіння видно: друга половина вдвічі повільніша
    series = [[t, 10000 if t <= 50 else 5000] for t in range(2, 101, 2)]
    assert windows(series, 100)["change"] == -50


def test_generator_settings_locked_during_benchmark(client):
    assert client.post("/api/benchmarks", json={"duration_s": 10, "max_speed": False,
                                                "docs_per_second": 100}).status_code == 201
    res = client.put("/api/generator", json={"index_search": False})
    assert res.status_code == 409 and "замір" in res.json()["detail"]
    assert client.post("/api/benchmarks/stop").json()["status"] == "stopped"
    assert client.put("/api/generator", json={"index_search": False}).status_code == 200


def test_quiesce_fails_loudly_instead_of_inexact_count():
    async def scenario():
        gen, _ = _generator(InMemoryMissionRepository())
        gen._active = 1                       # «зависла» операція запису
        with pytest.raises(TimeoutError):
            async with gen.quiesced(timeout=0.1):
                pass
        assert gen._gate.is_set()             # запис не лишився заблокованим

    asyncio.run(scenario())


def test_consistent_count_does_not_pause_writes_while_counting():
    """count(*) великої таблиці йде вже після паузи: запис відкритий, DELETE відкладено."""
    from application.consistent_count import consistent_count

    class SlowCountRepo(InMemoryMissionRepository):
        async def db_now(self):
            return "T"

        async def count_created_before(self, moment):
            assert moment == "T"
            self.gate_open_while_counting = gen._gate.is_set()
            self.deletes_held_while_counting = gen._hold_deletes > 0
            await asyncio.sleep(0.05)
            return 42

    async def scenario():
        nonlocal gen
        repo = SlowCountRepo()
        gen, ledger = _generator(repo)
        ledger.add_inserted(7)
        c = await consistent_count(repo, gen, ledger)
        assert c.rows == 42 and c.inserted == 7
        assert repo.gate_open_while_counting and repo.deletes_held_while_counting
        assert gen._hold_deletes == 0

    gen = None
    asyncio.run(scenario())


def test_deletes_are_held_during_count():
    async def scenario():
        repo = FlakyRepo()
        gen, _ = _generator(repo, docs_per_second=5000, batch_size=200, deletes_per_second=2000,
                            index_search=False)
        gen.MOD_TICK = 0.02
        task = await _run_for(gen, 0.3)
        async with gen.deletes_held():
            before, inserted_before = gen.deleted, gen.inserted
            await asyncio.sleep(0.3)
            assert gen.deleted == before and gen.inserted > inserted_before   # вставки йдуть, DELETE — ні
        await asyncio.sleep(0.2)
        task.cancel()
        assert gen.deleted > before

    asyncio.run(scenario())
