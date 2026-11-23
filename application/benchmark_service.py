"""Заміри швидкості запису з фіксованою тривалістю.

Замір: перемикає генератор у заданий режим (зазвичай «максимум»), чекає duration_s,
рахує підсумок і повертає попередні налаштування. Результат зберігається у спільний файл
(logs/benchmarks.jsonl) разом із назвою топології (1 вузол / 3 воркери), тож прогони на різних
конфігураціях кластера порівнюються в одній таблиці й на одному графіку.

Відповіді на питання методички беруться з підсумку:
* «чи зменшується швидкість з часом» — швидкість у першому (після розгону) й останньому вікні
  (change_pct) і графік;
* «1 вузол чи 3+» — середня швидкість прогонів із різною топологією;
* «після паузи» — окремий прогін одразу після зупинки, порівняння з кінцем попереднього.
"""
import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from statistics import mean
from typing import List, Optional

from application.load_metrics import MetricsRecorder
from application.mission_ports import (
    BenchmarkStorePort, BenchmarkUseCase, ClusterHealthPort, GeneratorControlPort,
)
from application.write_ledger import WriteLedger
from domain.mission_dtos import (
    BenchmarkRunDTO, BenchmarkStartRequest, GeneratorSettingsPatch, MetricSampleDTO,
)

log = logging.getLogger(__name__)
MAX_SERIES_POINTS = 400


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    v = sorted(values)
    k = min(len(v) - 1, max(0, round(p / 100 * (len(v) - 1))))
    return round(v[k], 1)


def windows(series: List[List[float]], elapsed: float) -> dict:
    """Швидкість у першому й останньому вікні заміру.

    Перше вікно починається ПІСЛЯ розгону (warmup): у перші секунди генератор ще перемикається
    з попередньої швидкості на максимум, а перша точка метрик частково захоплює час до старту.
    Без цього перше вікно виглядало б повільнішим, ніж є, і «зміна %» приховувала б реальне падіння
    швидкості з часом — саме те, про що питає методичка.
    """
    warmup = min(10.0, elapsed * 0.1)
    window = min(60.0, max(10.0, (elapsed - warmup) / 4))

    def avg(lo: float, hi: float) -> Optional[float]:
        vals = [r for t, r in series if lo <= t <= hi]
        return round(mean(vals), 1) if vals else None

    first = avg(warmup, warmup + window)
    last = avg(max(warmup, elapsed - window), elapsed)
    change = round(100 * (last - first) / first, 1) if first and last is not None else None
    return {"first": first, "last": last, "change": change, "window": round(window, 1), "warmup": round(warmup, 1)}


def _thin(points: List[List[float]], limit: int = MAX_SERIES_POINTS) -> List[List[float]]:
    if len(points) <= limit:
        return points
    step = len(points) / limit
    return [points[int(i * step)] for i in range(limit)]


class BenchmarkService(BenchmarkUseCase):
    def __init__(self, generator: GeneratorControlPort, recorder: MetricsRecorder, ledger: WriteLedger,
                 store: BenchmarkStorePort, health: Optional[ClusterHealthPort], topology: str):
        self._gen = generator
        self._rec = recorder
        self._ledger = ledger
        self._store = store
        self._health = health
        self._topology = topology
        self._run: Optional[BenchmarkRunDTO] = None
        self._task: Optional[asyncio.Task] = None
        self._prev_settings = None
        self._t0 = 0.0            # recorder.now_t() на старті
        self._mono0 = 0.0
        self._c0: dict = {}

    async def _node_count(self) -> int:
        if self._health is None:
            return 1
        try:
            nodes = await self._health.health()
            return max(1, sum(1 for n in nodes if n.shards > 0))
        except Exception:  # noqa: BLE001
            return 0

    async def start(self, req: BenchmarkStartRequest) -> BenchmarkRunDTO:
        if self.is_running():
            raise RuntimeError("Замір уже триває")
        self._prev_settings = self._gen.settings()
        settings = self._gen.update(GeneratorSettingsPatch(max_speed=req.max_speed,
                                                           docs_per_second=req.docs_per_second))
        self._t0 = self._rec.now_t()
        self._mono0 = time.monotonic()
        self._c0 = self._gen.counters()
        size = await self._rec.refresh_size(force=True)
        self._run = BenchmarkRunDTO(
            id=uuid.uuid4().hex[:12], label=req.label or ("максимум" if req.max_speed else f"{req.docs_per_second} док/с"),
            topology=self._topology, nodes=await self._node_count(),
            started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"), duration_s=0,
            planned_s=req.duration_s, status="running", settings=settings,
            rows_start=self._ledger.total(), size_start=size)
        self._task = asyncio.create_task(self._finish_after(req.duration_s))
        log.info("Замір %s: %s, %d с", self._run.id, self._run.label, req.duration_s)
        return self._run

    async def _finish_after(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
        await self._finish("done")

    async def _finish(self, status: str) -> BenchmarkRunDTO:
        run = self._summary(status)
        run.size_end = await self._rec.refresh_size(force=True)
        if self._prev_settings is not None:
            self._gen.update(GeneratorSettingsPatch(**self._prev_settings.model_dump()))
        self._run = run
        await self._store.save(run)
        log.info("Замір %s завершено: %.0f рядків/с у середньому", run.id, run.avg_rate)
        return run

    def _summary(self, status: str) -> BenchmarkRunDTO:
        assert self._run is not None
        elapsed = max(0.001, time.monotonic() - self._mono0)
        c = self._gen.counters()
        inserted = int(c["inserted"] - self._c0["inserted"])
        samples: List[MetricSampleDTO] = self._rec.samples(since_t=self._t0)
        series = [[round(s.t - self._t0, 1), s.insert_rate] for s in samples]
        w = windows(series, elapsed)
        lat = self._gen.batch_latencies_since(self._mono0)
        return self._run.model_copy(update=dict(
            status=status, duration_s=round(elapsed, 1), inserted=inserted,
            avg_rate=round(inserted / elapsed, 1), first_min_rate=w["first"], last_min_rate=w["last"],
            change_pct=w["change"], window_s=w["window"], warmup_s=w["warmup"],
            peak_rate=max((r for _, r in series), default=0.0),
            p50_batch_ms=_percentile(lat, 50), p95_batch_ms=_percentile(lat, 95),
            rows_end=self._ledger.total(), size_end=self._rec.db_size,
            failed_attempts=int(c["failed_attempts"] - self._c0["failed_attempts"]),
            series=_thin(series)))

    async def stop(self) -> Optional[BenchmarkRunDTO]:
        if self._run is None or self._run.status != "running":
            return None
        if self._task is not None:
            self._task.cancel()
        return await self._finish("stopped")

    def is_running(self) -> bool:
        """Поки триває замір, налаштування генератора заблоковані (див. GeneratorControlRestController):
        інакше збережений результат показував би одні налаштування, а виміряно було б з іншими."""
        return self._run is not None and self._run.status == "running"

    def current(self) -> Optional[BenchmarkRunDTO]:
        if self._run is None:
            return None
        return self._summary("running") if self._run.status == "running" else self._run

    async def history(self) -> List[BenchmarkRunDTO]:
        return await self._store.list()

    async def delete(self, run_id: str) -> bool:
        return await self._store.delete(run_id)
