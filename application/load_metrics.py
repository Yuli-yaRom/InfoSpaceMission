"""Вимірювання швидкості запису: часовий ряд кожні INTERVAL секунд.

Швидкість береться з лічильників генератора (рядки, підтверджені БД), а не з різниці count(*):
так замір не навантажує кластер і не залежить від того, чи доступні всі вузли.
Розмір БД (citus_total_relation_size — сума по всіх шардах) оновлюється рідше, раз на SIZE_EVERY с.
"""
import asyncio
import csv
import io
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional

from application.consistent_count import consistent_count
from application.mission_ports import GeneratorControlPort, MetricsUseCase, MissionRepositoryPort
from application.write_ledger import WriteLedger
from domain.mission_dtos import MetricSampleDTO, StatsDTO

log = logging.getLogger(__name__)


class MetricsRecorder(MetricsUseCase):
    INTERVAL = 2.0
    SIZE_EVERY = 15.0
    MAX_SAMPLES = 30_000   # ~16 год при інтервалі 2 с

    def __init__(self, repo: MissionRepositoryPort, generator: GeneratorControlPort, ledger: WriteLedger,
                 interval: Optional[float] = None):
        self._repo = repo
        self._gen = generator
        self._ledger = ledger
        self.interval = interval or self.INTERVAL
        self._t0 = time.monotonic()
        self._samples: Deque[MetricSampleDTO] = deque(maxlen=self.MAX_SAMPLES)
        self._prev: Optional[Dict[str, float]] = None
        self._prev_t: Optional[float] = None
        self.db_size: Optional[int] = None
        self._size_at = 0.0
        self._recount_lock = asyncio.Lock()
        self.counting = False

    def now_t(self) -> float:
        return round(time.monotonic() - self._t0, 3)

    # ---------- Фонова задача ----------
    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            try:
                await self.sample_once()
            except Exception:  # noqa: BLE001 — замір не має падати разом із БД
                log.exception("Не вдалося зняти метрики")

    async def refresh_size(self, force: bool = False) -> Optional[int]:
        now = time.monotonic()
        if force or now - self._size_at >= self.SIZE_EVERY:
            self._size_at = now
            try:
                self.db_size = await asyncio.wait_for(self._repo.storage_bytes(), timeout=15)
            except Exception as e:  # noqa: BLE001 — напр. вузол недоступний: лишаємо останнє значення
                log.info("Розмір БД недоступний: %s", e)
        return self.db_size

    async def sample_once(self) -> MetricSampleDTO:
        now = time.monotonic()
        cur = self._gen.counters()
        await self.refresh_size()
        if self._prev is None:
            dt, prev = None, None
        else:
            dt, prev = now - self._prev_t, self._prev

        def rate(key: str) -> float:
            return round((cur[key] - prev[key]) / dt, 1) if dt else 0.0

        def avg(sum_key: str, n_key: str) -> Optional[float]:
            if not prev:
                return None
            n = cur[n_key] - prev[n_key]
            return round((cur[sum_key] - prev[sum_key]) / n, 1) if n else None

        s = self._gen.settings()
        sample = MetricSampleDTO(
            t=round(now - self._t0, 2), at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            insert_rate=rate("inserted"), update_rate=rate("updated"), delete_rate=rate("deleted"),
            sql_batch_ms=avg("sql_ms_sum", "sql_batches"), es_batch_ms=avg("es_ms_sum", "es_batches"),
            pending=int(cur["pending"]), total_rows=self._ledger.total(), db_size_bytes=self.db_size,
            target_rate=None if s.max_speed else float(s.docs_per_second), state=self._gen.status().state)
        self._prev, self._prev_t = cur, now
        if dt is not None:   # перша точка — лише база для різниць
            self._samples.append(sample)
        return sample

    # ---------- MetricsUseCase ----------
    async def stats(self) -> StatsDTO:
        last = self._samples[-1] if self._samples else None
        return StatsDTO(total_documents=self._ledger.total(), counted_at=self._ledger.counted_at,
                        counting=self.counting,
                        insert_rate=last.insert_rate if last else 0.0, pending=self._gen.status().pending,
                        db_size_bytes=self.db_size, generator_state=self._gen.status().state)

    async def recount(self) -> StatsDTO:
        """Точний count(*) на момент зрізу; запис під час підрахунку не зупиняється
        (див. application/consistent_count.py)."""
        async with self._recount_lock:
            self.counting = True
            try:
                c = await consistent_count(self._repo, self._gen, self._ledger)
                self._ledger.rebase(c.rows, inserted=c.inserted, deleted=c.deleted)
            finally:
                self.counting = False
        await self.refresh_size(force=True)
        return await self.stats()

    def samples(self, since_t: Optional[float] = None) -> List[MetricSampleDTO]:
        if since_t is None:
            return list(self._samples)
        return [s for s in self._samples if s.t > since_t]

    def to_csv(self) -> str:
        buf = io.StringIO()
        fields = list(MetricSampleDTO.model_fields)
        w = csv.DictWriter(buf, fieldnames=fields)
        w.writeheader()
        for s in self._samples:
            w.writerow(s.model_dump())
        return buf.getvalue()
