"""Відмовостійкість: моніторинг вузлів, журнал подій і перевірка «нічого не загубилось».

Сценарій демонстрації (docs/load_testing.md, розділ 5):
1. «Зафіксувати стан» — точний count(*) на момент зрізу + лічильники записів у той самий момент
   (запис зупиняється лише на мить, див. application/consistent_count.py);
2. `docker compose stop citus-worker-2` — запис не вдається, пачки накопичуються в буфері генератора
   і повторюються (журнал подій це показує), дані на вимкненому вузлі лежать на його томі;
3. `docker compose start citus-worker-2` — буфер дописується;
4. «Перевірити» — очікувано = знімок + записано після знімка − видалено після знімка;
   фактично = count(*). Різниця (lost_rows) має бути 0.
"""
import asyncio
import logging
from collections import deque
from typing import Deque, Dict, List, Optional

from application.mission_ports import (
    ClusterHealthPort, GeneratorControlPort, MissionRepositoryPort, ResilienceUseCase,
    ShardingInfoPort,
)
from application.consistent_count import consistent_count
from application.write_ledger import WriteLedger, now_iso
from domain.mission_dtos import (
    ClusterEventDTO, NodeHealthDTO, ResilienceCheckDTO, ResilienceSnapshotDTO,
)

log = logging.getLogger(__name__)


class ResilienceService(ResilienceUseCase):
    INTERVAL = 2.0

    def __init__(self, repo: MissionRepositoryPort, ledger: WriteLedger, generator: GeneratorControlPort,
                 health: Optional[ClusterHealthPort], sharding: Optional[ShardingInfoPort]):
        self._repo = repo
        self._ledger = ledger
        self._gen = generator
        self._health = health
        self._sharding = sharding
        self._events: Deque[ClusterEventDTO] = deque(maxlen=200)
        self._last: List[NodeHealthDTO] = []
        self._since: Dict[str, str] = {}
        self._snap: Optional[ResilienceSnapshotDTO] = None

    def log_event(self, level: str, message: str) -> None:
        self._events.append(ClusterEventDTO(at=now_iso(), level=level, message=message))
        (log.warning if level == "warn" else log.info)("Подія: %s", message)

    # ---------- Моніторинг ----------
    async def run(self) -> None:
        while True:
            try:
                await self.poll()
            except Exception:  # noqa: BLE001
                log.exception("Моніторинг вузлів")
            await asyncio.sleep(self.INTERVAL)

    async def poll(self) -> List[NodeHealthDTO]:
        if self._health is None:
            return []
        nodes = await self._health.health()
        prev = {n.node: n.up for n in self._last}
        for n in nodes:
            was = prev.get(n.node)
            if was is None:
                self._since[n.node] = now_iso()
            elif was != n.up:
                self._since[n.node] = now_iso()
                if n.up:
                    self.log_event("ok", f"Вузол {n.node} знову доступний")
                else:
                    self.log_event("warn", f"Вузол {n.node} недоступний ({n.error or 'немає відповіді'}); "
                                           f"його шардів: {n.shards}")
            n.since = self._since.get(n.node)
        self._last = nodes
        return nodes

    # ---------- ResilienceUseCase ----------
    async def health(self) -> List[NodeHealthDTO]:
        return self._last or await self.poll()

    def events(self, limit: int = 50) -> List[ClusterEventDTO]:
        return list(self._events)[-limit:][::-1]

    async def _per_node(self) -> Dict[str, int]:
        if self._sharding is None:
            return {}
        try:
            d = await asyncio.wait_for(self._sharding.distribution(), timeout=60)
            return {n.node: n.rows for n in d.nodes}
        except Exception:  # noqa: BLE001 — напр. один із вузлів ще недоступний
            return {}

    async def snapshot(self) -> ResilienceSnapshotDTO:
        # Запис зупиняється лише на мить зрізу; сам count(*) іде паралельно із записом
        c = await consistent_count(self._repo, self._gen, self._ledger)
        snap = ResilienceSnapshotDTO(
            at=now_iso(), rows=c.rows, inserted_total=c.inserted, deleted_total=c.deleted,
            generated_total=int(c.counters["generated"]), pending=int(c.counters["pending"]))
        self._ledger.rebase(c.rows, inserted=c.inserted, deleted=c.deleted)
        snap.per_node = await self._per_node()
        self._snap = snap
        self.log_event("info", f"Знімок стану: {c.rows:,} рядків".replace(",", " "))
        return snap

    async def check(self) -> ResilienceCheckDTO:
        if self._snap is None:
            raise LookupError("Спершу зафіксуйте стан (знімок)")
        snap = self._snap
        c = await consistent_count(self._repo, self._gen, self._ledger)
        inserted = c.inserted - snap.inserted_total
        deleted = c.deleted - snap.deleted_total
        expected = snap.rows + inserted - deleted
        res = ResilienceCheckDTO(
            snapshot=snap, checked_at=now_iso(), inserted_since=inserted, deleted_since=deleted,
            expected_rows=expected, actual_rows=c.rows, lost_rows=expected - c.rows,
            generated_since=int(c.counters["generated"]) - snap.generated_total,
            pending=int(c.counters["pending"]), per_node_after=await self._per_node(), ok=expected == c.rows)
        self.log_event("ok" if res.ok else "warn",
                       f"Перевірка: очікувано {expected:,}, у БД {c.rows:,}, втрачено {expected - c.rows}"
                       .replace(",", " "))
        return res
