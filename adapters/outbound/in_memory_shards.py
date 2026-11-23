"""Імітація кластера Citus у пам'яті (для тестів і запуску без Docker).

Ті самі правила, що й у справжньому кластері: рядок іде в шард за хешем id,
12 шардів рівномірно розкладені по 3 вузлах. Map-фаза рахує те саме, що SQL у
CitusShardAdapter, але на Python.
"""
import re
import zlib
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from adapters.outbound.in_memory_mission_repository import InMemoryMissionRepository
from adapters.outbound.sql_mission_repository import DISTRIBUTION_COLUMN
from application.mission_ports import ClusterHealthPort, ShardingInfoPort, ShardMapPort
from domain.mission_dtos import (
    ExplainDTO, MissionDTO, NodeHealthDTO, NodeTotalDTO, ShardDistributionDTO, ShardInfoDTO,
)

SHARD_COUNT = 12
NODES = ["fake-worker-1", "fake-worker-2", "fake-worker-3"]
_FIRST_SHARD_ID = 102008  # як у Citus: shardid починаються з 102008
_TOKEN = re.compile(r"[^a-z]+")


def _shard_index(mission_id: str) -> int:
    return zlib.crc32(mission_id.encode()) % SHARD_COUNT


def _table(idx: int) -> str:
    return f"missions_{_FIRST_SHARD_ID + idx}"


def _node_of(idx: int) -> str:
    return NODES[idx % len(NODES)]


def map_decades(rows: Iterable[MissionDTO]) -> Dict[int, List[int]]:
    acc: Dict[int, List[int]] = defaultdict(lambda: [0, 0, 0])
    for m in rows:
        cur = acc[(m.launch_year // 10) * 10]
        cur[0] += 1
        cur[1] += m.status.upper() == "COMPLETED"
        cur[2] += m.status.upper() == "FAILED"
    return dict(acc)


def map_terms(rows: Iterable[MissionDTO]) -> Dict[str, Dict[str, int]]:
    acc: Dict[str, Counter] = defaultdict(Counter)
    for m in rows:
        acc[m.celestial_body_id.lower()].update(t for t in _TOKEN.split(m.report.lower()) if t)
    return {b: dict(c) for b, c in acc.items()}


_MAPPERS = {"decades": map_decades, "terms": map_terms}


class InMemoryShardCluster(ShardingInfoPort, ShardMapPort, ClusterHealthPort):
    def __init__(self, repo: InMemoryMissionRepository):
        self._repo = repo
        self.down: set = set()   # для тестів: «вимкнені» вузли

    async def health(self) -> List[NodeHealthDTO]:
        per_node = SHARD_COUNT // len(NODES)
        return [NodeHealthDTO(node="in-memory-coordinator", role="coordinator", up=True, latency_ms=0.0)] + [
            NodeHealthDTO(node=n, role="worker", up=n not in self.down, latency_ms=None if n in self.down else 0.0,
                          shards=per_node, error="імітація відмови" if n in self.down else None)
            for n in NODES]

    def _by_shard(self) -> Dict[int, List[MissionDTO]]:
        out: Dict[int, List[MissionDTO]] = {i: [] for i in range(SHARD_COUNT)}
        for m in self._repo.all():
            out[_shard_index(m.id)].append(m)
        return out

    # ---- ShardingInfoPort ----
    async def distribution(self) -> ShardDistributionDTO:
        by_shard = self._by_shard()
        shards = [ShardInfoDTO(shard_id=_FIRST_SHARD_ID + i, node=_node_of(i), rows=len(rows))
                  for i, rows in by_shard.items()]
        nodes = [NodeTotalDTO(node=n, shards=sum(1 for s in shards if s.node == n),
                              rows=sum(s.rows for s in shards if s.node == n)) for n in NODES]
        return ShardDistributionDTO(distribution_column=DISTRIBUTION_COLUMN, total_rows=sum(s.rows for s in shards),
                                    shards=shards, nodes=nodes)

    async def explain(self, body: str) -> List[ExplainDTO]:
        any_m = next((m for m in self._repo.all() if m.celestial_body_id.lower() == body), None) \
            or next(iter(self._repo.all()), None)
        out = [
            ExplainDTO(label=f"Запит за тілом ({body})",
                       sql=f"SELECT count(*) FROM missions WHERE lower(celestial_body_id) = '{body}'",
                       task_count=SHARD_COUNT,
                       plan="In-memory імітація: тіло не є ключем розподілу -> усі шарди паралельно"),
            ExplainDTO(label="Запит без фільтра", sql="SELECT count(*) FROM missions",
                       task_count=SHARD_COUNT, plan="In-memory імітація: усі шарди"),
        ]
        if any_m:
            out.append(ExplainDTO(
                label="Точковий запит за id (ключ розподілу)",
                sql=f"SELECT * FROM missions WHERE id = '{any_m.id}'",
                task_count=1, plan=f"In-memory імітація: шард {_table(_shard_index(any_m.id))}"))
        return out

    async def rebalance(self) -> str:
        return "У пам'яті вузли фіксовані — переносити нічого"

    async def rebalance_status(self) -> Dict[str, Any]:
        return {"state": "немає задач"}

    # ---- ShardMapPort ----
    async def placements(self) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {n: [] for n in NODES}
        for i in range(SHARD_COUNT):
            out[_node_of(i)].append(_table(i))
        return out

    async def cutoff(self) -> Optional[Any]:
        # У пам'яті map-фаза не віддає керування циклу подій, тож дані не змінюються посеред задачі
        return None

    async def run_map(self, node: str, shard_tables: List[str], job: str,
                      cutoff: Optional[Any] = None) -> Tuple[int, Any]:
        by_shard = self._by_shard()
        wanted = {int(t.rsplit("_", 1)[1]) - _FIRST_SHARD_ID for t in shard_tables}
        rows = [m for i in wanted for m in by_shard[i]]
        return len(rows), _MAPPERS[job](rows)

    async def run_on_coordinator(self, job: str, cutoff: Optional[Any] = None) -> Any:
        return _MAPPERS[job](self._repo.all())
