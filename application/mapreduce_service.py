"""П. 13–14: розподіл по шардах і власний map-reduce.

Через координатор Citus агрегація відбувається автоматично (він сам розсилає часткові
запити на воркери). Щоб показати процес явно, тут map-reduce написано вручну:
  map    — паралельно (asyncio.gather) напряму до кожного вузла; вузол рахує частковий результат
           по своїх таблицях-шардах;
  reduce — часткові результати зливаються в Python;
  check  — той самий результат через координатор має збігтися.
"""
import asyncio
import re
import time
from collections import Counter
from typing import Any, Callable, Dict, List

from application.mission_ports import (
    MapReduceUseCase, ShardingInfoPort, ShardingUseCase, ShardMapPort,
)
from domain.mission_dtos import (
    ExplainDTO, MapReduceResultDTO, ShardDistributionDTO, ShardMapResultDTO,
)

_BODY_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
TOP_TERMS = 20
MIN_TERM_LEN = 3
STOP_WORDS = frozenset("""
a an and are as at be been but by for from had has have in into is it its may near of on or
that the their there these this to was were which will with after before during below above
""".split())


def _ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


# ---------- decades: кількість місій і частка успішних за десятиліттями ----------
def _reduce_decades(partials: List[Dict]) -> Dict[int, List[int]]:
    acc: Dict[int, List[int]] = {}
    for part in partials:
        for decade, (total, completed, failed) in part.items():
            cur = acc.setdefault(int(decade), [0, 0, 0])
            cur[0] += total
            cur[1] += completed
            cur[2] += failed
    return acc


def _finalize_decades(acc: Dict[int, List[int]]) -> List[Dict[str, Any]]:
    out = []
    for decade in sorted(acc):
        total, completed, failed = acc[decade]
        finished = completed + failed
        out.append({"decade": decade, "missions": total, "completed": completed, "failed": failed,
                    # успіх = COMPLETED серед завершених (COMPLETED + FAILED)
                    "success_rate": round(completed / finished, 4) if finished else None})
    return out


# ---------- terms: топ-20 термінів у звітах за небесними тілами (word count) ----------
def _reduce_terms(partials: List[Dict]) -> Dict[str, Counter]:
    acc: Dict[str, Counter] = {}
    for part in partials:
        for body, counts in part.items():
            acc.setdefault(body, Counter()).update(counts)
    return acc


def _finalize_terms(acc: Dict[str, Counter]) -> Dict[str, List[List[Any]]]:
    out = {}
    for body in sorted(acc):
        terms = [(t, c) for t, c in acc[body].items() if len(t) >= MIN_TERM_LEN and t not in STOP_WORDS]
        terms.sort(key=lambda tc: (-tc[1], tc[0]))  # детерміновано при однакових частотах
        out[body] = [[t, c] for t, c in terms[:TOP_TERMS]]
    return out


JOBS: Dict[str, Dict[str, Callable]] = {
    "decades": {"reduce": _reduce_decades, "finalize": _finalize_decades},
    "terms": {"reduce": _reduce_terms, "finalize": _finalize_terms},
}


class MapReduceService(MapReduceUseCase):
    def __init__(self, shards: ShardMapPort):
        self._shards = shards

    async def run(self, job: str) -> MapReduceResultDTO:
        if job not in JOBS:
            raise KeyError(job)
        spec = JOBS[job]

        # Розміщення читаємо на початку: лише активні шарди, тож "осиротілі" копії
        # після переміщення шарда не будуть пораховані двічі
        placements = await self._shards.placements()
        # Один момент зрізу для map і для перевірки — обидва бачать той самий набір рядків
        cutoff = await self._shards.cutoff()

        async def map_node(node: str, tables: List[str]) -> ShardMapResultDTO:
            start = time.perf_counter()
            rows, partial = await self._shards.run_map(node, tables, job, cutoff)
            return ShardMapResultDTO(node=node, shard_tables=tables, rows=rows,
                                     map_ms=_ms(start), partial=partial)

        map_start = time.perf_counter()
        per_node = await asyncio.gather(*(map_node(n, t) for n, t in sorted(placements.items())))
        map_ms = _ms(map_start)

        reduce_start = time.perf_counter()
        result = spec["finalize"](spec["reduce"]([r.partial for r in per_node]))
        reduce_ms = _ms(reduce_start)

        coord_start = time.perf_counter()
        coordinator_result = spec["finalize"](spec["reduce"]([await self._shards.run_on_coordinator(job, cutoff)]))
        coordinator_ms = _ms(coord_start)

        return MapReduceResultDTO(job=job, result=result, per_node=list(per_node), map_ms=map_ms,
                                  reduce_ms=reduce_ms, coordinator_result=coordinator_result,
                                  coordinator_ms=coordinator_ms, match=result == coordinator_result)


class ShardingService(ShardingUseCase):
    def __init__(self, info: ShardingInfoPort):
        self._info = info

    async def distribution(self) -> ShardDistributionDTO:
        return await self._info.distribution()

    async def explain(self, body: str) -> List[ExplainDTO]:
        body = body.strip().lower()
        if not _BODY_RE.match(body):
            raise ValueError("Некоректна назва небесного тіла")
        return await self._info.explain(body)

    async def rebalance(self) -> str:
        return await self._info.rebalance()

    async def rebalance_status(self) -> Dict[str, Any]:
        return await self._info.rebalance_status()
