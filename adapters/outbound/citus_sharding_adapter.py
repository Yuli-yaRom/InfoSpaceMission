"""Адаптер Citus для демонстрації шардінгу (п. 13) і map-фази (п. 14).

* distribution() — аналог getShardDistribution(): рядки та розмір кожного шарда і вузла;
* explain()      — аналог explain(): "Task Count: 1" (запит за id, один шард) vs "Task Count: 12" (усі);
* placements()/run_map() — пряме підключення до кожного воркера в обхід координатора.
"""
import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from adapters.outbound.citus_cluster import PgConn
from adapters.outbound.sql_mission_repository import DISTRIBUTION_COLUMN
from application.mission_ports import ShardingInfoPort, ShardMapPort
from domain.mission_dtos import ExplainDTO, NodeTotalDTO, ShardDistributionDTO, ShardInfoDTO

_BODY_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
_TASK_COUNT_RE = re.compile(r"Task Count:\s*(\d+)")

# Map-запити: {table} — таблиця-шард на воркері (missions_102008) або missions на координаторі
_SQL = {
    "decades": """
        SELECT (launch_year / 10) * 10 AS decade,
               count(*) AS total,
               count(*) FILTER (WHERE upper(status) = 'COMPLETED') AS completed,
               count(*) FILTER (WHERE upper(status) = 'FAILED') AS failed
        FROM {table} GROUP BY 1""",
    "terms": """
        SELECT lower(celestial_body_id) AS body, w AS term, count(*) AS cnt
        FROM {table}, regexp_split_to_table(lower(report), '[^a-z]+') AS w
        WHERE w <> '' GROUP BY 1, 2""",
}

_PLACEMENTS_SQL = """
    SELECT s.shardid, n.nodename, n.nodeport
    FROM pg_dist_shard s
    JOIN pg_dist_placement p ON p.shardid = s.shardid
    JOIN pg_dist_node n ON n.groupid = p.groupid
    WHERE s.logicalrelid = 'missions'::regclass
      AND p.shardstate = 1            -- лише активні розміщення (без осиротілих копій)
      AND n.noderole = 'primary' AND n.isactive
    ORDER BY s.shardid"""


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class CitusShardAdapter(ShardingInfoPort, ShardMapPort):
    def __init__(self, coordinator: PgConn):
        self._coord = coordinator
        self._pool: Optional[asyncpg.Pool] = None
        self._nodes: Dict[str, Tuple[str, int]] = {}

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            c = self._coord
            self._pool = await asyncpg.create_pool(host=c.host, port=c.port, user=c.user, password=c.password,
                                                   database=c.database, min_size=1, max_size=4)
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    # ---------- ShardingInfoPort ----------
    async def distribution(self) -> ShardDistributionDTO:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            counts = {r["shardid"]: int(r["result"]) for r in await conn.fetch(
                "SELECT shardid, result FROM run_command_on_shards('missions', $$SELECT count(*) FROM %s$$)")}
            meta = await conn.fetch(
                "SELECT shardid, nodename, nodeport, shard_size FROM citus_shards "
                "WHERE table_name = 'missions'::regclass ORDER BY shardid")
        shards = [ShardInfoDTO(shard_id=r["shardid"], node=f"{r['nodename']}:{r['nodeport']}",
                               rows=counts.get(r["shardid"], 0), size_bytes=int(r["shard_size"] or 0))
                  for r in meta]
        per_node: Dict[str, List[ShardInfoDTO]] = defaultdict(list)
        for s in shards:
            per_node[s.node].append(s)
        nodes = [NodeTotalDTO(node=n, shards=len(ss), rows=sum(s.rows for s in ss))
                 for n, ss in sorted(per_node.items())]
        return ShardDistributionDTO(distribution_column=DISTRIBUTION_COLUMN, total_rows=sum(s.rows for s in shards),
                                    shards=shards, nodes=nodes)

    async def explain(self, body: str) -> List[ExplainDTO]:
        body = body.lower()
        if not _BODY_RE.match(body):
            raise ValueError("Некоректна назва небесного тіла")
        queries = [
            (f"Запит за тілом ({body})",
             f"SELECT count(*) FROM missions WHERE lower(celestial_body_id) = {_lit(body)}"),
            ("Запит без фільтра", "SELECT count(*) FROM missions"),
        ]
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT id FROM missions WHERE lower(celestial_body_id) = {_lit(body)} LIMIT 1"
            ) or await conn.fetchrow("SELECT id FROM missions LIMIT 1")
            if row:
                queries.append(("Точковий запит за id (ключ розподілу)",
                                f"SELECT * FROM missions WHERE id = {_lit(row['id'])}"))
            out = []
            for label, sql in queries:
                plan = "\n".join(r[0] for r in await conn.fetch("EXPLAIN " + sql))
                m = _TASK_COUNT_RE.search(plan)
                out.append(ExplainDTO(label=label, sql=sql, task_count=int(m.group(1)) if m else None, plan=plan))
        return out

    # ---------- ShardMapPort ----------
    async def placements(self) -> Dict[str, List[str]]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(_PLACEMENTS_SQL)
        out: Dict[str, List[str]] = defaultdict(list)
        for r in rows:
            node = f"{r['nodename']}:{r['nodeport']}"
            self._nodes[node] = (r["nodename"], r["nodeport"])
            out[node].append(f"missions_{r['shardid']}")
        return dict(out)

    @staticmethod
    def _fold(job: str, records: List[asyncpg.Record], acc: Any) -> Any:
        if job == "decades":
            for r in records:
                cur = acc.setdefault(int(r["decade"]), [0, 0, 0])
                cur[0] += r["total"]
                cur[1] += r["completed"]
                cur[2] += r["failed"]
        else:
            for r in records:
                acc.setdefault(r["body"], Counter())[r["term"]] += r["cnt"]
        return acc

    @staticmethod
    def _finish(job: str, acc: Any) -> Any:
        return acc if job == "decades" else {b: dict(c) for b, c in acc.items()}

    async def run_map(self, node: str, shard_tables: List[str], job: str) -> Tuple[int, Any]:
        host, port = self._nodes[node]
        conn = await self._coord.with_host(host, port).connect()  # напряму до воркера
        try:
            acc: Any = {}
            rows = 0
            for table in shard_tables:
                rows += await conn.fetchval(f"SELECT count(*) FROM {_ident(table)}")
                acc = self._fold(job, await conn.fetch(_SQL[job].format(table=_ident(table))), acc)
            return rows, self._finish(job, acc)
        finally:
            await conn.close()

    async def run_on_coordinator(self, job: str) -> Any:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            return self._finish(job, self._fold(job, await conn.fetch(_SQL[job].format(table="missions")), {}))
