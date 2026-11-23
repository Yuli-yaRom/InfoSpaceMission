"""Одноразове (ідемпотентне) налаштування кластера Citus (п. 13).

Топологія: координатор (аналог mongos — застосунок підключається тільки до нього)
+ 3 воркери (аналог шардів). Метадані кластера (pg_dist_*) живуть на координаторі —
аналог config server.
"""
import logging
from dataclasses import dataclass
from typing import List, Tuple

import asyncpg
from sqlalchemy.engine import make_url

from adapters.outbound.sql_mission_repository import DISTRIBUTION_COLUMN

log = logging.getLogger(__name__)

SHARD_COUNT = 12  # 4 шарди на кожен з 3 воркерів — легко читати в демо


@dataclass(frozen=True)
class PgConn:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_sqlalchemy_url(cls, url: str) -> "PgConn":
        u = make_url(url)
        return cls(host=u.host or "localhost", port=u.port or 5432, user=u.username or "postgres",
                   password=u.password or "", database=u.database or "postgres")

    def with_host(self, host: str, port: int) -> "PgConn":
        return PgConn(host, port, self.user, self.password, self.database)

    async def connect(self) -> asyncpg.Connection:
        return await asyncpg.connect(host=self.host, port=self.port, user=self.user,
                                     password=self.password, database=self.database)


def parse_workers(spec: str) -> List[Tuple[str, int]]:
    """"citus-worker-1:5432,citus-worker-2:5432" -> [("citus-worker-1", 5432), ...]"""
    out = []
    for item in filter(None, (s.strip() for s in spec.split(","))):
        host, _, port = item.partition(":")
        out.append((host, int(port or 5432)))
    return out


async def setup_citus(coordinator: PgConn, coordinator_host: str, workers: List[Tuple[str, int]]) -> bool:
    """Повертає False, якщо розширення citus недоступне (звичайний PostgreSQL) — тоді таблиця лишається локальною."""
    conn = await coordinator.connect()
    try:
        if not await conn.fetchval("SELECT count(*) FROM pg_available_extensions WHERE name = 'citus'"):
            log.info("Citus недоступний — працюємо зі звичайною таблицею PostgreSQL")
            return False
        await conn.execute("CREATE EXTENSION IF NOT EXISTS citus")

        for host, port in workers:  # розширення має бути і на кожному воркері
            wconn = await coordinator.with_host(host, port).connect()
            try:
                await wconn.execute("CREATE EXTENSION IF NOT EXISTS citus")
            finally:
                await wconn.close()

        if not await conn.fetchval("SELECT count(*) FROM pg_dist_node WHERE groupid = 0"):
            await conn.execute("SELECT citus_set_coordinator_host($1, $2)", coordinator_host, coordinator.port)
        if not workers:
            # Один вузол (docker-compose.single.yml): шарди зберігає сам координатор —
            # той самий Citus і ті самі 12 шардів, лише без розподілу по машинах. Так порівняння
            # «1 вузол vs 3 воркери» відрізняється тільки кількістю вузлів
            await conn.execute("SELECT citus_set_node_property($1, $2, 'shouldhaveshards', true)",
                               coordinator_host, coordinator.port)
            log.info("Citus: режим одного вузла — шарди на координаторі")
        # Пароль для з'єднань координатора з воркерами (nodeid 0 — для всіх вузлів). Локальним
        # воркерам у compose він не потрібен (trust у мережі compose), а воркеру на іншому комп'ютері
        # (docs/multi_host.md) — потрібен, бо там вхід лише за паролем
        try:
            if not await conn.fetchval("SELECT count(*) FROM pg_dist_authinfo WHERE nodeid = 0 AND rolename = $1",
                                       coordinator.user):
                await conn.execute("INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo) VALUES (0, $1, $2)",
                                   coordinator.user, f"password={coordinator.password}")
        except Exception as e:  # noqa: BLE001
            log.warning("Citus: не вдалося записати pg_dist_authinfo: %s", e)

        for host, port in workers:
            exists = await conn.fetchval(
                "SELECT count(*) FROM pg_dist_node WHERE nodename = $1 AND nodeport = $2", host, port)
            if not exists:
                await conn.execute("SELECT citus_add_node($1, $2)", host, port)
                log.info("Citus: додано воркер %s:%d", host, port)

        distributed = await conn.fetchval(
            "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'missions'::regclass")
        if not distributed:
            await conn.execute(
                f"SELECT create_distributed_table('missions', '{DISTRIBUTION_COLUMN}', shard_count => {SHARD_COUNT})")
            log.info("Citus: таблицю missions розподілено за %s на %d шардів", DISTRIBUTION_COLUMN, SHARD_COUNT)
        return True
    finally:
        await conn.close()
