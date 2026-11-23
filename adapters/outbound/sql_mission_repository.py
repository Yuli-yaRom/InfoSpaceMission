"""SQL-адаптер основного сховища (PostgreSQL / Citus через SQLAlchemy async + asyncpg).
Працює з будь-якою БД, яку підтримує SQLAlchemy async (у тестах — SQLite через aiosqlite).

Ключ шардування (п. 13): id (UUIDv4) з хеш-розподілом Citus — рівномірно по шардах,
а пошук місії за id іде рівно в один шард. Обґрунтування й заміри — docs/db_topology.md.
"""
import logging
from datetime import datetime
from typing import AsyncIterator, List, Optional

import asyncpg
from sqlalchemy import DateTime, Integer, String, Text, delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from application.mission_ports import MissionRepositoryPort
from domain.mission_dtos import MissionDTO, MissionFilter

DISTRIBUTION_COLUMN = "id"
# Порядок колонок для COPY / INSERT … VALUES; created_at заповнює БД (DEFAULT now())
COLUMNS = ("id", "name", "type", "status", "agency_id", "celestial_body_id", "launch_year",
           "description", "report")
_MAX_PARAMS = 32_000


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _values_insert(rows: int) -> str:
    n = len(COLUMNS)
    values = ",".join("(" + ",".join(f"${r * n + c + 1}" for c in range(n)) + ")" for r in range(rows))
    return (f"INSERT INTO missions ({', '.join(COLUMNS)}) VALUES {values} "
            f"ON CONFLICT (id) DO NOTHING")

log = logging.getLogger(__name__)

# Citus за замовчуванням тримає між транзакціями лише 1 з'єднання координатора з кожним воркером.
# COPY у 12 шардів відкриває кілька з'єднань на воркер — без кешу кожна пачка заново встановлювала
# SSL-з'єднання й запускала процеси на воркерах (замір: ~2 с на пачку з 1000 рядків, CPU воркерів 100+ %).
# Для звичайного PostgreSQL параметр із крапкою — просто невідомий користувацький параметр, він не заважає
CITUS_SESSION_SETTINGS = {"citus.max_cached_conns_per_worker": "16"}
VACUUM_TIMEOUT_S = 24 * 3600


class Base(DeclarativeBase):
    pass


class MissionRow(Base):
    __tablename__ = "missions"

    # Колонка розподілу Citus (входить у PRIMARY KEY, як того вимагає Citus)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    agency_id: Mapped[str] = mapped_column(String(64), index=True)
    celestial_body_id: Mapped[str] = mapped_column(String(64), index=True)
    launch_year: Mapped[int] = mapped_column(Integer, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    report: Mapped[str] = mapped_column(Text, default="")
    # Час вставки (годинник БД). Map-reduce рахує лише рядки до моменту зрізу, щоб
    # пряме читання воркерів і перевірка через координатор бачили однакові дані
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    @classmethod
    def from_dto(cls, m: MissionDTO) -> "MissionRow":
        return cls(**m.model_dump())

    def to_dto(self) -> MissionDTO:
        return MissionDTO(id=self.id, name=self.name, type=self.type, status=self.status,
                          agency_id=self.agency_id, celestial_body_id=self.celestial_body_id,
                          launch_year=self.launch_year, description=self.description or "",
                          report=self.report or "")


class SqlMissionRepository(MissionRepositoryPort):
    def __init__(self, url: str = "postgresql+asyncpg://space:space@localhost:5432/space",
                 engine: Optional[AsyncEngine] = None):
        self._engine = engine or create_async_engine(
            url, pool_pre_ping=True,
            connect_args={"server_settings": CITUS_SESSION_SETTINGS} if url.startswith("postgresql") else {})
        self._session = async_sessionmaker(self._engine, expire_on_commit=False)
        # Окремий пул asyncpg для масового запису (COPY) і службових команд (VACUUM, розмір).
        # Лише для PostgreSQL; на SQLite (тести) ті самі методи йдуть через SQLAlchemy Core
        self._pg_pool: Optional[asyncpg.Pool] = None
        self._is_citus: Optional[bool] = None

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    async def init_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            if conn.dialect.name == "postgresql":
                # Міграція для бази, створеної до появи created_at (Citus поширює ALTER на всі шарди)
                await conn.execute(text("ALTER TABLE missions ADD COLUMN IF NOT EXISTS "
                                        "created_at timestamptz NOT NULL DEFAULT now()"))

    async def close(self) -> None:
        if self._pg_pool is not None:
            await self._pg_pool.close()
        await self._engine.dispose()

    @property
    def _is_pg(self) -> bool:
        return self._engine.dialect.name == "postgresql"

    async def _pg(self) -> asyncpg.Pool:
        if self._pg_pool is None:
            u = self._engine.url
            self._pg_pool = await asyncpg.create_pool(
                host=u.host or "localhost", port=u.port or 5432, user=u.username, password=u.password,
                database=u.database, min_size=1, max_size=40, server_settings=CITUS_SESSION_SETTINGS,
                # Зависла операція (напр. вузол «завис», а не впав) не блокує генератор назавжди
                command_timeout=120)
        return self._pg_pool

    @staticmethod
    def _where(stmt, flt: Optional[MissionFilter]):
        if flt is None:
            return stmt
        if flt.body:
            stmt = stmt.where(func.lower(MissionRow.celestial_body_id) == flt.body.lower())
        if flt.type:
            stmt = stmt.where(func.lower(MissionRow.type) == flt.type.lower())
        if flt.status:
            stmt = stmt.where(func.lower(MissionRow.status) == flt.status.lower())
        if flt.year_from is not None:
            stmt = stmt.where(MissionRow.launch_year >= flt.year_from)
        if flt.year_to is not None:
            stmt = stmt.where(MissionRow.launch_year <= flt.year_to)
        return stmt

    async def save(self, mission: MissionDTO) -> None:
        await self.save_many([mission])

    async def save_many(self, missions: List[MissionDTO]) -> None:
        async with self._session.begin() as s:
            s.add_all(MissionRow.from_dto(m) for m in missions)

    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]:
        # Фільтр за колонкою розподілу -> Citus маршрутизує запит рівно в один шард
        async with self._session() as s:
            row = await s.get(MissionRow, mission_id)
            return row.to_dto() if row else None

    async def find_by_filters(self, flt: MissionFilter) -> List[MissionDTO]:
        stmt = (self._where(select(MissionRow), flt)
                .order_by(MissionRow.launch_year.desc(), MissionRow.name, MissionRow.id)
                .offset(flt.offset).limit(flt.limit))
        async with self._session() as s:
            return [r.to_dto() for r in (await s.scalars(stmt)).all()]

    async def count(self, flt: Optional[MissionFilter] = None) -> int:
        stmt = self._where(select(func.count()).select_from(MissionRow), flt)
        async with self._session() as s:
            return int((await s.execute(stmt)).scalar_one())

    async def iter_batches(self, batch_size: int = 1000) -> AsyncIterator[List[MissionDTO]]:
        last_id = ""
        while True:
            stmt = (select(MissionRow).where(MissionRow.id > last_id)
                    .order_by(MissionRow.id).limit(batch_size))
            async with self._session() as s:
                rows = (await s.scalars(stmt)).all()
            if not rows:
                return
            last_id = rows[-1].id
            yield [r.to_dto() for r in rows]

    # ---------- Масовий запис і модифікації (навантажувальне тестування) ----------
    async def insert_bulk(self, missions: List[MissionDTO], idempotent: bool = False) -> None:
        if not missions:
            return
        if not self._is_pg:
            await self._insert_core(missions, idempotent)
            return
        pool = await self._pg()
        async with pool.acquire() as conn:
            if idempotent:
                # Повтор після збою: якщо попередня спроба таки закомітилась, а відповідь загубилась,
                # ці id уже є — пропускаємо їх. Багаторядковий INSERT … VALUES: Citus маршрутизує
                # кожен рядок у свій шард. (INSERT … SELECT FROM unnest($1::text[], …) з параметрами
                # Citus 12.1 виконує некоректно — вставляє 1 рядок замість пачки; перевірено на кластері.)
                # Не більше ~3000 рядків на запит: ліміт PostgreSQL — 32767 параметрів
                async with conn.transaction():
                    for chunk in _chunks(missions, _MAX_PARAMS // len(COLUMNS)):
                        await conn.execute(_values_insert(len(chunk)),
                                           *[getattr(m, c) for m in chunk for c in COLUMNS])
            else:
                # COPY — найшвидший спосіб запису в PostgreSQL; координатор Citus розкладає
                # рядки по шардах за хешем id. Пачка атомарна: або вся, або нічого
                await conn.copy_records_to_table(
                    "missions", columns=list(COLUMNS),
                    records=[tuple(getattr(m, c) for c in COLUMNS) for m in missions])

    async def _insert_core(self, missions: List[MissionDTO], idempotent: bool) -> None:
        if self._engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        stmt = dialect_insert(MissionRow.__table__)
        if idempotent:
            stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        async with self._engine.begin() as conn:
            await conn.execute(stmt, [m.model_dump(include=set(COLUMNS)) for m in missions])

    async def update_status(self, ids: List[str], status: str) -> int:
        if not ids:
            return 0
        # Фільтр за колонкою розподілу: Citus надсилає UPDATE лише в шарди, де ці id лежать
        stmt = update(MissionRow).where(MissionRow.id.in_(ids)).values(status=status)
        async with self._engine.begin() as conn:
            return (await conn.execute(stmt)).rowcount or 0

    async def update_status_by_filter(self, flt: MissionFilter, status: str) -> int:
        stmt = self._where(update(MissionRow), flt).values(status=status)
        async with self._engine.begin() as conn:
            return (await conn.execute(stmt)).rowcount or 0

    async def delete_ids(self, ids: List[str]) -> int:
        if not ids:
            return 0
        async with self._engine.begin() as conn:
            return (await conn.execute(delete(MissionRow).where(MissionRow.id.in_(ids)))).rowcount or 0

    async def delete_by_filter(self, flt: MissionFilter) -> int:
        async with self._engine.begin() as conn:
            return (await conn.execute(self._where(delete(MissionRow), flt))).rowcount or 0

    async def delete_all(self) -> None:
        async with self._engine.begin() as conn:
            if self._is_pg:
                # Citus виконує TRUNCATE на кожному шарді; місце на диску звільняється одразу
                await conn.execute(text("TRUNCATE missions"))
            else:
                await conn.execute(delete(MissionRow))

    async def storage_bytes(self) -> Optional[int]:
        if not self._is_pg:
            return None
        pool = await self._pg()
        async with pool.acquire() as conn:
            if self._is_citus is None:
                self._is_citus = bool(await conn.fetchval(
                    "SELECT count(*) FROM pg_extension WHERE extname = 'citus'"))
            # citus_total_relation_size — сума розмірів усіх шардів (дані + індекси + TOAST) на всіх вузлах
            fn = "citus_total_relation_size" if self._is_citus else "pg_total_relation_size"
            return int(await conn.fetchval(f"SELECT {fn}('missions')", timeout=15))

    async def vacuum(self, full: bool = False) -> None:
        if not self._is_pg:
            return
        pool = await self._pg()
        async with pool.acquire() as conn:
            # VACUUM не можна виконувати в транзакції — asyncpg виконує команду в autocommit.
            # Citus розсилає VACUUM на всі шарди.
            # Без ліміту часу: timeout=None в asyncpg означає «взяти command_timeout пулу» (120 с), а на
            # десятках ГБ VACUUM триває значно довше — його б скасувало посеред роботи
            await conn.execute("VACUUM (FULL, ANALYZE) missions" if full else "VACUUM (ANALYZE) missions",
                               timeout=VACUUM_TIMEOUT_S)

    # ---------- Точний підрахунок без зупинки запису ----------
    async def db_now(self) -> Optional[datetime]:
        """Поточний час за годинником координатора (той самий годинник, що заповнює created_at)."""
        if not self._is_pg:
            return None
        pool = await self._pg()
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT now()")

    async def count_created_before(self, moment: datetime) -> int:
        """count(*) лише рядків, вставлених до моменту: нові вставки під час підрахунку не враховуються,
        тож запис може тривати, поки (довго) рахується велика таблиця."""
        async with self._session() as s:
            stmt = select(func.count()).select_from(MissionRow).where(MissionRow.created_at <= moment)
            return int((await s.execute(stmt)).scalar_one())
