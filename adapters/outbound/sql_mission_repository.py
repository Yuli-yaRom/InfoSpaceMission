"""SQL-адаптер основного сховища (PostgreSQL / Citus через SQLAlchemy async + asyncpg).
Працює з будь-якою БД, яку підтримує SQLAlchemy async (у тестах — SQLite через aiosqlite).

Ключ шардування (п. 13): id (UUIDv4) з хеш-розподілом Citus — рівномірно по шардах,
а пошук місії за id іде рівно в один шард. Обґрунтування й заміри — docs/db_topology.md.
"""
from typing import AsyncIterator, List, Optional

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from application.mission_ports import MissionRepositoryPort
from domain.mission_dtos import MissionDTO, MissionFilter

DISTRIBUTION_COLUMN = "id"


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
        self._engine = engine or create_async_engine(url, pool_pre_ping=True)
        self._session = async_sessionmaker(self._engine, expire_on_commit=False)

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
        await self._engine.dispose()

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
