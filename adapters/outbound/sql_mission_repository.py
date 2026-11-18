"""SQL-адаптер основного сховища (PostgreSQL через SQLAlchemy async + asyncpg).
Працює з будь-якою БД, яку підтримує SQLAlchemy async (у тестах — SQLite через aiosqlite)."""
from typing import List, Optional

from sqlalchemy import Integer, String, Text, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from application.mission_ports import MissionRepositoryPort
from domain.mission_dtos import MissionDTO, MissionFilter


class Base(DeclarativeBase):
    pass


class MissionRow(Base):
    __tablename__ = "missions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    agency_id: Mapped[str] = mapped_column(String(64), index=True)
    celestial_body_id: Mapped[str] = mapped_column(String(64), index=True)
    launch_year: Mapped[int] = mapped_column(Integer, index=True)
    description: Mapped[str] = mapped_column(Text, default="")

    def to_dto(self) -> MissionDTO:
        return MissionDTO(id=self.id, name=self.name, type=self.type, status=self.status,
                          agency_id=self.agency_id, celestial_body_id=self.celestial_body_id,
                          launch_year=self.launch_year, description=self.description or "")


class SqlMissionRepository(MissionRepositoryPort):
    def __init__(self, url: str = "postgresql+asyncpg://space:space@localhost:5432/space",
                 engine: Optional[AsyncEngine] = None):
        self._engine = engine or create_async_engine(url, pool_pre_ping=True)
        self._session = async_sessionmaker(self._engine, expire_on_commit=False)

    async def init_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

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
            s.add_all(MissionRow(**m.model_dump()) for m in missions)

    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]:
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
