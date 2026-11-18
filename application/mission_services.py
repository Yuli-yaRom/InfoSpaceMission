import uuid
from typing import Optional

from application.mission_ports import (
    MissionCommandUseCase, MissionQueryUseCase, MissionRepositoryPort,
)
from domain.mission_dtos import (
    MissionCreateRequest, MissionDTO, MissionFilter, MissionPageDTO, StatsDTO,
)


class MissionQueryService(MissionQueryUseCase):
    def __init__(self, repository: MissionRepositoryPort):
        self._repo = repository

    async def list_missions(self, flt: MissionFilter) -> MissionPageDTO:
        items = await self._repo.find_by_filters(flt)
        total = await self._repo.count(flt)
        return MissionPageDTO(total=total, items=items)

    async def get_mission(self, mission_id: str) -> Optional[MissionDTO]:
        return await self._repo.find_by_id(mission_id)

    async def get_stats(self) -> StatsDTO:
        return StatsDTO(total_documents=await self._repo.count())


class MissionCommandService(MissionCommandUseCase):
    def __init__(self, repository: MissionRepositoryPort):
        self._repo = repository

    async def create_mission(self, request: MissionCreateRequest) -> MissionDTO:
        mission = MissionDTO(id=str(uuid.uuid4()), **request.model_dump())
        await self._repo.save(mission)
        return mission
