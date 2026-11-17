from abc import ABC, abstractmethod
from typing import List, Optional

from domain.mission_dtos import (
    GeneratorRateDTO, MissionCreateRequest, MissionDTO, MissionFilter,
    MissionPageDTO, StatsDTO,
)


# ---- Вхідні порти (use cases) ----
class MissionQueryUseCase(ABC):
    @abstractmethod
    async def list_missions(self, flt: MissionFilter) -> MissionPageDTO: ...

    @abstractmethod
    async def get_mission(self, mission_id: str) -> Optional[MissionDTO]: ...

    @abstractmethod
    async def get_stats(self) -> StatsDTO: ...


class MissionCommandUseCase(ABC):
    @abstractmethod
    async def create_mission(self, request: MissionCreateRequest) -> MissionDTO: ...


# ---- Вихідні порти ----
class MissionRepositoryPort(ABC):
    @abstractmethod
    async def save(self, mission: MissionDTO) -> None: ...

    @abstractmethod
    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]: ...

    @abstractmethod
    async def find_by_filters(self, flt: MissionFilter) -> List[MissionDTO]: ...

    @abstractmethod
    async def count(self, flt: Optional[MissionFilter] = None) -> int: ...


class GeneratorControlPort(ABC):
    @abstractmethod
    def get_rate(self) -> int: ...

    @abstractmethod
    def set_rate(self, docs_per_second: int) -> None: ...
