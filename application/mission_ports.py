from abc import ABC, abstractmethod
from typing import List, Optional

from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    MissionCreateRequest, MissionDTO, MissionFilter, MissionPageDTO, StatsDTO,
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

    @abstractmethod
    async def create_missions(self, requests: List[MissionCreateRequest]) -> List[MissionDTO]: ...


class MissionSearchUseCase(ABC):
    """Повнотекстовий пошук (Elasticsearch)."""

    @abstractmethod
    async def search(self, text: str, flt: MissionFilter) -> MissionPageDTO: ...


class GetRecommendationsUseCase(ABC):
    """Схожі місії (Elasticsearch more_like_this + кеш у Redis)."""

    @abstractmethod
    async def recommend(self, mission_id: str, limit: int = 5) -> List[RecommendationDTO]: ...


# ---- Вихідні порти ----
class MissionRepositoryPort(ABC):
    """Основне сховище — SQL (PostgreSQL)."""

    @abstractmethod
    async def save(self, mission: MissionDTO) -> None: ...

    @abstractmethod
    async def save_many(self, missions: List[MissionDTO]) -> None: ...

    @abstractmethod
    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]: ...

    @abstractmethod
    async def find_by_filters(self, flt: MissionFilter) -> List[MissionDTO]: ...

    @abstractmethod
    async def count(self, flt: Optional[MissionFilter] = None) -> int: ...


class MissionSearchPort(ABC):
    """Пошуковий індекс — Elasticsearch."""

    @abstractmethod
    async def index_many(self, missions: List[MissionDTO]) -> None: ...

    @abstractmethod
    async def search(self, text: str, flt: MissionFilter) -> MissionPageDTO: ...

    @abstractmethod
    async def find_similar(self, mission: MissionDTO, limit: int) -> List[MissionDTO]: ...


class RecommendationCachePort(ABC):
    """Кеш рекомендацій — Redis."""

    @abstractmethod
    async def get(self, mission_id: str) -> Optional[List[RecommendationDTO]]: ...

    @abstractmethod
    async def put(self, mission_id: str, items: List[RecommendationDTO]) -> None: ...


class GeneratorControlPort(ABC):
    @abstractmethod
    def get_rate(self) -> int: ...

    @abstractmethod
    def set_rate(self, docs_per_second: int) -> None: ...
