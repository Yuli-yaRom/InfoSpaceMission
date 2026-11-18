from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    ExplainDTO, MapReduceResultDTO, MissionCreateRequest, MissionDTO, MissionFilter,
    MissionPageDTO, RecentQueryDTO, SearchPageDTO, SearchResultDTO, ShardDistributionDTO,
    StatsDTO,
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
    """П. 15: повнотекстовий пошук (Elasticsearch) з кешем cache-aside (Redis)."""

    @abstractmethod
    async def search(self, text: str, flt: MissionFilter) -> SearchResultDTO: ...

    @abstractmethod
    async def recent(self, limit: int = 20) -> List[RecentQueryDTO]: ...


class GetRecommendationsUseCase(ABC):
    """Схожі місії (Elasticsearch more_like_this + кеш у Redis)."""

    @abstractmethod
    async def recommend(self, mission_id: str, limit: int = 5) -> List[RecommendationDTO]: ...


class ShardingUseCase(ABC):
    """П. 13: розподіл даних по шардах і план виконання запитів."""

    @abstractmethod
    async def distribution(self) -> ShardDistributionDTO: ...

    @abstractmethod
    async def explain(self, body: str) -> List[ExplainDTO]: ...


class MapReduceUseCase(ABC):
    """П. 14: власний map-reduce поверх шардів."""

    @abstractmethod
    async def run(self, job: str) -> MapReduceResultDTO: ...


# ---- Вихідні порти ----
class MissionRepositoryPort(ABC):
    """Основне сховище — SQL (PostgreSQL / Citus)."""

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

    @abstractmethod
    def iter_batches(self, batch_size: int = 1000) -> AsyncIterator[List[MissionDTO]]:
        """Усі місії пачками (для переіндексації в Elasticsearch)."""


class MissionSearchPort(ABC):
    """Пошуковий індекс — Elasticsearch."""

    @abstractmethod
    async def index_many(self, missions: List[MissionDTO]) -> None: ...

    @abstractmethod
    async def count(self) -> int: ...

    @abstractmethod
    async def search(self, text: str, flt: MissionFilter) -> SearchPageDTO: ...

    @abstractmethod
    async def find_similar(self, mission: MissionDTO, limit: int) -> List[MissionDTO]: ...


class RecommendationCachePort(ABC):
    """Кеш рекомендацій — Redis."""

    @abstractmethod
    async def get(self, mission_id: str) -> Optional[List[RecommendationDTO]]: ...

    @abstractmethod
    async def put(self, mission_id: str, items: List[RecommendationDTO]) -> None: ...


class SearchCachePort(ABC):
    """Кеш результатів пошуку — Redis, ключ search:<hash>, TTL."""

    @abstractmethod
    async def get(self, key: str) -> Optional[SearchPageDTO]: ...

    @abstractmethod
    async def put(self, key: str, page: SearchPageDTO, ttl_seconds: int) -> None: ...


class RecentQueriesPort(ABC):
    """Список останніх запитів — Redis LPUSH + LTRIM."""

    @abstractmethod
    async def push(self, entry: RecentQueryDTO, keep: int = 20) -> None: ...

    @abstractmethod
    async def list(self, limit: int = 20) -> List[RecentQueryDTO]: ...


class ShardingInfoPort(ABC):
    @abstractmethod
    async def distribution(self) -> ShardDistributionDTO: ...

    @abstractmethod
    async def explain(self, body: str) -> List[ExplainDTO]: ...


class ShardMapPort(ABC):
    """Прямий доступ до вузлів-шардів в обхід координатора (для map-фази).

    Часткові результати (partial) мають форму, яку очікує MapReduceService:
      decades: {decade: [total, completed, failed]}
      terms:   {body: {term: count}}
    """

    @abstractmethod
    async def placements(self) -> Dict[str, List[str]]:
        """Вузол -> активні таблиці-шарди на ньому (лише активні розміщення)."""

    @abstractmethod
    async def run_map(self, node: str, shard_tables: List[str], job: str) -> Tuple[int, Any]:
        """Повертає (кількість рядків, частковий результат) для шардів одного вузла."""

    @abstractmethod
    async def run_on_coordinator(self, job: str) -> Any:
        """Та сама агрегація через координатор (для перевірки результату)."""


class GeneratorControlPort(ABC):
    @abstractmethod
    def get_rate(self) -> int: ...

    @abstractmethod
    def set_rate(self, docs_per_second: int) -> None: ...
