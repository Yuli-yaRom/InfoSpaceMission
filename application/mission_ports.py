from abc import ABC, abstractmethod
from typing import Any, AsyncContextManager, AsyncIterator, Dict, List, Optional, Tuple

from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    BenchmarkRunDTO, BenchmarkStartRequest, ClusterEventDTO, DeleteResultDTO, ExplainDTO,
    GeneratorSettingsDTO, GeneratorSettingsPatch, GeneratorStatusDTO, MapReduceResultDTO,
    MetricSampleDTO, MissionCreateRequest, MissionDTO, MissionFilter, MissionPageDTO,
    NodeHealthDTO, RecentQueryDTO, ResilienceCheckDTO, ResilienceSnapshotDTO, SearchPageDTO,
    SearchResultDTO, ShardDistributionDTO, StatsDTO, VacuumResultDTO,
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

    @abstractmethod
    async def rebalance(self) -> str: ...

    @abstractmethod
    async def rebalance_status(self) -> Dict[str, Any]: ...


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

    # ---- Навантажувальне тестування, модифікації, видалення ----
    @abstractmethod
    async def insert_bulk(self, missions: List[MissionDTO], idempotent: bool = False) -> None:
        """Швидкий масовий запис (у PostgreSQL — COPY). idempotent=True — повтор пачки після
        збою: рядки, що вже є (той самий id), пропускаються (ON CONFLICT DO NOTHING)."""

    @abstractmethod
    async def update_status(self, ids: List[str], status: str) -> int: ...

    @abstractmethod
    async def update_status_by_filter(self, flt: MissionFilter, status: str) -> int:
        """Масове оновлення (зачіпає всі шарди) — «важкий» запит на модифікацію."""

    @abstractmethod
    async def delete_ids(self, ids: List[str]) -> int: ...

    @abstractmethod
    async def delete_by_filter(self, flt: MissionFilter) -> int: ...

    @abstractmethod
    async def delete_all(self) -> None:
        """Повне видалення (TRUNCATE): миттєво звільняє місце на диску."""

    @abstractmethod
    async def storage_bytes(self) -> Optional[int]:
        """Розмір таблиці з індексами на всіх вузлах (None — невідомо/недоступно)."""

    @abstractmethod
    async def vacuum(self, full: bool = False) -> None: ...

    @abstractmethod
    async def db_now(self) -> Optional[Any]:
        """Час БД (годинник, що заповнює created_at); None — сховище без created_at."""

    @abstractmethod
    async def count_created_before(self, moment: Any) -> int:
        """Кількість рядків, вставлених не пізніше моменту `moment` (див. db_now)."""


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

    @abstractmethod
    async def update_status(self, ids: List[str], status: str) -> None: ...

    @abstractmethod
    async def update_status_by_filter(self, flt: MissionFilter, status: str) -> None: ...

    @abstractmethod
    async def delete_ids(self, ids: List[str]) -> None: ...

    @abstractmethod
    async def delete_by_filter(self, flt: MissionFilter) -> str:
        """Повертає коротку примітку для UI (напр. «видалення у фоні, задача …»)."""

    @abstractmethod
    async def clear(self) -> None:
        """Видалити всі документи (перестворити індекс)."""


class RecommendationCachePort(ABC):
    """Кеш рекомендацій — Redis."""

    @abstractmethod
    async def get(self, mission_id: str) -> Optional[List[RecommendationDTO]]: ...

    @abstractmethod
    async def put(self, mission_id: str, items: List[RecommendationDTO]) -> None: ...

    @abstractmethod
    async def clear(self) -> int:
        """Скинути кеш (після видалення даних). Повертає кількість видалених ключів."""


class SearchCachePort(ABC):
    """Кеш результатів пошуку — Redis, ключ search:<hash>, TTL."""

    @abstractmethod
    async def get(self, key: str) -> Optional[SearchPageDTO]: ...

    @abstractmethod
    async def put(self, key: str, page: SearchPageDTO, ttl_seconds: int) -> None: ...

    @abstractmethod
    async def clear(self) -> int: ...


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

    @abstractmethod
    async def rebalance(self) -> str:
        """Запустити перенесення шардів на нові вузли (напр. воркер на іншому комп'ютері)."""

    @abstractmethod
    async def rebalance_status(self) -> Dict[str, Any]: ...


class ShardMapPort(ABC):
    """Прямий доступ до вузлів-шардів в обхід координатора (для map-фази).

    Часткові результати (partial) мають форму, яку очікує MapReduceService:
      decades: {decade: [total, completed, failed]}
      terms:   {body: {term: count}}
    """

    @abstractmethod
    async def cutoff(self) -> Optional[Any]:
        """Момент зрізу за годинником БД: рахуються лише рядки, вставлені до нього.
        Генератор пише постійно, тож без зрізу map і перевірка побачили б різні дані."""

    @abstractmethod
    async def placements(self) -> Dict[str, List[str]]:
        """Вузол -> активні таблиці-шарди на ньому (лише активні розміщення)."""

    @abstractmethod
    async def run_map(self, node: str, shard_tables: List[str], job: str,
                      cutoff: Optional[Any] = None) -> Tuple[int, Any]:
        """Повертає (кількість рядків, частковий результат) для шардів одного вузла."""

    @abstractmethod
    async def run_on_coordinator(self, job: str, cutoff: Optional[Any] = None) -> Any:
        """Та сама агрегація через координатор (для перевірки результату)."""


class GeneratorControlPort(ABC):
    """Генератор навантаження (джерело даних). Реалізація — FakeGeneratorAdapter;
    генератор іншого учасника команди може реалізувати той самий порт."""

    @abstractmethod
    def settings(self) -> GeneratorSettingsDTO: ...

    @abstractmethod
    def update(self, patch: GeneratorSettingsPatch) -> GeneratorSettingsDTO: ...

    @abstractmethod
    def status(self) -> GeneratorStatusDTO: ...

    @abstractmethod
    def forget_ids(self) -> None:
        """Забути id, які генератор використовує для UPDATE/DELETE (після повного видалення)."""

    @abstractmethod
    def counters(self) -> Dict[str, float]:
        """Кумулятивні лічильники: inserted, updated, deleted, failed_attempts, sql_ms_sum,
        sql_batches, es_ms_sum, es_batches, pending."""

    @abstractmethod
    def batch_latencies_since(self, t_monotonic: float) -> List[float]:
        """Час запису кожної пачки (мс) з моменту t (для перцентилів у замірі)."""

    @abstractmethod
    def quiesced(self, timeout: float = 30.0) -> AsyncContextManager[None]:
        """Тимчасово зупинити нові операції з БД. Якщо поточні не завершились за timeout —
        TimeoutError (краще помилка, ніж неточний підрахунок)."""

    @abstractmethod
    def deletes_held(self) -> AsyncContextManager[None]:
        """Не виконувати DELETE генератора, поки рахується таблиця (вставки тривають)."""


class ClusterHealthPort(ABC):
    """Стан вузлів кластера: чи відповідає кожен вузол напряму (в обхід координатора)."""

    @abstractmethod
    async def health(self) -> List[NodeHealthDTO]: ...


class BenchmarkStorePort(ABC):
    """Збережені результати замірів. Файл спільний для різних топологій (1 вузол / 3 воркери),
    тож результати можна порівняти в одній таблиці."""

    @abstractmethod
    async def save(self, run: BenchmarkRunDTO) -> None: ...

    @abstractmethod
    async def list(self) -> List[BenchmarkRunDTO]: ...

    @abstractmethod
    async def delete(self, run_id: str) -> bool: ...


# ---- Вхідні порти навантажувального тестування ----
class DataAdminUseCase(ABC):
    @abstractmethod
    async def delete_one(self, mission_id: str) -> DeleteResultDTO: ...

    @abstractmethod
    async def delete_by_filter(self, flt: MissionFilter) -> DeleteResultDTO: ...

    @abstractmethod
    async def delete_all(self) -> DeleteResultDTO: ...

    @abstractmethod
    async def vacuum(self, full: bool) -> VacuumResultDTO: ...

    @abstractmethod
    async def preview(self, flt: MissionFilter) -> int: ...


class MetricsUseCase(ABC):
    @abstractmethod
    async def stats(self) -> StatsDTO: ...

    @abstractmethod
    async def recount(self) -> StatsDTO: ...

    @abstractmethod
    def samples(self, since_t: Optional[float] = None) -> List[MetricSampleDTO]: ...

    @abstractmethod
    def to_csv(self) -> str: ...


class BenchmarkUseCase(ABC):
    @abstractmethod
    async def start(self, req: BenchmarkStartRequest) -> BenchmarkRunDTO: ...

    @abstractmethod
    async def stop(self) -> Optional[BenchmarkRunDTO]: ...

    @abstractmethod
    def current(self) -> Optional[BenchmarkRunDTO]: ...

    @abstractmethod
    def is_running(self) -> bool: ...

    @abstractmethod
    async def history(self) -> List[BenchmarkRunDTO]: ...

    @abstractmethod
    async def delete(self, run_id: str) -> bool: ...


class ResilienceUseCase(ABC):
    @abstractmethod
    async def health(self) -> List[NodeHealthDTO]: ...

    @abstractmethod
    def events(self, limit: int = 50) -> List[ClusterEventDTO]: ...

    @abstractmethod
    async def snapshot(self) -> ResilienceSnapshotDTO: ...

    @abstractmethod
    async def check(self) -> ResilienceCheckDTO: ...
