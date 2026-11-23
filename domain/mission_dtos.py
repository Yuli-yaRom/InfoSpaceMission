"""DTO для п. 12 (UI + повний потік), п. 13–15 (шардінг, map-reduce, пошук).
Схема тимчасова: фінальні поля затверджуються на командній зустрічі.
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MissionCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    type: str  # напр. ROVER, ORBITER, CREWED
    status: str = "PLANNED"
    agency_id: str
    celestial_body_id: str  # напр. mars
    launch_year: int = Field(ge=1950, le=2100)
    description: str = ""
    report: str = ""  # англомовний звіт місії -> Elasticsearch (analyzer: english)


class MissionDTO(MissionCreateRequest):
    id: str


class MissionFilter(BaseModel):
    body: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    limit: int = Field(default=10, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class MissionPageDTO(BaseModel):
    total: int
    items: List[MissionDTO]


class StatsDTO(BaseModel):
    total_documents: int
    # Лічильник застосунку: точний підрахунок на старті (або після «Перерахувати»)
    # + усі вставки й видалення, що пройшли через застосунок. Без count(*) щосекунди,
    # бо на десятках ГБ він сам навантажував би кластер під час заміру
    counted_at: Optional[str] = None   # None — точний підрахунок ще не завершився
    counting: bool = False             # зараз іде count(*) (на великій таблиці — хвилини)
    insert_rate: float = 0.0      # рядків/с за останнє вікно вимірювання
    pending: int = 0              # рядки в буфері генератора, ще не записані в БД
    db_size_bytes: Optional[int] = None
    generator_state: str = "paused"


class GeneratorSettingsDTO(BaseModel):
    """Налаштування генератора навантаження (вкладка «Навантаження»).
    docs_per_second = 0 — пауза; max_speed — писати так швидко, як встигає БД (ліміт ігнорується)."""
    docs_per_second: int = Field(default=100, ge=0, le=200_000)
    max_speed: bool = False
    batch_size: int = Field(default=1000, ge=10, le=50_000)
    writers: int = Field(default=4, ge=1, le=32)          # паралельних з'єднань на запис
    index_search: bool = True                            # індексувати в Elasticsearch синхронно
    # Бонус: модифікації під навантаженням (заповільнюють запис)
    updates_per_second: int = Field(default=0, ge=0, le=50_000)
    deletes_per_second: int = Field(default=0, ge=0, le=50_000)
    bulk_update_every_s: int = Field(default=0, ge=0, le=3600)  # 0 — вимкнено


class GeneratorSettingsPatch(BaseModel):
    docs_per_second: Optional[int] = Field(default=None, ge=0, le=200_000)
    max_speed: Optional[bool] = None
    batch_size: Optional[int] = Field(default=None, ge=10, le=50_000)
    writers: Optional[int] = Field(default=None, ge=1, le=32)
    index_search: Optional[bool] = None
    updates_per_second: Optional[int] = Field(default=None, ge=0, le=50_000)
    deletes_per_second: Optional[int] = Field(default=None, ge=0, le=50_000)
    bulk_update_every_s: Optional[int] = Field(default=None, ge=0, le=3600)


class GeneratorStatusDTO(BaseModel):
    settings: GeneratorSettingsDTO
    state: str                    # running | paused | retrying
    generated: int                # прийнято від джерела даних (згенеровано)
    inserted: int                 # підтверджено записаних у БД
    pending: int                  # у буфері: прийнято, але ще не записано
    buffer_limit: int
    failed_attempts: int          # невдалих спроб запису (пачки повторюються, а не губляться)
    updated: int
    deleted: int
    bulk_updates: int
    es_failures: int
    last_error: Optional[str] = None
    last_error_at: Optional[str] = None
    retrying_since: Optional[str] = None


class MetricSampleDTO(BaseModel):
    """Одна точка часового ряду (знімок кожні кілька секунд)."""
    t: float                      # секунди від старту застосунку
    at: str                       # ISO-час
    insert_rate: float
    update_rate: float
    delete_rate: float
    sql_batch_ms: Optional[float] = None   # середній час запису однієї пачки в БД за вікно
    es_batch_ms: Optional[float] = None    # середній час індексації пачки в Elasticsearch
    pending: int = 0
    total_rows: int = 0
    db_size_bytes: Optional[int] = None
    target_rate: Optional[float] = None    # None — режим «максимум»
    state: str = "running"


class BenchmarkStartRequest(BaseModel):
    label: str = Field(default="", max_length=80)
    duration_s: int = Field(default=300, ge=10, le=4 * 3600)
    max_speed: bool = True
    docs_per_second: int = Field(default=5000, ge=1, le=200_000)


class BenchmarkRunDTO(BaseModel):
    id: str
    label: str
    topology: str                 # напр. "3 воркери" / "1 вузол"
    nodes: int                    # вузлів, що зберігають шарди
    started_at: str
    duration_s: float
    planned_s: int
    status: str                   # running | done | stopped
    settings: GeneratorSettingsDTO
    inserted: int = 0
    avg_rate: float = 0.0
    first_min_rate: Optional[float] = None
    last_min_rate: Optional[float] = None
    change_pct: Optional[float] = None     # (останнє вікно - перше) / перше, %
    window_s: float = 60                   # довжина «першого» і «останнього» вікна
    warmup_s: float = 0                    # пропущений розгін на початку (перше вікно — після нього)
    peak_rate: float = 0.0
    p50_batch_ms: Optional[float] = None
    p95_batch_ms: Optional[float] = None
    rows_start: int = 0
    rows_end: int = 0
    size_start: Optional[int] = None
    size_end: Optional[int] = None
    failed_attempts: int = 0
    series: List[List[float]] = []          # [[секунда від старту, рядків/с], ...] (проріджено)


class DeleteResultDTO(BaseModel):
    scope: str                    # filter | all | id
    deleted_rows: int
    took_ms: float
    search_note: str = ""
    caches_cleared: int = 0
    size_before: Optional[int] = None
    size_after: Optional[int] = None


class VacuumResultDTO(BaseModel):
    took_ms: float
    full: bool
    size_before: Optional[int] = None
    size_after: Optional[int] = None


class NodeHealthDTO(BaseModel):
    node: str                     # host:port
    role: str                     # coordinator | worker
    up: bool
    latency_ms: Optional[float] = None
    shards: int = 0
    error: Optional[str] = None
    since: Optional[str] = None   # коли змінився стан


class ClusterEventDTO(BaseModel):
    at: str
    level: str                    # info | warn | ok
    message: str


class ResilienceSnapshotDTO(BaseModel):
    at: str
    rows: int
    inserted_total: int
    deleted_total: int
    generated_total: int = 0
    pending: int = 0
    per_node: Dict[str, int] = {}


class ResilienceCheckDTO(BaseModel):
    snapshot: ResilienceSnapshotDTO
    checked_at: str
    inserted_since: int
    deleted_since: int
    expected_rows: int
    actual_rows: int
    lost_rows: int                # expected - actual: має бути 0
    generated_since: int = 0      # згенеровано після знімка = записано + приріст буфера
    pending: int                  # якщо > 0 — буфер ще дописується, перевірку варто повторити
    per_node_after: Dict[str, int] = {}
    ok: bool


# ---- П. 15: пошук ----
class SearchHitDTO(BaseModel):
    mission: MissionDTO
    # поле -> фрагменти з <mark>…</mark>; текст уже HTML-екранований (encoder: html)
    highlights: Dict[str, List[str]] = {}


class SearchPageDTO(BaseModel):
    """Те, що повертає пошуковий індекс (і що кешується в Redis)."""
    total: int
    items: List[SearchHitDTO]


class SearchResultDTO(SearchPageDTO):
    query: str
    cache: str  # "HIT" | "MISS"
    took_ms: float


class RecentQueryDTO(BaseModel):
    query: str
    filters: Dict[str, Any] = {}
    total: int
    status: str  # "Знайдено" | "Пусто"
    cache: str
    at: str  # ISO-час


# ---- П. 13: шардінг ----
class ShardInfoDTO(BaseModel):
    shard_id: int
    node: str
    rows: int
    size_bytes: int = 0


class NodeTotalDTO(BaseModel):
    node: str
    shards: int
    rows: int


class ShardDistributionDTO(BaseModel):
    distribution_column: str
    total_rows: int
    shards: List[ShardInfoDTO]
    nodes: List[NodeTotalDTO]


class ExplainDTO(BaseModel):
    label: str
    sql: str
    task_count: Optional[int]
    plan: str


# ---- П. 14: map-reduce ----
class ShardMapResultDTO(BaseModel):
    node: str
    shard_tables: List[str]
    rows: int
    map_ms: float
    partial: Any


class MapReduceResultDTO(BaseModel):
    job: str
    result: Any
    per_node: List[ShardMapResultDTO]
    map_ms: float  # тривалість усієї паралельної фази map
    reduce_ms: float
    coordinator_result: Any
    coordinator_ms: float
    match: bool
