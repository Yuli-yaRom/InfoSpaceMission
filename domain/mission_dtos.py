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


class GeneratorRateDTO(BaseModel):
    docs_per_second: int = Field(ge=10, le=5000)


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
