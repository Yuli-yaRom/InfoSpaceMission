"""DTO для п. 12 (UI + повний потік).
Схема тимчасова: фінальні поля затверджуються на командній зустрічі.
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class MissionCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    type: str  # напр. ROVER, ORBITER, CREWED
    status: str = "PLANNED"
    agency_id: str
    celestial_body_id: str  # напр. mars
    launch_year: int = Field(ge=1950, le=2100)
    description: str = ""


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
