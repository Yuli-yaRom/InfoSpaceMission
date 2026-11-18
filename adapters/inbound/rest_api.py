from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from application.mission_ports import (
    GeneratorControlPort, GetRecommendationsUseCase, MissionCommandUseCase,
    MissionQueryUseCase, MissionSearchUseCase,
)
from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    GeneratorRateDTO, MissionCreateRequest, MissionDTO, MissionFilter,
    MissionPageDTO, StatsDTO,
)


class MissionQueryRestController:
    """Читання: GET /api/missions?body=mars&type=ROVER -> MissionQueryService."""

    def __init__(self, use_case: MissionQueryUseCase):
        self._uc = use_case
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/missions", self.list_missions, methods=["GET"], response_model=MissionPageDTO)
        self.router.add_api_route("/missions/{mission_id}", self.get_mission, methods=["GET"], response_model=MissionDTO)
        self.router.add_api_route("/stats", self.get_stats, methods=["GET"], response_model=StatsDTO)

    async def list_missions(
        self,
        body: Optional[str] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> MissionPageDTO:
        flt = MissionFilter(body=body, type=type, status=status, year_from=year_from,
                            year_to=year_to, limit=limit, offset=offset)
        return await self._uc.list_missions(flt)

    async def get_mission(self, mission_id: str) -> MissionDTO:
        mission = await self._uc.get_mission(mission_id)
        if mission is None:
            raise HTTPException(status_code=404, detail="Місію не знайдено")
        return mission

    async def get_stats(self) -> StatsDTO:
        return await self._uc.get_stats()


class MissionSearchRestController:
    """Пошук і рекомендації (схема Query, Рис. 3.2):
    GET /api/search?q=марсохід&body=mars -> Elasticsearch
    GET /api/missions/{id}/recommendations -> Redis (кеш) / Elasticsearch more_like_this"""

    def __init__(self, search: MissionSearchUseCase, recommendations: GetRecommendationsUseCase):
        self._search = search
        self._recs = recommendations
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/search", self.search, methods=["GET"], response_model=MissionPageDTO)
        self.router.add_api_route("/missions/{mission_id}/recommendations", self.recommend,
                                  methods=["GET"], response_model=List[RecommendationDTO])

    async def search(
        self,
        q: str = "",
        body: Optional[str] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> MissionPageDTO:
        flt = MissionFilter(body=body, type=type, status=status, year_from=year_from,
                            year_to=year_to, limit=limit, offset=offset)
        return await self._search.search(q, flt)

    async def recommend(self, mission_id: str, limit: int = Query(5, ge=1, le=20)) -> List[RecommendationDTO]:
        try:
            return await self._recs.recommend(mission_id, limit)
        except KeyError:
            raise HTTPException(status_code=404, detail="Місію не знайдено")


class MissionRestController:
    """Запис: POST /api/missions -> MissionCommandService -> repository.save()."""

    def __init__(self, use_case: MissionCommandUseCase):
        self._uc = use_case
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/missions", self.create_mission, methods=["POST"],
                                  response_model=MissionDTO, status_code=201)

    async def create_mission(self, request: MissionCreateRequest) -> MissionDTO:
        return await self._uc.create_mission(request)


class GeneratorControlRestController:
    """Слайдер швидкості генератора."""

    def __init__(self, generator: GeneratorControlPort):
        self._gen = generator
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/generator", self.get_rate, methods=["GET"], response_model=GeneratorRateDTO)
        self.router.add_api_route("/generator", self.set_rate, methods=["PUT"], response_model=GeneratorRateDTO)

    async def get_rate(self) -> GeneratorRateDTO:
        return GeneratorRateDTO(docs_per_second=self._gen.get_rate())

    async def set_rate(self, body: GeneratorRateDTO) -> GeneratorRateDTO:
        self._gen.set_rate(body.docs_per_second)
        return body
