from typing import Callable, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from application.mission_ports import (
    BenchmarkUseCase, DataAdminUseCase, GeneratorControlPort, GetRecommendationsUseCase,
    MapReduceUseCase, MetricsUseCase, MissionCommandUseCase, MissionQueryUseCase,
    MissionSearchUseCase, ResilienceUseCase, ShardingUseCase,
)
from domain.dtos import RecommendationDTO
from domain.mission_dtos import (
    BenchmarkRunDTO, BenchmarkStartRequest, ClusterEventDTO, DeleteResultDTO, ExplainDTO,
    GeneratorSettingsPatch, GeneratorStatusDTO, MapReduceResultDTO, MetricSampleDTO,
    MissionCreateRequest, MissionDTO, MissionFilter, MissionPageDTO, NodeHealthDTO,
    RecentQueryDTO, ResilienceCheckDTO, ResilienceSnapshotDTO, SearchResultDTO,
    ShardDistributionDTO, StatsDTO, VacuumResultDTO,
)


def _db_unavailable(e: Exception) -> HTTPException:
    """Запит до БД під час відмови вузла: 503 з причиною замість 500 без пояснень."""
    return HTTPException(status_code=503, detail=f"БД недоступна: {type(e).__name__}: {str(e)[:300]}")


class MissionQueryRestController:
    """Читання: GET /api/missions?body=mars&type=ROVER -> MissionQueryService."""

    def __init__(self, use_case: MissionQueryUseCase):
        self._uc = use_case
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/missions", self.list_missions, methods=["GET"], response_model=MissionPageDTO)
        self.router.add_api_route("/missions/{mission_id}", self.get_mission, methods=["GET"], response_model=MissionDTO)

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
        try:
            return await self._uc.list_missions(flt)
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def get_mission(self, mission_id: str) -> MissionDTO:
        try:
            mission = await self._uc.get_mission(mission_id)
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)
        if mission is None:
            raise HTTPException(status_code=404, detail="Місію не знайдено")
        return mission


class MissionSearchRestController:
    """Пошук і рекомендації (схема Query, Рис. 3.2; п. 15):
    GET /api/search?q=landing&body=mars -> Redis (кеш) / Elasticsearch, з HIT/MISS і часом у мс
    GET /api/search/recent -> останні запити (Redis LPUSH/LTRIM)
    GET /api/missions/{id}/recommendations -> Redis (кеш) / Elasticsearch more_like_this"""

    def __init__(self, search: MissionSearchUseCase, recommendations: GetRecommendationsUseCase):
        self._search = search
        self._recs = recommendations
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/search", self.search, methods=["GET"], response_model=SearchResultDTO)
        self.router.add_api_route("/search/recent", self.recent, methods=["GET"],
                                  response_model=List[RecentQueryDTO])
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
    ) -> SearchResultDTO:
        flt = MissionFilter(body=body, type=type, status=status, year_from=year_from,
                            year_to=year_to, limit=limit, offset=offset)
        return await self._search.search(q, flt)

    async def recent(self, limit: int = Query(20, ge=1, le=50)) -> List[RecentQueryDTO]:
        return await self._search.recent(limit)

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
    """Керування генератором навантаження: швидкість, режим «максимум», пауза, модифікації."""

    def __init__(self, generator: GeneratorControlPort, locked: Callable[[], bool] = lambda: False):
        self._gen = generator
        self._locked = locked
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/generator", self.get_status, methods=["GET"], response_model=GeneratorStatusDTO)
        self.router.add_api_route("/generator", self.update, methods=["PUT"], response_model=GeneratorStatusDTO)

    async def get_status(self) -> GeneratorStatusDTO:
        return self._gen.status()

    async def update(self, patch: GeneratorSettingsPatch) -> GeneratorStatusDTO:
        if self._locked():
            # Інакше збережений результат заміру показував би не ті налаштування, з якими вимірювали
            raise HTTPException(status_code=409, detail="Триває замір — налаштування генератора заблоковані. "
                                                        "Зупиніть замір, щоб їх змінити.")
        self._gen.update(patch)
        return self._gen.status()


class MetricsRestController:
    """Швидкість запису в часі (графік, CSV) і статистика в шапці."""

    def __init__(self, metrics: MetricsUseCase):
        self._m = metrics
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/stats", self.stats, methods=["GET"], response_model=StatsDTO)
        self.router.add_api_route("/stats/recount", self.recount, methods=["POST"], response_model=StatsDTO)
        self.router.add_api_route("/metrics", self.samples, methods=["GET"], response_model=List[MetricSampleDTO])
        self.router.add_api_route("/metrics.csv", self.csv, methods=["GET"], response_class=PlainTextResponse)

    async def stats(self) -> StatsDTO:
        return await self._m.stats()

    async def recount(self) -> StatsDTO:
        try:
            return await self._m.recount()
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def samples(self, since: Optional[float] = None) -> List[MetricSampleDTO]:
        return self._m.samples(since)

    async def csv(self) -> PlainTextResponse:
        return PlainTextResponse(self._m.to_csv(), media_type="text/csv; charset=utf-8",
                                 headers={"Content-Disposition": 'attachment; filename="write_speed.csv"'})


class BenchmarkRestController:
    """Заміри фіксованої тривалості; результати зберігаються для порівняння топологій."""

    def __init__(self, bench: BenchmarkUseCase):
        self._b = bench
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/benchmarks", self.history, methods=["GET"], response_model=List[BenchmarkRunDTO])
        self.router.add_api_route("/benchmarks", self.start, methods=["POST"], response_model=BenchmarkRunDTO,
                                  status_code=201)
        self.router.add_api_route("/benchmarks/current", self.current, methods=["GET"],
                                  response_model=Optional[BenchmarkRunDTO])
        self.router.add_api_route("/benchmarks/stop", self.stop, methods=["POST"],
                                  response_model=Optional[BenchmarkRunDTO])
        self.router.add_api_route("/benchmarks/{run_id}", self.delete, methods=["DELETE"], status_code=204)

    async def history(self) -> List[BenchmarkRunDTO]:
        return await self._b.history()

    async def start(self, req: BenchmarkStartRequest) -> BenchmarkRunDTO:
        try:
            return await self._b.start(req)
        except RuntimeError as e:
            raise HTTPException(status_code=409, detail=str(e))

    async def current(self) -> Optional[BenchmarkRunDTO]:
        return self._b.current()

    async def stop(self) -> Optional[BenchmarkRunDTO]:
        return await self._b.stop()

    async def delete(self, run_id: str) -> None:
        if not await self._b.delete(run_id):
            raise HTTPException(status_code=404, detail="Замір не знайдено")


class DataAdminRestController:
    """Часткове й повне видалення даних, VACUUM."""

    def __init__(self, admin: DataAdminUseCase):
        self._a = admin
        self.router = APIRouter(prefix="/api")
        # /data/all зареєстровано раніше за /data, а /missions/{id} — окремий шлях
        self.router.add_api_route("/data/all", self.delete_all, methods=["DELETE"], response_model=DeleteResultDTO)
        self.router.add_api_route("/data/preview", self.preview, methods=["GET"])
        self.router.add_api_route("/data", self.delete_filtered, methods=["DELETE"], response_model=DeleteResultDTO)
        self.router.add_api_route("/data/vacuum", self.vacuum, methods=["POST"], response_model=VacuumResultDTO)
        self.router.add_api_route("/missions/{mission_id}", self.delete_one, methods=["DELETE"],
                                  response_model=DeleteResultDTO)

    @staticmethod
    def _flt(body, type, status, year_from, year_to) -> MissionFilter:
        return MissionFilter(body=body or None, type=type or None, status=status or None,
                             year_from=year_from, year_to=year_to)

    async def preview(self, body: Optional[str] = None, type: Optional[str] = None,
                      status: Optional[str] = None, year_from: Optional[int] = None,
                      year_to: Optional[int] = None) -> dict:
        try:
            return {"rows": await self._a.preview(self._flt(body, type, status, year_from, year_to))}
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def delete_filtered(self, body: Optional[str] = None, type: Optional[str] = None,
                              status: Optional[str] = None, year_from: Optional[int] = None,
                              year_to: Optional[int] = None) -> DeleteResultDTO:
        try:
            return await self._a.delete_by_filter(self._flt(body, type, status, year_from, year_to))
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def delete_all(self, confirm: str = "") -> DeleteResultDTO:
        if confirm != "yes":
            raise HTTPException(status_code=422, detail="Повне видалення потребує ?confirm=yes")
        try:
            return await self._a.delete_all()
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def vacuum(self, full: bool = False) -> VacuumResultDTO:
        try:
            return await self._a.vacuum(full)
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def delete_one(self, mission_id: str) -> DeleteResultDTO:
        try:
            res = await self._a.delete_one(mission_id)
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)
        if not res.deleted_rows:
            raise HTTPException(status_code=404, detail="Місію не знайдено")
        return res


class ResilienceRestController:
    """Стан вузлів, журнал подій, знімок і перевірка «дані не загубились»."""

    def __init__(self, res: ResilienceUseCase):
        self._r = res
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/cluster/health", self.health, methods=["GET"], response_model=List[NodeHealthDTO])
        self.router.add_api_route("/cluster/events", self.events, methods=["GET"],
                                  response_model=List[ClusterEventDTO])
        self.router.add_api_route("/resilience/snapshot", self.snapshot, methods=["POST"],
                                  response_model=ResilienceSnapshotDTO)
        self.router.add_api_route("/resilience/check", self.check, methods=["POST"],
                                  response_model=ResilienceCheckDTO)

    async def health(self) -> List[NodeHealthDTO]:
        return await self._r.health()

    async def events(self, limit: int = Query(50, ge=1, le=200)) -> List[ClusterEventDTO]:
        return self._r.events(limit)

    async def snapshot(self) -> ResilienceSnapshotDTO:
        try:
            return await self._r.snapshot()
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def check(self) -> ResilienceCheckDTO:
        try:
            return await self._r.check()
        except LookupError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)


class ShardingRestController:
    """П. 13–14: розподіл по шардах, EXPLAIN, власний map-reduce."""

    def __init__(self, sharding: ShardingUseCase, mapreduce: MapReduceUseCase):
        self._sharding = sharding
        self._mapreduce = mapreduce
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/sharding/distribution", self.distribution, methods=["GET"],
                                  response_model=ShardDistributionDTO)
        self.router.add_api_route("/sharding/explain", self.explain, methods=["GET"],
                                  response_model=List[ExplainDTO])
        self.router.add_api_route("/mapreduce/{job}", self.run_job, methods=["GET"],
                                  response_model=MapReduceResultDTO)
        self.router.add_api_route("/sharding/rebalance", self.rebalance, methods=["POST"])
        self.router.add_api_route("/sharding/rebalance", self.rebalance_status, methods=["GET"])

    async def rebalance(self) -> dict:
        try:
            return {"message": await self._sharding.rebalance()}
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def rebalance_status(self) -> dict:
        try:
            return await self._sharding.rebalance_status()
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def distribution(self) -> ShardDistributionDTO:
        try:
            return await self._sharding.distribution()
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)

    async def explain(self, body: str = "mars") -> List[ExplainDTO]:
        try:
            return await self._sharding.explain(body)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    async def run_job(self, job: str) -> MapReduceResultDTO:
        try:
            return await self._mapreduce.run(job)
        except KeyError:
            raise HTTPException(status_code=404, detail="Невідома задача map-reduce (є: decades, terms)")
        except Exception as e:  # noqa: BLE001
            raise _db_unavailable(e)
