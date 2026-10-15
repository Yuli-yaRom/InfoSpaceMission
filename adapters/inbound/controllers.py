from application.ports_in import CreateMissionUseCase, GetRecommendationsUseCase
from domain.dtos import (
    BatchIngestRequest,
    BatchIngestResponse,
    RecommendationDTO,
)


# Вхідний контролер для прийому пакету місій від генератора (згідно зі схемою Command)
class GeneratorIngestController:
    def __init__(self, create_mission_use_case: CreateMissionUseCase):
        self.create_mission_use_case = create_mission_use_case

    def handle_batch_ingest(self, request: BatchIngestRequest) -> BatchIngestResponse:
        count = 0
        for item in request.items:
            self.create_mission_use_case.execute(item)
            count += 1

        return BatchIngestResponse(
            batch_id=request.batch_id,
            total_received=count,
            status="SUCCESS",
            message=f"Successfully processed {count} missions via CreateMissionUseCase",
        )


# Контролер запитів на читання згідно зі схемою Query 
class QueryRestController:
    def __init__(self, recommendation_use_case: GetRecommendationsUseCase):
        self.recommendation_use_case = recommendation_use_case

    def search_missions(self, query: str) -> list:
        # Метод пошуку (заглушка відповідно до Рис. 3.2)
        raise NotImplementedError("TODO: Implement Elasticsearch search missions")

    def get_recommendations(
        self, mission_id: str, limit: int = 5
    ) -> list[RecommendationDTO]:
        # Метод отримання рекомендацій, якого бракувало на схемі
        return self.recommendation_use_case.recommend(mission_id, limit=limit)