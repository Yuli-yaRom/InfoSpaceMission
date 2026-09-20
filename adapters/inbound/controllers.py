from application.ports_in import BatchIngestUseCase, GetRecommendationsUseCase
from domain.dtos import BatchIngestRequest, BatchIngestResponse, RecommendationDTO


# Контролер для прийому високонавантаженого потоку місій від генератора
class GeneratorIngestController:
    def __init__(self, ingest_use_case: BatchIngestUseCase):
        self.ingest_use_case = ingest_use_case

    def handle_batch_ingest(self, request: BatchIngestRequest) -> BatchIngestResponse:
        raise NotImplementedError("TODO: Ingest batch endpoint")

# Контролер для запиту списку рекомендацій із веб-інтерфейсу
class RecommendationQueryController:
    def __init__(self, recommendation_use_case: GetRecommendationsUseCase):
        self.recommendation_use_case = recommendation_use_case

    def get_recommendation(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        raise NotImplementedError("TODO: Recommendations endpoint")