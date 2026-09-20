from application.ports_in import GetRecommendationsUseCase, BatchIngestUseCase
from application.ports_out import GraphQueryPort, EventPublisherPort
from domain.dtos import BatchIngestRequest, BatchIngestResponse, RecommendationDTO


# Сервіс бізнес-логіки для розрахунку рекомендацій через граф
class RecommendationService(GetRecommendationsUseCase):
    def __init__(self, graph_port: GraphQueryPort):
        self.graph_port = graph_port

    def recommend(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        raise NotImplementedError("TODO: Implement graph recommendation traversal logic")

# Сервіс прийому потоку даних та публікації нових подій у систему
class GeneratorIngestService(BatchIngestUseCase):
    def __init__(self, event_publisher: EventPublisherPort):
        self.event_publisher = event_publisher

    def ingest(self, request: BatchIngestRequest) -> BatchIngestResponse:
        raise NotImplementedError("TODO: Implement batch ingestion logic")