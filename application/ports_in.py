from abc import ABC, abstractmethod
from domain.dtos import BatchIngestRequest, BatchIngestResponse, RecommendationDTO


# Контракт на отримання списку схожих рекомендованих місій
class GetRecommendationsUseCase(ABC):
    @abstractmethod
    def recommend(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        pass

# Контракт на прийом і завантаження пачки місій від генератора
class BatchIngestUseCase(ABC):
    @abstractmethod
    def ingest(self, request: BatchIngestRequest) -> BatchIngestResponse:
        pass