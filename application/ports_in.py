from abc import ABC, abstractmethod
from domain.dtos import RecommendationDTO, IngestMissionItemDTO

# Контракт на отримання списку схожих рекомендованих місій
class GetRecommendationsUseCase(ABC):
    @abstractmethod
    def recommend(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        pass

# Єдиний вхідний порт для створення місій відповідно до схеми Command (Рис. 3.1)
class CreateMissionUseCase(ABC):
    @abstractmethod
    def execute(self, command: IngestMissionItemDTO):
        pass