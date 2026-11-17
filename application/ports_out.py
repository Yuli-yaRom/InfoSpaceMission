from abc import ABC, abstractmethod
from domain.dtos import RecommendationDTO, DomainEvent

# Контракт для збереження та читання рекомендацій у Redis (кеш рекомендацій)
class RecommendationStorePort(ABC):
    @abstractmethod
    def save_recommendations(self, mission_id: str, items: list[RecommendationDTO]) -> None:
        pass

    @abstractmethod
    def get_recommendations(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        pass

# Контракт для відправки подій у чергу повідомлень Kafka
class EventPublisherPort(ABC):
    @abstractmethod
    def publish(self, event: DomainEvent) -> None:
        pass