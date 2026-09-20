from abc import ABC, abstractmethod
from domain.dtos import RecommendationDTO, DomainEvent


# Контракт для роботи з вершинами та зв'язками в графовій базі Neo4j
class GraphQueryPort(ABC):
    @abstractmethod
    def upsert_mission_node(self, mission_id: str, name: str) -> None:
        pass

    @abstractmethod
    def upsert_relationship(self, from_id: str, to_id: str, rel_type: str) -> None:
        pass

    @abstractmethod
    def find_related(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        pass

# Контракт для відправки подій у чергу повідомлень Kafka
class EventPublisherPort(ABC):
    @abstractmethod
    def publish(self, event: DomainEvent) -> None:
        pass