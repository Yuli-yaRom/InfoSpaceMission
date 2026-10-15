from application.ports_in import GetRecommendationsUseCase, CreateMissionUseCase
from application.ports_out import GraphQueryPort, EventPublisherPort
from domain.dtos import RecommendationDTO, IngestMissionItemDTO, MissionCreatedEvent
from datetime import datetime
import uuid

# Сервіс бізнес-логіки для розрахунку рекомендацій через граф
class RecommendationService(GetRecommendationsUseCase):
    def __init__(self, graph_port: GraphQueryPort):
        self.graph_port = graph_port

    def recommend(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        raise NotImplementedError("TODO: Implement graph recommendation traversal logic")

# Реалізація порту створення місії (на схемі Рис. 3.1 це MissionService)
class MissionService(CreateMissionUseCase):
    def __init__(self, event_publisher: EventPublisherPort):
        self.event_publisher = event_publisher

    def execute(self, command: IngestMissionItemDTO):
        mission_id = str(uuid.uuid4())
        event = MissionCreatedEvent(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.utcnow(),
            mission_id=mission_id,
            name=command.name,
            agency_id=command.agency_id,
            celestial_body_id=command.celestial_body_id,
        )
        self.event_publisher.publish(event)