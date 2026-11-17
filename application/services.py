from application.ports_in import GetRecommendationsUseCase, CreateMissionUseCase
from application.ports_out import RecommendationStorePort, EventPublisherPort
from domain.dtos import RecommendationDTO, IngestMissionItemDTO, MissionCreatedEvent
from datetime import datetime
import uuid

# Сервіс бізнес-логіки для видачі рекомендацій (читає їх із Redis)
class RecommendationService(GetRecommendationsUseCase):
    def __init__(self, store_port: RecommendationStorePort):
        self.store_port = store_port

    def recommend(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        return self.store_port.get_recommendations(mission_id, limit)

# Реалізація порту створення місії (на схемі Рис. 3.1 це MissionService)
class MissionService(CreateMissionUseCase):
    def __init__(self, event_publisher: EventPublisherPort):
        self.event_publisher = event_publisher

    def execute(self, command: IngestMissionItemDTO):
        # Логіка обробки команди та публікації події в брокер
        mission_id = str(uuid.uuid4())  # генеруємо id один раз
        event = MissionCreatedEvent(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.utcnow(),
            mission_id=mission_id,
            name=command.name,
            agency_id=command.agency_id,
            celestial_body_id=command.celestial_body_id
        )
        self.event_publisher.publish(event)