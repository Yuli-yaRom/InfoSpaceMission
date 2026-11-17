from datetime import datetime
from pydantic import BaseModel


# Опис однієї згенерованої місії від генератора
class IngestMissionItemDTO(BaseModel):
    name: str
    agency_id: str
    celestial_body_id: str
    mission_type: str
    description: str

# Запит із пачкою згенерованих місій для масового завантаження
class BatchIngestRequest(BaseModel):
    batch_id: str
    items: list[IngestMissionItemDTO]

# Відповідь системи про успішний прийом пачки місій
class BatchIngestResponse(BaseModel):
    batch_id: str
    total_received: int
    status: str
    message: str

# Картка рекомендованої схожої місії для відображення на UI
class RecommendationDTO(BaseModel):
    mission_id: str
    name: str
    similarity_reason: str

# Базовий клас для всіх подій системи з ідентифікатором та часом
class DomainEvent(BaseModel):
    event_id: str
    occurred_at: datetime

# Подія: створено нову космічну місію
class MissionCreatedEvent(DomainEvent):
    mission_id: str
    name: str
    agency_id: str
    celestial_body_id: str

# Подія: астронавта призначено до складу екіпажу
class AstronautAssignedToMissionEvent(DomainEvent):
    mission_id: str
    astronaut_id: str

# Подія: під час місії зроблено наукове відкриття
class DiscoveryRecordedEvent(DomainEvent):
    mission_id: str
    discovery_title: str