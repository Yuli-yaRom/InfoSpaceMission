from pydantic import BaseModel


# Картка рекомендованої схожої місії для відображення на UI
class RecommendationDTO(BaseModel):
    mission_id: str
    name: str
    similarity_reason: str
