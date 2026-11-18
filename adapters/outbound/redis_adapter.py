from application.ports_out import RecommendationStorePort
from domain.dtos import RecommendationDTO


# Адаптер для кешу рекомендацій у Redis
class RedisRecommendationAdapter(RecommendationStorePort):
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port

    def save_recommendations(self, mission_id: str, items: list[RecommendationDTO]) -> None:
        raise NotImplementedError("TODO: Implement Redis save_recommendations")

    def get_recommendations(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        raise NotImplementedError("TODO: Implement Redis get_recommendations")
