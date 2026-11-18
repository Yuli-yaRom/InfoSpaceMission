"""Адаптер кешу рекомендацій (Redis). Записи живуть TTL секунд."""
import json
from typing import Any, List, Optional

from redis.asyncio import Redis

from application.mission_ports import RecommendationCachePort
from domain.dtos import RecommendationDTO


class RedisRecommendationCacheAdapter(RecommendationCachePort):
    def __init__(self, url: str = "redis://localhost:6379/0", ttl_seconds: int = 300,
                 client: Any = None):
        self._redis = client or Redis.from_url(url, decode_responses=True)
        self._ttl = ttl_seconds

    @staticmethod
    def _key(mission_id: str) -> str:
        return f"recs:{mission_id}"

    async def get(self, mission_id: str) -> Optional[List[RecommendationDTO]]:
        raw = await self._redis.get(self._key(mission_id))
        if raw is None:
            return None
        return [RecommendationDTO(**d) for d in json.loads(raw)]

    async def put(self, mission_id: str, items: List[RecommendationDTO]) -> None:
        payload = json.dumps([i.model_dump() for i in items], ensure_ascii=False)
        await self._redis.set(self._key(mission_id), payload, ex=self._ttl)

    async def close(self) -> None:
        await self._redis.aclose()
