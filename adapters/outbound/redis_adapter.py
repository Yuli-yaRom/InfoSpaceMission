"""Redis-адаптери (п. 15): кеш рекомендацій, кеш пошуку (cache-aside + TTL),
список останніх запитів (LPUSH + LTRIM)."""
import json
from typing import Any, List, Optional

from redis.asyncio import Redis

from application.mission_ports import (
    RecentQueriesPort, RecommendationCachePort, SearchCachePort,
)
from domain.dtos import RecommendationDTO
from domain.mission_dtos import RecentQueryDTO, SearchPageDTO


def make_client(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


class RedisRecommendationCacheAdapter(RecommendationCachePort):
    def __init__(self, url: str = "redis://localhost:6379/0", ttl_seconds: int = 300,
                 client: Any = None):
        self._redis = client or make_client(url)
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


class RedisSearchCacheAdapter(SearchCachePort):
    """Ключ — search:<sha1 нормалізованого запиту> (див. application/search_normalization.py)."""

    def __init__(self, client: Any):
        self._redis = client

    async def get(self, key: str) -> Optional[SearchPageDTO]:
        raw = await self._redis.get(key)
        return SearchPageDTO.model_validate_json(raw) if raw is not None else None

    async def put(self, key: str, page: SearchPageDTO, ttl_seconds: int) -> None:
        await self._redis.set(key, page.model_dump_json(), ex=ttl_seconds)


class RedisRecentQueriesAdapter(RecentQueriesPort):
    KEY = "recent_queries"

    def __init__(self, client: Any):
        self._redis = client

    async def push(self, entry: RecentQueryDTO, keep: int = 20) -> None:
        pipe = self._redis.pipeline()
        pipe.lpush(self.KEY, entry.model_dump_json())
        pipe.ltrim(self.KEY, 0, keep - 1)
        await pipe.execute()

    async def list(self, limit: int = 20) -> List[RecentQueryDTO]:
        raw = await self._redis.lrange(self.KEY, 0, limit - 1)
        return [RecentQueryDTO.model_validate_json(r) for r in raw]
