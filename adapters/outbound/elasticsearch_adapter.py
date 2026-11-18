"""Адаптер пошукового індексу (Elasticsearch 8): повнотекстовий пошук і схожі місії."""
from typing import Any, Dict, List

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from application.mission_ports import MissionSearchPort
from domain.mission_dtos import MissionDTO, MissionFilter, MissionPageDTO

_MAPPING = {
    "properties": {
        "name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
        "description": {"type": "text"},
        "type": {"type": "keyword", "normalizer": "lower"},
        "status": {"type": "keyword", "normalizer": "lower"},
        "agency_id": {"type": "keyword", "normalizer": "lower"},
        "celestial_body_id": {"type": "keyword", "normalizer": "lower"},
        "launch_year": {"type": "integer"},
    }
}
_SETTINGS = {"analysis": {"normalizer": {"lower": {"type": "custom", "filter": ["lowercase"]}}}}


class ElasticsearchMissionSearchAdapter(MissionSearchPort):
    def __init__(self, url: str = "http://localhost:9200", index: str = "missions",
                 client: Any = None):
        self._es = client or AsyncElasticsearch(url)
        self._index = index

    async def init_index(self) -> None:
        if not await self._es.indices.exists(index=self._index):
            await self._es.indices.create(index=self._index, mappings=_MAPPING, settings=_SETTINGS)

    async def close(self) -> None:
        await self._es.close()

    async def index_many(self, missions: List[MissionDTO]) -> None:
        actions = ({"_index": self._index, "_id": m.id,
                    "_source": m.model_dump(exclude={"id"})} for m in missions)
        # Без примусового refresh: документи з'являються в пошуку протягом ~1 с (refresh_interval),
        # що не гальмує генератор на тисячах док/с
        await async_bulk(self._es, actions)

    @staticmethod
    def _filters(flt: MissionFilter) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for field, value in (("celestial_body_id", flt.body), ("type", flt.type), ("status", flt.status)):
            if value:
                out.append({"term": {field: value.lower()}})
        rng = {k: v for k, v in (("gte", flt.year_from), ("lte", flt.year_to)) if v is not None}
        if rng:
            out.append({"range": {"launch_year": rng}})
        return out

    @staticmethod
    def _to_dto(hit: Dict[str, Any]) -> MissionDTO:
        return MissionDTO(id=hit["_id"], **hit["_source"])

    async def search(self, text: str, flt: MissionFilter) -> MissionPageDTO:
        must = ([{"multi_match": {"query": text, "fields": ["name^3", "description"],
                                  "fuzziness": "AUTO"}}] if text.strip() else [{"match_all": {}}])
        res = await self._es.search(
            index=self._index, from_=flt.offset, size=flt.limit, track_total_hits=True,
            query={"bool": {"must": must, "filter": self._filters(flt)}})
        hits = res["hits"]
        return MissionPageDTO(total=hits["total"]["value"], items=[self._to_dto(h) for h in hits["hits"]])

    async def find_similar(self, mission: MissionDTO, limit: int) -> List[MissionDTO]:
        res = await self._es.search(index=self._index, size=limit, query={
            "bool": {
                "should": [
                    {"more_like_this": {"fields": ["name", "description"],
                                        "like": [{"_index": self._index, "_id": mission.id}],
                                        "min_term_freq": 1, "min_doc_freq": 1}},
                    {"term": {"celestial_body_id": {"value": mission.celestial_body_id.lower(), "boost": 2}}},
                    {"term": {"type": {"value": mission.type.lower(), "boost": 1.5}}},
                    {"term": {"agency_id": {"value": mission.agency_id.lower(), "boost": 0.5}}},
                ],
                "minimum_should_match": 1,
                "must_not": [{"ids": {"values": [mission.id]}}],
            }})
        return [self._to_dto(h) for h in res["hits"]["hits"]]
