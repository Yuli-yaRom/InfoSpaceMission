"""Адаптер пошукового індексу (Elasticsearch 8): повнотекстовий пошук і схожі місії (п. 15).

Маппінг: report/description — text з analyzer: english (стемінг: landing ≈ landed ≈ lands),
фільтри — keyword (з нормалізатором до нижнього регістру).
"""
from typing import Any, Dict, List

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from application.mission_ports import MissionSearchPort
from domain.mission_dtos import MissionDTO, MissionFilter, SearchHitDTO, SearchPageDTO

INDEX = "missions-v2"  # v2: додано report + english-аналізатор

_MAPPING = {
    "properties": {
        "name": {"type": "text", "analyzer": "english", "fields": {"raw": {"type": "keyword"}}},
        "description": {"type": "text", "analyzer": "english"},
        "report": {"type": "text", "analyzer": "english"},
        "type": {"type": "keyword", "normalizer": "lower"},
        "status": {"type": "keyword", "normalizer": "lower"},
        "agency_id": {"type": "keyword", "normalizer": "lower"},
        "celestial_body_id": {"type": "keyword", "normalizer": "lower"},
        "launch_year": {"type": "integer"},
    }
}
_SETTINGS = {"analysis": {"normalizer": {"lower": {"type": "custom", "filter": ["lowercase"]}}}}

_HIGHLIGHT = {
    "pre_tags": ["<mark>"],
    "post_tags": ["</mark>"],
    "encoder": "html",  # екранує текст, тож UI може безпечно вставити фрагмент як HTML
    "fields": {
        "report": {"fragment_size": 150, "number_of_fragments": 2},
        "description": {"fragment_size": 150, "number_of_fragments": 1},
        "name": {"number_of_fragments": 0},
    },
}


class ElasticsearchMissionSearchAdapter(MissionSearchPort):
    def __init__(self, url: str = "http://localhost:9200", index: str = INDEX, client: Any = None):
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

    async def count(self) -> int:
        return int((await self._es.count(index=self._index))["count"])

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

    async def search(self, text: str, flt: MissionFilter) -> SearchPageDTO:
        has_text = bool(text.strip())
        must = ([{"multi_match": {"query": text, "fields": ["name^3", "report", "description"],
                                  "fuzziness": "AUTO"}}] if has_text else [{"match_all": {}}])
        kwargs: Dict[str, Any] = dict(
            index=self._index, from_=flt.offset, size=flt.limit, track_total_hits=True,
            query={"bool": {"must": must, "filter": self._filters(flt)}})
        if has_text:
            kwargs["highlight"] = _HIGHLIGHT
        else:
            kwargs["sort"] = [{"launch_year": "desc"}]
        res = await self._es.search(**kwargs)
        hits = res["hits"]
        return SearchPageDTO(
            total=hits["total"]["value"],
            items=[SearchHitDTO(mission=self._to_dto(h), highlights=h.get("highlight", {}))
                   for h in hits["hits"]])

    async def find_similar(self, mission: MissionDTO, limit: int) -> List[MissionDTO]:
        res = await self._es.search(index=self._index, size=limit, query={
            "bool": {
                "should": [
                    {"more_like_this": {"fields": ["name", "report", "description"],
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
