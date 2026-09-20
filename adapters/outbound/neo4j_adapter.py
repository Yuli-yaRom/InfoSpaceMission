from application.ports_out import GraphQueryPort
from domain.dtos import RecommendationDTO


# Адаптер для виконання Cypher-запитів до графової бази даних Neo4j
class Neo4jGraphAdapter(GraphQueryPort):
    def __init__(self, uri: str = "bolt://localhost:7687", auth: tuple[str, str] = ("neo4j", "password")):
        self.uri = uri
        self.auth = auth

    def upsert_mission_node(self, mission_id: str, name: str) -> None:
        raise NotImplementedError("TODO: Implement Neo4j upsert_mission_node")

    def upsert_relationship(self, from_id: str, to_id: str, rel_type: str) -> None:
        raise NotImplementedError("TODO: Implement Neo4j upsert_relationship")

    def find_related(self, mission_id: str, limit: int = 5) -> list[RecommendationDTO]:
        raise NotImplementedError("TODO: Implement Neo4j find_related")