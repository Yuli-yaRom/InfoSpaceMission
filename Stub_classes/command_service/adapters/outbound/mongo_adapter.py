from typing import List, Optional

from command_service.ports.output import MissionRepositoryPort
from shared_domain.models import Mission


class MongoMissionRepositoryAdapter(MissionRepositoryPort):
    def __init__(self, mongo_client=None):
        self._mongo_client = mongo_client  # TODO: підключення до MongoDB

    def save(self, mission: Mission) -> Mission:
        raise NotImplementedError

    def find_by_id(self, mission_id: str) -> Optional[Mission]:
        raise NotImplementedError

    def find_all(self) -> List[Mission]:
        raise NotImplementedError

    def update(self, mission: Mission) -> Mission:
        raise NotImplementedError

    def delete(self, mission_id: str) -> None:
        raise NotImplementedError