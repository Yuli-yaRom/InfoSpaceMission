from abc import ABC, abstractmethod
from typing import List, Optional

from shared_domain.models import Mission


class MissionRepositoryPort(ABC):
    @abstractmethod
    def save(self, mission: Mission) -> Mission:
        raise NotImplementedError

    @abstractmethod
    def find_by_id(self, mission_id: str) -> Optional[Mission]:
        raise NotImplementedError

    @abstractmethod
    def find_all(self) -> List[Mission]:
        raise NotImplementedError

    @abstractmethod
    def update(self, mission: Mission) -> Mission:
        raise NotImplementedError

    @abstractmethod
    def delete(self, mission_id: str) -> None:
        raise NotImplementedError


class EventPublisherPort(ABC):
    @abstractmethod
    def publish(self, topic: str, payload: dict) -> None:
        raise NotImplementedError