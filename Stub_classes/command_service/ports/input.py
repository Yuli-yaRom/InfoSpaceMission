from abc import ABC, abstractmethod

from command_service.dtos import (
    CreateMissionRequest,
    CreateMissionResponse,
    AssignAstronautRequest,
    RecordDiscoveryRequest,
)


class CreateMissionUseCase(ABC):
    @abstractmethod
    def create_mission(self, request: CreateMissionRequest) -> CreateMissionResponse:
        # TODO: реалізувати у MissionService; публікує подію MissionCreated
        raise NotImplementedError


class AssignAstronautUseCase(ABC):
    @abstractmethod
    def assign_astronaut(self, request: AssignAstronautRequest) -> None:
        # TODO: реалізувати у MissionService; публікує подію AstronautAssignedToMission
        raise NotImplementedError


class RecordDiscoveryUseCase(ABC):
    @abstractmethod
    def record_discovery(self, request: RecordDiscoveryRequest) -> None:
        # TODO: реалізувати у MissionService; публікує подію DiscoveryRecorded
        raise NotImplementedError