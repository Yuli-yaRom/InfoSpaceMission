from command_service.ports.input import (
    CreateMissionUseCase,
    AssignAstronautUseCase,
    RecordDiscoveryUseCase,
)
from command_service.ports.output import MissionRepositoryPort, EventPublisherPort
from command_service.dtos import (
    CreateMissionRequest,
    CreateMissionResponse,
    AssignAstronautRequest,
    RecordDiscoveryRequest,
)


class MissionService(CreateMissionUseCase, AssignAstronautUseCase, RecordDiscoveryUseCase):
    def __init__(
        self,
        mission_repository: MissionRepositoryPort,
        event_publisher: EventPublisherPort,
    ):
        self._mission_repository = mission_repository
        self._event_publisher = event_publisher

    def create_mission(self, request: CreateMissionRequest) -> CreateMissionResponse:
        # TODO: побудувати доменну сутність Mission зі статусом PLANNED
        raise NotImplementedError

    def assign_astronaut(self, request: AssignAstronautRequest) -> None:
        # TODO: знайти місію через find_by_id, додати астронавта, зберегти,
        # опублікувати подію "AstronautAssignedToMission"
        raise NotImplementedError

    def record_discovery(self, request: RecordDiscoveryRequest) -> None:
        # TODO: зафіксувати відкриття для місії
        raise NotImplementedError