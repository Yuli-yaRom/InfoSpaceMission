from typing import List

from command_service.ports.input import (
    CreateMissionUseCase,
    AssignAstronautUseCase,
    RecordDiscoveryUseCase,
)
from command_service.dtos import (
    CreateMissionRequest,
    CreateMissionResponse,
    AssignAstronautRequest,
    RecordDiscoveryRequest,
)


class MissionRestController:
    def __init__(
        self,
        create_mission_use_case: CreateMissionUseCase,
        assign_astronaut_use_case: AssignAstronautUseCase,
        record_discovery_use_case: RecordDiscoveryUseCase,
    ):
        self._create_mission_use_case = create_mission_use_case
        self._assign_astronaut_use_case = assign_astronaut_use_case
        self._record_discovery_use_case = record_discovery_use_case

    def post_mission(self, request: CreateMissionRequest) -> CreateMissionResponse:
        # TODO: викликати self._create_mission_use_case.create_mission(request)
        raise NotImplementedError

    def post_assign_astronaut(self, request: AssignAstronautRequest) -> None:
        # TODO: викликати self._assign_astronaut_use_case.assign_astronaut(request)
        raise NotImplementedError

    def post_discovery(self, request: RecordDiscoveryRequest) -> None:
        # TODO: викликати self._record_discovery_use_case.record_discovery(request)
        raise NotImplementedError


class GeneratorIngestController:
    def __init__(self, create_mission_use_case: CreateMissionUseCase):
        self._create_mission_use_case = create_mission_use_case

    def ingest_batch(self, requests: List[CreateMissionRequest]) -> List[CreateMissionResponse]:
        # TODO: пройтись по requests, викликати create_mission для кожного
        raise NotImplementedError