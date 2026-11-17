from dataclasses import dataclass
from datetime import date


@dataclass
class CreateMissionRequest:
    agency_id: str
    celestial_body_id: str
    rocket_id: str
    name: str
    type: str
    net: date
    description: str = ""


@dataclass
class CreateMissionResponse:
    id: str
    name: str
    status: str


@dataclass
class AssignAstronautRequest:
    mission_id: str
    astronaut_id: str


@dataclass
class RecordDiscoveryRequest:
    mission_id: str
    celestial_body_id: str
    description: str