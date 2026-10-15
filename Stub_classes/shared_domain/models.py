from dataclasses import dataclass
from datetime import date
from typing import Optional

from shared_domain.enums import MissionType, MissionStatus, AstronautStatus


@dataclass
class Agency:
    id: str
    name: str
    abbrev: str
    country: str
    type: str
    founding_year: int


@dataclass
class Astronaut:
    id: str
    agency_id: str
    name: str
    status: AstronautStatus
    nationality: str
    date_of_birth: date
    date_of_death: Optional[date] = None
    time_in_space: int = 0  
    eva_time: int = 0       
    bio: str = ""


@dataclass
class CelestialBody:
    id: str
    name: str
    type: str
    diameter: float
    mass: float
    gravity: float
    description: str = ""


@dataclass
class Rocket:
    id: str
    name: str
    full_name: str
    manufacturer: str
    reusable: bool
    length: float
    diameter: float
    leo_capacity: float
    launch_cost: float
    total_launch_count: int = 0
    successful_launches: int = 0
    failed_launches: int = 0


@dataclass
class SpaceStation:
    id: str
    name: str
    status: str
    founded: date
    orbit: str
    type: str
    deorbited: Optional[date] = None
    description: str = ""


@dataclass
class Mission:
    id: str
    agency_id: str
    celestial_body_id: str
    rocket_id: str
    name: str
    type: MissionType
    status: MissionStatus
    net: date 
    description: str = ""


@dataclass
class Spacecraft:
    id: str
    mission_id: str
    agency_id: str
    name: str
    serial_number: str
    status: str
    time_in_space: int = 0  # у годинах