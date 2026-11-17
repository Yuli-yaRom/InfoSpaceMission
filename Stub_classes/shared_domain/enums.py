from enum import Enum


class MissionType(Enum):
    ORBITAL = "ORBITAL"
    LUNAR = "LUNAR"
    INTERPLANETARY = "INTERPLANETARY"
    DEEP_SPACE = "DEEP_SPACE"


class MissionStatus(Enum):
    PLANNED = "PLANNED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class AstronautStatus(Enum):
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"
    DECEASED = "DECEASED"