"""Тимчасовий адаптер у пам'яті. Замінюється на Mongo/SQL-адаптер того ж порту."""
import asyncio
import random
import uuid
from typing import List, Optional

from application.mission_ports import GeneratorControlPort, MissionRepositoryPort
from domain.mission_dtos import MissionDTO, MissionFilter

_SEED = [
    ("Curiosity", "ROVER", "COMPLETED", "NASA", "mars", 2011),
    ("Perseverance", "ROVER", "ACTIVE", "NASA", "mars", 2020),
    ("Opportunity", "ROVER", "COMPLETED", "NASA", "mars", 2003),
    ("Zhurong", "ROVER", "COMPLETED", "CNSA", "mars", 2020),
    ("Mars Express", "ORBITER", "ACTIVE", "ESA", "mars", 2003),
    ("Mars Polar Lander", "LANDER", "FAILED", "NASA", "mars", 1999),
    ("Apollo 11", "CREWED", "COMPLETED", "NASA", "moon", 1969),
    ("Chandrayaan-3", "LANDER", "COMPLETED", "ISRO", "moon", 2023),
    ("Luna 9", "LANDER", "COMPLETED", "Roscosmos", "moon", 1966),
    ("Artemis III", "CREWED", "PLANNED", "NASA", "moon", 2027),
    ("Voyager 2", "PROBE", "ACTIVE", "NASA", "neptune", 1977),
]


class InMemoryMissionRepository(MissionRepositoryPort):
    def __init__(self, seed: bool = True):
        self._items: List[MissionDTO] = []
        if seed:
            for name, typ, status, agency, body, year in _SEED:
                self._items.append(MissionDTO(
                    id=str(uuid.uuid4()), name=name, type=typ, status=status,
                    agency_id=agency, celestial_body_id=body, launch_year=year))

    @staticmethod
    def _match(m: MissionDTO, f: MissionFilter) -> bool:
        return ((f.body is None or m.celestial_body_id.lower() == f.body.lower())
                and (f.type is None or m.type.lower() == f.type.lower())
                and (f.status is None or m.status.lower() == f.status.lower())
                and (f.year_from is None or m.launch_year >= f.year_from)
                and (f.year_to is None or m.launch_year <= f.year_to))

    async def save(self, mission: MissionDTO) -> None:
        self._items.append(mission)

    async def find_by_id(self, mission_id: str) -> Optional[MissionDTO]:
        return next((m for m in self._items if m.id == mission_id), None)

    async def find_by_filters(self, flt: MissionFilter) -> List[MissionDTO]:
        found = sorted((m for m in self._items if self._match(m, flt)),
                       key=lambda m: m.launch_year, reverse=True)
        return found[flt.offset: flt.offset + flt.limit]

    async def count(self, flt: Optional[MissionFilter] = None) -> int:
        if flt is None:
            return len(self._items)
        return sum(1 for m in self._items if self._match(m, flt))


class FakeGeneratorAdapter(GeneratorControlPort):
    """Імітація генератора (замінюється на генератор Катерини)."""
    MAX_DOCS = 100_000
    TICK = 0.1

    def __init__(self, repository: InMemoryMissionRepository, rate: int = 100):
        self._repo = repository
        self._rate = rate

    def get_rate(self) -> int:
        return self._rate

    def set_rate(self, docs_per_second: int) -> None:
        self._rate = docs_per_second

    async def run(self) -> None:
        carry = 0.0
        while True:
            await asyncio.sleep(self.TICK)
            carry += self._rate * self.TICK
            n, carry = int(carry), carry - int(carry)
            n = min(n, self.MAX_DOCS - len(self._repo._items))
            for _ in range(n):
                typ, status = random.choice(["ROVER", "ORBITER", "LANDER", "CREWED", "PROBE"]), \
                    random.choice(["ACTIVE", "COMPLETED", "FAILED", "PLANNED"])
                self._repo._items.append(MissionDTO(
                    id=str(uuid.uuid4()), name=f"Gen-{random.randint(1, 10**6)}", type=typ,
                    status=status, agency_id=random.choice(["NASA", "ESA", "ISRO"]),
                    celestial_body_id=random.choice(["mars", "moon", "venus"]),
                    launch_year=random.randint(1960, 2030)))
