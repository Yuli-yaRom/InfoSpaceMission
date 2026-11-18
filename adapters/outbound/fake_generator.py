"""Імітація генератора (замінюється на генератор Катерини).
Пише через MissionCommandUseCase, тож дані потрапляють і в SQL, і в Elasticsearch."""
import asyncio
import logging
import random

from application.mission_ports import GeneratorControlPort, MissionCommandUseCase
from domain.mission_dtos import MissionCreateRequest

log = logging.getLogger(__name__)

# Шаблони звітів з різними формами слів (landed / landing / lands, sample / sampled / sampling),
# щоб стемінг аналізатора english був помітний у пошуку, а word count (п. 14) мав реальні дані
_OPENINGS = [
    "The {kind} landed safely on {body} after a long cruise.",
    "Landing on {body} was confirmed by the ground station.",
    "The {kind} lands near an old impact crater on {body}.",
    "The {kind} entered orbit around {body} and began mapping the surface.",
    "Mission control lost contact with the {kind} during descent to {body}.",
]
_FINDINGS = [
    "Instruments sampled the soil and detected traces of {thing}.",
    "Sampling of the regolith revealed {thing}.",
    "The team collected a rock sample rich in {thing}.",
    "Cameras photographed layered terrain that may contain {thing}.",
    "Radar measurements suggest {thing} below the surface.",
    "Spectrometers measured {thing} in the thin atmosphere.",
]
_CLOSINGS = [
    "The data were transmitted to Earth for analysis.",
    "Engineers are planning an extended mission.",
    "A dust storm reduced solar power for several weeks.",
    "The results will guide future crewed exploration.",
    "",
]
_THINGS = ["water ice", "methane", "iron oxide", "sulphur", "ancient sediments", "organic molecules",
           "volcanic basalt", "frozen carbon dioxide"]
_KINDS = {"ROVER": "rover", "ORBITER": "orbiter", "LANDER": "lander", "CREWED": "crew", "PROBE": "probe"}


class FakeGeneratorAdapter(GeneratorControlPort):
    MAX_DOCS = 100_000
    TICK = 0.1

    def __init__(self, commands: MissionCommandUseCase, rate: int = 100):
        self._commands = commands
        self._rate = rate
        self._generated = 0

    def get_rate(self) -> int:
        return self._rate

    def set_rate(self, docs_per_second: int) -> None:
        self._rate = docs_per_second

    @staticmethod
    def _random_report(kind: str, body: str) -> str:
        parts = [random.choice(_OPENINGS), random.choice(_FINDINGS), random.choice(_CLOSINGS)]
        text = " ".join(p for p in parts if p)
        return text.format(kind=_KINDS.get(kind, "spacecraft"), body=body.capitalize(),
                           thing=random.choice(_THINGS))

    @classmethod
    def _random_request(cls) -> MissionCreateRequest:
        typ = random.choice(["ROVER", "ORBITER", "LANDER", "CREWED", "PROBE"])
        body = random.choice(["mars", "moon", "venus"])
        return MissionCreateRequest(
            name=f"Gen-{random.randint(1, 10**6)}",
            type=typ,
            status=random.choice(["ACTIVE", "COMPLETED", "FAILED", "PLANNED"]),
            agency_id=random.choice(["NASA", "ESA", "ISRO"]),
            celestial_body_id=body,
            launch_year=random.randint(1960, 2030),
            report=cls._random_report(typ, body))

    async def run(self) -> None:
        carry = 0.0
        while True:
            await asyncio.sleep(self.TICK)
            carry += self._rate * self.TICK
            n, carry = int(carry), carry - int(carry)
            n = min(n, self.MAX_DOCS - self._generated)
            if n <= 0:
                continue
            try:
                await self._commands.create_missions([self._random_request() for _ in range(n)])
                self._generated += n
            except Exception:
                log.exception("Генератор: не вдалося записати пачку з %d місій", n)
