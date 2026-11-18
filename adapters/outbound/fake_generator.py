"""Імітація генератора (замінюється на генератор Катерини).
Пише через MissionCommandUseCase, тож дані потрапляють і в SQL, і в Elasticsearch."""
import asyncio
import logging
import random

from application.mission_ports import GeneratorControlPort, MissionCommandUseCase
from domain.mission_dtos import MissionCreateRequest

log = logging.getLogger(__name__)


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
    def _random_request() -> MissionCreateRequest:
        return MissionCreateRequest(
            name=f"Gen-{random.randint(1, 10**6)}",
            type=random.choice(["ROVER", "ORBITER", "LANDER", "CREWED", "PROBE"]),
            status=random.choice(["ACTIVE", "COMPLETED", "FAILED", "PLANNED"]),
            agency_id=random.choice(["NASA", "ESA", "ISRO"]),
            celestial_body_id=random.choice(["mars", "moon", "venus"]),
            launch_year=random.randint(1960, 2030))

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
