"""Генератор навантаження (імітація джерела даних; замінюється на генератор Катерини —
достатньо реалізувати той самий GeneratorControlPort).

Як пише:
* продюсер створює пачки (`batch_size` рядків) і кладе їх у буфер: у режимі «N док/с» — рівно
  стільки, скільки задано, у режимі «максимум» — щоб записувачі ніколи не простоювали;
* `writers` паралельних записувачів беруть пачки з буфера й пишуть у БД через COPY;
* якщо запис не вдався (вузол недоступний), пачка НЕ відкидається: записувач повторює її
  (ідемпотентно, ON CONFLICT DO NOTHING) з паузою, поки БД не прийме. Тим часом буфер
  накопичує нові пачки до `buffer_limit` рядків, далі продюсер пригальмовує (backpressure).
  Отже згенеровано = записано + у буфері — жоден рядок не губиться;
* окремий цикл (бонус) виконує UPDATE / DELETE за id і масові UPDATE під час запису.
"""
import asyncio
import logging
import random
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Callable, Deque, Dict, List, Optional, Tuple

from application.mission_ports import GeneratorControlPort
from application.mission_services import MissionCommandService
from application.write_ledger import now_iso
from domain.mission_dtos import (
    GeneratorSettingsDTO, GeneratorSettingsPatch, GeneratorStatusDTO, MissionDTO, MissionFilter,
)

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

_TYPES = ["ROVER", "ORBITER", "LANDER", "CREWED", "PROBE"]
_BODIES = ["mars", "moon", "venus"]
_STATUSES = ["ACTIVE", "COMPLETED", "FAILED", "PLANNED"]
_AGENCIES = ["NASA", "ESA", "ISRO"]

EventSink = Callable[[str, str], None]  # (level, message)


class _Batch:
    __slots__ = ("rows", "attempts")

    def __init__(self, rows: List[MissionDTO]):
        self.rows = rows
        self.attempts = 0


class _IdPool:
    """Кільцевий буфер id нещодавно вставлених рядків — для випадкових UPDATE/DELETE за id
    (точковий запит, Citus надсилає його в один шард)."""

    def __init__(self, size: int = 200_000):
        self._ids: List[Optional[str]] = []
        self._size = size
        self._pos = 0

    def add(self, ids: List[str]) -> None:
        for i in ids:
            if len(self._ids) < self._size:
                self._ids.append(i)
            else:
                self._ids[self._pos] = i
                self._pos = (self._pos + 1) % self._size

    def sample(self, k: int, remove: bool = False) -> List[str]:
        if not self._ids:
            return []
        out = []
        for _ in range(k):
            j = random.randrange(len(self._ids))
            v = self._ids[j]
            if v is not None:
                out.append(v)
                if remove:
                    self._ids[j] = None
        return out

    def clear(self) -> None:
        self._ids.clear()
        self._pos = 0


class FakeGeneratorAdapter(GeneratorControlPort):
    TICK = 0.1
    MOD_TICK = 0.2

    def __init__(self, commands: MissionCommandService, settings: Optional[GeneratorSettingsDTO] = None,
                 buffer_limit: int = 100_000, retry_max_s: float = 5.0):
        self._cmd = commands
        self._s = settings or GeneratorSettingsDTO()
        self.buffer_limit = buffer_limit
        self._retry_max_s = retry_max_s
        self.on_event: EventSink = lambda level, msg: None

        self._queue: Deque[_Batch] = deque()
        self._queued_rows = 0
        self._in_flight_rows = 0
        self._ids = _IdPool()

        # Лічильники (кумулятивні; MetricsRecorder рахує різниці між знімками)
        self.generated = 0
        self.inserted = 0
        self.updated = 0
        self.deleted = 0
        self.bulk_updates = 0
        self.failed_attempts = 0
        self.sql_ms_sum = 0.0
        self.sql_batches = 0
        self.es_ms_sum = 0.0
        self.es_batches = 0
        self.batch_latencies: Deque[Tuple[float, float]] = deque(maxlen=200_000)  # (monotonic, sql_ms)

        self._last_error: Optional[str] = None
        self._last_error_at: Optional[str] = None
        self._retrying_since: Optional[float] = None
        self._retrying_since_iso: Optional[str] = None
        self._failing: set = set()   # записувачі, що зараз повторюють пачку

        self._gate: Optional[asyncio.Event] = None      # закрито — нові операції з БД чекають
        self._work: Optional[asyncio.Event] = None      # у буфері з'явилась пачка
        self._space: Optional[asyncio.Event] = None     # записувач забрав пачку
        self._active = 0                                # операцій з БД, що виконуються зараз
        self._hold_deletes = 0                          # > 0 — DELETE генератора відкладено
        self._writers: Dict[int, asyncio.Task] = {}

    # ---------- GeneratorControlPort ----------
    def settings(self) -> GeneratorSettingsDTO:
        return self._s

    def update(self, patch: GeneratorSettingsPatch) -> GeneratorSettingsDTO:
        self._s = self._s.model_copy(update=patch.model_dump(exclude_none=True))
        if self._space is not None:
            self._space.set()
        return self._s

    @property
    def pending(self) -> int:
        return self._queued_rows + self._in_flight_rows

    @property
    def state(self) -> str:
        if self._failing:
            return "retrying"
        s = self._s
        if not s.max_speed and s.docs_per_second == 0 and self.pending == 0:
            return "paused"
        return "running"

    def status(self) -> GeneratorStatusDTO:
        return GeneratorStatusDTO(
            settings=self._s, state=self.state, generated=self.generated, inserted=self.inserted,
            pending=self.pending, buffer_limit=self.buffer_limit, failed_attempts=self.failed_attempts,
            updated=self.updated, deleted=self.deleted, bulk_updates=self.bulk_updates,
            es_failures=self._cmd.es_failures, last_error=self._last_error,
            last_error_at=self._last_error_at, retrying_since=self._retrying_since_iso)

    def forget_ids(self) -> None:
        self._ids.clear()

    def counters(self) -> Dict[str, float]:
        return {"inserted": self.inserted, "updated": self.updated, "deleted": self.deleted,
                "failed_attempts": self.failed_attempts, "generated": self.generated,
                "sql_ms_sum": self.sql_ms_sum, "sql_batches": self.sql_batches,
                "es_ms_sum": self.es_ms_sum, "es_batches": self.es_batches, "pending": self.pending}

    def batch_latencies_since(self, t_monotonic: float) -> List[float]:
        return [ms for t, ms in self.batch_latencies if t >= t_monotonic]

    # ---------- Пауза записів на час точного підрахунку ----------
    @asynccontextmanager
    async def quiesced(self, timeout: float = 30.0):
        """Нові операції з БД чекають, поки виконується тіло блоку; поточні — завершуються.
        Тримається лише мить: зафіксувати час БД і лічильники (сам count(*) іде вже без паузи,
        див. application/consistent_count.py)."""
        self._ensure_events()
        self._gate.clear()
        try:
            deadline = time.monotonic() + timeout
            while self._active > 0:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Генератор не завершив поточні записи за {timeout:.0f} с — "
                                       f"точний підрахунок зараз неможливий, спробуйте ще раз")
                await asyncio.sleep(0.02)
            yield
        finally:
            self._gate.set()

    @asynccontextmanager
    async def deletes_held(self):
        self._hold_deletes += 1
        try:
            yield
        finally:
            self._hold_deletes -= 1

    # ---------- Робочі цикли ----------
    def _ensure_events(self) -> None:
        if self._gate is None:
            self._gate = asyncio.Event()
            self._gate.set()
            self._work = asyncio.Event()
            self._space = asyncio.Event()

    async def run(self) -> None:
        self._ensure_events()
        mod_task = asyncio.create_task(self._modifier_loop())
        try:
            await self._producer_loop()
        finally:
            mod_task.cancel()
            for t in self._writers.values():
                t.cancel()

    def _sync_writers(self) -> None:
        for i in range(self._s.writers):
            t = self._writers.get(i)
            if t is None or t.done():
                self._writers[i] = asyncio.create_task(self._writer(i))

    async def _producer_loop(self) -> None:
        carry = 0.0
        last = time.monotonic()
        while True:
            self._sync_writers()
            s = self._s
            if s.max_speed:
                # Тримаємо в буфері ~2 пачки на записувача: БД завжди має роботу,
                # а пам'ять не забивається наперед згенерованими рядками
                target = min(self.buffer_limit, 2 * s.writers * s.batch_size)
                # Дозаповнюємо буфер один раз за ітерацію і повертаємось до перевірки налаштувань
                # (якщо записувачі встигають за генерацією, безумовний цикл тут ніколи б не завершився)
                for _ in range(max(1, (target - self.pending) // s.batch_size)):
                    if self.pending + s.batch_size > target and self.pending > 0:
                        break
                    self._enqueue(s.batch_size, s.batch_size)
                    await asyncio.sleep(0)  # генерація займає CPU — не блокуємо HTTP-запити
                self._space.clear()
                try:
                    await asyncio.wait_for(self._space.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
                carry = 0.0
                last = time.monotonic()
            else:
                await asyncio.sleep(self.TICK)
                # Рахуємо за фактично минулим часом, а не за TICK: генерація пачки й планувальник
                # теж займають час, інакше реальна швидкість була б на 10–15 % нижча за задану
                now = time.monotonic()
                carry += s.docs_per_second * min(now - last, 1.0)
                last = now
                n = int(carry)
                carry -= n
                room = max(0, self.buffer_limit - self.pending)
                if n > room:  # буфер повний (БД не встигає або недоступна) — пригальмовуємо джерело
                    n, carry = room, 0.0
                if n:
                    self._enqueue(n, s.batch_size)

    def _enqueue(self, n: int, batch_size: int) -> None:
        # Дозаповнюємо останню пачку, якщо записувачі її ще не взяли (менше дрібних пачок)
        if self._queue and len(self._queue[-1].rows) < batch_size and self._queue[-1].attempts == 0:
            last = self._queue[-1]
            add = min(n, batch_size - len(last.rows))
            last.rows.extend(self._random_rows(add))
            self._queued_rows += add
            self.generated += add
            n -= add
        while n > 0:
            k = min(n, batch_size)
            self._queue.append(_Batch(self._random_rows(k)))
            self._queued_rows += k
            self.generated += k
            n -= k
        self._work.set()

    async def _next_batch(self, idx: int) -> Optional[_Batch]:
        while not self._queue:
            if idx >= self._s.writers:
                return None
            self._work.clear()
            try:
                await asyncio.wait_for(self._work.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
        batch = self._queue.popleft()
        self._queued_rows -= len(batch.rows)
        self._in_flight_rows += len(batch.rows)
        self._space.set()
        return batch

    async def _writer(self, idx: int) -> None:
        while idx < self._s.writers:   # зайві записувачі завершуються після зменшення writers
            batch = await self._next_batch(idx)
            if batch is None:
                return
            try:
                await self._write_until_success(idx, batch)
            finally:
                self._in_flight_rows -= len(batch.rows)

    async def _write_until_success(self, idx: int, batch: _Batch) -> None:
        while True:
            await self._gate.wait()
            self._active += 1
            try:
                timing = await self._cmd.ingest(batch.rows, idempotent=batch.attempts > 0,
                                                index=self._s.index_search)
            except Exception as e:  # noqa: BLE001 — будь-яка помилка БД: повторюємо ту саму пачку
                error: Optional[Exception] = e
            else:
                error = None
            finally:
                self._active -= 1
            if error is None:
                self._on_success(idx, batch, timing)
                return
            self._on_failure(idx, batch, error)
            await asyncio.sleep(min(self._retry_max_s, 0.5 * 2 ** min(batch.attempts - 1, 4)))

    def _on_failure(self, idx: int, batch: _Batch, e: Exception) -> None:
        batch.attempts += 1
        self.failed_attempts += 1
        self._last_error = f"{type(e).__name__}: {str(e)[:300]}"
        self._last_error_at = now_iso()
        self._failing.add(idx)
        if self._retrying_since is None:
            self._retrying_since = time.monotonic()
            self._retrying_since_iso = self._last_error_at
            self.on_event("warn", f"Запис у БД не вдається — пачки тримаються в буфері й повторюються. "
                                  f"Помилка: {self._last_error}")
            log.warning("Генератор: запис не вдався, пачка %d рядків буде повторена: %s",
                        len(batch.rows), self._last_error)

    def _on_success(self, idx: int, batch: _Batch, timing) -> None:
        n = len(batch.rows)
        self.inserted += n
        self._ids.add([m.id for m in batch.rows])
        self.sql_ms_sum += timing.sql_ms
        self.sql_batches += 1
        self.batch_latencies.append((time.monotonic(), timing.sql_ms))
        if timing.es_ms is not None:
            self.es_ms_sum += timing.es_ms
            self.es_batches += 1
        self._failing.discard(idx)
        if self._retrying_since is not None and not self._failing:
            down = time.monotonic() - self._retrying_since
            self._retrying_since = None
            self._retrying_since_iso = None
            self.on_event("ok", f"Запис відновлено через {down:.0f} с; у буфері лишилось "
                                f"{self.pending:,} рядків — дописуються".replace(",", " "))

    async def _modifier_loop(self) -> None:
        """Бонус: запити на модифікацію під навантаженням."""
        carry_u = carry_d = 0.0
        last_bulk = time.monotonic()
        while True:
            await asyncio.sleep(self.MOD_TICK)
            s = self._s
            carry_u += s.updates_per_second * self.MOD_TICK
            carry_d += s.deletes_per_second * self.MOD_TICK
            nu, nd = int(carry_u), int(carry_d)
            carry_u -= nu
            carry_d -= nd
            if self._hold_deletes:   # іде точний підрахунок — DELETE пропускаємо (вставки тривають)
                nd = 0
            due_bulk = s.bulk_update_every_s and time.monotonic() - last_bulk >= s.bulk_update_every_s
            if not (nu or nd or due_bulk):
                continue
            await self._gate.wait()
            self._active += 1
            try:
                if nu:
                    ids = self._ids.sample(nu)
                    self.updated += await self._cmd.update_status(ids, random.choice(_STATUSES),
                                                                  index=s.index_search)
                if nd:
                    ids = self._ids.sample(nd, remove=True)
                    self.deleted += await self._cmd.delete_ids(ids, index=s.index_search)
                if due_bulk:
                    last_bulk = time.monotonic()
                    # «Важкий» UPDATE без ключа розподілу: координатор розсилає його на всі шарди
                    flt = MissionFilter(body=random.choice(_BODIES),
                                        year_from=(y := random.randint(1960, 2030)), year_to=y)
                    n = await self._cmd.update_status_by_filter(flt, random.choice(_STATUSES),
                                                                index=s.index_search)
                    self.bulk_updates += 1
                    log.info("Масове оновлення (%s, %s): %d рядків", flt.body, flt.year_from, n)
            except Exception as e:  # noqa: BLE001 — модифікації синтетичні, їх не повторюємо
                log.warning("Генератор: модифікація не вдалася: %s", e)
            finally:
                self._active -= 1

    # ---------- Дані ----------
    @staticmethod
    def _random_report(kind: str, body: str) -> str:
        parts = [random.choice(_OPENINGS), random.choice(_FINDINGS), random.choice(_CLOSINGS)]
        text = " ".join(p for p in parts if p)
        return text.format(kind=_KINDS.get(kind, "spacecraft"), body=body.capitalize(),
                           thing=random.choice(_THINGS))

    @classmethod
    def _random_rows(cls, n: int) -> List[MissionDTO]:
        out = []
        for _ in range(n):
            typ = random.choice(_TYPES)
            body = random.choice(_BODIES)
            # model_construct без валідації: на десятках тисяч рядків/с валідація помітно їсть CPU,
            # а значення тут і так коректні
            out.append(MissionDTO.model_construct(
                id=str(uuid.uuid4()), name=f"Gen-{random.randint(1, 10**6)}", type=typ,
                status=random.choice(_STATUSES), agency_id=random.choice(_AGENCIES),
                celestial_body_id=body, launch_year=random.randint(1960, 2030), description="",
                report=cls._random_report(typ, body)))
        return out
