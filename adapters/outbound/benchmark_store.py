"""Сховище результатів замірів.

JSONL-файл у ./logs (змонтований в обидві конфігурації compose — 3 воркери і 1 вузол),
тож результати з різних топологій лежать поруч і порівнюються в одній таблиці UI."""
import asyncio
from pathlib import Path
from typing import Dict, List

from application.mission_ports import BenchmarkStorePort
from domain.mission_dtos import BenchmarkRunDTO


class InMemoryBenchmarkStore(BenchmarkStorePort):
    def __init__(self):
        self._runs: Dict[str, BenchmarkRunDTO] = {}

    async def save(self, run: BenchmarkRunDTO) -> None:
        self._runs[run.id] = run

    async def list(self) -> List[BenchmarkRunDTO]:
        return sorted(self._runs.values(), key=lambda r: r.started_at, reverse=True)

    async def delete(self, run_id: str) -> bool:
        return self._runs.pop(run_id, None) is not None


class JsonlBenchmarkStore(BenchmarkStorePort):
    def __init__(self, path: str):
        self._path = Path(path)
        self._lock = asyncio.Lock()

    def _read(self) -> Dict[str, BenchmarkRunDTO]:
        runs: Dict[str, BenchmarkRunDTO] = {}
        if self._path.exists():
            for line in self._path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    run = BenchmarkRunDTO.model_validate_json(line)
                    runs[run.id] = run
        return runs

    def _write(self, runs: Dict[str, BenchmarkRunDTO]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text("".join(r.model_dump_json() + "\n" for r in runs.values()), encoding="utf-8")
        tmp.replace(self._path)

    async def save(self, run: BenchmarkRunDTO) -> None:
        async with self._lock:
            runs = self._read()
            runs[run.id] = run
            self._write(runs)

    async def list(self) -> List[BenchmarkRunDTO]:
        async with self._lock:
            return sorted(self._read().values(), key=lambda r: r.started_at, reverse=True)

    async def delete(self, run_id: str) -> bool:
        async with self._lock:
            runs = self._read()
            if runs.pop(run_id, None) is None:
                return False
            self._write(runs)
            return True


def make_store(path: str = "") -> BenchmarkStorePort:
    return JsonlBenchmarkStore(path) if path else InMemoryBenchmarkStore()

