"""Точний підрахунок рядків, узгоджений із лічильниками генератора, без довгої паузи запису.

Проблема: count(*) на десятках ГБ триває хвилину й більше. Якщо весь цей час тримати запис на паузі,
на графіку швидкості з'являється провал, а замір спотворюється.

Рішення — «момент зрізу»:
1. На мить призупинити записи генератора (лише поки допишуться поточні пачки, ~0,3 с) і в цю мить
   зафіксувати час БД (`SELECT now()` на координаторі) та лічильники записаних і видалених рядків.
2. Відновити запис.
3. Порахувати `count(*) WHERE created_at <= момент`. created_at — час початку транзакції за тим самим
   годинником: усі пачки, що закомітились до паузи, мають created_at < моменту, а всі пачки після
   паузи — пізніший. Отже нові вставки під час підрахунку не потрапляють у число.
4. DELETE генератора на час підрахунку відкладаються (інакше старі рядки зникали б посеред count).

Для сховищ без created_at (у пам'яті, SQLite у тестах) — підрахунок усередині паузи: там він миттєвий.
"""
from dataclasses import dataclass
from typing import Any, Dict

from application.mission_ports import GeneratorControlPort, MissionRepositoryPort
from application.write_ledger import WriteLedger


@dataclass
class ConsistentCount:
    rows: int              # рядків у БД на момент зрізу
    inserted: int          # WriteLedger.inserted на момент зрізу
    deleted: int           # WriteLedger.deleted на момент зрізу
    counters: Dict[str, Any]   # лічильники генератора на момент зрізу
    moment: Any            # час БД (None — сховище без created_at)


async def consistent_count(repo: MissionRepositoryPort, gen: GeneratorControlPort,
                           ledger: WriteLedger) -> ConsistentCount:
    async with gen.deletes_held():
        async with gen.quiesced():
            moment = await repo.db_now()
            inserted, deleted, counters = ledger.inserted, ledger.deleted, gen.counters()
            rows = await repo.count() if moment is None else None
        if rows is None:
            rows = await repo.count_created_before(moment)   # запис уже йде далі
    return ConsistentCount(rows=rows, inserted=inserted, deleted=deleted, counters=counters, moment=moment)
