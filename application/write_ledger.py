"""Облік записів застосунку: скільки рядків вставлено й видалено через застосунок.

Навіщо: показувати кількість рядків у БД без `count(*)` щосекунди (на десятках ГБ такий запит
сам навантажує кластер і спотворює замір швидкості запису), а також перевіряти, що після
відключення вузла жоден підтверджений запис не загубився:
    очікувано = рядків у знімку + вставлено після знімка - видалено після знімка.
Точна кількість (`count(*)`) рахується на старті, за кнопкою «Перерахувати» і при перевірці.
Записи в обхід застосунку (напр. DELETE із psql) лічильник не бачить — після них треба перерахувати.
"""
from datetime import datetime, timezone
from typing import Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class WriteLedger:
    def __init__(self) -> None:
        self.inserted = 0
        self.deleted = 0
        self._base_rows: Optional[int] = None
        self._base_inserted = 0
        self._base_deleted = 0
        self.counted_at: Optional[str] = None

    def add_inserted(self, n: int) -> None:
        self.inserted += n

    def add_deleted(self, n: int) -> None:
        self.deleted += n

    def rebase(self, rows: int, inserted: Optional[int] = None, deleted: Optional[int] = None) -> None:
        """Точна кількість рядків `rows` у момент, коли лічильники мали значення inserted/deleted
        (за замовчуванням — поточні). Явні значення потрібні, бо count(*) великої таблиці рахується
        довше, ніж триває пауза запису: база — момент зрізу, а не кінець підрахунку."""
        self._base_rows = rows
        self._base_inserted = self.inserted if inserted is None else inserted
        self._base_deleted = self.deleted if deleted is None else deleted
        self.counted_at = now_iso()

    @property
    def known(self) -> bool:
        return self._base_rows is not None

    def total(self) -> int:
        return ((self._base_rows or 0) + (self.inserted - self._base_inserted)
                - (self.deleted - self._base_deleted))
