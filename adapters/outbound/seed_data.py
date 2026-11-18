"""Початкові реальні місії; завантажуються, якщо SQL-сховище порожнє."""
from typing import List

from domain.mission_dtos import MissionCreateRequest

_SEED = [
    ("Curiosity", "ROVER", "COMPLETED", "NASA", "mars", 2011, "Марсохід, що досліджує кратер Гейл"),
    ("Perseverance", "ROVER", "ACTIVE", "NASA", "mars", 2020, "Марсохід у кратері Єзеро, збирає зразки ґрунту"),
    ("Opportunity", "ROVER", "COMPLETED", "NASA", "mars", 2003, "Марсохід, що пропрацював майже 15 років"),
    ("Zhurong", "ROVER", "COMPLETED", "CNSA", "mars", 2020, "Перший китайський марсохід"),
    ("Mars Express", "ORBITER", "ACTIVE", "ESA", "mars", 2003, "Орбітальний апарат ЄКА для картографування Марса"),
    ("Mars Polar Lander", "LANDER", "FAILED", "NASA", "mars", 1999, "Посадковий апарат, втрачений під час посадки"),
    ("Apollo 11", "CREWED", "COMPLETED", "NASA", "moon", 1969, "Перша пілотована висадка людини на Місяць"),
    ("Chandrayaan-3", "LANDER", "COMPLETED", "ISRO", "moon", 2023, "Посадка поблизу південного полюса Місяця"),
    ("Luna 9", "LANDER", "COMPLETED", "Roscosmos", "moon", 1966, "Перша м'яка посадка на Місяць"),
    ("Artemis III", "CREWED", "PLANNED", "NASA", "moon", 2027, "Пілотована місія висадки на Місяць"),
    ("Voyager 2", "PROBE", "ACTIVE", "NASA", "neptune", 1977, "Зонд, що пролетів повз Уран і Нептун"),
]


def seed_requests() -> List[MissionCreateRequest]:
    return [MissionCreateRequest(name=n, type=t, status=s, agency_id=a, celestial_body_id=b,
                                 launch_year=y, description=d)
            for n, t, s, a, b, y, d in _SEED]
