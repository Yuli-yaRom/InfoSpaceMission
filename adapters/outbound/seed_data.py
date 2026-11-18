"""Початкові реальні місії; завантажуються, якщо SQL-сховище порожнє.
Опис — українською, звіт (report) — англійською для аналізатора english (стемінг)."""
from typing import List

from domain.mission_dtos import MissionCreateRequest

_SEED = [
    ("Curiosity", "ROVER", "COMPLETED", "NASA", "mars", 2011, "Марсохід, що досліджує кратер Гейл", "The rover landed in Gale Crater in 2012 using a sky crane. It drilled rock samples and found evidence of an ancient lake. The rover is still driving up Mount Sharp."),
    ("Perseverance", "ROVER", "ACTIVE", "NASA", "mars", 2020, "Марсохід у кратері Єзеро, збирає зразки ґрунту", "The rover landed in Jezero Crater and is collecting rock samples for a future return mission. It carried the Ingenuity helicopter, which made the first powered flight on another planet."),
    ("Opportunity", "ROVER", "COMPLETED", "NASA", "mars", 2003, "Марсохід, що пропрацював майже 15 років", "The rover landed at Meridiani Planum and found hematite that formed in water. It was planned for 90 days but kept driving for almost 15 years until a dust storm ended the mission."),
    ("Zhurong", "ROVER", "COMPLETED", "CNSA", "mars", 2020, "Перший китайський марсохід", "The rover landed on Utopia Planitia together with the Tianwen-1 orbiter. Its radar studied layers of buried sediment before the rover went into hibernation."),
    ("Mars Express", "ORBITER", "ACTIVE", "ESA", "mars", 2003, "Орбітальний апарат ЄКА для картографування Марса", "The orbiter has been mapping the surface and atmosphere since 2003. Its radar detected signs of liquid water beneath the south polar ice."),
    ("Mars Polar Lander", "LANDER", "FAILED", "NASA", "mars", 1999, "Посадковий апарат, втрачений під час посадки", "The lander was lost during descent near the south pole. Investigators concluded the engines probably shut down early after a false touchdown signal."),
    ("Apollo 11", "CREWED", "COMPLETED", "NASA", "moon", 1969, "Перша пілотована висадка людини на Місяць", "Astronauts landed on the Moon in the Sea of Tranquility and walked on the surface. The crew collected lunar samples and returned safely to Earth."),
    ("Chandrayaan-3", "LANDER", "COMPLETED", "ISRO", "moon", 2023, "Посадка поблизу південного полюса Місяця", "The lander touched down near the lunar south pole and deployed a small rover. The rover measured sulphur and other elements in the soil."),
    ("Luna 9", "LANDER", "COMPLETED", "Roscosmos", "moon", 1966, "Перша м'яка посадка на Місяць", "The probe made the first soft landing on the Moon and transmitted panoramic images of the surface, showing that the ground could support a landing craft."),
    ("Artemis III", "CREWED", "PLANNED", "NASA", "moon", 2027, "Пілотована місія висадки на Місяць", "The planned mission will land astronauts near the lunar south pole. The crew will sample ice deposits in permanently shadowed craters."),
    ("Voyager 2", "PROBE", "ACTIVE", "NASA", "neptune", 1977, "Зонд, що пролетів повз Уран і Нептун", "The probe flew past Jupiter, Saturn, Uranus and Neptune. It is the only spacecraft that has visited the ice giants and it is still sending data from interstellar space."),
]


def seed_requests() -> List[MissionCreateRequest]:
    return [MissionCreateRequest(name=n, type=t, status=s, agency_id=a, celestial_body_id=b,
                                 launch_year=y, description=d, report=r)
            for n, t, s, a, b, y, d, r in _SEED]
