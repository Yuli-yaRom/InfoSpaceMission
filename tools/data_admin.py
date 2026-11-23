"""Керування даними й навантаженням з консолі (через REST API застосунку).

Працює через API, а не напряму з БД, щоб видалення проходило той самий шлях, що й з UI:
Citus + Elasticsearch + скидання кешу Redis + облік у лічильнику рядків.

    docker compose exec app python -m tools.data_admin stats
    docker compose exec app python -m tools.data_admin delete --body mars --year-to 1990
    docker compose exec app python -m tools.data_admin delete-all --yes
    docker compose exec app python -m tools.data_admin vacuum [--full]
    docker compose exec app python -m tools.data_admin generator --max | --rate 5000 | --pause
    docker compose exec app python -m tools.data_admin recount

Напряму в БД (в обхід застосунку; лічильник рядків потім уточніть командою recount):
    docker compose exec citus-coordinator psql -U space -c "DELETE FROM missions WHERE celestial_body_id = 'mars'"
    docker compose exec citus-coordinator psql -U space -c "TRUNCATE missions"
"""
import argparse
import os
import sys

import httpx

APP_URL = os.getenv("APP_URL", "http://localhost:8000")


def _size(b):
    if b is None:
        return "–"
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if b < 1024 or unit == "ТБ":
            return f"{b:.1f} {unit}"
        b /= 1024


def _filters(a) -> dict:
    return {k: v for k, v in dict(body=a.body, type=a.type, status=a.status,
                                  year_from=a.year_from, year_to=a.year_to).items() if v not in (None, "")}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m tools.data_admin", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats", help="кількість рядків, розмір, стан генератора")
    sub.add_parser("recount", help="точний count(*)")
    for name in ("preview", "delete"):
        sp = sub.add_parser(name, help="скільки рядків під фільтром" if name == "preview" else "часткове видалення")
        sp.add_argument("--body")
        sp.add_argument("--type")
        sp.add_argument("--status")
        sp.add_argument("--year-from", type=int)
        sp.add_argument("--year-to", type=int)
    da = sub.add_parser("delete-all", help="повне видалення (TRUNCATE)")
    da.add_argument("--yes", action="store_true", help="підтвердження")
    va = sub.add_parser("vacuum", help="VACUUM після часткового видалення")
    va.add_argument("--full", action="store_true", help="VACUUM FULL: повернути місце ОС (блокує таблицю)")
    g = sub.add_parser("generator", help="режим генератора")
    mode = g.add_mutually_exclusive_group(required=True)
    mode.add_argument("--max", action="store_true")
    mode.add_argument("--rate", type=int)
    mode.add_argument("--pause", action="store_true")
    a = p.parse_args(argv)

    with httpx.Client(base_url=APP_URL, timeout=None) as c:
        def call(method, url, **kw):
            r = c.request(method, url, **kw)
            if r.status_code >= 400:
                print(f"Помилка {r.status_code}: {r.json().get('detail', r.text)}", file=sys.stderr)
                sys.exit(1)
            return r.json()

        if a.cmd in ("stats", "recount"):
            s = call("POST", "/api/stats/recount") if a.cmd == "recount" else call("GET", "/api/stats")
            print(f"Рядків: {s['total_documents']:,}  ·  розмір: {_size(s['db_size_bytes'])}  ·  "
                  f"запис: {s['insert_rate']:.0f} р/с  ·  генератор: {s['generator_state']}  ·  "
                  f"у буфері: {s['pending']:,}".replace(",", " "))
        elif a.cmd == "preview":
            print(f"Під фільтр потрапляє {call('GET', '/api/data/preview', params=_filters(a))['rows']:,} рядків"
                  .replace(",", " "))
        elif a.cmd == "delete":
            if not _filters(a):
                print("Вкажіть хоча б один фільтр (для повного видалення: delete-all --yes)", file=sys.stderr)
                return 2
            r = call("DELETE", "/api/data", params=_filters(a))
            print(f"Видалено {r['deleted_rows']:,} рядків за {r['took_ms'] / 1000:.2f} с. {r['search_note']}"
                  .replace(",", " "))
            print(f"Розмір: {_size(r['size_before'])} → {_size(r['size_after'])} "
                  f"(місце повертається після VACUUM)")
        elif a.cmd == "delete-all":
            if not a.yes:
                print("Додайте --yes, щоб підтвердити повне видалення", file=sys.stderr)
                return 2
            r = call("DELETE", "/api/data/all", params={"confirm": "yes"})
            print(f"Видалено ~{r['deleted_rows']:,} рядків за {r['took_ms'] / 1000:.2f} с. {r['search_note']}. "
                  f"Розмір: {_size(r['size_before'])} → {_size(r['size_after'])}".replace(",", " "))
        elif a.cmd == "vacuum":
            r = call("POST", "/api/data/vacuum", params={"full": str(a.full).lower()})
            print(f"{'VACUUM FULL' if a.full else 'VACUUM'} за {r['took_ms'] / 1000:.1f} с. "
                  f"Розмір: {_size(r['size_before'])} → {_size(r['size_after'])}")
        elif a.cmd == "generator":
            patch = ({"max_speed": True} if a.max else {"max_speed": False, "docs_per_second": 0} if a.pause
                     else {"max_speed": False, "docs_per_second": a.rate})
            st = call("PUT", "/api/generator", json=patch)
            print(f"Генератор: {st['state']}, налаштування: {st['settings']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
