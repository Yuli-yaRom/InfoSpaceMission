#!/usr/bin/env bash
# Демонстрація: вимкнення шарда (воркера Citus) під час запису і його повернення без втрати даних.
#
#   ./scripts/outage_demo.sh [воркер] [секунд_простою] [рядків/с]
#   ./scripts/outage_demo.sh citus-worker-2 60 2000
#
# Кроки: генератор пише -> знімок (точний count) -> docker compose stop воркер -> запис повторюється,
# пачки чекають у буфері -> docker compose start воркер -> буфер дописується -> перевірка:
# очікувано (знімок + записано − видалено) == фактично (count(*)).
# Усе це видно наживо на вкладці «Відмовостійкість» (http://localhost:8000).
set -euo pipefail

WORKER="${1:-citus-worker-2}"
DOWN_FOR="${2:-60}"
RATE="${3:-2000}"
APP="${APP_URL:-http://localhost:8000}"

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
say() { printf '\n\033[1m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

say "Генератор: ${RATE} рядків/с, без модифікацій (щоб арифметика перевірки була простою)"
curl -fsS -X PUT "$APP/api/generator" -H 'Content-Type: application/json' \
  -d "{\"max_speed\": false, \"docs_per_second\": ${RATE}, \"deletes_per_second\": 0}" >/dev/null
sleep 5

say "Знімок стану"
curl -fsS -X POST "$APP/api/resilience/snapshot" | json '"рядків у БД: %s; по вузлах: %s" % (d["rows"], d["per_node"])'

say "Вимикаю ${WORKER} на ${DOWN_FOR} с"
docker compose stop "$WORKER"
for ((i = 0; i < DOWN_FOR; i += 10)); do
  sleep 10
  curl -fsS "$APP/api/generator" | json '"  стан: %s · згенеровано %s · записано %s · у буфері %s · невдалих спроб %s" % (d["state"], d["generated"], d["inserted"], d["pending"], d["failed_attempts"])'
done

say "Повертаю ${WORKER}"
docker compose start "$WORKER"

say "Чекаю, поки буфер дописується…"
for _ in $(seq 1 120); do
  pending=$(curl -fsS "$APP/api/generator" | json 'd["pending"]')
  state=$(curl -fsS "$APP/api/generator" | json 'd["state"]')
  echo "  у буфері: ${pending}, стан: ${state}"
  # під час запису буфер ніколи не буває рівно 0 надовго, тож достатньо, щоб запис ішов і буфер був малим
  if [[ "$state" == "running" && "$pending" -le $((RATE * 2)) ]]; then break; fi
  sleep 2
done

say "Перевірка"
curl -fsS -X POST "$APP/api/resilience/check" | json '"знімок: %s + записано: %s − видалено: %s = очікувано %s; фактично (count): %s; ВТРАЧЕНО: %s; ще в буфері: %s\n%s" % (d["snapshot"]["rows"], d["inserted_since"], d["deleted_since"], d["expected_rows"], d["actual_rows"], d["lost_rows"], d["pending"], "OK — жоден рядок не загубився" if d["ok"] else "РОЗБІЖНІСТЬ")'
