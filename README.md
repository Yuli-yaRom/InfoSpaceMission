# InfoSpaceMission

Інформаційна система про космічні місії (FastAPI, гексагональна архітектура).
Топологія БД, ключ шардування, map-reduce, пошук і кеш — у [docs/db_topology.md](docs/db_topology.md).

## Сховища

| Сховище | Роль | Адаптер |
|---|---|---|
| **PostgreSQL + Citus** | Основне сховище; координатор + 3 воркери, 12 шардів за хешем `id` (п. 13) | `adapters/outbound/sql_mission_repository.py`, `citus_cluster.py`, `citus_sharding_adapter.py` |
| **Elasticsearch** | Повнотекстовий пошук (`english`-аналізатор, підсвічування), схожі місії (п. 15) | `adapters/outbound/elasticsearch_adapter.py` |
| **Redis** | Кеш пошуку `search:<hash>` і рекомендацій (TTL 5 хв), останні запити (п. 15) | `adapters/outbound/redis_adapter.py` |

Запис: `POST /api/missions` → Citus (через координатор) → індексація в Elasticsearch.
Пошук: Redis → (промах) → Elasticsearch → запис у Redis.
Map-reduce (п. 14): паралельно напряму до кожного воркера → злиття в Python → перевірка через координатор.

Без змінних `DATABASE_URL` / `CITUS_WORKERS` / `ES_URL` / `REDIS_URL` застосунок використовує адаптери
в пам'яті та імітацію кластера на 3 вузли (`adapters/outbound/in_memory_*.py`) — для тестів і розробки без Docker.

## Запуск

```bash
docker compose up --build      # UI та API: http://localhost:8000, Swagger: /docs
```

Схема таблиці змінилася (колонка `report`, розподілена таблиця), тож якщо раніше запускали старий
`docker-compose.yml`, видаліть старі томи: `docker compose down -v`.

Локально без Docker:

```bash
pip install -r requirements.txt
python app.py
pytest
```

## API

- `GET  /api/missions?body=&type=&status=&year_from=&year_to=&limit=&offset=` — фільтри (SQL)
- `GET  /api/missions/{id}` — картка місії
- `POST /api/missions` — створити місію
- `GET  /api/search?q=...` (+ ті самі фільтри) — пошук з підсвічуванням, `cache: HIT|MISS`, `took_ms`
- `GET  /api/search/recent?limit=20` — останні запити зі статусом «Знайдено» / «Пусто»
- `GET  /api/missions/{id}/recommendations?limit=5` — схожі місії (Elasticsearch + Redis)
- `GET  /api/sharding/distribution` — розподіл рядків по шардах і вузлах
- `GET  /api/sharding/explain?body=mars` — план: один шард vs усі
- `GET  /api/mapreduce/decades`, `GET /api/mapreduce/terms` — власний map-reduce по шардах
- `GET  /api/stats`, `GET/PUT /api/generator` — статистика і швидкість генератора
