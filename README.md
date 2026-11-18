# InfoSpaceMission

Інформаційна система про космічні місії (FastAPI, гексагональна архітектура).

## Сховища

| Сховище | Роль | Адаптер |
|---|---|---|
| **PostgreSQL** | Основне сховище (джерело істини), фільтри та пагінація | `adapters/outbound/sql_mission_repository.py` |
| **Elasticsearch** | Повнотекстовий пошук, схожі місії (`more_like_this`) | `adapters/outbound/elasticsearch_adapter.py` |
| **Redis** | Кеш рекомендацій (TTL 5 хв) | `adapters/outbound/redis_adapter.py` |

Запис: `POST /api/missions` → PostgreSQL → індексація в Elasticsearch.
Рекомендації: Redis → (промах) → Elasticsearch → запис у Redis.

Без змінних `DATABASE_URL` / `ES_URL` / `REDIS_URL` застосунок використовує адаптери в пам'яті
(`adapters/outbound/in_memory_mission_repository.py`) — зручно для тестів і розробки без Docker.

## Запуск

```bash
docker compose up --build      # UI та API: http://localhost:8000, Swagger: /docs
```

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
- `GET  /api/search?q=...` (+ ті самі фільтри) — повнотекстовий пошук (Elasticsearch)
- `GET  /api/missions/{id}/recommendations?limit=5` — схожі місії (Elasticsearch + Redis)
- `GET  /api/stats`, `GET/PUT /api/generator` — статистика і швидкість генератора
