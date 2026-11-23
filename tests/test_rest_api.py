import pytest
from fastapi.testclient import TestClient

from app import create_app


@pytest.fixture
def client():
    with TestClient(create_app(run_generator=False)) as c:
        yield c


def test_list_all_has_total_and_items(client):
    data = client.get("/api/missions").json()
    assert data["total"] == 11
    assert len(data["items"]) == 10  # limit за замовчуванням


def test_filter_body_and_type(client):
    data = client.get("/api/missions", params={"body": "mars", "type": "ROVER"}).json()
    assert data["total"] == 4
    assert all(m["celestial_body_id"] == "mars" and m["type"] == "ROVER" for m in data["items"])


def test_year_range_and_pagination(client):
    data = client.get("/api/missions", params={"year_from": 2000, "limit": 2, "offset": 2}).json()
    assert data["total"] == 7 and len(data["items"]) == 2


def test_create_then_read(client):
    payload = {"name": "Test-1", "type": "ROVER", "agency_id": "ESA",
               "celestial_body_id": "mars", "launch_year": 2030}
    res = client.post("/api/missions", json=payload)
    assert res.status_code == 201
    created = res.json()
    assert created["status"] == "PLANNED" and created["id"]
    assert client.get(f"/api/missions/{created['id']}").json()["name"] == "Test-1"
    assert client.get("/api/stats").json()["total_documents"] == 12


def test_create_validation_error(client):
    bad = {"name": "X", "type": "ROVER", "agency_id": "A", "celestial_body_id": "mars", "launch_year": 1800}
    assert client.post("/api/missions", json=bad).status_code == 422


def test_get_missing_returns_404(client):
    assert client.get("/api/missions/nope").status_code == 404


def test_generator_settings(client):
    res = client.put("/api/generator", json={"docs_per_second": 500, "batch_size": 2000})
    assert res.status_code == 200
    st = client.get("/api/generator").json()
    assert st["settings"]["docs_per_second"] == 500 and st["settings"]["batch_size"] == 2000
    assert client.put("/api/generator", json={"docs_per_second": 0}).status_code == 200  # пауза
    assert client.get("/api/generator").json()["state"] == "paused"
    assert client.put("/api/generator", json={"docs_per_second": -1}).status_code == 422
