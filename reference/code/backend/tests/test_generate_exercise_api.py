from fastapi.testclient import TestClient

from backend.main import app


def test_generate_exercise_default_returns_ok():
    client = TestClient(app)
    resp = client.post("/generate_exercise", json={})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["count"] == 1
    assert "exercise" in data
    assert "exercises" in data
    assert isinstance(data["exercises"], list)
    assert data["exercise"]["id"]


def test_generate_exercise_respects_count_and_seed():
    client = TestClient(app)
    payload = {"topic": "r_multiple", "difficulty": "easy", "count": 2, "seed": 123}
    resp = client.post("/generate_exercise", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    ids = [ex["id"] for ex in data["exercises"]]
    assert len(ids) == len(set(ids))  # unique ids

