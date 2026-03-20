"""Tests for Workflow Symbols API multi-workflow configurations."""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.core.database import Base, get_db
from backend.main import app
from backend.models.workflow import Workflow
from backend.models.workflow_symbol import SymbolWorkflowLink, WorkflowSymbol

engine = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


def setup_app_dependency():
    app.dependency_overrides[get_db] = override_get_db


setup_app_dependency()


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def db_session():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def workflow_factory(db_session):
    def _factory(name="Workflow"):
        wf = Workflow(name=name)
        db_session.add(wf)
        db_session.commit()
        db_session.refresh(wf)
        return wf

    return _factory


class TestWorkflowSymbolAPI:
    def test_create_symbol_with_workflow_configs(self, client, workflow_factory):
        wf1 = workflow_factory("Primary")
        wf2 = workflow_factory("Secondary")
        payload = {
            "symbol": "TSLA",
            "name": "Tesla",
            "workflow_type": "trading_signal",
            "priority": 1,
            "workflows": [
                {
                    "workflow_id": wf1.id,
                    "timezone": "UTC",
                    "session_start": "09:30",
                    "session_end": "16:00",
                    "allow_weekend": False,
                },
                {
                    "workflow_id": wf2.id,
                    "is_active": False,
                },
            ],
        }
        response = client.post("/api/workflow-symbols/", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["symbol"] == "TSLA"
        assert len(data["workflows"]) == 2
        assert data["workflows"][0]["workflow_id"] == wf1.id
        assert data["workflows"][0]["timezone"] == "UTC"
        assert data["workflows"][1]["is_active"] is False

    def test_create_symbol_requires_workflows(self, client):
        payload = {
            "symbol": "AAPL",
            "workflows": [],
        }
        response = client.post("/api/workflow-symbols/", json=payload)
        assert response.status_code == 422

    def test_update_symbol_replaces_workflows_and_priority(self, client, workflow_factory):
        wf1 = workflow_factory("First")
        wf2 = workflow_factory("Second")
        wf3 = workflow_factory("Third")
        create_payload = {
            "symbol": "NVDA",
            "workflows": [
                {"workflow_id": wf1.id},
                {"workflow_id": wf2.id},
            ],
        }
        create_resp = client.post("/api/workflow-symbols/", json=create_payload)
        assert create_resp.status_code == 201

        update_resp = client.patch(
            "/api/workflow-symbols/NVDA",
            json={"workflows": [{"workflow_id": wf3.id}, {"workflow_id": wf1.id}]},
        )
        assert update_resp.status_code == 200
        data = update_resp.json()
        assert [wf["workflow_id"] for wf in data["workflows"]] == [wf3.id, wf1.id]
        assert data["workflows"][0]["priority"] == 0
        assert data["workflows"][1]["priority"] == 1

    def test_update_symbol_requires_non_empty_workflows(self, client, workflow_factory):
        wf = workflow_factory()
        payload = {
            "symbol": "META",
            "workflows": [{"workflow_id": wf.id}],
        }
        create_resp = client.post("/api/workflow-symbols/", json=payload)
        assert create_resp.status_code == 201

        update_resp = client.patch("/api/workflow-symbols/META", json={"workflows": []})
        assert update_resp.status_code == 400
        assert "at least one" in update_resp.json()["detail"].lower()

    def test_duplicate_workflow_ids_rejected(self, client, workflow_factory):
        wf = workflow_factory()
        payload = {
            "symbol": "GOOG",
            "workflows": [
                {"workflow_id": wf.id},
                {"workflow_id": wf.id},
            ],
        }
        resp = client.post("/api/workflow-symbols/", json=payload)
        assert resp.status_code == 400
        assert "duplicate" in resp.json()["detail"].lower()

    def test_session_window_requires_pair(self, client, workflow_factory):
        wf = workflow_factory()
        payload = {
            "symbol": "MSFT",
            "workflows": [
                {
                    "workflow_id": wf.id,
                    "session_start": "09:30",
                }
            ],
        }
        resp = client.post("/api/workflow-symbols/", json=payload)
        assert resp.status_code == 400
        assert "both" in resp.json()["detail"].lower()
