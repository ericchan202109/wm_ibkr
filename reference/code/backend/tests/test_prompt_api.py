"""
Unit Tests for Prompt API Endpoints
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.main import app
from backend.core.database import Base, get_db
from backend.models.prompt import PromptTemplate, PromptPerformance
from backend.models.strategy import Strategy
from datetime import date
from decimal import Decimal


# Test database setup
engine = create_engine("sqlite:///:memory:")
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """Override database dependency for testing."""
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(scope="function", autouse=True)
def setup_database():
    """Create tables before each test and drop after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def db_session():
    """Get a database session."""
    db = TestingSessionLocal()
    yield db
    db.close()


@pytest.fixture
def sample_prompt(db_session):
    """Create a sample prompt for testing."""
    prompt = PromptTemplate(
        name="Test Prompt",
        description="Test description",
        template_type="analysis",
        template_content="Analysis for {{ symbol }}",
        version=1,
        is_active=True,
        is_global=True,
        strategy_id=None
    )
    db_session.add(prompt)
    db_session.commit()
    db_session.refresh(prompt)
    return prompt


@pytest.fixture
def sample_strategy(db_session):
    """Create a sample strategy."""
    strategy = Strategy(
        name="Test Strategy",
        description="Test strategy",
        workflow_id=1,
        is_active=True
    )
    db_session.add(strategy)
    db_session.commit()
    db_session.refresh(strategy)
    return strategy


class TestPromptListAPI:
    """Test GET /api/v1/prompts/ endpoint."""
    
    def test_list_prompts_empty(self, client):
        """Test listing prompts when database is empty."""
        response = client.get("/api/v1/prompts/")
        assert response.status_code == 200
        data = response.json()
        assert data["templates"] == []
        assert data["total"] == 0
    
    def test_list_prompts_with_data(self, client, sample_prompt):
        """Test listing prompts with data."""
        response = client.get("/api/v1/prompts/")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["templates"]) == 1
        assert data["templates"][0]["name"] == "Test Prompt"
    
    def test_list_prompts_filter_by_type(self, client, db_session):
        """Test filtering prompts by template type."""
        # Create prompts of different types
        prompt1 = PromptTemplate(
            name="Analysis Prompt", template_type="analysis",
            template_content="Test", version=1, is_active=True, is_global=True
        )
        prompt2 = PromptTemplate(
            name="Consolidation Prompt", template_type="consolidation",
            template_content="Test", version=1, is_active=True, is_global=True
        )
        db_session.add_all([prompt1, prompt2])
        db_session.commit()
        
        response = client.get("/api/v1/prompts/?template_type=analysis")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["templates"][0]["template_type"] == "analysis"
    
    def test_list_prompts_filter_by_active(self, client, db_session):
        """Test filtering prompts by active status."""
        prompt1 = PromptTemplate(
            name="Active Prompt", template_type="analysis",
            template_content="Test", version=1, is_active=True, is_global=True
        )
        prompt2 = PromptTemplate(
            name="Inactive Prompt", template_type="analysis",
            template_content="Test", version=1, is_active=False, is_global=True
        )
        db_session.add_all([prompt1, prompt2])
        db_session.commit()
        
        response = client.get("/api/v1/prompts/?is_active=true")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["templates"][0]["is_active"] is True
    
    def test_list_prompts_pagination(self, client, db_session):
        """Test pagination."""
        # Create 15 prompts
        for i in range(15):
            prompt = PromptTemplate(
                name=f"Prompt {i}", template_type="analysis",
                template_content="Test", version=1, is_active=True, is_global=True
            )
            db_session.add(prompt)
        db_session.commit()
        
        # Get first page (10 items)
        response = client.get("/api/v1/prompts/?page=1&page_size=10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["templates"]) == 10
        assert data["total"] == 15
        
        # Get second page (5 items)
        response = client.get("/api/v1/prompts/?page=2&page_size=10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["templates"]) == 5


class TestPromptCRUDAPI:
    """Test CRUD operations for prompts."""
    
    def test_get_prompt_by_id(self, client, sample_prompt):
        """Test getting a specific prompt by ID."""
        response = client.get(f"/api/v1/prompts/{sample_prompt.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Test Prompt"
        assert data["id"] == sample_prompt.id
    
    def test_get_prompt_not_found(self, client):
        """Test getting non-existent prompt returns 404."""
        response = client.get("/api/v1/prompts/99999")
        assert response.status_code == 404
    
    def test_create_prompt(self, client):
        """Test creating a new prompt."""
        payload = {
            "name": "New Prompt",
            "description": "New description",
            "template_type": "analysis",
            "template_content": "New content for {{ symbol }}",
            "is_active": True,
            "is_global": True,
            "strategy_id": None
        }
        response = client.post("/api/v1/prompts/", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "New Prompt"
        assert data["version"] == 1
        assert data["id"] is not None
    
    def test_create_prompt_validation_error(self, client):
        """Test creating prompt with missing required fields."""
        payload = {
            "name": "Incomplete Prompt"
            # Missing template_type and template_content
        }
        response = client.post("/api/v1/prompts/", json=payload)
        assert response.status_code == 422  # Validation error
    
    def test_update_prompt(self, client, sample_prompt):
        """Test updating an existing prompt."""
        payload = {
            "name": "Updated Prompt",
            "template_content": "Updated content {{ symbol }}",
            "template_type": "analysis",
            "is_active": False,
            "is_global": True
        }
        response = client.put(f"/api/v1/prompts/{sample_prompt.id}", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Prompt"
        assert data["is_active"] is False
        assert data["version"] == 2  # Version should increment
    
    def test_delete_prompt(self, client, sample_prompt):
        """Test deleting a prompt."""
        response = client.delete(f"/api/v1/prompts/{sample_prompt.id}")
        assert response.status_code == 200
        
        # Verify deletion
        response = client.get(f"/api/v1/prompts/{sample_prompt.id}")
        assert response.status_code == 404


class TestPromptValidationAPI:
    """Test prompt validation endpoints."""
    
    def test_validate_valid_template(self, client):
        """Test validating a valid Jinja2 template."""
        payload = {"template_text": "Hello {{ name }}!"}
        response = client.post("/api/v1/prompts/validate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["is_valid"] is True
        assert data["error_message"] is None
    
    def test_validate_invalid_template(self, client):
        """Test validating an invalid Jinja2 template."""
        payload = {"template_text": "{% if unclosed"}
        response = client.post("/api/v1/prompts/validate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["is_valid"] is False
        assert data["error_message"] is not None


class TestPromptRenderAPI:
    """Test prompt rendering endpoints."""
    
    def test_render_template(self, client):
        """Test rendering a template with context."""
        payload = {
            "template_text": "Price: ${{ price }}",
            "context": {"price": 150.50}
        }
        response = client.post("/api/v1/prompts/render", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "150.5" in data["rendered_text"]
    
    def test_render_template_with_filters(self, client):
        """Test rendering with Jinja2 filters."""
        payload = {
            "template_text": "Win rate: {{ rate|format_percent }}",
            "context": {"rate": 0.653}
        }
        response = client.post("/api/v1/prompts/render", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "65.30%" in data["rendered_text"]
    
    def test_render_template_error(self, client):
        """Test rendering with undefined variable."""
        payload = {
            "template_text": "{{ undefined_var }}",
            "context": {}
        }
        response = client.post("/api/v1/prompts/render", json=payload)
        assert response.status_code == 400  # Bad request


class TestPromptPerformanceAPI:
    """Test prompt performance endpoints."""
    
    def test_get_performance_empty(self, client, sample_prompt):
        """Test getting performance when no data exists."""
        response = client.get(f"/api/v1/prompts/{sample_prompt.id}/performance")
        assert response.status_code == 200
        data = response.json()
        assert data == []
    
    def test_get_performance_with_data(self, client, sample_prompt, db_session):
        """Test getting performance with data."""
        # Add performance record
        perf = PromptPerformance(
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            evaluation_date=date.today(),
            signals_generated=10,
            win_count=6,
            loss_count=4,
            win_rate=Decimal("0.6"),
            avg_r_multiple=Decimal("1.5"),
            total_profit_loss=Decimal("1000.00")
        )
        db_session.add(perf)
        db_session.commit()
        
        response = client.get(f"/api/v1/prompts/{sample_prompt.id}/performance")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["signals_generated"] == 10
        assert data[0]["win_count"] == 6
    
    def test_compare_prompts(self, client, db_session):
        """Test comparing two prompts."""
        # Create two prompts with performance
        prompt1 = PromptTemplate(
            name="Prompt 1", template_type="analysis",
            template_content="Test", version=1, is_active=True, is_global=True
        )
        prompt2 = PromptTemplate(
            name="Prompt 2", template_type="analysis",
            template_content="Test", version=1, is_active=True, is_global=True
        )
        db_session.add_all([prompt1, prompt2])
        db_session.commit()
        
        perf1 = PromptPerformance(
            prompt_template_id=prompt1.id, prompt_version=1,
            evaluation_date=date.today(), signals_generated=10,
            win_count=7, loss_count=3, win_rate=Decimal("0.7"),
            avg_r_multiple=Decimal("2.0"), total_profit_loss=Decimal("2000.00")
        )
        perf2 = PromptPerformance(
            prompt_template_id=prompt2.id, prompt_version=1,
            evaluation_date=date.today(), signals_generated=10,
            win_count=5, loss_count=5, win_rate=Decimal("0.5"),
            avg_r_multiple=Decimal("1.0"), total_profit_loss=Decimal("500.00")
        )
        db_session.add_all([perf1, perf2])
        db_session.commit()
        
        response = client.get(f"/api/v1/prompts/compare?prompt_id_1={prompt1.id}&prompt_id_2={prompt2.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["prompt_id_1"] == prompt1.id
        assert data["prompt_id_2"] == prompt2.id
        assert float(data["win_rate_1"]) > float(data["win_rate_2"])


class TestStrategySpecificPrompts:
    """Test strategy-specific prompt endpoints."""
    
    def test_get_strategy_prompts(self, client, sample_strategy, db_session):
        """Test getting prompts for a specific strategy."""
        # Create strategy-specific prompt
        prompt = PromptTemplate(
            name="Strategy Prompt", template_type="analysis",
            template_content="Test", version=1, is_active=True,
            is_global=False, strategy_id=sample_strategy.id
        )
        db_session.add(prompt)
        db_session.commit()
        
        response = client.get(f"/api/v1/strategies/{sample_strategy.id}/prompts")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1
        # Should find strategy-specific prompt
        strategy_prompts = [p for p in data if p["strategy_id"] == sample_strategy.id]
        assert len(strategy_prompts) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

