"""
Unit Tests for Prompt Template and Performance Models
"""
import pytest
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models.prompt import PromptTemplate, PromptPerformance
from backend.models.strategy import Strategy
from backend.core.database import Base


@pytest.fixture(scope="function")
def db_session():
    """Create a test database session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_strategy(db_session):
    """Create a sample strategy for testing."""
    strategy = Strategy(
        name="Test Strategy",
        description="Test strategy for prompt tests",
        workflow_id=1,
        is_active=True
    )
    db_session.add(strategy)
    db_session.commit()
    db_session.refresh(strategy)
    return strategy


class TestPromptTemplate:
    """Test PromptTemplate model."""
    
    def test_create_global_prompt(self, db_session):
        """Test creating a global prompt template."""
        prompt = PromptTemplate(
            name="Test Global Prompt",
            description="A test global prompt",
            template_type="analysis",
            template_content="Analysis for {{ symbol }}",
            version=1,
            is_active=True,
            is_global=True,
            strategy_id=None
        )
        db_session.add(prompt)
        db_session.commit()
        
        assert prompt.id is not None
        assert prompt.name == "Test Global Prompt"
        assert prompt.is_global is True
        assert prompt.strategy_id is None
        assert prompt.created_at is not None
    
    def test_create_strategy_specific_prompt(self, db_session, sample_strategy):
        """Test creating a strategy-specific prompt."""
        prompt = PromptTemplate(
            name="Test Strategy Prompt",
            description="A test strategy-specific prompt",
            template_type="analysis",
            template_content="Strategy analysis for {{ symbol }}",
            version=1,
            is_active=True,
            is_global=False,
            strategy_id=sample_strategy.id
        )
        db_session.add(prompt)
        db_session.commit()
        
        assert prompt.id is not None
        assert prompt.is_global is False
        assert prompt.strategy_id == sample_strategy.id
        assert prompt.strategy.name == "Test Strategy"
    
    def test_prompt_version_increment(self, db_session):
        """Test that prompt versions can be incremented."""
        prompt_v1 = PromptTemplate(
            name="Versioned Prompt",
            template_type="analysis",
            template_content="Version 1 content",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt_v1)
        db_session.commit()
        
        prompt_v2 = PromptTemplate(
            name="Versioned Prompt",
            template_type="analysis",
            template_content="Version 2 content",
            version=2,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt_v2)
        db_session.commit()
        
        prompts = db_session.query(PromptTemplate).filter_by(name="Versioned Prompt").all()
        assert len(prompts) == 2
        assert {p.version for p in prompts} == {1, 2}
    
    def test_prompt_active_inactive(self, db_session):
        """Test active/inactive prompt status."""
        prompt = PromptTemplate(
            name="Test Active Prompt",
            template_type="analysis",
            template_content="Test content",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt)
        db_session.commit()
        
        # Deactivate prompt
        prompt.is_active = False
        db_session.commit()
        
        assert prompt.is_active is False
    
    def test_prompt_repr(self, db_session):
        """Test string representation of prompt."""
        prompt = PromptTemplate(
            name="Test Prompt",
            template_type="analysis",
            template_content="Test content",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt)
        db_session.commit()
        
        repr_str = repr(prompt)
        assert "PromptTemplate" in repr_str
        assert "Test Prompt" in repr_str
        assert "analysis" in repr_str


class TestPromptPerformance:
    """Test PromptPerformance model."""
    
    def test_create_performance_record(self, db_session, sample_strategy):
        """Test creating a performance record."""
        prompt = PromptTemplate(
            name="Test Prompt",
            template_type="analysis",
            template_content="Test content",
            version=1,
            is_active=True,
            is_global=False,
            strategy_id=sample_strategy.id
        )
        db_session.add(prompt)
        db_session.commit()
        
        performance = PromptPerformance(
            prompt_template_id=prompt.id,
            prompt_version=prompt.version,
            strategy_id=sample_strategy.id,
            evaluation_date=date.today(),
            signals_generated=10,
            total_profit_loss=Decimal("1500.00"),
            win_count=7,
            loss_count=3,
            breakeven_count=0,
            avg_r_multiple=Decimal("2.5"),
            win_rate=Decimal("0.7"),
            avg_profit_per_trade=Decimal("250.00"),
            avg_loss_per_trade=Decimal("-100.00"),
            max_r_multiple=Decimal("5.0"),
            min_r_multiple=Decimal("-1.5")
        )
        db_session.add(performance)
        db_session.commit()
        
        assert performance.id is not None
        assert performance.signals_generated == 10
        assert performance.win_count == 7
        assert performance.loss_count == 3
        assert float(performance.win_rate) == 0.7
        assert float(performance.avg_r_multiple) == 2.5
    
    def test_performance_metrics_calculation(self, db_session):
        """Test that performance metrics are correctly stored."""
        prompt = PromptTemplate(
            name="Test Prompt",
            template_type="analysis",
            template_content="Test",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt)
        db_session.commit()
        
        # Create performance with calculated metrics
        total_signals = 20
        wins = 12
        losses = 8
        win_rate = Decimal(wins) / Decimal(total_signals)
        
        performance = PromptPerformance(
            prompt_template_id=prompt.id,
            prompt_version=1,
            evaluation_date=date.today(),
            signals_generated=total_signals,
            win_count=wins,
            loss_count=losses,
            win_rate=win_rate,
            avg_r_multiple=Decimal("1.8"),
            total_profit_loss=Decimal("3600.00")
        )
        db_session.add(performance)
        db_session.commit()
        
        assert float(performance.win_rate) == 0.6
        assert performance.signals_generated == 20
    
    def test_performance_repr(self, db_session):
        """Test string representation of performance."""
        prompt = PromptTemplate(
            name="Test Prompt",
            template_type="analysis",
            template_content="Test",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt)
        db_session.commit()
        
        performance = PromptPerformance(
            prompt_template_id=prompt.id,
            prompt_version=1,
            evaluation_date=date.today(),
            signals_generated=10,
            win_count=6,
            loss_count=4,
            win_rate=Decimal("0.6"),
            total_profit_loss=Decimal("1000.00")
        )
        db_session.add(performance)
        db_session.commit()
        
        repr_str = repr(performance)
        assert "PromptPerformance" in repr_str
        assert "1000" in repr_str
    
    def test_multiple_performance_records(self, db_session):
        """Test storing multiple performance records over time."""
        prompt = PromptTemplate(
            name="Test Prompt",
            template_type="analysis",
            template_content="Test",
            version=1,
            is_active=True,
            is_global=True
        )
        db_session.add(prompt)
        db_session.commit()
        
        # Create performance records for multiple days
        from datetime import timedelta
        for i in range(5):
            perf = PromptPerformance(
                prompt_template_id=prompt.id,
                prompt_version=1,
                evaluation_date=date.today() - timedelta(days=i),
                signals_generated=10,
                win_count=6,
                loss_count=4,
                win_rate=Decimal("0.6"),
                total_profit_loss=Decimal("500.00")
            )
            db_session.add(perf)
        db_session.commit()
        
        performances = db_session.query(PromptPerformance).filter_by(
            prompt_template_id=prompt.id
        ).all()
        assert len(performances) == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

