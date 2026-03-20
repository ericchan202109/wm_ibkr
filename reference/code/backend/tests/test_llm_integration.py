"""
Integration Tests for LLM Service with Database Prompts
"""
import pytest
from unittest.mock import Mock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.services.llm_service import LLMService
from backend.models.prompt import PromptTemplate
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
    """Create a sample strategy."""
    strategy = Strategy(
        name="Test Strategy",
        description="Test",
        workflow_id=1,
        is_active=True
    )
    db_session.add(strategy)
    db_session.commit()
    db_session.refresh(strategy)
    return strategy


@pytest.fixture
def global_prompt(db_session):
    """Create a global prompt."""
    prompt = PromptTemplate(
        name="Global Analysis Prompt",
        template_type="analysis",
        template_content="Analysis for {{ symbol }}: Price={{ current_price }}",
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
def strategy_prompt(db_session, sample_strategy):
    """Create a strategy-specific prompt."""
    prompt = PromptTemplate(
        name="Strategy Specific Prompt",
        template_type="analysis",
        template_content="Strategy analysis for {{ symbol }}: RSI={{ rsi }}",
        version=1,
        is_active=True,
        is_global=False,
        strategy_id=sample_strategy.id
    )
    db_session.add(prompt)
    db_session.commit()
    db_session.refresh(prompt)
    return prompt


class TestLLMServiceIntegration:
    """Test LLM service integration with database prompts."""
    
    @patch('backend.services.llm_service.SessionLocal')
    def test_load_global_prompt(self, mock_session_class, db_session, global_prompt):
        """Test loading global prompt from database."""
        mock_session_class.return_value = db_session
        
        service = LLMService()
        prompt = service.get_prompt_for_analysis(
            template_type="analysis",
            language="en",
            strategy_id=None
        )
        
        assert prompt is not None
        assert prompt.name == "Global Analysis Prompt"
        assert prompt.is_global is True
    
    @patch('backend.services.llm_service.SessionLocal')
    def test_load_strategy_specific_prompt(self, mock_session_class, db_session, 
                                          sample_strategy, strategy_prompt):
        """Test loading strategy-specific prompt."""
        mock_session_class.return_value = db_session
        
        service = LLMService()
        prompt = service.get_prompt_for_analysis(
            template_type="analysis",
            language="en",
            strategy_id=sample_strategy.id
        )
        
        assert prompt is not None
        assert prompt.name == "Strategy Specific Prompt"
        assert prompt.strategy_id == sample_strategy.id
    
    @patch('backend.services.llm_service.SessionLocal')
    def test_fallback_to_global_prompt(self, mock_session_class, db_session, 
                                       sample_strategy, global_prompt):
        """Test fallback to global prompt when strategy-specific not found."""
        mock_session_class.return_value = db_session
        
        service = LLMService()
        # Request for strategy that has no specific prompt
        prompt = service.get_prompt_for_analysis(
            template_type="analysis",
            language="en",
            strategy_id=sample_strategy.id
        )
        
        assert prompt is not None
        assert prompt.name == "Global Analysis Prompt"
        assert prompt.is_global is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

