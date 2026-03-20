"""
Unit Tests for Trading Signal Tracking with Prompt Metadata
"""
import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models.trading_signal import TradingSignal
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
def sample_prompt(db_session):
    """Create a sample prompt."""
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
    db_session.refresh(prompt)
    return prompt


class TestTradingSignalWithPromptTracking:
    """Test trading signal with prompt tracking."""
    
    def test_create_signal_with_prompt_metadata(self, db_session, sample_strategy, sample_prompt):
        """Test creating a signal with prompt tracking."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="AAPL",
            signal_type="BUY",
            entry_price=150.00,
            stop_loss=145.00,
            target_price=160.00,
            reasoning="Test signal",
            confidence=0.85,
            generated_at=datetime.now(),
            # Prompt tracking
            prompt_template_id=sample_prompt.id,
            prompt_version=sample_prompt.version,
            prompt_type="analysis"
        )
        db_session.add(signal)
        db_session.commit()
        
        assert signal.id is not None
        assert signal.prompt_template_id == sample_prompt.id
        assert signal.prompt_version == 1
        assert signal.prompt_type == "analysis"
    
    def test_signal_outcome_tracking(self, db_session, sample_strategy, sample_prompt):
        """Test tracking signal outcomes."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="AAPL",
            signal_type="BUY",
            entry_price=150.00,
            stop_loss=145.00,
            target_price=160.00,
            reasoning="Test",
            confidence=0.85,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            prompt_type="analysis"
        )
        db_session.add(signal)
        db_session.commit()
        
        # Update with outcome
        signal.outcome = "win"
        signal.exit_price = 158.00
        signal.exit_time = datetime.now()
        signal.profit_loss = 800.00  # (158-150) * 100 shares
        signal.actual_r_multiple = 1.6  # (158-150)/(150-145)
        db_session.commit()
        
        assert signal.outcome == "win"
        assert signal.exit_price == 158.00
        assert signal.profit_loss == 800.00
        assert signal.actual_r_multiple == 1.6
    
    def test_signal_loss_outcome(self, db_session, sample_strategy, sample_prompt):
        """Test tracking a losing signal."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="TSLA",
            signal_type="BUY",
            entry_price=200.00,
            stop_loss=195.00,
            target_price=210.00,
            reasoning="Test",
            confidence=0.75,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            prompt_type="analysis",
            # Losing outcome
            outcome="loss",
            exit_price=195.00,
            exit_time=datetime.now(),
            profit_loss=-500.00,
            actual_r_multiple=-1.0
        )
        db_session.add(signal)
        db_session.commit()
        
        assert signal.outcome == "loss"
        assert signal.profit_loss < 0
        assert signal.actual_r_multiple == -1.0
    
    def test_signal_pending_outcome(self, db_session, sample_strategy, sample_prompt):
        """Test signal with pending outcome."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="GOOGL",
            signal_type="BUY",
            entry_price=140.00,
            stop_loss=135.00,
            target_price=150.00,
            reasoning="Test",
            confidence=0.9,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            prompt_type="analysis",
            outcome="pending"  # Still open
        )
        db_session.add(signal)
        db_session.commit()
        
        assert signal.outcome == "pending"
        assert signal.exit_price is None
        assert signal.exit_time is None
        assert signal.profit_loss is None
    
    def test_signal_to_dict_includes_prompt_tracking(self, db_session, sample_strategy, sample_prompt):
        """Test that to_dict includes prompt tracking fields."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="NVDA",
            signal_type="BUY",
            entry_price=500.00,
            stop_loss=490.00,
            target_price=520.00,
            reasoning="Test",
            confidence=0.88,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            prompt_type="analysis",
            outcome="win",
            exit_price=518.00,
            profit_loss=1800.00,
            actual_r_multiple=1.8
        )
        db_session.add(signal)
        db_session.commit()
        
        signal_dict = signal.to_dict()
        assert "prompt_template_id" in signal_dict
        assert "prompt_version" in signal_dict
        assert "prompt_type" in signal_dict
        assert "outcome" in signal_dict
        assert "actual_r_multiple" in signal_dict
        assert "profit_loss" in signal_dict
        assert signal_dict["prompt_template_id"] == sample_prompt.id
    
    def test_query_signals_by_prompt(self, db_session, sample_strategy, sample_prompt):
        """Test querying signals by prompt template."""
        # Create multiple signals with same prompt
        for i in range(5):
            signal = TradingSignal(
                strategy_id=sample_strategy.id,
                symbol=f"STOCK{i}",
                signal_type="BUY",
                entry_price=100.00 + i,
                stop_loss=95.00 + i,
                target_price=110.00 + i,
                reasoning="Test",
                confidence=0.8,
                generated_at=datetime.now(),
                prompt_template_id=sample_prompt.id,
                prompt_version=1,
                prompt_type="analysis"
            )
            db_session.add(signal)
        db_session.commit()
        
        # Query by prompt
        signals = db_session.query(TradingSignal).filter_by(
            prompt_template_id=sample_prompt.id
        ).all()
        
        assert len(signals) == 5
        assert all(s.prompt_template_id == sample_prompt.id for s in signals)
    
    def test_query_signals_by_outcome(self, db_session, sample_strategy, sample_prompt):
        """Test querying signals by outcome."""
        # Create winning and losing signals
        win_signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="WIN",
            signal_type="BUY",
            entry_price=100.00,
            stop_loss=95.00,
            target_price=110.00,
            reasoning="Test",
            confidence=0.8,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            outcome="win"
        )
        loss_signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="LOSS",
            signal_type="BUY",
            entry_price=100.00,
            stop_loss=95.00,
            target_price=110.00,
            reasoning="Test",
            confidence=0.8,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            outcome="loss"
        )
        db_session.add_all([win_signal, loss_signal])
        db_session.commit()
        
        # Query wins
        wins = db_session.query(TradingSignal).filter_by(outcome="win").all()
        assert len(wins) == 1
        assert wins[0].symbol == "WIN"
        
        # Query losses
        losses = db_session.query(TradingSignal).filter_by(outcome="loss").all()
        assert len(losses) == 1
        assert losses[0].symbol == "LOSS"
    
    def test_signal_relationship_with_prompt(self, db_session, sample_strategy, sample_prompt):
        """Test SQLAlchemy relationship between signal and prompt."""
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="AAPL",
            signal_type="BUY",
            entry_price=150.00,
            stop_loss=145.00,
            target_price=160.00,
            reasoning="Test",
            confidence=0.85,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            prompt_type="analysis"
        )
        db_session.add(signal)
        db_session.commit()
        db_session.refresh(signal)
        
        # Access prompt through relationship
        assert signal.prompt_template is not None
        assert signal.prompt_template.name == "Test Prompt"
        assert signal.prompt_template.id == sample_prompt.id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

