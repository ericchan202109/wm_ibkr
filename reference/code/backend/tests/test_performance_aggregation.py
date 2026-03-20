"""
Unit Tests for Prompt Performance Aggregation
"""
import pytest
from datetime import datetime, date, timedelta
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models.trading_signal import TradingSignal
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
def sample_prompt(db_session, sample_strategy):
    """Create a sample prompt."""
    prompt = PromptTemplate(
        name="Test Prompt",
        template_type="analysis",
        template_content="Test",
        version=1,
        is_active=True,
        is_global=False,
        strategy_id=sample_strategy.id
    )
    db_session.add(prompt)
    db_session.commit()
    db_session.refresh(prompt)
    return prompt


def calculate_performance(db_session, prompt_id, strategy_id, eval_date):
    """
    Helper function to calculate performance metrics for a prompt.
    This simulates the Celery task logic.
    """
    # Query completed signals for the given date
    signals = db_session.query(TradingSignal).filter(
        TradingSignal.prompt_template_id == prompt_id,
        TradingSignal.strategy_id == strategy_id,
        TradingSignal.outcome.in_(["win", "loss", "breakeven"]),
        TradingSignal.exit_time >= datetime.combine(eval_date, datetime.min.time()),
        TradingSignal.exit_time < datetime.combine(eval_date + timedelta(days=1), datetime.min.time())
    ).all()
    
    if not signals:
        return None
    
    signals_generated = len(signals)
    win_count = sum(1 for s in signals if s.outcome == "win")
    loss_count = sum(1 for s in signals if s.outcome == "loss")
    breakeven_count = sum(1 for s in signals if s.outcome == "breakeven")
    
    total_profit_loss = sum(s.profit_loss or 0 for s in signals)
    
    r_multiples = [s.actual_r_multiple for s in signals if s.actual_r_multiple is not None]
    avg_r_multiple = sum(r_multiples) / len(r_multiples) if r_multiples else 0
    max_r_multiple = max(r_multiples) if r_multiples else 0
    min_r_multiple = min(r_multiples) if r_multiples else 0
    
    win_rate = Decimal(win_count) / Decimal(signals_generated) if signals_generated > 0 else Decimal(0)
    
    winning_signals = [s for s in signals if s.outcome == "win" and s.profit_loss]
    losing_signals = [s for s in signals if s.outcome == "loss" and s.profit_loss]
    
    avg_profit_per_trade = (
        sum(s.profit_loss for s in winning_signals) / len(winning_signals)
        if winning_signals else 0
    )
    avg_loss_per_trade = (
        sum(s.profit_loss for s in losing_signals) / len(losing_signals)
        if losing_signals else 0
    )
    
    # Get prompt version
    prompt = db_session.query(PromptTemplate).get(prompt_id)
    
    return {
        "prompt_template_id": prompt_id,
        "prompt_version": prompt.version,
        "strategy_id": strategy_id,
        "evaluation_date": eval_date,
        "signals_generated": signals_generated,
        "total_profit_loss": Decimal(str(total_profit_loss)),
        "win_count": win_count,
        "loss_count": loss_count,
        "breakeven_count": breakeven_count,
        "avg_r_multiple": Decimal(str(avg_r_multiple)),
        "win_rate": win_rate,
        "avg_profit_per_trade": Decimal(str(avg_profit_per_trade)),
        "avg_loss_per_trade": Decimal(str(avg_loss_per_trade)),
        "max_r_multiple": Decimal(str(max_r_multiple)),
        "min_r_multiple": Decimal(str(min_r_multiple))
    }


class TestPerformanceAggregation:
    """Test performance aggregation logic."""
    
    def test_aggregate_winning_signals(self, db_session, sample_strategy, sample_prompt):
        """Test aggregating performance for winning signals."""
        eval_date = date.today()
        
        # Create 3 winning signals
        for i in range(3):
            signal = TradingSignal(
                strategy_id=sample_strategy.id,
                symbol=f"STOCK{i}",
                signal_type="BUY",
                entry_price=100.00,
                stop_loss=95.00,
                target_price=110.00,
                reasoning="Test",
                confidence=0.8,
                generated_at=datetime.now(),
                prompt_template_id=sample_prompt.id,
                prompt_version=1,
                prompt_type="analysis",
                outcome="win",
                exit_price=108.00,
                exit_time=datetime.now(),
                profit_loss=800.00,
                actual_r_multiple=1.6
            )
            db_session.add(signal)
        db_session.commit()
        
        # Calculate performance
        perf_data = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, eval_date
        )
        
        assert perf_data is not None
        assert perf_data["signals_generated"] == 3
        assert perf_data["win_count"] == 3
        assert perf_data["loss_count"] == 0
        assert float(perf_data["win_rate"]) == 1.0
        assert float(perf_data["avg_r_multiple"]) == 1.6
        assert float(perf_data["total_profit_loss"]) == 2400.00
    
    def test_aggregate_mixed_signals(self, db_session, sample_strategy, sample_prompt):
        """Test aggregating performance with wins and losses."""
        eval_date = date.today()
        
        # Create 6 wins and 4 losses
        for i in range(6):
            signal = TradingSignal(
                strategy_id=sample_strategy.id,
                symbol=f"WIN{i}",
                signal_type="BUY",
                entry_price=100.00,
                stop_loss=95.00,
                target_price=110.00,
                reasoning="Test",
                confidence=0.8,
                generated_at=datetime.now(),
                prompt_template_id=sample_prompt.id,
                prompt_version=1,
                outcome="win",
                exit_price=108.00,
                exit_time=datetime.now(),
                profit_loss=800.00,
                actual_r_multiple=1.6
            )
            db_session.add(signal)
        
        for i in range(4):
            signal = TradingSignal(
                strategy_id=sample_strategy.id,
                symbol=f"LOSS{i}",
                signal_type="BUY",
                entry_price=100.00,
                stop_loss=95.00,
                target_price=110.00,
                reasoning="Test",
                confidence=0.8,
                generated_at=datetime.now(),
                prompt_template_id=sample_prompt.id,
                prompt_version=1,
                outcome="loss",
                exit_price=95.00,
                exit_time=datetime.now(),
                profit_loss=-500.00,
                actual_r_multiple=-1.0
            )
            db_session.add(signal)
        
        db_session.commit()
        
        # Calculate performance
        perf_data = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, eval_date
        )
        
        assert perf_data["signals_generated"] == 10
        assert perf_data["win_count"] == 6
        assert perf_data["loss_count"] == 4
        assert float(perf_data["win_rate"]) == 0.6
        
        # Total P/L: (6 * 800) - (4 * 500) = 4800 - 2000 = 2800
        assert float(perf_data["total_profit_loss"]) == 2800.00
        
        # Avg R: (6 * 1.6 + 4 * -1.0) / 10 = (9.6 - 4.0) / 10 = 0.56
        assert abs(float(perf_data["avg_r_multiple"]) - 0.56) < 0.01
    
    def test_store_performance_in_database(self, db_session, sample_strategy, sample_prompt):
        """Test storing calculated performance in database."""
        eval_date = date.today()
        
        # Create some signals
        signal = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="AAPL",
            signal_type="BUY",
            entry_price=150.00,
            stop_loss=145.00,
            target_price=165.00,
            reasoning="Test",
            confidence=0.85,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            outcome="win",
            exit_price=163.00,
            exit_time=datetime.now(),
            profit_loss=1300.00,
            actual_r_multiple=2.6
        )
        db_session.add(signal)
        db_session.commit()
        
        # Calculate and store performance
        perf_data = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, eval_date
        )
        
        performance = PromptPerformance(**perf_data)
        db_session.add(performance)
        db_session.commit()
        
        # Verify stored performance
        stored_perf = db_session.query(PromptPerformance).filter_by(
            prompt_template_id=sample_prompt.id,
            evaluation_date=eval_date
        ).first()
        
        assert stored_perf is not None
        assert stored_perf.signals_generated == 1
        assert stored_perf.win_count == 1
        assert float(stored_perf.total_profit_loss) == 1300.00
    
    def test_no_performance_for_pending_signals(self, db_session, sample_strategy, sample_prompt):
        """Test that pending signals are not included in performance."""
        eval_date = date.today()
        
        # Create pending signals
        for i in range(3):
            signal = TradingSignal(
                strategy_id=sample_strategy.id,
                symbol=f"PENDING{i}",
                signal_type="BUY",
                entry_price=100.00,
                stop_loss=95.00,
                target_price=110.00,
                reasoning="Test",
                confidence=0.8,
                generated_at=datetime.now(),
                prompt_template_id=sample_prompt.id,
                prompt_version=1,
                outcome="pending"  # Still open
            )
            db_session.add(signal)
        db_session.commit()
        
        # Calculate performance
        perf_data = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, eval_date
        )
        
        # Should be None since no completed signals
        assert perf_data is None
    
    def test_performance_for_different_dates(self, db_session, sample_strategy, sample_prompt):
        """Test calculating performance for different dates."""
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        # Create signal from yesterday
        signal_yesterday = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="YESTERDAY",
            signal_type="BUY",
            entry_price=100.00,
            stop_loss=95.00,
            target_price=110.00,
            reasoning="Test",
            confidence=0.8,
            generated_at=datetime.now() - timedelta(days=1),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            outcome="win",
            exit_price=108.00,
            exit_time=datetime.now() - timedelta(days=1),
            profit_loss=800.00,
            actual_r_multiple=1.6
        )
        
        # Create signal from today
        signal_today = TradingSignal(
            strategy_id=sample_strategy.id,
            symbol="TODAY",
            signal_type="BUY",
            entry_price=100.00,
            stop_loss=95.00,
            target_price=110.00,
            reasoning="Test",
            confidence=0.8,
            generated_at=datetime.now(),
            prompt_template_id=sample_prompt.id,
            prompt_version=1,
            outcome="win",
            exit_price=108.00,
            exit_time=datetime.now(),
            profit_loss=800.00,
            actual_r_multiple=1.6
        )
        
        db_session.add_all([signal_yesterday, signal_today])
        db_session.commit()
        
        # Calculate performance for yesterday
        perf_yesterday = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, yesterday
        )
        
        # Calculate performance for today
        perf_today = calculate_performance(
            db_session, sample_prompt.id, sample_strategy.id, today
        )
        
        assert perf_yesterday["signals_generated"] == 1
        assert perf_today["signals_generated"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

