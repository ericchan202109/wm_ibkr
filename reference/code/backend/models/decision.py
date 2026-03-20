"""Decision model for storing trading decisions."""
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Index
from backend.models import Base
from datetime import datetime, timezone


class Decision(Base):
    """
    Trading decision model.
    
    Stores decisions made by strategies after analyzing market data.
    Records the decision type (buy/sell/hold), target prices, stop losses,
    and the analysis that led to the decision.
    """
    __tablename__ = "decisions"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign keys (removed ForeignKey constraints - tables don't exist)
    strategy_id = Column(Integer, nullable=False, index=True)
    code_id = Column(Integer, nullable=True)
    
    # Decision data
    type = Column(String(50), nullable=False)  # 'buy', 'sell', 'hold'
    current_price = Column(Float, nullable=True)
    target_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    profit_margin = Column(Float, nullable=True)
    r_coefficient = Column(Float, nullable=True)  # R coefficient for risk assessment
    
    # Analysis text (consolidated from LLM)
    analysis_text = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    
    def __repr__(self):
        return f"<Decision(id={self.id}, strategy_id={self.strategy_id}, type={self.type}, current_price={self.current_price})>"
    
    def to_dict(self):
        """Convert decision to dictionary."""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "code_id": self.code_id,
            "type": self.type,
            "current_price": self.current_price,
            "target_price": self.target_price,
            "stop_loss": self.stop_loss,
            "profit_margin": self.profit_margin,
            "r_coefficient": self.r_coefficient,
            "analysis_text": self.analysis_text,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


# Create indexes for common queries
Index('idx_decisions_strategy_created', Decision.strategy_id, Decision.created_at.desc())

