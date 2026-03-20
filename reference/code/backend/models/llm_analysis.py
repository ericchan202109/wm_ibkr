"""LLM Analysis model for storing AI-generated market analysis."""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, DECIMAL, JSON
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from backend.core.database import Base


class LLMAnalysis(Base):
    """
    Model for storing LLM analysis of technical charts.
    
    Each analysis is linked to a chart and contains the LLM's interpretation,
    identified patterns, support/resistance levels, and trading recommendations.
    """
    __tablename__ = "llm_analyses"
    
    # Primary Key
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign Keys
    execution_id = Column(Integer, ForeignKey("workflow_executions.id"), nullable=True, index=True)
    chart_id = Column(Integer, ForeignKey("charts.id"), nullable=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=True)
    
    # Symbol Information
    symbol = Column(String(20), nullable=False, index=True)
    
    # LLM Request Details
    prompt_text = Column(Text, nullable=False)
    prompt_template_id = Column(Integer, nullable=True)
    model_name = Column(String(100), nullable=True)  # 'gpt-4-turbo-preview'
    timeframe = Column(String(10), nullable=True)  # '1d', '1w', '1mo'
    
    # Strategy Context
    indicators_metadata = Column(JSON, nullable=True)  # Full indicator configuration used
    
    # LLM Response
    response_text = Column(Text, nullable=True)
    response_json = Column(JSON, nullable=True)  # Structured response if applicable
    confidence_score = Column(DECIMAL(5, 4), nullable=True)
    
    # Technical Insights Extracted from LLM Response
    trend_direction = Column(String(20), nullable=True)  # bullish, bearish, neutral
    support_levels = Column(JSON, nullable=True)  # [150.20, 148.50]
    resistance_levels = Column(JSON, nullable=True)  # [155.00, 158.30]
    key_patterns = Column(JSON, nullable=True)  # ['double_bottom', 'breakout']
    
    # Performance Metadata
    tokens_used = Column(Integer, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    
    # Status & Timestamps
    analyzed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(String(20), default='completed', nullable=False)
    error_message = Column(Text, nullable=True)
    
    # Relationships
    # execution = relationship("WorkflowExecution", back_populates="llm_analyses")  # Removed - workflows deprecated
    chart = relationship("Chart", back_populates="llm_analyses")
    # strategy = relationship("Strategy")  # Removed - strategies feature deprecated
    # signals = relationship("TradingSignal", back_populates="llm_analysis")  # Removed - TradingSignal model deprecated
    
    def to_dict(self):
        """Convert LLM analysis to dictionary."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "chart_id": self.chart_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "prompt_text": self.prompt_text,
            "prompt_template_id": self.prompt_template_id,
            "model_name": self.model_name,
            "timeframe": self.timeframe,
            "indicators_metadata": self.indicators_metadata,
            "response_text": self.response_text,
            "response_json": self.response_json,
            "confidence_score": float(self.confidence_score) if self.confidence_score else None,
            "trend_direction": self.trend_direction,
            "support_levels": self.support_levels,
            "resistance_levels": self.resistance_levels,
            "key_patterns": self.key_patterns,
            "tokens_used": self.tokens_used,
            "latency_ms": self.latency_ms,
            "analyzed_at": self.analyzed_at.isoformat() if self.analyzed_at else None,
            "status": self.status,
            "error_message": self.error_message
        }
    
    def __repr__(self):
        return f"<LLMAnalysis(id={self.id}, symbol={self.symbol}, model={self.model_name}, analyzed_at={self.analyzed_at})>"
