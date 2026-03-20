"""Trading signal model with prompt + outcome tracking."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    JSON,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from backend.core.database import Base


class TradingSignal(Base):
    """Stores generated trading signals, chart metadata, and outcomes."""

    __tablename__ = "trading_signals"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="SET NULL"))

    signal_type = Column(String(10), nullable=False)
    trend = Column(String(20))
    confidence = Column(Float)
    reasoning = Column(Text)

    timeframe_primary = Column(String(10), nullable=False, default="1d")
    timeframe_confirmation = Column(String(10))

    # Trading parameters
    entry_price_low = Column(Float)
    entry_price_high = Column(Float)
    entry_price = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    target_conservative = Column(Float)
    target_aggressive = Column(Float)
    r_multiple_conservative = Column(Float)
    r_multiple_aggressive = Column(Float)
    position_size_percent = Column(Float)
    confirmation_signals = Column(JSON)

    # LLM + summary fields
    analysis_daily = Column(Text)
    analysis_weekly = Column(Text)
    analysis_consolidated = Column(Text)
    chart_url_daily = Column(String(500))
    chart_url_weekly = Column(String(500))
    chart_html_daily = Column(String(500))
    chart_html_weekly = Column(String(500))

    language = Column(String(5), nullable=False, default="en")
    model_used = Column(String(50))
    provider = Column(String(20))

    prompt_template_id = Column(Integer, ForeignKey("prompt_templates.id", ondelete="SET NULL"))
    prompt_version = Column(Integer)
    prompt_type = Column(String(50))

    generated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    status = Column(String(20), nullable=False, default="active")
    execution_price = Column(Float)
    execution_time = Column(DateTime(timezone=True))
    execution_notes = Column(Text)

    # Outcome tracking
    outcome = Column(String(20), default="pending")
    actual_r_multiple = Column(Numeric(10, 4))
    profit_loss = Column(Numeric(18, 4))
    exit_price = Column(Float)
    exit_time = Column(DateTime(timezone=True))

    strategy = relationship("Strategy", backref="trading_signals", lazy="selectin")
    prompt_template = relationship("PromptTemplate", backref="signals", lazy="selectin")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"<TradingSignal(id={self.id}, symbol={self.symbol}, signal={self.signal_type}, "
            f"strategy_id={self.strategy_id}, outcome={self.outcome})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the signal for API responses."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
            "signal_type": self.signal_type,
            "trend": self.trend,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "entry_price_low": self.entry_price_low,
            "entry_price_high": self.entry_price_high,
            "stop_loss": self.stop_loss,
            "target_price": self.target_price,
            "target_conservative": self.target_conservative,
            "target_aggressive": self.target_aggressive,
            "r_multiple_conservative": self.r_multiple_conservative,
            "r_multiple_aggressive": self.r_multiple_aggressive,
            "position_size_percent": self.position_size_percent,
            "confirmation_signals": self.confirmation_signals,
            "analysis_daily": self.analysis_daily,
            "analysis_weekly": self.analysis_weekly,
            "analysis_consolidated": self.analysis_consolidated,
            "chart_url_daily": self.chart_url_daily,
            "chart_url_weekly": self.chart_url_weekly,
            "language": self.language,
            "model_used": self.model_used,
            "provider": self.provider,
            "prompt_template_id": self.prompt_template_id,
            "prompt_version": self.prompt_version,
            "prompt_type": self.prompt_type,
            "generated_at": self._datetime_to_iso(self.generated_at),
            "expires_at": self._datetime_to_iso(self.expires_at),
            "status": self.status,
            "execution_price": self.execution_price,
            "execution_time": self._datetime_to_iso(self.execution_time),
            "execution_notes": self.execution_notes,
            "outcome": self.outcome,
            "exit_price": self.exit_price,
            "exit_time": self._datetime_to_iso(self.exit_time),
            "profit_loss": self._decimal_to_float(self.profit_loss),
            "actual_r_multiple": self._decimal_to_float(self.actual_r_multiple),
        }

    @staticmethod
    def _datetime_to_iso(value: Optional[datetime]) -> Optional[str]:
        if not value:
            return None
        return value.isoformat()

    @staticmethod
    def _decimal_to_float(value: Optional[Decimal]) -> Optional[float]:
        if value is None:
            return None
        return float(value)
