"""Strategy, Code, and association models."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.sql import func

from backend.core.database import Base

JSONType = JSON


StrategyCode = Table(
    "strategy_codes",
    Base.metadata,
    Column("strategy_id", Integer, ForeignKey("strategies.id", ondelete="CASCADE"), primary_key=True),
    Column("code_id", Integer, ForeignKey("codes.id", ondelete="CASCADE"), primary_key=True),
    UniqueConstraint("strategy_id", "code_id", name="uq_strategy_code"),
)


class Code(Base):
    """Tradable instrument metadata synced from IBKR."""

    __tablename__ = "codes"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), nullable=False)
    conid = Column(Integer, nullable=False, unique=True, index=True)
    exchange = Column(String(50))
    name = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    strategies = relationship("Strategy", secondary=StrategyCode, back_populates="codes")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Code(symbol={self.symbol}, conid={self.conid})>"


class Strategy(Base):
    """Trading strategy definition with scheduling + LLM config."""

    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)
    workflow_id = Column(Integer, ForeignKey("workflows.id", ondelete="SET NULL"))
    type = Column(String(50))
    param = Column(MutableDict.as_mutable(JSONType), default=dict)
    _is_active = Column("active", Boolean, nullable=False, default=True)
    schedule = Column(String(100))  # Cron expression
    symbol_conid = Column(Integer)
    prompt_template_id = Column(Integer)
    risk_params = Column(MutableDict.as_mutable(JSONType), default=dict)
    last_executed_at = Column(DateTime(timezone=True))
    next_execution_at = Column(DateTime(timezone=True))

    # LLM configuration
    llm_enabled = Column(Boolean, default=False)
    llm_model = Column(String(50), default="gpt-4-vision-preview")
    llm_language = Column(String(5), default="en")
    llm_timeframes = Column(MutableList.as_mutable(JSONType), default=lambda: ["1d", "1w"])
    llm_consolidate = Column(Boolean, default=True)
    llm_prompt_custom = Column(String(500))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    workflow = relationship("Workflow", back_populates="strategies")
    codes = relationship("Code", secondary=StrategyCode, back_populates="strategies", lazy="selectin")

    # Provide both is_active and active attribute names for compatibility
    is_active = synonym("_is_active")
    active = synonym("_is_active")

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Serialize key strategy fields for APIs."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "workflow_id": self.workflow_id,
            "type": self.type,
            "param": self.param or {},
            "schedule": self.schedule,
            "symbol_conid": self.symbol_conid,
            "is_active": bool(self.is_active),
            "llm_enabled": bool(self.llm_enabled),
            "llm_model": self.llm_model,
            "llm_language": self.llm_language,
            "llm_timeframes": list(self.llm_timeframes or []),
            "llm_consolidate": bool(self.llm_consolidate),
            "llm_prompt_custom": self.llm_prompt_custom,
            "next_execution_at": self.next_execution_at.isoformat() if self.next_execution_at else None,
            "last_executed_at": self.last_executed_at.isoformat() if self.last_executed_at else None,
        }

    def mark_executed(self, executed_at: Optional[datetime] = None) -> None:
        """Update bookkeeping after a successful run."""
        executed_ts = executed_at or datetime.now(timezone.utc)
        if executed_ts.tzinfo is None:
            executed_ts = executed_ts.replace(tzinfo=timezone.utc)
        self.last_executed_at = executed_ts

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Strategy(id={self.id}, name={self.name}, active={self.is_active})>"

