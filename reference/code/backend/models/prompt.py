"""Prompt template and performance models."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    event,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.sql import func

from backend.core.database import Base


class PromptTemplate(Base):
    """Configurable Jinja2 template used by the LLM service."""

    __tablename__ = "prompt_templates"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    template_type = Column(String(50), nullable=False, default="analysis")

    # Template body + versioning
    prompt_text = Column(Text, nullable=False)
    template_content = synonym("prompt_text")
    template_version = Column(Integer, nullable=False, default=1)
    version = synonym("template_version")

    language = Column(String(5), nullable=False, default="en")
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="CASCADE"))
    is_active = Column(Boolean, nullable=False, default=True)
    is_global = Column(Boolean, nullable=False, default=True)
    is_default = Column(Boolean, nullable=False, default=False)
    created_by = Column(String(100))
    tags = Column(String(255))
    notes = Column(Text)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    strategy = relationship("Strategy", backref="prompt_templates", lazy="selectin")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"<PromptTemplate(id={self.id}, name={self.name}, type={self.template_type}, "
            f"version={self.template_version}, active={self.is_active})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize template for API responses."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "template_type": self.template_type,
            "template_content": self.prompt_text,
            "version": self.template_version,
            "language": self.language,
            "strategy_id": self.strategy_id,
            "is_active": bool(self.is_active),
            "is_global": bool(self.is_global),
            "is_default": bool(self.is_default),
            "tags": self.tags,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class PromptPerformance(Base):
    """Daily performance snapshot for a prompt template."""

    __tablename__ = "prompt_performance"

    id = Column(Integer, primary_key=True, index=True)
    prompt_template_id = Column(Integer, ForeignKey("prompt_templates.id", ondelete="CASCADE"), nullable=False)
    prompt_version = Column(Integer)
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="CASCADE"))

    date = Column(Date, nullable=False)
    evaluation_date = synonym("date")

    signals_generated = Column(Integer, nullable=False, default=0)
    signals_executed = Column(Integer, nullable=False, default=0)
    total_profit_loss = Column(Numeric(18, 4))
    win_count = Column(Integer, nullable=False, default=0)
    loss_count = Column(Integer, nullable=False, default=0)
    breakeven_count = Column(Integer, nullable=False, default=0)
    win_rate = Column(Numeric(6, 4))

    avg_r_multiple = Column(Numeric(10, 4))
    best_r_multiple = Column(Numeric(10, 4))
    worst_r_multiple = Column(Numeric(10, 4))
    max_r_multiple = synonym("best_r_multiple")
    min_r_multiple = synonym("worst_r_multiple")

    avg_profit_pct = Column(Numeric(10, 4))
    avg_loss_pct = Column(Numeric(10, 4))
    avg_profit_per_trade = Column(Numeric(18, 4))
    avg_loss_per_trade = Column(Numeric(18, 4))
    avg_confidence = Column(Numeric(6, 4))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    prompt_template = relationship("PromptTemplate", backref="performance_records", lazy="selectin")
    strategy = relationship("Strategy", backref="prompt_performance", lazy="selectin")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"<PromptPerformance(id={self.id}, prompt_id={self.prompt_template_id}, "
            f"date={self.date}, signals={self.signals_generated}, win_rate={self.win_rate})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize performance metrics."""
        return {
            "id": self.id,
            "prompt_template_id": self.prompt_template_id,
            "strategy_id": self.strategy_id,
            "date": self.date.isoformat() if isinstance(self.date, (date, datetime)) else self.date,
            "signals_generated": self.signals_generated,
            "signals_executed": self.signals_executed,
            "total_profit_loss": self._decimal_to_float(self.total_profit_loss),
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "breakeven_count": self.breakeven_count,
            "win_rate": self._decimal_to_float(self.win_rate),
            "avg_r_multiple": self._decimal_to_float(self.avg_r_multiple),
            "max_r_multiple": self._decimal_to_float(self.max_r_multiple),
            "min_r_multiple": self._decimal_to_float(self.min_r_multiple),
            "avg_profit_pct": self._decimal_to_float(self.avg_profit_pct),
            "avg_loss_pct": self._decimal_to_float(self.avg_loss_pct),
            "avg_profit_per_trade": self._decimal_to_float(self.avg_profit_per_trade),
            "avg_loss_per_trade": self._decimal_to_float(self.avg_loss_per_trade),
            "avg_confidence": self._decimal_to_float(self.avg_confidence),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @staticmethod
    def _decimal_to_float(value: Optional[Decimal]) -> Optional[float]:
        """Convert Decimal to float for JSON responses."""
        if value is None:
            return None
        return float(value)


# ---------------------------------------------------------------------------
# In-memory caches used for sqlite in-memory testing scenarios
# ---------------------------------------------------------------------------
PROMPT_CACHE: Dict[int, Dict[str, Any]] = {}
PROMPT_PERFORMANCE_CACHE: Dict[int, List[Dict[str, Any]]] = {}


def _snapshot_prompt(prompt: PromptTemplate) -> Dict[str, Any]:
    return prompt.to_dict()


def _snapshot_performance(perf: PromptPerformance) -> Dict[str, Any]:
    return perf.to_dict()


@event.listens_for(PromptTemplate, "after_insert")
def _prompt_after_insert(mapper, connection, target):
    PROMPT_CACHE[target.id] = _snapshot_prompt(target)


@event.listens_for(PromptTemplate, "after_update")
def _prompt_after_update(mapper, connection, target):
    PROMPT_CACHE[target.id] = _snapshot_prompt(target)


@event.listens_for(PromptTemplate, "after_delete")
def _prompt_after_delete(mapper, connection, target):
    PROMPT_CACHE.pop(target.id, None)


@event.listens_for(PromptTemplate.__table__, "after_drop")
def _prompt_table_drop(*_, **__):
    PROMPT_CACHE.clear()


@event.listens_for(PromptPerformance, "after_insert")
def _perf_after_insert(mapper, connection, target):
    data = _snapshot_performance(target)
    PROMPT_PERFORMANCE_CACHE.setdefault(target.prompt_template_id, [])
    PROMPT_PERFORMANCE_CACHE[target.prompt_template_id] = [
        rec for rec in PROMPT_PERFORMANCE_CACHE[target.prompt_template_id] if rec.get("id") != target.id
    ]
    PROMPT_PERFORMANCE_CACHE[target.prompt_template_id].append(data)


@event.listens_for(PromptPerformance, "after_update")
def _perf_after_update(mapper, connection, target):
    _perf_after_insert(mapper, connection, target)


@event.listens_for(PromptPerformance, "after_delete")
def _perf_after_delete(mapper, connection, target):
    records = PROMPT_PERFORMANCE_CACHE.get(target.prompt_template_id)
    if records is not None:
        PROMPT_PERFORMANCE_CACHE[target.prompt_template_id] = [
            rec for rec in records if rec.get("id") != target.id
        ]


@event.listens_for(PromptPerformance.__table__, "after_drop")
def _perf_table_drop(*_, **__):
    PROMPT_PERFORMANCE_CACHE.clear()
