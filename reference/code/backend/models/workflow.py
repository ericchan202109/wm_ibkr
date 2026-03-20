"""Workflow model to back strategy workflow references."""
from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from backend.core.database import Base


class Workflow(Base):
    """Represents a logical workflow (Airflow DAG + metadata)."""

    __tablename__ = "workflows"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)
    dag_id = Column(String(255))
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    strategies = relationship("Strategy", back_populates="workflow")

    def to_dict(self) -> dict:
        """Serialize workflow metadata for APIs."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "dag_id": self.dag_id,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<Workflow(id={self.id}, name={self.name})>"


class WorkflowExecution(Base):
    """Represents an individual workflow run (strategy execution)."""

    __tablename__ = "workflow_executions"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id", ondelete="SET NULL"))
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="SET NULL"))
    status = Column(String(20), nullable=False, default="pending")
    run_type = Column(String(20), default="scheduled")  # scheduled, manual, retry
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    result = Column(JSON)
    error = Column(Text)

    workflow = relationship("Workflow", backref="executions")

    def to_dict(self) -> dict:
        """Serialize execution metadata."""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "strategy_id": self.strategy_id,
            "status": self.status,
            "run_type": self.run_type,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<WorkflowExecution(id={self.id}, workflow_id={self.workflow_id}, status={self.status})>"
