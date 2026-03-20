"""Workflow lineage tracking model."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, Integer, JSON, String, Text
from sqlalchemy.sql import func

from backend.core.database import Base


class LineageRecord(Base):
    """Stores per-step lineage for workflow executions."""

    __tablename__ = "workflow_lineage"

    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(150), nullable=False, index=True)
    step_name = Column(String(100), nullable=False, index=True)
    step_number = Column(Integer)
    input_data = Column(JSON)
    output_data = Column(JSON)
    _metadata = Column("metadata", JSON)
    status = Column(String(20), nullable=False, default="success")
    error = Column(Text)
    duration_ms = Column(Integer)
    recorded_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the record for tracing APIs."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "step_name": self.step_name,
            "step_number": self.step_number,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "metadata": self.metadata,
            "status": self.status,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "recorded_at": self.recorded_at.isoformat() if isinstance(self.recorded_at, datetime) else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"<LineageRecord(id={self.id}, execution_id={self.execution_id}, step={self.step_name}, status={self.status})>"

    def __init__(self, **kwargs):
        metadata_payload = kwargs.pop("metadata", None)
        super().__init__(**kwargs)
        if metadata_payload is not None:
            setattr(self, "_metadata", metadata_payload)

    def __getattribute__(self, item):
        if item == "metadata":
            return object.__getattribute__(self, "_metadata")
        return object.__getattribute__(self, item)
