"""Service helpers for workflow lineage tracking."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from backend.models.lineage import LineageRecord


class LineageTracker:
    """Persist and query workflow lineage records."""

    def __init__(self, db_session: Optional[Session] = None):
        self._db = db_session

    def _get_db(self, db: Optional[Session] = None) -> Session:
        """Return the active database session."""
        if db is not None:
            return db
        if self._db is None:
            raise ValueError("Database session is required for lineage operations")
        return self._db

    async def record_step(
        self,
        execution_id: str,
        step_name: str,
        step_number: int,
        input_data: Optional[Dict[str, Any]] = None,
        output_data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[int] = None,
        error: Optional[str] = None,
        status: Optional[str] = None,
        db: Optional[Session] = None,
    ) -> LineageRecord:
        """
        Create a lineage record for a workflow step.

        Args:
            execution_id: Identifier for workflow execution
            step_name: Name of the step/function
            step_number: Execution order
            input_data: Serialized inputs
            output_data: Serialized outputs
            metadata: Extra bookkeeping info
            duration_ms: Duration in milliseconds
            error: Error message (optional)
            status: Explicit status override
            db: Optional session override
        """
        session = self._get_db(db)
        record_status = status or ("error" if error else "success")

        record = LineageRecord(
            execution_id=execution_id,
            step_name=step_name,
            step_number=step_number,
            input_data=input_data,
            output_data=output_data,
            metadata=metadata,
            status=record_status,
            duration_ms=duration_ms,
            error=error,
        )

        session.add(record)
        session.commit()
        session.refresh(record)
        return record

    async def get_execution_lineage(
        self,
        execution_id: str,
        db: Optional[Session] = None,
    ) -> List[LineageRecord]:
        """Return every recorded step for a workflow execution."""
        session = self._get_db(db)
        return (
            session.query(LineageRecord)
            .filter(LineageRecord.execution_id == execution_id)
            .order_by(LineageRecord.step_number.asc(), LineageRecord.recorded_at.asc())
            .all()
        )

    async def get_step_lineage(
        self,
        execution_id: str,
        step_name: str,
        db: Optional[Session] = None,
    ) -> Optional[LineageRecord]:
        """Fetch a single step record for a workflow execution."""
        session = self._get_db(db)
        return (
            session.query(LineageRecord)
            .filter(
                LineageRecord.execution_id == execution_id,
                LineageRecord.step_name == step_name,
            )
            .order_by(LineageRecord.recorded_at.desc())
            .first()
        )
