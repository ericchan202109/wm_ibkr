"""Strategy metadata endpoints for scheduler UI."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.core.database import get_db
from backend.services.strategy_service import StrategyService

router = APIRouter(prefix="/api/strategies", tags=["strategies"])


class StrategySummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    workflow_id: Optional[int] = None
    workflow_name: Optional[str] = None
    is_active: bool
    next_execution_at: Optional[datetime] = None
    last_executed_at: Optional[datetime] = None


@router.get("/", response_model=List[StrategySummary])
def list_strategies(
    active_only: bool = Query(False, description="Filter to active strategies"),
    db: Session = Depends(get_db),
):
    """Return strategies with minimal metadata for dropdowns."""
    service = StrategyService(db)
    strategies = service.list_strategies(active_only=active_only, limit=500)
    summaries: List[StrategySummary] = []
    for strategy in strategies:
        summaries.append(
            StrategySummary(
                id=strategy.id,
                name=strategy.name,
                description=strategy.description,
                workflow_id=strategy.workflow_id,
                workflow_name=strategy.workflow.name if strategy.workflow else None,
                is_active=bool(strategy.is_active),
                next_execution_at=strategy.next_execution_at,
                last_executed_at=strategy.last_executed_at,
            )
        )
    return summaries
