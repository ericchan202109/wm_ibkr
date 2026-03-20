"""Workflow metadata endpoints."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.core.database import get_db
from backend.models.workflow import Workflow

router = APIRouter(prefix="/api/workflows", tags=["workflows"])


class WorkflowResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    dag_id: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True


@router.get("/", response_model=List[WorkflowResponse])
def list_workflows(
    active_only: bool = Query(False, description="Return only active workflows"),
    db: Session = Depends(get_db),
):
    query = db.query(Workflow)
    if active_only:
        query = query.filter(Workflow.is_active.is_(True))
    return query.order_by(Workflow.name.asc()).all()


@router.get("/{workflow_id}", response_model=WorkflowResponse)
def get_workflow(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow
