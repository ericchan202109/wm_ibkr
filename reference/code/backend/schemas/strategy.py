"""Strategy schemas for API validation."""
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class StrategyBase(BaseModel):
    """Base strategy schema."""
    name: str
    workflow_id: Optional[int] = None
    param: Optional[Dict[str, Any]] = None
    active: Optional[bool] = True


class StrategyCreate(StrategyBase):
    """Schema for creating a strategy."""
    symbol_ids: Optional[List[int]] = []  # List of Code IDs (symbols) to associate
    indicator_ids: Optional[List[int]] = []  # List of Indicator IDs to associate


class StrategyUpdate(BaseModel):
    """Schema for updating a strategy."""
    name: Optional[str] = None
    workflow_id: Optional[int] = None
    param: Optional[Dict[str, Any]] = None
    active: Optional[bool] = None
    symbol_ids: Optional[List[int]] = None  # List of Code IDs to update
    indicator_ids: Optional[List[int]] = None  # List of Indicator IDs to update


class CodeInfo(BaseModel):
    """Code (symbol) information."""
    id: int
    symbol: str
    conid: int
    exchange: Optional[str] = None
    name: Optional[str] = None
    
    class Config:
        from_attributes = True


class IndicatorInfo(BaseModel):
    """Indicator information for strategy response."""
    id: int
    name: str
    type: str
    parameters: Dict[str, Any]
    
    class Config:
        from_attributes = True


class StrategyResponse(StrategyBase):
    """Schema for strategy response."""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    codes: List[CodeInfo] = []  # Associated symbols
    indicators: List[IndicatorInfo] = []  # Associated indicators
    
    class Config:
        from_attributes = True
