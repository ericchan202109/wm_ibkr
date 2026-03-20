"""Order schemas for API validation."""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class OrderBase(BaseModel):
    """Base order schema."""
    conid: int
    code: str
    order_type: str  # MKT, LMT, STP, etc.
    side: str  # BUY, SELL
    quantity: float
    price: Optional[float] = None
    aux_price: Optional[float] = None
    tif: str = "DAY"  # Time in force


class OrderCreate(OrderBase):
    """Schema for creating an order."""
    strategy_id: Optional[int] = None
    decision_id: Optional[int] = None


class OrderResponse(OrderBase):
    """Schema for order response."""
    id: int
    strategy_id: Optional[int]
    decision_id: Optional[int]
    status: str
    type: str
    submitted_at: datetime
    filled_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    ibkr_order_id: Optional[str] = None
    
    class Config:
        from_attributes = True

