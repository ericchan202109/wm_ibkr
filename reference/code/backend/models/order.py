"""Order model for tracking IBKR orders."""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from backend.core.database import Base
from datetime import datetime
from typing import Optional, Dict, Any


class Order(Base):
    """
    Order tracking model for IBKR orders.
    
    Tracks orders throughout their lifecycle:
    - Submission to IBKR
    - Status updates (pending, submitted, filled, cancelled, rejected)
    - Fill prices and quantities
    - Parent strategy and signal that triggered the order
    """
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Link to strategy and signal - tables removed, keeping as regular columns
    strategy_id = Column(Integer, nullable=True, index=True)
    signal_id = Column(Integer, nullable=True, index=True)
    
    # IBKR order details
    ibkr_order_id = Column(String(100), nullable=True, index=True)  # IBKR's order ID
    conid = Column(Integer, nullable=False, index=True)  # IBKR contract ID
    
    # Order parameters
    side = Column(String(10), nullable=False)  # BUY, SELL, SELL_SHORT
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=True)  # Limit price (null for market orders)
    order_type = Column(String(10), nullable=False, default="LMT")  # LMT, MKT, STP, STPLMT
    tif = Column(String(10), nullable=False, default="DAY")  # GTC, DAY, IOC
    
    # Order status and fills
    status = Column(String(50), nullable=False, default="pending", index=True)
    # Status values: pending, submitted, pre_submitted, filled, partially_filled, 
    #                cancelled, rejected, inactive
    filled_price = Column(Float, nullable=True)
    filled_quantity = Column(Integer, default=0)
    remaining_quantity = Column(Integer, nullable=True)
    
    # Commission and fees
    commission = Column(Float, nullable=True)
    realized_pnl = Column(Float, nullable=True)
    
    # Additional IBKR response data
    ibkr_response = Column(JSON, nullable=True)  # Full IBKR API response
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    submitted_at = Column(DateTime(timezone=True), nullable=True)
    filled_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    # Strategy relationship removed - strategies feature deprecated
    # strategy = relationship("Strategy", foreign_keys=[strategy_id])
    # TradingSignal relationship removed - model not available
    # signal = relationship("TradingSignal", back_populates="orders")
    trades = relationship("Trade", back_populates="order")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary for API responses."""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "signal_id": self.signal_id,
            "ibkr_order_id": self.ibkr_order_id,
            "conid": self.conid,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "order_type": self.order_type,
            "tif": self.tif,
            "status": self.status,
            "filled_price": self.filled_price,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "commission": self.commission,
            "realized_pnl": self.realized_pnl,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def is_active(self) -> bool:
        """Check if order is still active (not terminal state)."""
        terminal_statuses = ["filled", "cancelled", "rejected", "inactive"]
        return self.status not in terminal_statuses
    
    def is_filled(self) -> bool:
        """Check if order is fully filled."""
        return self.status == "filled"
    
    def is_partial_fill(self) -> bool:
        """Check if order is partially filled."""
        return self.status == "partially_filled" or (
            self.filled_quantity > 0 and self.filled_quantity < self.quantity
        )
    
    def __repr__(self):
        return f"<Order(id={self.id}, ibkr_id='{self.ibkr_order_id}', side='{self.side}', qty={self.quantity}, status='{self.status}')>"

