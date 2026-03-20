"""Trade model."""
from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from backend.core.database import Base


class Trade(Base):
    """Completed trade model."""
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    conid = Column(Integer, nullable=False, index=True)
    entry_price = Column(Float, nullable=False)
    entry_quantity = Column(Integer, nullable=False)
    entry_date = Column(DateTime(timezone=True), nullable=False)
    exit_price = Column(Float)
    exit_quantity = Column(Integer)
    exit_date = Column(DateTime(timezone=True))
    realized_pnl = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    order = relationship("Order", back_populates="trades")

