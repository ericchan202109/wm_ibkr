"""Position model."""
from sqlalchemy import Column, Integer, Float, DateTime, Boolean, ForeignKey, String
from sqlalchemy.sql import func
from backend.core.database import Base


class Position(Base):
    """Open position model."""

    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, index=True)
    conid = Column(Integer, nullable=False, unique=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True)
    symbol = Column(String(20))
    quantity = Column(Integer, nullable=False)
    average_cost = Column("avg_price", Float, nullable=False)  # Map to database column 'avg_price'
    current_price = Column("market_price", Float)  # Map to database column 'market_price'
    unrealized_pnl = Column(Float)
    realized_pnl = Column(Float, nullable=True)
    is_closed = Column(Boolean, nullable=False, default=False)
    opened_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    closed_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __init__(self, **kwargs):
        avg_override = kwargs.pop("average_price", None)
        last_override = kwargs.pop("last_price", None)
        super().__init__(**kwargs)
        if avg_override is not None:
            self.average_cost = avg_override
        if last_override is not None:
            self.current_price = last_override

    @property
    def average_price(self) -> float:
        return self.average_cost

    @average_price.setter
    def average_price(self, value: float) -> None:
        self.average_cost = value

    @property
    def last_price(self) -> Float:
        return self.current_price

    @last_price.setter
    def last_price(self, value: float) -> None:
        self.current_price = value

    def to_dict(self):
        """Convert position to dictionary for API responses."""
        return {
            "id": self.id,
            "conid": self.conid,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "average_cost": self.average_cost,
            "current_price": self.current_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "is_closed": self.is_closed,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
