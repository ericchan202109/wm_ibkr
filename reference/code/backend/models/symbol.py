"""Symbol model for IBKR contract caching."""
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.sql import func
from backend.core.database import Base
from datetime import datetime


class Symbol(Base):
    """
    Cached IBKR contract information.
    
    This table serves as a cache for IBKR contract lookups to reduce API calls
    and improve performance. Data is refreshed periodically based on last_updated.
    """
    __tablename__ = "symbols"

    id = Column(Integer, primary_key=True, index=True)
    conid = Column(Integer, unique=True, nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    name = Column(Text, nullable=True)
    exchange = Column(String(20), nullable=True)
    currency = Column(String(3), nullable=True)
    asset_type = Column(String(20), nullable=True)  # STK, OPT, FUT, etc.
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def to_dict(self):
        """Convert model to dictionary for API responses."""
        return {
            "id": self.id,
            "conid": self.conid,
            "symbol": self.symbol,
            "name": self.name,
            "exchange": self.exchange,
            "currency": self.currency,
            "asset_type": self.asset_type,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }

    def is_stale(self, max_age_days: int = 7) -> bool:
        """
        Check if the cached data is stale.
        
        Args:
            max_age_days: Maximum age in days before data is considered stale
            
        Returns:
            True if data should be refreshed
        """
        if not self.last_updated:
            return True
        
        age = datetime.now(self.last_updated.tzinfo) - self.last_updated
        return age.days > max_age_days

    def __repr__(self):
        return f"<Symbol(conid={self.conid}, symbol='{self.symbol}', exchange='{self.exchange}')>"

