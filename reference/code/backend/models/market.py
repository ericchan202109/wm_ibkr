"""Market data model."""
from sqlalchemy import Column, Integer, String, JSON, DateTime
from sqlalchemy.sql import func
from backend.core.database import Base


class MarketData(Base):
    """Market data cache model."""
    __tablename__ = "market_data"
    
    id = Column(Integer, primary_key=True, index=True)
    conid = Column(Integer, nullable=False, index=True)  # Contract ID (removed FK constraint)
    period = Column(String(20), nullable=False)  # 1d, 1w, 1m, 1y
    bar = Column(String(20), nullable=False)  # 1min, 5min, 1h, 1d, 1w
    data = Column(JSON, nullable=False)  # OHLCV and indicators
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

