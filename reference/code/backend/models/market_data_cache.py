"""Market data cache model for storing IBKR data locally."""
from sqlalchemy import Column, Integer, String, JSON, DateTime, Boolean, Index, UniqueConstraint
from sqlalchemy.sql import func
from backend.core.database import Base
from datetime import datetime, timedelta, timezone


class MarketDataCache(Base):
    """
    Market data cache for storing historical data from IBKR.
    
    Enables debug mode and reduces API calls by caching market data locally.
    Supports OHLCV data with configurable expiration.
    """
    __tablename__ = "market_data_cache"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Symbol identification
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, default="NASDAQ")
    conid = Column(Integer, nullable=True, index=True)  # IBKR contract ID if available
    
    # Data type and timeframe
    data_type = Column(String(20), nullable=False, default="daily")  # daily, weekly, monthly, intraday
    timeframe = Column(String(10), nullable=True)  # 1d, 1w, 1m, 1h, etc.
    
    # Market data (OHLCV)
    ohlcv_data = Column(JSON, nullable=False)  # Array of {time, open, high, low, close, volume}
    
    # Calculated indicators (optional)
    indicators = Column(JSON, nullable=True)  # Pre-calculated indicators if needed
    
    # Cache metadata
    source = Column(String(20), nullable=False, default="IBKR")  # Source of data
    cached_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    data_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)  # Timestamp of the actual market data
    
    # Additional metadata
    extra_metadata = Column(JSON, nullable=True)  # Additional info like fetch params, data quality, etc.
    is_active = Column(Boolean, default=True, nullable=False)  # Soft delete flag
    
    # Composite unique constraint
    __table_args__ = (
        UniqueConstraint('symbol', 'exchange', 'data_timestamp', 'data_type', name='uix_symbol_exchange_timestamp_type'),
        Index('ix_symbol_timestamp_desc', 'symbol', 'data_timestamp', postgresql_using='btree', postgresql_ops={'data_timestamp': 'DESC'}),
        Index('ix_symbol_datatype_timestamp', 'symbol', 'data_type', 'data_timestamp', postgresql_using='btree'),
        Index('ix_expires_at', 'expires_at'),
    )
    
    def is_expired(self) -> bool:
        """Check if cached data has expired."""
        return datetime.now(timezone.utc) > self.expires_at
    
    def to_dict(self):
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "conid": self.conid,
            "data_type": self.data_type,
            "timeframe": self.timeframe,
            "ohlcv_data": self.ohlcv_data,
            "indicators": self.indicators,
            "source": self.source,
            "cached_at": self.cached_at.isoformat() if self.cached_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "data_timestamp": self.data_timestamp.isoformat() if self.data_timestamp else None,
            "is_expired": self.is_expired(),
            "metadata": self.extra_metadata,
        }
    
    @classmethod
    def set_expiration(cls, hours: int = 24) -> datetime:
        """Calculate expiration datetime from now."""
        return datetime.now(timezone.utc) + timedelta(hours=hours)
    
    def __repr__(self):
        return f"<MarketDataCache(id={self.id}, symbol='{self.symbol}', exchange='{self.exchange}', data_type='{self.data_type}', timestamp={self.data_timestamp})>"

