"""Indicator database models."""
from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from backend.core.database import Base


# Association table for many-to-many relationship between strategies and indicators
StrategyIndicator = Table(
    'strategy_indicators',
    Base.metadata,
    Column('strategy_id', Integer, ForeignKey('strategies.id'), primary_key=True),
    Column('indicator_id', Integer, ForeignKey('indicators.id'), primary_key=True),
    Column('display_order', Integer, default=0)
)


class Indicator(Base):
    """Technical indicator model."""
    __tablename__ = "indicators"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)  # MA, RSI, MACD, BB, SuperTrend, ATR, etc.
    parameters = Column(JSON, nullable=False)  # Indicator-specific parameters
    period = Column(Integer, default=100)  # Number of data points for chart (20-500)
    frequency = Column(String(10), default='1D')  # Data frequency: 1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    # Note: Removed back_populates to fix SQLAlchemy relationship conflicts


class IndicatorChart(Base):
    """Indicator chart model for storing chart metadata and MinIO references."""
    __tablename__ = "indicator_charts"
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="CASCADE"))
    symbol = Column(String(50), nullable=False)
    indicator_ids = Column(JSON, nullable=False)  # List of indicator IDs
    period = Column(Integer, nullable=False)  # Number of data points used
    frequency = Column(String(10), nullable=False)  # Data frequency used
    chart_url_jpeg = Column(String(500))  # MinIO URL for JPEG chart
    chart_url_html = Column(String(500))  # MinIO URL for interactive HTML chart
    chart_metadata = Column("metadata", JSON, default={})  # Additional metadata (mapped to 'metadata' column)
    generated_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True))  # Auto-delete date (30 days default)
    
    # Relationships
    # strategy = relationship("Strategy")  # Removed - strategies feature deprecated
