"""Chart model for storing generated technical analysis charts."""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, DECIMAL, BigInteger, JSON
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from backend.core.database import Base


class Chart(Base):
    """
    Model for storing technical analysis chart metadata.
    
    Charts are generated for different timeframes (daily, weekly, monthly)
    and stored in MinIO. This model tracks the chart metadata and MinIO URLs.
    """
    __tablename__ = "charts"
    
    # Primary Key
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign Keys
    execution_id = Column(Integer, ForeignKey("workflow_executions.id"), nullable=True, index=True)
    
    # Symbol Information
    symbol = Column(String(20), nullable=False, index=True)
    conid = Column(Integer, nullable=True)
    
    # Chart Configuration
    timeframe = Column(String(10), nullable=False)  # '1d', '1w', '1mo'
    chart_type = Column(String(20), nullable=False)  # 'daily', 'weekly', 'monthly'
    
    # MinIO Storage URLs
    chart_url_jpeg = Column(String(500), nullable=False)
    chart_url_html = Column(String(500), nullable=True)
    minio_bucket = Column(String(100), nullable=True)
    minio_object_key = Column(String(500), nullable=True)
    
    # Chart Metadata
    indicators_applied = Column(JSON, nullable=True)  # {"RSI": {"period": 14}, "MACD": {...}}
    data_points = Column(Integer, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    
    # Technical Analysis Summary
    price_current = Column(DECIMAL(12, 2), nullable=True)
    price_change_pct = Column(DECIMAL(8, 4), nullable=True)
    volume_avg = Column(BigInteger, nullable=True)
    
    # Status & Timestamps
    generated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(String(20), default='active', nullable=False)  # active, archived, deleted
    
    # Relationships
    # execution = relationship("WorkflowExecution", back_populates="charts")  # Removed - workflows deprecated
    llm_analyses = relationship("LLMAnalysis", back_populates="chart", cascade="all, delete-orphan")
    # signals = relationship("TradingSignal", back_populates="chart")  # Removed - TradingSignal model deprecated
    
    def to_dict(self):
        """Convert chart to dictionary."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "symbol": self.symbol,
            "conid": self.conid,
            "timeframe": self.timeframe,
            "chart_type": self.chart_type,
            "chart_url_jpeg": self.chart_url_jpeg,
            "chart_url_html": self.chart_url_html,
            "minio_bucket": self.minio_bucket,
            "minio_object_key": self.minio_object_key,
            "indicators_applied": self.indicators_applied,
            "data_points": self.data_points,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "price_current": float(self.price_current) if self.price_current else None,
            "price_change_pct": float(self.price_change_pct) if self.price_change_pct else None,
            "volume_avg": self.volume_avg,
            "generated_at": self.generated_at.isoformat() if self.generated_at else None,
            "status": self.status
        }
    
    def __repr__(self):
        return f"<Chart(id={self.id}, symbol={self.symbol}, timeframe={self.timeframe}, generated_at={self.generated_at})>"
