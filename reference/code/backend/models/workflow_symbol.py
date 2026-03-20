"""Workflow Symbol Model - Manage symbols for trading workflows"""
from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Time,
    UniqueConstraint,
    inspect,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func, text

from backend.core.database import Base


class SymbolWorkflowLink(Base):
    """Association model linking symbols to workflows with specific scheduling config."""

    __tablename__ = "symbol_workflow_links"

    id = Column(Integer, primary_key=True, index=True)
    workflow_symbol_id = Column(
        Integer,
        ForeignKey("workflow_symbols.id", ondelete="CASCADE"),
        nullable=False,
    )
    dag_id = Column(String(255), nullable=False)  # Airflow DAG ID

    is_active = Column(Boolean, default=True, nullable=False)
    priority = Column(Integer, default=0, nullable=False)
    timezone = Column(String(64), nullable=False, default="America/New_York")
    session_start = Column(Time(timezone=False))
    session_end = Column(Time(timezone=False))
    allow_weekend = Column(Boolean, default=False, nullable=False)
    schedule_interval = Column(String(64), default="0 9 * * *")  # Cron expression or preset
    config = Column(JSON)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("workflow_symbol_id", "dag_id", name="uq_symbol_workflow_link"),
        Index("idx_symbol_workflow_links_symbol_priority", "workflow_symbol_id", "priority"),
    )

    # Relationship to parent symbol
    symbol = relationship("WorkflowSymbol", back_populates="workflow_links")

    def format_time(self, value):
        return value.strftime("%H:%M") if value else None

    def to_dict(self) -> dict:
        return {
            "link_id": self.id,
            "dag_id": self.dag_id,
            "is_active": bool(self.is_active),
            "priority": self.priority,
            "timezone": self.timezone,
            "session_start": self.format_time(self.session_start),
            "session_end": self.format_time(self.session_end),
            "allow_weekend": bool(self.allow_weekend),
            "schedule_interval": self.schedule_interval,
            "config": self.config,
        }


class WorkflowSymbol(Base):
    """Model for managing workflow symbols."""

    __tablename__ = "workflow_symbols"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), nullable=False, unique=True, index=True)
    name = Column(String(100))  # Company name
    enabled = Column(Boolean, default=True, nullable=False, index=True)
    priority = Column(Integer, default=0)  # Global priority
    conid = Column(Integer, nullable=True)  # IBKR Contract ID
    
    # Legacy fields (kept for backward compatibility or migration if needed, but effectively deprecated)
    workflow_type = Column(String(50), default="trading_signal")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    workflow_links = relationship(
        "SymbolWorkflowLink",
        back_populates="symbol",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="SymbolWorkflowLink.priority",
    )

    def __repr__(self):
        return f"<WorkflowSymbol(symbol={self.symbol}, enabled={self.enabled})>"

    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "name": self.name,
            "enabled": self.enabled,
            "priority": self.priority,
            "conid": self.conid,
            "workflow_type": self.workflow_type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "workflows": [link.to_dict() for link in self.workflow_links],
        }


def ensure_workflow_symbol_schema(engine) -> None:
    """Ensure symbol_workflow_links table exists and workflow_symbols has conid."""
    try:
        inspector = inspect(engine)
        if "symbol_workflow_links" not in inspector.get_table_names():
            SymbolWorkflowLink.__table__.create(engine)
            
        # Check for conid column in workflow_symbols
        if "workflow_symbols" in inspector.get_table_names():
            columns = [c["name"] for c in inspector.get_columns("workflow_symbols")]
            if "conid" not in columns:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE workflow_symbols ADD COLUMN conid INTEGER"))
                    conn.commit()

        # Check for schedule_interval in symbol_workflow_links
        if "symbol_workflow_links" in inspector.get_table_names():
            columns = [c["name"] for c in inspector.get_columns("symbol_workflow_links")]
            if"schedule_interval" not in columns:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE symbol_workflow_links ADD COLUMN schedule_interval VARCHAR(64) DEFAULT '0 9 * * *'"))
                    conn.commit()
            
            # Add dag_id column if missing
            if "dag_id" not in columns:
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE symbol_workflow_links ADD COLUMN dag_id VARCHAR(255)"))
                    conn.commit()
    except Exception:
        pass
