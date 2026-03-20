"""
Artifact Model - Store LLM inputs/outputs, charts, and trading signals
OpenSpec 2 Compliant
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Float
from sqlalchemy.sql import func
from backend.core.database import Base
from decimal import Decimal


class Artifact(Base):
    """Model for storing ML/LLM artifacts."""
    
    __tablename__ = "artifacts"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Artifact identification
    name = Column(String, nullable=False, index=True)
    type = Column(String, nullable=False, index=True)  # 'llm', 'chart', 'signal'
    
    # Associated data
    symbol = Column(String, index=True)  # Trading symbol if applicable
    run_id = Column(String, index=True)  # MLflow run ID
    experiment_id = Column(String, index=True)  # MLflow experiment ID
    
    # Workflow tracking
    workflow_id = Column(String, index=True)  # Workflow identifier (e.g., ibkr_trading_signal_workflow)
    execution_id = Column(String, index=True)  # Specific workflow execution/run ID
    step_name = Column(String, index=True)  # Workflow step that generated this artifact
    dag_id = Column(String, index=True)  # Airflow DAG ID
    task_id = Column(String)  # Airflow task ID
    
    # LLM-specific fields
    prompt = Column(Text)  # LLM input prompt
    response = Column(Text)  # LLM response
    prompt_length = Column(Integer)
    response_length = Column(Integer)
    model_name = Column(String)
    
    # Chart-specific fields
    image_path = Column(String)  # Path to chart image
    chart_type = Column(String)  # Type of chart (candlestick, line, etc.)
    chart_data = Column(JSON)  # Chart data as JSON
    
    # Signal-specific fields
    action = Column(String)  # BUY, SELL, HOLD
    confidence = Column(Float)  # Confidence score 0-1
    signal_data = Column(JSON)  # Full signal details
    
    # Metadata (renamed to avoid SQLAlchemy reserved word)
    artifact_metadata = Column(JSON)  # Additional metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<Artifact(id={self.id}, type={self.type}, name={self.name})>"
    
    def to_dict(self):
        """Convert to dictionary for API responses."""
        def convert_decimals(obj):
            """Recursively convert Decimal objects to float for JSON serialization."""
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {key: convert_decimals(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimals(item) for item in obj]
            else:
                return obj
        
        result = {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'symbol': self.symbol,
            'run_id': self.run_id,
            'experiment_id': self.experiment_id,
            'workflow_id': self.workflow_id,
            'execution_id': self.execution_id,
            'step_name': self.step_name,
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'prompt': self.prompt,
            'response': self.response,
            'prompt_length': self.prompt_length,
            'response_length': self.response_length,
            'model_name': self.model_name,
            'image_path': self.image_path,
            'chart_type': self.chart_type,
            'chart_data': convert_decimals(self.chart_data) if self.chart_data else None,
            'action': self.action,
            'confidence': float(self.confidence) if self.confidence is not None else None,
            'signal_data': convert_decimals(self.signal_data) if self.signal_data else None,
            'metadata': convert_decimals(self.artifact_metadata) if self.artifact_metadata else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
        return result

