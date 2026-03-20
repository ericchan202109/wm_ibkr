"""AutoGen agent conversation model."""
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func
from backend.core.database import Base


class AgentConversation(Base):
    """AutoGen agent conversation log."""
    __tablename__ = "agent_conversations"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_execution_id = Column(Integer, nullable=False, index=True)  # Removed FK - table doesn't exist
    agent_name = Column(String(100), nullable=False)
    message_type = Column(String(20), nullable=False)  # request, response
    message_content = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    tokens_used = Column(Integer)

