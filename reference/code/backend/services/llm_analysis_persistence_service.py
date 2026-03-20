"""LLM Analysis Persistence Service for saving LLM responses to database."""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from decimal import Decimal

from backend.models.llm_analysis import LLMAnalysis
from backend.models.chart import Chart

logger = logging.getLogger(__name__)


class LLMAnalysisPersistenceService:
    """Service for persisting LLM analysis data to database."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def save_analysis(
        self,
        execution_id: int,
        chart_id: int,
        symbol: str,
        prompt_text: str,
        response_text: str,
        model_name: str = None,
        timeframe: str = None,
        strategy_id: int = None,
        indicators_metadata: Dict[str, Any] = None,
        response_json: Dict[str, Any] = None,
        confidence_score: float = None,
        trend_direction: str = None,
        support_levels: List[float] = None,
        resistance_levels: List[float] = None,
        key_patterns: List[str] = None,
        tokens_used: int = None,
        latency_ms: int = None
    ) -> LLMAnalysis:
        """
        Save LLM analysis record to database.
        
        Args:
            execution_id: Workflow execution ID
            chart_id: Associated chart ID
            symbol: Stock symbol
            prompt_text: Full prompt sent to LLM
            response_text: LLM response text
            model_name: LLM model name (e.g., 'gpt-4-turbo-preview')
            timeframe: Timeframe analyzed
            strategy_id: Strategy ID
            indicators_metadata: Full indicator configuration
            response_json: Structured response (if applicable)
            confidence_score: Confidence score (0.0-1.0)
            trend_direction: Identified trend (bullish/bearish/neutral)
            support_levels: Support price levels
            resistance_levels: Resistance price levels
            key_patterns: Identified chart patterns
            tokens_used: Number of tokens used
            latency_ms: Request latency in milliseconds
            
        Returns:
            LLMAnalysis: Saved analysis record
        """
        try:
            analysis = LLMAnalysis(
                execution_id=execution_id,
                chart_id=chart_id,
                symbol=symbol,
                prompt_text=prompt_text,
                response_text=response_text,
                model_name=model_name,
                timeframe=timeframe,
                strategy_id=strategy_id,
                indicators_metadata=indicators_metadata,
                response_json=response_json,
                confidence_score=Decimal(str(confidence_score)) if confidence_score else None,
                trend_direction=trend_direction,
                support_levels=support_levels,
                resistance_levels=resistance_levels,
                key_patterns=key_patterns,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
                analyzed_at=datetime.now(timezone.utc),
                status='completed'
            )
            
            self.db.add(analysis)
            self.db.commit()
            self.db.refresh(analysis)
            
            logger.info(f"✅ Saved LLM analysis: {symbol} {timeframe} (ID: {analysis.id})")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to save LLM analysis: {e}")
            self.db.rollback()
            raise
    
    def get_analysis_by_id(self, analysis_id: int) -> Optional[LLMAnalysis]:
        """Get analysis by ID."""
        return self.db.query(LLMAnalysis).filter(LLMAnalysis.id == analysis_id).first()
    
    def get_analyses_by_execution(self, execution_id: int) -> List[LLMAnalysis]:
        """Get all analyses for a specific execution."""
        return self.db.query(LLMAnalysis).filter(
            LLMAnalysis.execution_id == execution_id
        ).order_by(LLMAnalysis.analyzed_at).all()
    
    def get_analyses_by_chart(self, chart_id: int) -> List[LLMAnalysis]:
        """Get all analyses for a specific chart."""
        return self.db.query(LLMAnalysis).filter(
            LLMAnalysis.chart_id == chart_id
        ).all()
    
    def get_analyses_by_symbol(
        self,
        symbol: str,
        timeframe: str = None,
        limit: int = 10
    ) -> List[LLMAnalysis]:
        """Get recent analyses for a symbol."""
        query = self.db.query(LLMAnalysis).filter(LLMAnalysis.symbol == symbol)
        
        if timeframe:
            query = query.filter(LLMAnalysis.timeframe == timeframe)
        
        return query.order_by(LLMAnalysis.analyzed_at.desc()).limit(limit).all()
    
    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get statistics about LLM analyses."""
        total = self.db.query(LLMAnalysis).count()
        completed = self.db.query(LLMAnalysis).filter(
            LLMAnalysis.status == 'completed'
        ).count()
        
        # Average confidence
        avg_confidence = self.db.query(LLMAnalysis).filter(
            LLMAnalysis.confidence_score.isnot(None)
        ).with_entities(LLMAnalysis.confidence_score).all()
        
        avg_conf = sum(float(c[0]) for c in avg_confidence) / len(avg_confidence) if avg_confidence else 0
        
        return {
            "total_analyses": total,
            "completed": completed,
            "average_confidence": round(avg_conf, 4),
            "unique_symbols": self.db.query(LLMAnalysis.symbol).distinct().count()
        }

