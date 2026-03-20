"""LLM Analyses API endpoints for managing AI-generated market analysis."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from pydantic import BaseModel

from backend.core.database import get_db
from backend.services.llm_analysis_persistence_service import LLMAnalysisPersistenceService
from backend.models.llm_analysis import LLMAnalysis
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class LLMAnalysisResponse(BaseModel):
    """LLM Analysis response model."""
    id: int
    execution_id: Optional[int]
    chart_id: Optional[int]
    symbol: str
    prompt_text: str
    response_text: Optional[str]
    model_name: Optional[str]
    timeframe: Optional[str]
    confidence_score: Optional[float]
    trend_direction: Optional[str]
    tokens_used: Optional[int]
    latency_ms: Optional[int]
    analyzed_at: str
    status: str

    class Config:
        from_attributes = True


@router.get("/llm-analyses", response_model=dict)
async def list_analyses(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    timeframe: Optional[str] = Query(None, description="Filter by timeframe"),
    execution_id: Optional[int] = Query(None, description="Filter by execution ID"),
    chart_id: Optional[int] = Query(None, description="Filter by chart ID"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    """
    List LLM analyses with optional filtering.
    
    Returns paginated list of analyses with metadata.
    """
    try:
        # Build query
        query = db.query(LLMAnalysis).filter(LLMAnalysis.status == 'completed')
        
        if symbol:
            query = query.filter(LLMAnalysis.symbol == symbol)
        
        if timeframe:
            query = query.filter(LLMAnalysis.timeframe == timeframe)
        
        if execution_id:
            query = query.filter(LLMAnalysis.execution_id == execution_id)
        
        if chart_id:
            query = query.filter(LLMAnalysis.chart_id == chart_id)
        
        # Get total count
        total = query.count()
        
        # Get paginated results
        analyses = query.order_by(LLMAnalysis.analyzed_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "analyses": [analysis.to_dict() for analysis in analyses]
        }
        
    except Exception as e:
        logger.error(f"Failed to list analyses: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-analyses/{analysis_id}", response_model=LLMAnalysisResponse)
async def get_analysis(
    analysis_id: int,
    db: Session = Depends(get_db)
):
    """Get specific LLM analysis by ID."""
    try:
        llm_service = LLMAnalysisPersistenceService(db)
        analysis = llm_service.get_analysis_by_id(analysis_id)
        
        if not analysis:
            raise HTTPException(status_code=404, detail=f"Analysis {analysis_id} not found")
        
        return analysis
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get analysis {analysis_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-analyses/symbol/{symbol}", response_model=dict)
async def get_analyses_by_symbol(
    symbol: str,
    timeframe: Optional[str] = Query(None, description="Filter by timeframe"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get recent analyses for a specific symbol."""
    try:
        llm_service = LLMAnalysisPersistenceService(db)
        analyses = llm_service.get_analyses_by_symbol(symbol, timeframe, limit)
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(analyses),
            "analyses": [analysis.to_dict() for analysis in analyses]
        }
        
    except Exception as e:
        logger.error(f"Failed to get analyses for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-analyses/execution/{execution_id}", response_model=dict)
async def get_analyses_by_execution(
    execution_id: int,
    db: Session = Depends(get_db)
):
    """Get all analyses for a specific workflow execution."""
    try:
        llm_service = LLMAnalysisPersistenceService(db)
        analyses = llm_service.get_analyses_by_execution(execution_id)
        
        return {
            "execution_id": execution_id,
            "count": len(analyses),
            "analyses": [analysis.to_dict() for analysis in analyses]
        }
        
    except Exception as e:
        logger.error(f"Failed to get analyses for execution {execution_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-analyses/chart/{chart_id}", response_model=dict)
async def get_analyses_by_chart(
    chart_id: int,
    db: Session = Depends(get_db)
):
    """Get all analyses for a specific chart."""
    try:
        llm_service = LLMAnalysisPersistenceService(db)
        analyses = llm_service.get_analyses_by_chart(chart_id)
        
        return {
            "chart_id": chart_id,
            "count": len(analyses),
            "analyses": [analysis.to_dict() for analysis in analyses]
        }
        
    except Exception as e:
        logger.error(f"Failed to get analyses for chart {chart_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-analyses/stats/summary")
async def get_analysis_stats(db: Session = Depends(get_db)):
    """Get LLM analysis statistics."""
    try:
        llm_service = LLMAnalysisPersistenceService(db)
        stats = llm_service.get_analysis_stats()
        
        # Additional stats
        by_model = db.query(
            LLMAnalysis.model_name,
            func.count(LLMAnalysis.id).label('count')
        ).group_by(LLMAnalysis.model_name).all()
        
        by_trend = db.query(
            LLMAnalysis.trend_direction,
            func.count(LLMAnalysis.id).label('count')
        ).filter(
            LLMAnalysis.trend_direction.isnot(None)
        ).group_by(LLMAnalysis.trend_direction).all()
        
        stats["by_model"] = {model: count for model, count in by_model}
        stats["by_trend"] = {trend: count for trend, count in by_trend}
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get analysis stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

