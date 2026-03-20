"""Charts API endpoints for managing generated technical analysis charts."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel

from backend.core.database import get_db
from backend.services.chart_persistence_service import ChartPersistenceService
from backend.models.chart import Chart
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class ChartResponse(BaseModel):
    """Chart response model."""
    id: int
    execution_id: Optional[int]
    symbol: str
    conid: Optional[int]
    timeframe: str
    chart_type: str
    chart_url_jpeg: str
    chart_url_html: Optional[str]
    indicators_applied: Optional[dict]
    data_points: Optional[int]
    price_current: Optional[float]
    price_change_pct: Optional[float]
    volume_avg: Optional[int]
    generated_at: str
    status: str

    class Config:
        from_attributes = True


@router.get("/charts", response_model=dict)
async def list_charts(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    timeframe: Optional[str] = Query(None, description="Filter by timeframe (1d, 1w, 1mo)"),
    execution_id: Optional[int] = Query(None, description="Filter by execution ID"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    """
    List generated charts with optional filtering.
    
    Returns paginated list of charts with metadata.
    """
    try:
        chart_service = ChartPersistenceService(db)
        
        # Build query
        query = db.query(Chart).filter(Chart.status == 'active')
        
        if symbol:
            query = query.filter(Chart.symbol == symbol)
        
        if timeframe:
            query = query.filter(Chart.timeframe == timeframe)
        
        if execution_id:
            query = query.filter(Chart.execution_id == execution_id)
        
        # Get total count
        total = query.count()
        
        # Get paginated results
        charts = query.order_by(Chart.generated_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "charts": [chart.to_dict() for chart in charts]
        }
        
    except Exception as e:
        logger.error(f"Failed to list charts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/charts/{chart_id}", response_model=ChartResponse)
async def get_chart(
    chart_id: int,
    db: Session = Depends(get_db)
):
    """Get specific chart by ID."""
    try:
        chart_service = ChartPersistenceService(db)
        chart = chart_service.get_chart_by_id(chart_id)
        
        if not chart:
            raise HTTPException(status_code=404, detail=f"Chart {chart_id} not found")
        
        return chart
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get chart {chart_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/charts/symbol/{symbol}", response_model=dict)
async def get_charts_by_symbol(
    symbol: str,
    timeframe: Optional[str] = Query(None, description="Filter by timeframe"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get recent charts for a specific symbol."""
    try:
        chart_service = ChartPersistenceService(db)
        charts = chart_service.get_charts_by_symbol(symbol, timeframe, limit)
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(charts),
            "charts": [chart.to_dict() for chart in charts]
        }
        
    except Exception as e:
        logger.error(f"Failed to get charts for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/charts/execution/{execution_id}", response_model=dict)
async def get_charts_by_execution(
    execution_id: int,
    db: Session = Depends(get_db)
):
    """Get all charts for a specific workflow execution."""
    try:
        chart_service = ChartPersistenceService(db)
        charts = chart_service.get_charts_by_execution(execution_id)
        
        return {
            "execution_id": execution_id,
            "count": len(charts),
            "charts": [chart.to_dict() for chart in charts]
        }
        
    except Exception as e:
        logger.error(f"Failed to get charts for execution {execution_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/charts/{chart_id}")
async def archive_chart(
    chart_id: int,
    db: Session = Depends(get_db)
):
    """Archive a chart (soft delete)."""
    try:
        chart = db.query(Chart).filter(Chart.id == chart_id).first()
        
        if not chart:
            raise HTTPException(status_code=404, detail=f"Chart {chart_id} not found")
        
        chart.status = 'archived'
        db.commit()
        
        return {"message": f"Chart {chart_id} archived successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to archive chart {chart_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/charts/stats/summary")
async def get_chart_stats(db: Session = Depends(get_db)):
    """Get chart statistics."""
    try:
        total_charts = db.query(Chart).count()
        active_charts = db.query(Chart).filter(Chart.status == 'active').count()
        unique_symbols = db.query(Chart.symbol).distinct().count()
        
        # Recent charts by timeframe
        daily_count = db.query(Chart).filter(Chart.timeframe == '1d', Chart.status == 'active').count()
        weekly_count = db.query(Chart).filter(Chart.timeframe == '1w', Chart.status == 'active').count()
        
        return {
            "total_charts": total_charts,
            "active_charts": active_charts,
            "archived_charts": total_charts - active_charts,
            "unique_symbols": unique_symbols,
            "by_timeframe": {
                "daily": daily_count,
                "weekly": weekly_count
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get chart stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

