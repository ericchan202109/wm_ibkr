"""Position management endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from backend.core.database import get_db
from backend.models.position import Position
from backend.services.position_manager import PositionManager, get_position_manager
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/positions", tags=["positions"])


@router.get("/", response_model=Dict[str, Any])
async def list_positions(
    strategy_id: Optional[int] = Query(None, description="Filter by strategy ID"),
    include_closed: bool = Query(False, description="Include closed positions"),
    db: Session = Depends(get_db)
):
    """
    List all positions.
    
    Returns current open positions by default.
    """
    try:
        manager = get_position_manager(db)
        # Note: strategy_id parameter removed as Position model doesn't have strategy_id
        positions = await manager.get_all_positions(include_closed)
        
        return {
            "positions": [pos.to_dict() for pos in positions],
            "count": len(positions),
            "strategy_id": strategy_id,
            "include_closed": include_closed
        }
    except Exception as e:
        logger.error(f"Error listing positions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{conid}", response_model=Dict[str, Any])
async def get_position(
    conid: int,
    strategy_id: Optional[int] = Query(None, description="Filter by strategy ID"),
    db: Session = Depends(get_db)
):
    """Get a specific position by contract ID."""
    try:
        manager = get_position_manager(db)
        # Note: strategy_id parameter removed as Position model doesn't have strategy_id
        position = await manager.get_position(conid)
        
        if not position:
            raise HTTPException(status_code=404, detail="Position not found")
        
        # Get risk metrics
        risk_metrics = await manager.get_position_risk_metrics(position)
        
        return {
            "position": position.to_dict(),
            "risk_metrics": risk_metrics
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting position {conid}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/value", response_model=Dict[str, Any])
async def get_portfolio_value(
    strategy_id: Optional[int] = Query(None, description="Calculate for specific strategy"),
    db: Session = Depends(get_db)
):
    """
    Calculate total portfolio value and P&L.
    
    Returns aggregated metrics across all positions.
    """
    try:
        manager = get_position_manager(db)
        # Note: strategy_id parameter removed as Position model doesn't have strategy_id
        portfolio = await manager.calculate_portfolio_value()
        
        return portfolio
    except Exception as e:
        logger.error(f"Error calculating portfolio value: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync", response_model=Dict[str, Any])
async def sync_positions(
    strategy_id: Optional[int] = Query(None, description="Sync for specific strategy"),
    db: Session = Depends(get_db)
):
    """
    Synchronize positions with IBKR account.
    
    Fetches current positions from IBKR and updates local database.
    """
    try:
        manager = get_position_manager(db)
        # Note: strategy_id parameter removed as Position model doesn't have strategy_id
        result = await manager.sync_with_ibkr()
        
        return result
    except Exception as e:
        logger.error(f"Error syncing positions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/risk/metrics", response_model=Dict[str, Any])
async def get_all_risk_metrics(
    strategy_id: Optional[int] = Query(None, description="Filter by strategy ID"),
    db: Session = Depends(get_db)
):
    """
    Get risk metrics for all open positions.
    """
    try:
        manager = get_position_manager(db)
        # Note: strategy_id parameter removed as Position model doesn't have strategy_id
        positions = await manager.get_all_positions(include_closed=False)
        
        metrics_list = []
        for position in positions:
            metrics = await manager.get_position_risk_metrics(position)
            metrics['conid'] = position.conid
            # Note: strategy_id removed as Position model doesn't have this field
            metrics_list.append(metrics)
        
        # Calculate aggregate risk
        total_value = sum(m.get('position_value', 0) for m in metrics_list)
        total_pnl = sum(m.get('unrealized_pnl', 0) + m.get('realized_pnl', 0) for m in metrics_list)
        
        return {
            "positions": metrics_list,
            "aggregate": {
                "total_value": total_value,
                "total_pnl": total_pnl,
                "position_count": len(metrics_list)
            }
        }
    except Exception as e:
        logger.error(f"Error getting risk metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

