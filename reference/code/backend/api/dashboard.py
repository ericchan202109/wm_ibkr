"""Dashboard API endpoints."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from backend.core.database import get_db
from backend.models.position import Position
from backend.models.order import Order
# from backend.models.decision import Decision  # Decision model not available
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/stats")
async def get_dashboard_stats(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get dashboard statistics."""

    # Strategies feature removed - now using Airflow DAGs
    active_strategies = 0

    # Count open positions
    try:
        open_positions = db.query(func.count(Position.id)).filter(
            Position.quantity != 0
        ).scalar() or 0
    except Exception as e:
        logger.warning(f"Error counting positions: {e}")
        open_positions = 0
    
    # Count pending orders
    try:
        pending_orders = db.query(func.count(Order.id)).filter(
            Order.status.in_(['Submitted', 'PreSubmitted', 'PendingSubmit'])
        ).scalar() or 0
    except Exception as e:
        logger.warning(f"Error counting pending orders: {e}")
        pending_orders = 0

    # Workflow executions removed - now using Airflow DAGs
    running_tasks = 0

    # Get total trades count
    try:
        total_trades = db.query(func.count(Order.id)).filter(
            Order.status == 'Filled'
        ).scalar() or 0
    except Exception as e:
        logger.warning(f"Error counting filled orders: {e}")
        total_trades = 0
    
    # Get recent decisions count - Decision model not available, using 0
    recent_decisions = 0
    
    return {
        "active_strategies": active_strategies,
        "open_positions": open_positions,
        "pending_orders": pending_orders,
        "running_tasks": running_tasks,
        "total_trades": total_trades,
        "recent_decisions": recent_decisions
    }


@router.get("/recent-activity")
async def get_recent_activity(
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get recent trading activity."""
    
    # Get recent orders
    try:
        recent_orders = db.query(Order).order_by(
            Order.created_at.desc()
        ).limit(limit).all()
        recent_orders = [order.to_dict() for order in recent_orders]
    except Exception as e:
        logger.warning(f"Error getting recent orders: {e}")
        recent_orders = []
    
    # Get recent workflow executions - removed, now using Airflow DAGs
    recent_executions = []

    return {
        "recent_orders": recent_orders,
        "recent_executions": recent_executions
    }

