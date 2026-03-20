"""Celery tasks for order monitoring and management."""
import logging
from datetime import datetime
from backend.celery_app import celery_app
from backend.core.database import SessionLocal
from backend.services.order_manager import OrderManager

logger = logging.getLogger(__name__)


@celery_app.task(name="monitor_active_orders")
def monitor_active_orders():
    """
    Periodic task to monitor active orders and sync status with IBKR.
    
    This task runs every minute to:
    - Check all active orders
    - Update their status from IBKR
    - Record fills and completions
    - Log any errors
    
    Returns:
        Dictionary with monitoring results
    """
    db = SessionLocal()
    try:
        manager = OrderManager(db)
        result = manager.monitor_active_orders()
        
        logger.info(
            f"Order monitoring complete: {result['updated']} orders updated, "
            f"{len(result['errors'])} errors"
        )
        
        return {
            "monitored_at": datetime.now().isoformat(),
            **result
        }
        
    except Exception as e:
        logger.error(f"Error in monitor_active_orders task: {str(e)}", exc_info=True)
        raise
    finally:
        db.close()


@celery_app.task(name="cleanup_old_orders")
def cleanup_old_orders(days_old: int = 90):
    """
    Clean up old terminal orders.
    
    Args:
        days_old: Number of days before orders are considered old
        
    Returns:
        Dictionary with cleanup results
    """
    from datetime import timedelta
    
    db = SessionLocal()
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        # Find old terminal orders
        from backend.models.order import Order
        old_orders = db.query(Order).filter(
            Order.status.in_(["filled", "cancelled", "rejected"]),
            Order.updated_at < cutoff_date
        ).all()
        
        logger.info(f"Found {len(old_orders)} orders older than {days_old} days")
        
        # For now, just log them (actual cleanup would require more careful handling)
        cleanup_candidates = [
            {
                "id": o.id,
                "status": o.status,
                "updated_at": o.updated_at.isoformat() if o.updated_at else None
            }
            for o in old_orders
        ]
        
        return {
            "candidates": len(cleanup_candidates),
            "details": cleanup_candidates[:100]  # Limit to first 100
        }
        
    except Exception as e:
        logger.error(f"Error in cleanup_old_orders task: {str(e)}")
        raise
    finally:
        db.close()


@celery_app.task(name="retry_failed_orders")
def retry_failed_orders():
    """
    Retry orders that failed submission.
    
    Finds orders in 'error' status and attempts to resubmit them.
    
    Returns:
        Dictionary with retry results
    """
    db = SessionLocal()
    try:
        from backend.models.order import Order
        
        # Find failed orders from last hour
        from datetime import timedelta
        recent = datetime.now() - timedelta(hours=1)
        
        failed_orders = db.query(Order).filter(
            Order.status == "error",
            Order.created_at >= recent
        ).all()
        
        logger.info(f"Found {len(failed_orders)} failed orders to retry")
        
        manager = OrderManager(db)
        retried = 0
        errors = []
        
        for order in failed_orders:
            try:
                # Reset status and retry
                order.status = "pending"
                order.error_message = None
                db.commit()
                
                manager.submit_order(order)
                retried += 1
            except Exception as e:
                errors.append({
                    "order_id": order.id,
                    "error": str(e)
                })
        
        logger.info(f"Retried {retried} orders, {len(errors)} failures")
        
        return {
            "total_failed": len(failed_orders),
            "retried": retried,
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Error in retry_failed_orders task: {str(e)}")
        raise
    finally:
        db.close()

