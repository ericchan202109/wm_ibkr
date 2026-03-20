"""Celery tasks for strategy execution and scheduling."""
import logging
from datetime import datetime
from typing import List
from backend.celery_app import celery_app
from backend.core.database import SessionLocal
from backend.services.strategy_service import StrategyService
from backend.models.strategy import Strategy

logger = logging.getLogger(__name__)


@celery_app.task(name="check_and_execute_strategies")
def check_and_execute_strategies():
    """
    Periodic task to check for strategies due for execution.
    
    This task runs every minute and triggers strategy execution
    for any strategies that are due based on their cron schedule.
    
    Returns:
        Dictionary with execution results
    """
    db = SessionLocal()
    try:
        service = StrategyService(db)
        
        # Get strategies due for execution (5 minute tolerance)
        due_strategies = service.get_strategies_due_for_execution(tolerance_minutes=5)
        
        if not due_strategies:
            logger.debug("No strategies due for execution")
            return {
                "checked_at": datetime.now().isoformat(),
                "strategies_executed": 0,
                "strategies_failed": 0,
                "strategies": []
            }
        
        logger.info(f"Found {len(due_strategies)} strategies due for execution")
        
        executed = []
        failed = []
        
        for strategy in due_strategies:
            try:
                # Trigger strategy execution (async)
                execute_strategy.delay(strategy.id)
                executed.append({
                    "id": strategy.id,
                    "name": strategy.name,
                    "scheduled_time": strategy.next_execution_at.isoformat()
                })
                logger.info(f"Triggered execution for strategy: {strategy.name}")
            except Exception as e:
                failed.append({
                    "id": strategy.id,
                    "name": strategy.name,
                    "error": str(e)
                })
                logger.error(f"Failed to trigger strategy {strategy.name}: {str(e)}")
        
        return {
            "checked_at": datetime.now().isoformat(),
            "strategies_executed": len(executed),
            "strategies_failed": len(failed),
            "executed": executed,
            "failed": failed
        }
        
    except Exception as e:
        logger.error(f"Error in check_and_execute_strategies: {str(e)}")
        raise
    finally:
        db.close()


@celery_app.task(name="execute_strategy", bind=True)
def execute_strategy(self, strategy_id: int):
    """
    Execute a single trading strategy.
    
    This is the main workflow execution task that:
    1. Fetches market data
    2. Calculates indicators
    3. Generates charts
    4. Runs LLM analysis
    5. Generates trading signals
    6. Places orders (if configured)
    7. Records lineage
    
    Args:
        strategy_id: ID of the strategy to execute
        
    Returns:
        Dictionary with execution results
    """
    db = SessionLocal()
    execution_id = f"exec_{strategy_id}_{int(datetime.now().timestamp())}"
    
    try:
        service = StrategyService(db)
        strategy = service.get_strategy(strategy_id)
        
        if not strategy:
            logger.error(f"Strategy {strategy_id} not found")
            return {"error": f"Strategy {strategy_id} not found"}
        
        if not strategy.is_active:
            logger.warning(f"Strategy {strategy.name} is not active, skipping execution")
            return {"skipped": True, "reason": "Strategy not active"}
        
        logger.info(f"Executing strategy: {strategy.name} (ID: {strategy_id}, execution_id: {execution_id})")
        
        # Validation
        validation = service.validate_strategy_config(strategy)
        if not validation['valid']:
            logger.error(f"Strategy {strategy.name} validation failed: {validation['issues']}")
            return {
                "error": "Strategy validation failed",
                "issues": validation['issues']
            }
        
        # Import the strategy executor (will be created in Phase 3)
        from backend.services.strategy_executor import StrategyExecutor
        
        executor = StrategyExecutor(db)
        result = executor.execute_strategy(strategy, execution_id)
        
        # Mark strategy as executed
        service.mark_strategy_executed(strategy_id)
        
        logger.info(f"Strategy {strategy.name} executed successfully: {result}")
        return result
        
    except ImportError:
        # StrategyExecutor not yet implemented - log and skip
        logger.warning(f"StrategyExecutor not yet implemented, marking strategy {strategy_id} as executed")
        service = StrategyService(db)
        service.mark_strategy_executed(strategy_id)
        return {
            "execution_id": execution_id,
            "status": "skipped",
            "reason": "StrategyExecutor not yet implemented"
        }
    except Exception as e:
        logger.error(f"Error executing strategy {strategy_id}: {str(e)}", exc_info=True)
        return {
            "execution_id": execution_id,
            "error": str(e),
            "status": "failed"
        }
    finally:
        db.close()


@celery_app.task(name="recalculate_strategy_schedules")
def recalculate_strategy_schedules():
    """
    Recalculate next_execution_at for all active strategies.
    
    This is a maintenance task that ensures all strategies have
    correct next_execution_at times. Useful after system restarts
    or schedule changes.
    
    Returns:
        Dictionary with recalculation results
    """
    db = SessionLocal()
    try:
        service = StrategyService(db)
        strategies = service.list_strategies(active_only=True)
        
        updated = 0
        errors = []
        
        for strategy in strategies:
            if not strategy.schedule:
                continue
            
            try:
                next_exec = service._calculate_next_execution(strategy.schedule)
                strategy.next_execution_at = next_exec
                updated += 1
            except Exception as e:
                errors.append({
                    "strategy_id": strategy.id,
                    "name": strategy.name,
                    "error": str(e)
                })
                logger.error(f"Failed to recalculate schedule for {strategy.name}: {str(e)}")
        
        db.commit()
        
        logger.info(f"Recalculated schedules for {updated} strategies")
        return {
            "updated": updated,
            "errors": len(errors),
            "error_details": errors
        }
        
    except Exception as e:
        logger.error(f"Error in recalculate_strategy_schedules: {str(e)}")
        raise
    finally:
        db.close()


@celery_app.task(name="cleanup_inactive_strategies")
def cleanup_inactive_strategies(days_inactive: int = 90):
    """
    Clean up strategies that have been inactive for a long time.
    
    Args:
        days_inactive: Number of days of inactivity before cleanup
        
    Returns:
        Dictionary with cleanup results
    """
    from datetime import timedelta
    
    db = SessionLocal()
    try:
        cutoff_date = datetime.now() - timedelta(days=days_inactive)
        
        # Find inactive strategies
        strategies = db.query(Strategy).filter(
            Strategy.is_active == 0,
            Strategy.updated_at < cutoff_date
        ).all()
        
        logger.info(f"Found {len(strategies)} inactive strategies older than {days_inactive} days")
        
        # For now, just log them (actual cleanup would require more careful handling)
        cleanup_candidates = [
            {
                "id": s.id,
                "name": s.name,
                "last_updated": s.updated_at.isoformat() if s.updated_at else None
            }
            for s in strategies
        ]
        
        return {
            "candidates": len(cleanup_candidates),
            "details": cleanup_candidates
        }
        
    except Exception as e:
        logger.error(f"Error in cleanup_inactive_strategies: {str(e)}")
        raise
    finally:
        db.close()

