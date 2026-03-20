"""
Celery tasks for prompt performance tracking and aggregation.

Daily aggregation of prompt template performance metrics based on trading signal outcomes.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any
from decimal import Decimal

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from backend.celery_app import celery_app
from backend.core.database import SessionLocal
from backend.models.prompt import PromptTemplate, PromptPerformance
from backend.models.trading_signal import TradingSignal

logger = logging.getLogger(__name__)


@celery_app.task(name="calculate_prompt_performance_daily")
def calculate_prompt_performance_daily(target_date: Optional[str] = None):
    """
    Calculate daily prompt performance metrics.
    
    Args:
        target_date: Date to calculate for (YYYY-MM-DD). Defaults to yesterday.
    
    Returns:
        Dict with summary of calculated metrics
    """
    try:
        # Parse target date or use yesterday
        if target_date:
            calc_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        else:
            calc_date = date.today() - timedelta(days=1)
        
        logger.info(f"Starting daily prompt performance calculation for {calc_date}")
        
        db = SessionLocal()
        try:
            result = calculate_prompt_performance(db, calc_date)
            logger.info(f"Completed prompt performance calculation for {calc_date}: {result}")
            return result
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Error in daily prompt performance task: {e}")
        raise


@celery_app.task(name="calculate_prompt_performance_range")
def calculate_prompt_performance_range(start_date: str, end_date: str):
    """
    Calculate prompt performance for a date range (for backfilling).
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        Dict with summary of calculated metrics
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        logger.info(f"Starting prompt performance calculation for range {start} to {end}")
        
        db = SessionLocal()
        try:
            total_prompts = 0
            total_days = 0
            
            current_date = start
            while current_date <= end:
                result = calculate_prompt_performance(db, current_date)
                total_prompts += result.get('prompts_updated', 0)
                total_days += 1
                current_date += timedelta(days=1)
            
            summary = {
                "start_date": start_date,
                "end_date": end_date,
                "days_processed": total_days,
                "total_prompts_updated": total_prompts
            }
            
            logger.info(f"Completed range calculation: {summary}")
            return summary
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Error in range prompt performance task: {e}")
        raise


def calculate_prompt_performance(db: Session, calc_date: date) -> Dict[str, Any]:
    """
    Calculate prompt performance metrics for a specific date.
    
    Aggregates trading signal outcomes by prompt_template_id and strategy_id.
    
    Args:
        db: Database session
        calc_date: Date to calculate metrics for
    
    Returns:
        Dict with summary statistics
    """
    try:
        # Query signals generated on calc_date with prompt metadata and outcomes
        signals = db.query(TradingSignal).filter(
            func.date(TradingSignal.generated_at) == calc_date,
            TradingSignal.prompt_template_id.isnot(None),
            TradingSignal.outcome.in_(['win', 'loss'])  # Only completed signals
        ).all()
        
        if not signals:
            logger.info(f"No completed signals found for {calc_date}")
            return {"date": str(calc_date), "signals_found": 0, "prompts_updated": 0}
        
        logger.info(f"Found {len(signals)} completed signals for {calc_date}")
        
        # Group signals by (prompt_template_id, strategy_id)
        grouped_signals: Dict[tuple, List[TradingSignal]] = {}
        for signal in signals:
            key = (signal.prompt_template_id, signal.strategy_id)
            if key not in grouped_signals:
                grouped_signals[key] = []
            grouped_signals[key].append(signal)
        
        prompts_updated = 0
        
        # Calculate metrics for each group
        for (prompt_id, strategy_id), group_signals in grouped_signals.items():
            metrics = _calculate_metrics(group_signals, calc_date)
            
            # Check if performance record already exists
            existing = db.query(PromptPerformance).filter(
                PromptPerformance.prompt_template_id == prompt_id,
                PromptPerformance.strategy_id == strategy_id if strategy_id else PromptPerformance.strategy_id.is_(None),
                PromptPerformance.date == calc_date
            ).first()
            
            if existing:
                # Update existing record
                for key, value in metrics.items():
                    setattr(existing, key, value)
                existing.updated_at = datetime.now()
                logger.debug(f"Updated performance for prompt {prompt_id}, strategy {strategy_id}")
            else:
                # Create new record
                perf = PromptPerformance(
                    prompt_template_id=prompt_id,
                    strategy_id=strategy_id,
                    date=calc_date,
                    **metrics
                )
                db.add(perf)
                logger.debug(f"Created performance for prompt {prompt_id}, strategy {strategy_id}")
            
            prompts_updated += 1
        
        db.commit()
        
        summary = {
            "date": str(calc_date),
            "signals_found": len(signals),
            "prompts_updated": prompts_updated,
            "unique_prompts": len(grouped_signals)
        }
        
        return summary
    
    except Exception as e:
        logger.error(f"Error calculating performance for {calc_date}: {e}")
        db.rollback()
        raise


def _calculate_metrics(signals: List[TradingSignal], calc_date: date) -> Dict[str, Any]:
    """
    Calculate aggregated metrics for a group of signals.
    
    Args:
        signals: List of trading signals
        calc_date: Date these metrics are for
    
    Returns:
        Dict of metrics for PromptPerformance model
    """
    # Count signals by outcome
    win_signals = [s for s in signals if s.outcome == 'win']
    loss_signals = [s for s in signals if s.outcome == 'loss']
    
    # Count executed signals (those with execution_price)
    executed_signals = [s for s in signals if s.execution_price is not None]
    
    # Calculate R-multiples
    r_multiples = [s.actual_r_multiple for s in signals if s.actual_r_multiple is not None]
    avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else None
    best_r = max(r_multiples) if r_multiples else None
    worst_r = min(r_multiples) if r_multiples else None
    
    # Calculate profit/loss
    profits_losses = [s.profit_loss for s in signals if s.profit_loss is not None]
    total_pl = sum(profits_losses) if profits_losses else None
    
    # Calculate percentage returns
    wins_with_pct = [
        (s.profit_loss / s.execution_price * 100) 
        for s in win_signals 
        if s.profit_loss and s.execution_price
    ]
    losses_with_pct = [
        (abs(s.profit_loss) / s.execution_price * 100) 
        for s in loss_signals 
        if s.profit_loss and s.execution_price
    ]
    
    avg_profit_pct = sum(wins_with_pct) / len(wins_with_pct) if wins_with_pct else None
    avg_loss_pct = sum(losses_with_pct) / len(losses_with_pct) if losses_with_pct else None
    
    # Calculate average confidence
    confidences = [s.confidence for s in signals if s.confidence is not None]
    avg_confidence = sum(confidences) / len(confidences) if confidences else None
    
    return {
        "signals_generated": len(signals),
        "signals_executed": len(executed_signals),
        "total_profit_loss": Decimal(str(total_pl)) if total_pl else None,
        "win_count": len(win_signals),
        "loss_count": len(loss_signals),
        "avg_r_multiple": Decimal(str(avg_r)) if avg_r else None,
        "best_r_multiple": Decimal(str(best_r)) if best_r else None,
        "worst_r_multiple": Decimal(str(worst_r)) if worst_r else None,
        "avg_profit_pct": Decimal(str(avg_profit_pct)) if avg_profit_pct else None,
        "avg_loss_pct": Decimal(str(avg_loss_pct)) if avg_loss_pct else None,
        "avg_confidence": Decimal(str(avg_confidence)) if avg_confidence else None,
    }


@celery_app.task(name="cleanup_old_performance_records")
def cleanup_old_performance_records(days_to_keep: int = 365):
    """
    Clean up old performance records (optional housekeeping).
    
    Args:
        days_to_keep: Number of days of history to keep (default: 365)
    """
    try:
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        db = SessionLocal()
        try:
            deleted = db.query(PromptPerformance).filter(
                PromptPerformance.date < cutoff_date
            ).delete()
            
            db.commit()
            
            logger.info(f"Cleaned up {deleted} old performance records before {cutoff_date}")
            return {"deleted": deleted, "cutoff_date": str(cutoff_date)}
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Error cleaning up old performance records: {e}")
        raise

