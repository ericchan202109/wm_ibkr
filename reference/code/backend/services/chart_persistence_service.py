"""Chart Persistence Service for saving generated charts to database."""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from decimal import Decimal

from backend.models.chart import Chart
# WorkflowExecution model removed - workflows now handled by Airflow

logger = logging.getLogger(__name__)


class ChartPersistenceService:
    """Service for persisting chart data to database."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def save_chart(
        self,
        execution_id: int,
        symbol: str,
        conid: int,
        timeframe: str,
        chart_type: str,
        chart_url_jpeg: str,
        chart_url_html: str = None,
        indicators_applied: Dict[str, Any] = None,
        data_points: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
        price_current: float = None,
        price_change_pct: float = None,
        volume_avg: int = None,
        minio_bucket: str = None,
        minio_object_key: str = None
    ) -> Chart:
        """
        Save chart record to database.
        
        Args:
            execution_id: Workflow execution ID
            symbol: Stock symbol
            conid: IBKR contract ID
            timeframe: Chart timeframe ('1d', '1w', '1mo')
            chart_type: Chart type ('daily', 'weekly', 'monthly')
            chart_url_jpeg: MinIO URL for JPEG chart
            chart_url_html: MinIO URL for HTML chart
            indicators_applied: Dictionary of indicators and their config
            data_points: Number of data points in chart
            start_date: Start date of data
            end_date: End date of data
            price_current: Current price
            price_change_pct: Price change percentage
            volume_avg: Average volume
            minio_bucket: MinIO bucket name
            minio_object_key: MinIO object key
            
        Returns:
            Chart: Saved chart record
        """
        try:
            chart = Chart(
                execution_id=execution_id,
                symbol=symbol,
                conid=conid,
                timeframe=timeframe,
                chart_type=chart_type,
                chart_url_jpeg=chart_url_jpeg,
                chart_url_html=chart_url_html,
                minio_bucket=minio_bucket,
                minio_object_key=minio_object_key,
                indicators_applied=indicators_applied,
                data_points=data_points,
                start_date=start_date,
                end_date=end_date,
                price_current=Decimal(str(price_current)) if price_current else None,
                price_change_pct=Decimal(str(price_change_pct)) if price_change_pct else None,
                volume_avg=volume_avg,
                generated_at=datetime.now(timezone.utc),
                status='active'
            )
            
            self.db.add(chart)
            self.db.commit()
            self.db.refresh(chart)
            
            logger.info(f"✅ Saved chart record: {symbol} {timeframe} (ID: {chart.id})")
            return chart
            
        except Exception as e:
            logger.error(f"Failed to save chart: {e}")
            self.db.rollback()
            raise
    
    def get_chart_by_id(self, chart_id: int) -> Optional[Chart]:
        """Get chart by ID."""
        return self.db.query(Chart).filter(Chart.id == chart_id).first()
    
    def get_charts_by_execution(self, execution_id: int) -> List[Chart]:
        """Get all charts for a specific execution."""
        return self.db.query(Chart).filter(
            Chart.execution_id == execution_id
        ).order_by(Chart.timeframe).all()
    
    def get_charts_by_symbol(
        self,
        symbol: str,
        timeframe: str = None,
        limit: int = 10
    ) -> List[Chart]:
        """Get recent charts for a symbol."""
        query = self.db.query(Chart).filter(Chart.symbol == symbol)
        
        if timeframe:
            query = query.filter(Chart.timeframe == timeframe)
        
        return query.order_by(Chart.generated_at.desc()).limit(limit).all()
    
    def archive_old_charts(self, days: int = 90):
        """Archive charts older than specified days."""
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        result = self.db.query(Chart).filter(
            Chart.generated_at < cutoff_date,
            Chart.status == 'active'
        ).update({'status': 'archived'})
        
        self.db.commit()
        logger.info(f"Archived {result} old charts")
        return result

