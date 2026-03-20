"""Technical indicators management endpoints."""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from backend.core.database import get_db
from backend.models.indicator import Indicator
# Strategy model removed - indicators now managed independently
from backend.schemas.indicator import (
    IndicatorCreate,
    IndicatorUpdate,
    IndicatorResponse,
    IndicatorTemplate,
    IndicatorCalculationRequest,
    IndicatorCalculationResponse
)
from typing import List
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


# Indicator templates with default parameters
INDICATOR_TEMPLATES = [
    {
        "type": "MA",
        "name": "Moving Average",
        "description": "Simple, Exponential, or Weighted Moving Average",
        "default_parameters": {"period": 20, "ma_type": "SMA", "source": "close"},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 500, "description": "Number of periods"},
            "ma_type": {"type": "string", "enum": ["SMA", "EMA", "WMA"], "description": "MA type"},
            "source": {"type": "string", "enum": ["close", "open", "high", "low"], "description": "Price source"}
        }
    },
    {
        "type": "RSI",
        "name": "Relative Strength Index",
        "description": "Momentum oscillator measuring speed and magnitude of price changes",
        "default_parameters": {"period": 14, "overbought": 70, "oversold": 30},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 100, "description": "RSI period"},
            "overbought": {"type": "number", "min": 50, "max": 100, "description": "Overbought threshold"},
            "oversold": {"type": "number", "min": 0, "max": 50, "description": "Oversold threshold"}
        }
    },
    {
        "type": "MACD",
        "name": "MACD",
        "description": "Moving Average Convergence Divergence",
        "default_parameters": {"fast_period": 12, "slow_period": 26, "signal_period": 9},
        "parameter_schema": {
            "fast_period": {"type": "integer", "min": 1, "max": 100, "description": "Fast EMA period"},
            "slow_period": {"type": "integer", "min": 1, "max": 100, "description": "Slow EMA period"},
            "signal_period": {"type": "integer", "min": 1, "max": 100, "description": "Signal line period"}
        }
    },
    {
        "type": "BB",
        "name": "Bollinger Bands",
        "description": "Volatility bands placed above and below moving average",
        "default_parameters": {"period": 20, "std_dev": 2, "source": "close"},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 500, "description": "BB period"},
            "std_dev": {"type": "number", "min": 0.5, "max": 5, "description": "Standard deviations"},
            "source": {"type": "string", "enum": ["close", "open", "high", "low"], "description": "Price source"}
        }
    },
    {
        "type": "SuperTrend",
        "name": "SuperTrend",
        "description": "Trend-following indicator using ATR",
        "default_parameters": {"period": 10, "multiplier": 3},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 100, "description": "ATR period"},
            "multiplier": {"type": "number", "min": 0.5, "max": 10, "description": "ATR multiplier"}
        }
    },
    {
        "type": "ATR",
        "name": "Average True Range",
        "description": "Volatility indicator measuring market volatility",
        "default_parameters": {"period": 14},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 100, "description": "ATR period"}
        }
    },
    {
        "type": "OBV",
        "name": "On-Balance Volume",
        "description": "Volume-based momentum indicator showing buying and selling pressure",
        "default_parameters": {},
        "parameter_schema": {}
    },
    {
        "type": "Volume",
        "name": "Volume Analysis",
        "description": "Trading volume with moving average comparison",
        "default_parameters": {"period": 20},
        "parameter_schema": {
            "period": {"type": "integer", "min": 1, "max": 100, "description": "Volume MA period"}
        }
    }
]


@router.get("", response_model=List[IndicatorResponse])
async def list_indicators(
    skip: int = 0,
    limit: int = 100,
    type: str = None,
    db: Session = Depends(get_db)
):
    """List all indicators with optional filtering."""
    query = db.query(Indicator)
    
    if type:
        query = query.filter(Indicator.type == type)
    
    indicators = query.offset(skip).limit(limit).all()
    return indicators


@router.get("/templates", response_model=List[IndicatorTemplate])
async def get_indicator_templates():
    """Get list of available indicator templates."""
    return INDICATOR_TEMPLATES


@router.post("", response_model=IndicatorResponse, status_code=201)
async def create_indicator(
    indicator_data: IndicatorCreate,
    db: Session = Depends(get_db)
):
    """Create a new indicator."""
    indicator = Indicator(
        name=indicator_data.name,
        type=indicator_data.type,
        parameters=indicator_data.parameters
    )
    
    db.add(indicator)
    db.commit()
    db.refresh(indicator)
    
    logger.info(f"Created indicator: {indicator.id} - {indicator.name}")
    return indicator


@router.post("/from-template", response_model=IndicatorResponse, status_code=201)
async def create_indicator_from_template(
    template_type: str,
    name: str,
    custom_params: dict = {},
    db: Session = Depends(get_db)
):
    """Create indicator from a template with optional custom parameters."""
    # Find template
    template = next((t for t in INDICATOR_TEMPLATES if t["type"] == template_type), None)
    if not template:
        raise HTTPException(status_code=404, detail=f"Template '{template_type}' not found")
    
    # Merge default and custom parameters
    parameters = {**template["default_parameters"], **custom_params}
    
    # Create indicator
    indicator = Indicator(
        name=name,
        type=template_type,
        parameters=parameters
    )
    
    db.add(indicator)
    db.commit()
    db.refresh(indicator)
    
    logger.info(f"Created indicator from template: {indicator.id} - {indicator.name}")
    return indicator


@router.get("/{indicator_id}", response_model=IndicatorResponse)
async def get_indicator(indicator_id: int, db: Session = Depends(get_db)):
    """Get indicator by ID."""
    indicator = db.query(Indicator).filter(Indicator.id == indicator_id).first()
    if not indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    return indicator


@router.put("/{indicator_id}", response_model=IndicatorResponse)
async def update_indicator(
    indicator_id: int,
    indicator_data: IndicatorUpdate,
    db: Session = Depends(get_db)
):
    """Update an indicator."""
    indicator = db.query(Indicator).filter(Indicator.id == indicator_id).first()
    if not indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    
    # Update fields
    update_data = indicator_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(indicator, field, value)
    
    db.commit()
    db.refresh(indicator)
    
    logger.info(f"Updated indicator: {indicator.id}")
    return indicator


@router.delete("/{indicator_id}")
async def delete_indicator(indicator_id: int, db: Session = Depends(get_db)):
    """Delete an indicator."""
    indicator = db.query(Indicator).filter(Indicator.id == indicator_id).first()
    if not indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    
    # Check if indicator is used by any strategies
    if indicator.strategies:
        strategy_names = [s.name for s in indicator.strategies]
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete indicator - it is used by strategies: {', '.join(strategy_names)}"
        )
    
    db.delete(indicator)
    db.commit()
    
    logger.info(f"Deleted indicator: {indicator_id}")
    return {"message": "Indicator deleted successfully"}


@router.post("/{indicator_id}/calculate", response_model=IndicatorCalculationResponse)
async def calculate_indicator(
    indicator_id: int,
    calc_request: IndicatorCalculationRequest,
    db: Session = Depends(get_db)
):
    """Calculate indicator values for given market data."""
    indicator = db.query(Indicator).filter(Indicator.id == indicator_id).first()
    if not indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")
    
    try:
        from backend.services.indicator_service import IndicatorService
        
        indicator_service = IndicatorService()
        values = await indicator_service.calculate(
            indicator_type=indicator.type,
            parameters=indicator.parameters,
            symbol=calc_request.symbol,
            timeframe=calc_request.timeframe,
            data_points=calc_request.data_points
        )
        
        return {
            "indicator_id": indicator.id,
            "symbol": calc_request.symbol,
            "timeframe": calc_request.timeframe,
            "values": values
        }
    
    except Exception as e:
        logger.error(f"Indicator calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Calculation failed: {str(e)}")

