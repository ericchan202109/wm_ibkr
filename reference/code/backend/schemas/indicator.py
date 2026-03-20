"""Indicator schemas for API validation."""
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime


class IndicatorBase(BaseModel):
    """Base indicator schema."""
    name: str
    type: str
    parameters: Dict[str, Any]
    period: Optional[int] = Field(default=100, ge=20, le=500, description="Number of data points for chart")
    frequency: Optional[str] = Field(default="1D", description="Data frequency: 1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M")


class IndicatorCreate(IndicatorBase):
    """Schema for creating an indicator."""
    
    @validator('type')
    def validate_type(cls, v):
        """Validate indicator type."""
        valid_types = [
            'MA', 'EMA', 'SMA', 'WMA',  # Moving Averages
            'RSI',  # Relative Strength Index
            'MACD',  # Moving Average Convergence Divergence
            'BB',  # Bollinger Bands
            'SuperTrend',
            'ATR',  # Average True Range
            'Stochastic',
            'ADX',  # Average Directional Index
            'CCI',  # Commodity Channel Index
            'OBV'   # On-Balance Volume
        ]
        if v not in valid_types:
            raise ValueError(f"Invalid indicator type. Must be one of: {', '.join(valid_types)}")
        return v
    
    @validator('parameters')
    def validate_parameters(cls, v, values):
        """Validate parameters based on indicator type."""
        if 'type' not in values:
            return v
            
        indicator_type = values['type']
        
        # Validate Moving Averages
        if indicator_type in ['MA', 'EMA', 'SMA', 'WMA']:
            if 'period' not in v:
                raise ValueError("Moving Average requires 'period' parameter")
            if not isinstance(v['period'], int) or v['period'] < 1:
                raise ValueError("period must be a positive integer")
        
        # Validate RSI
        elif indicator_type == 'RSI':
            if 'period' not in v:
                raise ValueError("RSI requires 'period' parameter")
            if not isinstance(v['period'], int) or v['period'] < 1:
                raise ValueError("period must be a positive integer")
            if 'overbought' in v and (v['overbought'] < 0 or v['overbought'] > 100):
                raise ValueError("overbought must be between 0 and 100")
            if 'oversold' in v and (v['oversold'] < 0 or v['oversold'] > 100):
                raise ValueError("oversold must be between 0 and 100")
        
        # Validate MACD
        elif indicator_type == 'MACD':
            required = ['fast_period', 'slow_period', 'signal_period']
            for param in required:
                if param not in v:
                    raise ValueError(f"MACD requires '{param}' parameter")
                if not isinstance(v[param], int) or v[param] < 1:
                    raise ValueError(f"{param} must be a positive integer")
        
        # Validate Bollinger Bands
        elif indicator_type == 'BB':
            if 'period' not in v:
                raise ValueError("Bollinger Bands requires 'period' parameter")
            if 'std_dev' not in v:
                raise ValueError("Bollinger Bands requires 'std_dev' parameter")
            if not isinstance(v['period'], int) or v['period'] < 1:
                raise ValueError("period must be a positive integer")
            if not isinstance(v['std_dev'], (int, float)) or v['std_dev'] <= 0:
                raise ValueError("std_dev must be a positive number")
        
        # Validate ATR
        elif indicator_type == 'ATR':
            if 'period' not in v:
                raise ValueError("ATR requires 'period' parameter")
            if not isinstance(v['period'], int) or v['period'] < 1:
                raise ValueError("period must be a positive integer")
        
        # Validate SuperTrend
        elif indicator_type == 'SuperTrend':
            if 'period' not in v:
                raise ValueError("SuperTrend requires 'period' parameter")
            if 'multiplier' not in v:
                raise ValueError("SuperTrend requires 'multiplier' parameter")
            if not isinstance(v['period'], int) or v['period'] < 1:
                raise ValueError("period must be a positive integer")
            if not isinstance(v['multiplier'], (int, float)) or v['multiplier'] <= 0:
                raise ValueError("multiplier must be a positive number")
        
        return v


class IndicatorUpdate(BaseModel):
    """Schema for updating an indicator."""
    name: Optional[str] = None
    type: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    period: Optional[int] = Field(None, ge=20, le=500)
    frequency: Optional[str] = None


class IndicatorResponse(IndicatorBase):
    """Schema for indicator response."""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class IndicatorTemplate(BaseModel):
    """Schema for indicator template."""
    type: str
    name: str
    description: str
    default_parameters: Dict[str, Any]
    parameter_schema: Dict[str, Any]


class IndicatorCalculationRequest(BaseModel):
    """Schema for indicator calculation request."""
    symbol: str
    timeframe: str = Field(default="1D", description="Timeframe: 1D, 1W, 1M")
    data_points: int = Field(default=100, ge=20, le=500, description="Number of historical data points")


class IndicatorCalculationResponse(BaseModel):
    """Schema for indicator calculation response."""
    indicator_id: int
    symbol: str
    timeframe: str
    values: List[Dict[str, Any]]  # [{time: str, value: float}, ...]


class ChartGenerateRequest(BaseModel):
    """Schema for chart generation request."""
    strategy_id: Optional[int] = None
    symbol: str
    indicator_ids: List[int] = Field(..., min_items=1, description="List of indicator IDs to include in chart")
    period: int = Field(default=100, ge=20, le=500, description="Number of data points")
    frequency: str = Field(default="1D", description="Data frequency: 1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M")


class ChartResponse(BaseModel):
    """Schema for chart response."""
    id: int
    strategy_id: Optional[int] = None
    symbol: str
    indicator_ids: List[int]
    period: int
    frequency: str
    chart_url_jpeg: Optional[str] = None
    chart_url_html: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime
    expires_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

