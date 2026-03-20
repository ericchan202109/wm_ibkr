"""Technical analysis schemas for request/response models."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class TrendDirection(str, Enum):
    """Trend direction enumeration."""
    STRONG_BULLISH = "strong_bullish"  # 強烈看漲
    BULLISH = "bullish"  # 看漲
    NEUTRAL = "neutral"  # 中性
    BEARISH = "bearish"  # 看跌
    STRONG_BEARISH = "strong_bearish"  # 強烈看跌


class TradeSignal(str, Enum):
    """Trade signal enumeration."""
    BUY = "buy"  # 進場
    HOLD = "hold"  # 持有
    SELL = "sell"  # 出場


class SignalStatus(str, Enum):
    """Individual signal status."""
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class AnalysisRequest(BaseModel):
    """Request model for generating technical analysis."""
    symbol: str = Field(..., description="Stock symbol to analyze")
    period: int = Field(default=100, ge=50, le=500, description="Number of data points for analysis")
    timeframe: str = Field(default="1d", description="Data timeframe (1d, 1w, etc.)")
    language: str = Field(default="en", description="Report language (en=English, zh=Chinese)")


class SignalConfirmation(BaseModel):
    """Signal confirmation status from 3/4 rule."""
    supertrend_signal: SignalStatus = Field(..., description="SuperTrend direction signal")
    price_vs_ma20: SignalStatus = Field(..., description="Price position vs 20-day SMA")
    macd_signal: SignalStatus = Field(..., description="MACD crossover signal")
    rsi_signal: SignalStatus = Field(..., description="RSI position signal")
    confirmed_signals: int = Field(..., ge=0, le=4, description="Number of confirming signals")
    confirmation_passed: bool = Field(..., description="Whether 3/4 rule is satisfied")
    overall_direction: SignalStatus = Field(..., description="Overall confirmed direction")


class TradeRecommendation(BaseModel):
    """Trade recommendation with entry, stop-loss, and targets."""
    direction: TradeSignal = Field(..., description="Trade direction")
    entry_price_low: Optional[float] = Field(None, description="Entry price range low")
    entry_price_high: Optional[float] = Field(None, description="Entry price range high")
    stop_loss: Optional[float] = Field(None, description="Stop loss price")
    stop_loss_reasoning: Optional[str] = Field(None, description="Stop loss calculation explanation")
    
    target_conservative: Optional[float] = Field(None, description="Conservative profit target")
    target_conservative_reasoning: Optional[str] = Field(None, description="Conservative target justification")
    target_aggressive: Optional[float] = Field(None, description="Aggressive profit target")
    target_aggressive_reasoning: Optional[str] = Field(None, description="Aggressive target justification")
    
    r_multiple_conservative: Optional[float] = Field(None, description="R-multiple for conservative target")
    r_multiple_aggressive: Optional[float] = Field(None, description="R-multiple for aggressive target")
    
    position_size_percent: Optional[float] = Field(None, ge=0, le=100, description="Recommended position size %")


class IndicatorAnalysis(BaseModel):
    """Individual indicator analysis results."""
    name: str = Field(..., description="Indicator name")
    current_value: Optional[float] = Field(None, description="Current indicator value")
    status: str = Field(..., description="Indicator status description")
    signal: SignalStatus = Field(..., description="Signal from this indicator")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")


class PriceAnalysis(BaseModel):
    """Core price analysis."""
    current_price: float = Field(..., description="Current price")
    price_change: float = Field(..., description="Price change from previous period")
    price_change_percent: float = Field(..., description="Price change percentage")
    trend_description: str = Field(..., description="Price trend description")
    support_levels: List[float] = Field(default_factory=list, description="Key support levels")
    resistance_levels: List[float] = Field(default_factory=list, description="Key resistance levels")
    key_patterns: List[str] = Field(default_factory=list, description="Identified chart patterns")


class VolumeAnalysis(BaseModel):
    """Volume analysis results."""
    current_volume: float = Field(..., description="Current volume")
    avg_volume_20d: float = Field(..., description="20-day average volume")
    volume_ratio: float = Field(..., description="Current vs average volume ratio")
    volume_trend: str = Field(..., description="Volume trend description")
    obv_trend: str = Field(..., description="OBV trend description")


class RiskAssessment(BaseModel):
    """Risk assessment for the trade."""
    technical_risks: List[str] = Field(default_factory=list, description="Identified technical risks")
    reversal_price: Optional[float] = Field(None, description="Key reversal price level")
    failure_scenarios: List[str] = Field(default_factory=list, description="Potential signal failure scenarios")
    weekly_confirmation: Optional[str] = Field(None, description="Weekly timeframe confirmation status")


class ComprehensiveAnalysis(BaseModel):
    """Complete comprehensive technical analysis result."""
    symbol: str = Field(..., description="Analyzed symbol")
    analysis_date: datetime = Field(..., description="Analysis timestamp")
    timeframe: str = Field(..., description="Data timeframe")
    
    # Core analysis
    price_analysis: PriceAnalysis = Field(..., description="Price analysis")
    
    # Trend indicators
    supertrend: IndicatorAnalysis = Field(..., description="SuperTrend analysis")
    ma_20: IndicatorAnalysis = Field(..., description="20-day SMA analysis")
    ma_50: IndicatorAnalysis = Field(..., description="50-day SMA analysis")
    ma_200: IndicatorAnalysis = Field(..., description="200-day SMA analysis")
    ma_crosses: Dict[str, str] = Field(default_factory=dict, description="Moving average crossovers")
    
    # Confirmation indicators
    macd: IndicatorAnalysis = Field(..., description="MACD analysis")
    rsi: IndicatorAnalysis = Field(..., description="RSI analysis")
    atr: IndicatorAnalysis = Field(..., description="ATR analysis")
    bollinger_bands: IndicatorAnalysis = Field(..., description="Bollinger Bands analysis")
    
    # Volume analysis
    volume_analysis: VolumeAnalysis = Field(..., description="Volume analysis")
    
    # Signal confirmation
    signal_confirmation: SignalConfirmation = Field(..., description="3/4 signal confirmation")
    
    # Trade recommendation
    overall_trend: TrendDirection = Field(..., description="Overall trend assessment")
    trade_recommendation: TradeRecommendation = Field(..., description="Trade recommendation")
    
    # Risk assessment
    risk_assessment: RiskAssessment = Field(..., description="Risk assessment")
    
    # Full report
    report_markdown: str = Field(..., description="Complete analysis report in markdown")
    
    # Chart visualization (reference: reference/webapp/services/chart_service.py)
    chart_url_jpeg: Optional[str] = Field(None, description="Public URL to chart image (JPEG)")
    chart_url_html: Optional[str] = Field(None, description="Public URL to interactive HTML chart")


class AnalysisHistoryItem(BaseModel):
    """Historical analysis record."""
    id: int = Field(..., description="Analysis ID")
    symbol: str = Field(..., description="Symbol")
    analysis_date: datetime = Field(..., description="Analysis timestamp")
    timeframe: str = Field(..., description="Timeframe")
    overall_trend: TrendDirection = Field(..., description="Trend assessment")
    trade_signal: TradeSignal = Field(..., description="Trade signal")
    entry_price: Optional[float] = Field(None, description="Recommended entry price")
    target_price: Optional[float] = Field(None, description="Target price")
    actual_outcome: Optional[str] = Field(None, description="Actual outcome if traded")


class AnalysisHistoryResponse(BaseModel):
    """Response model for analysis history."""
    total: int = Field(..., description="Total number of analyses")
    items: List[AnalysisHistoryItem] = Field(..., description="Analysis history items")

