"""Comprehensive technical analysis service."""
import logging
from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime
import pandas as pd
import numpy as np
import ta

from backend.schemas.analysis import (
    ComprehensiveAnalysis,
    PriceAnalysis,
    IndicatorAnalysis,
    VolumeAnalysis,
    SignalConfirmation,
    TradeRecommendation,
    RiskAssessment,
    TrendDirection,
    TradeSignal,
    SignalStatus
)

logger = logging.getLogger(__name__)


class AnalysisService:
    """Service for generating comprehensive technical analysis."""
    
    def __init__(self):
        """Initialize the analysis service."""
        pass
    
    @staticmethod
    def _safe_format(value: Optional[Union[float, int]], format_type: str = "price", default: str = "N/A") -> str:
        """
        Safely format a numeric value, handling None values.
        
        Args:
            value: The numeric value to format (can be None)
            format_type: Type of formatting - "price", "percent", "int", "decimal"
            default: Default string to return if value is None
            
        Returns:
            Formatted string or default if value is None
        """
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        
        try:
            if format_type == "price":
                return f"${float(value):.2f}"
            elif format_type == "percent":
                return f"{float(value):.2f}%"
            elif format_type == "int":
                return f"{int(value):,}"
            elif format_type == "decimal":
                return f"{float(value):.2f}"
            else:
                return str(value)
        except (ValueError, TypeError):
            return default
    
    async def generate_comprehensive_analysis(
        self,
        symbol: str,
        market_data: pd.DataFrame,
        period: int = 100,
        timeframe: str = "1d",
        language: str = "zh"
    ) -> ComprehensiveAnalysis:
        """
        Generate comprehensive technical analysis for a symbol.
        
        Args:
            symbol: Stock symbol
            market_data: DataFrame with OHLCV data
            period: Number of periods to analyze
            timeframe: Data timeframe
            language: Report language (zh/en)
            
        Returns:
            ComprehensiveAnalysis object with full analysis
        """
        try:
            # Prepare data
            df = market_data.tail(period).copy()
            if 'date' not in df.columns:
                df['date'] = pd.to_datetime(df.index)
            
            # Calculate all indicators
            indicators = self._calculate_all_indicators(df)
            
            # Analyze price
            price_analysis = self._analyze_price(df, indicators)
            
            # Analyze trend indicators
            supertrend_analysis = self._analyze_supertrend(df, indicators)
            ma20_analysis = self._analyze_ma(df, indicators, 20)
            ma50_analysis = self._analyze_ma(df, indicators, 50)
            ma200_analysis = self._analyze_ma(df, indicators, 200)
            ma_crosses = self._detect_ma_crosses(indicators)
            
            # Analyze confirmation indicators
            macd_analysis = self._analyze_macd(df, indicators)
            rsi_analysis = self._analyze_rsi(df, indicators)
            atr_analysis = self._analyze_atr(df, indicators)
            bb_analysis = self._analyze_bollinger_bands(df, indicators)
            
            # Analyze volume
            volume_analysis = self._analyze_volume(df, indicators)
            
            # 3/4 Signal confirmation
            signal_confirmation = self._confirm_signals_3_of_4(
                supertrend_analysis,
                ma20_analysis,
                macd_analysis,
                rsi_analysis
            )
            
            # Overall trend assessment
            overall_trend = self._assess_overall_trend(signal_confirmation, indicators)
            
            # Trade recommendation
            trade_recommendation = self._generate_trade_recommendation(
                signal_confirmation,
                overall_trend,
                df,
                indicators,
                price_analysis,
                atr_analysis
            )
            
            # Risk assessment
            risk_assessment = self._assess_risks(
                df,
                indicators,
                signal_confirmation,
                overall_trend
            )
            
            # Generate markdown report
            report_markdown = self._generate_report(
                symbol=symbol,
                price_analysis=price_analysis,
                supertrend=supertrend_analysis,
                ma20=ma20_analysis,
                ma50=ma50_analysis,
                ma200=ma200_analysis,
                ma_crosses=ma_crosses,
                macd=macd_analysis,
                rsi=rsi_analysis,
                atr=atr_analysis,
                bb=bb_analysis,
                volume_analysis=volume_analysis,
                signal_confirmation=signal_confirmation,
                overall_trend=overall_trend,
                trade_recommendation=trade_recommendation,
                risk_assessment=risk_assessment,
                language=language
            )
            
            # Build comprehensive analysis object
            analysis = ComprehensiveAnalysis(
                symbol=symbol,
                analysis_date=datetime.now(),
                timeframe=timeframe,
                price_analysis=price_analysis,
                supertrend=supertrend_analysis,
                ma_20=ma20_analysis,
                ma_50=ma50_analysis,
                ma_200=ma200_analysis,
                ma_crosses=ma_crosses,
                macd=macd_analysis,
                rsi=rsi_analysis,
                atr=atr_analysis,
                bollinger_bands=bb_analysis,
                volume_analysis=volume_analysis,
                signal_confirmation=signal_confirmation,
                overall_trend=overall_trend,
                trade_recommendation=trade_recommendation,
                risk_assessment=risk_assessment,
                report_markdown=report_markdown
            )
            
            logger.info(f"Generated comprehensive analysis for {symbol}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error generating analysis for {symbol}: {str(e)}")
            raise
    
    def _calculate_all_indicators(self, df: pd.DataFrame) -> Dict:
        """Calculate all technical indicators."""
        close = df['close'].astype(float).values
        high = df['high'].astype(float).values
        low = df['low'].astype(float).values
        volume = df['volume'].astype(float).values
        
        indicators = {}
        
        # Moving Averages
        indicators['sma_20'] = ta.trend.sma_indicator(pd.Series(close), window=20).values
        indicators['sma_50'] = ta.trend.sma_indicator(pd.Series(close), window=50).values
        indicators['sma_200'] = ta.trend.sma_indicator(pd.Series(close), window=200).values

        # MACD
        close_series = pd.Series(close)
        macd = ta.trend.macd(close_series, window_fast=12, window_slow=26).values
        signal = ta.trend.macd_signal(close_series, window_fast=12, window_slow=26, window_sign=9).values
        histogram = ta.trend.macd_diff(close_series, window_fast=12, window_slow=26, window_sign=9).values
        indicators['macd'] = macd
        indicators['macd_signal'] = signal
        indicators['macd_histogram'] = histogram

        # RSI
        indicators['rsi'] = ta.momentum.rsi(pd.Series(close), window=14).values

        # ATR
        indicators['atr'] = ta.volatility.average_true_range(pd.Series(high), pd.Series(low), pd.Series(close), window=14).values

        # Bollinger Bands
        close_series = pd.Series(close)
        upper = ta.volatility.bollinger_hband(close_series, window=20, window_dev=2).values
        middle = ta.volatility.bollinger_mavg(close_series, window=20).values
        lower = ta.volatility.bollinger_lband(close_series, window=20, window_dev=2).values
        indicators['bb_upper'] = upper
        indicators['bb_middle'] = middle
        indicators['bb_lower'] = lower
        
        # SuperTrend
        period = 10
        multiplier = 3
        atr = ta.volatility.average_true_range(pd.Series(high), pd.Series(low), pd.Series(close), window=period).values
        hl_avg = (high + low) / 2
        upper_band = hl_avg + (multiplier * atr)
        lower_band = hl_avg - (multiplier * atr)
        supertrend = np.where(close > upper_band, lower_band, upper_band)
        direction = np.where(close > supertrend, 1, -1)
        indicators['supertrend'] = supertrend
        indicators['supertrend_direction'] = direction

        # OBV
        indicators['obv'] = self._calculate_obv(df)

        # Volume SMA
        indicators['volume_sma_20'] = ta.trend.sma_indicator(pd.Series(volume), window=20).values
        
        return indicators
    
    def _calculate_obv(self, df: pd.DataFrame) -> np.ndarray:
        """Calculate On-Balance Volume."""
        obv = [0.0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i - 1]:
                obv.append(obv[-1] + df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i - 1]:
                obv.append(obv[-1] - df['volume'].iloc[i])
            else:
                obv.append(obv[-1])
        return np.array(obv)
    
    def _analyze_price(self, df: pd.DataFrame, indicators: Dict) -> PriceAnalysis:
        """Analyze price action."""
        current_price = float(df['close'].iloc[-1])
        prev_price = float(df['close'].iloc[-2] if len(df) > 1 else current_price)
        price_change = current_price - prev_price
        price_change_percent = (price_change / prev_price * 100) if prev_price != 0 else 0
        
        # Determine trend
        if price_change_percent > 2:
            trend_desc = "強勁上漲" if price_change_percent > 5 else "上漲"
        elif price_change_percent < -2:
            trend_desc = "強勁下跌" if price_change_percent < -5 else "下跌"
        else:
            trend_desc = "盤整"
        
        # Find support/resistance (simple approach: recent highs/lows)
        recent_data = df.tail(20)
        resistance_levels = sorted(recent_data.nlargest(3, 'high')['high'].tolist(), reverse=True)
        support_levels = sorted(recent_data.nsmallest(3, 'low')['low'].tolist())
        
        return PriceAnalysis(
            current_price=current_price,
            price_change=price_change,
            price_change_percent=price_change_percent,
            trend_description=trend_desc,
            support_levels=[float(s) for s in support_levels],
            resistance_levels=[float(r) for r in resistance_levels],
            key_patterns=[]  # TODO: Pattern detection
        )
    
    def _analyze_supertrend(self, df: pd.DataFrame, indicators: Dict) -> IndicatorAnalysis:
        """Analyze SuperTrend indicator."""
        st_value = float(indicators['supertrend'][-1])
        st_direction = int(indicators['supertrend_direction'][-1])
        current_price = float(df['close'].iloc[-1])
        
        if st_direction == 1:
            status = "上升趨勢，綠色信號"
            signal = SignalStatus.BULLISH
        else:
            status = "下降趨勢，紅色信號"
            signal = SignalStatus.BEARISH
        
        return IndicatorAnalysis(
            name="SuperTrend (10,3)",
            current_value=st_value,
            status=status,
            signal=signal,
            details={
                "direction": "up" if st_direction == 1 else "down",
                "price_vs_supertrend": "above" if current_price > st_value else "below"
            }
        )
    
    def _analyze_ma(self, df: pd.DataFrame, indicators: Dict, period: int) -> IndicatorAnalysis:
        """Analyze Moving Average."""
        ma_key = f'sma_{period}'
        ma_value = float(indicators[ma_key][-1]) if not np.isnan(indicators[ma_key][-1]) else 0
        current_price = float(df['close'].iloc[-1])
        
        if current_price > ma_value:
            status = f"價格在{period}日均線上方，支撐作用"
            signal = SignalStatus.BULLISH
        else:
            status = f"價格在{period}日均線下方，阻力作用"
            signal = SignalStatus.BEARISH
        
        return IndicatorAnalysis(
            name=f"{period}日SMA",
            current_value=ma_value,
            status=status,
            signal=signal,
            details={"position": "above" if current_price > ma_value else "below"}
        )
    
    def _detect_ma_crosses(self, indicators: Dict) -> Dict[str, str]:
        """Detect moving average crossovers."""
        crosses = {}
        
        # Check 20/50 cross
        sma20 = indicators['sma_20']
        sma50 = indicators['sma_50']
        if not (np.isnan(sma20[-1]) or np.isnan(sma50[-1])):
            if sma20[-1] > sma50[-1]:
                crosses['20_50'] = "20日均線在50日均線上方"
            else:
                crosses['20_50'] = "20日均線在50日均線下方"
        
        # Check 50/200 cross (Golden/Death cross)
        sma200 = indicators['sma_200']
        if not (np.isnan(sma50[-1]) or np.isnan(sma200[-1])):
            if sma50[-1] > sma200[-1]:
                crosses['50_200'] = "黃金交叉(50日>200日)"
            else:
                crosses['50_200'] = "死亡交叉(50日<200日)"
        
        return crosses
    
    def _analyze_macd(self, df: pd.DataFrame, indicators: Dict) -> IndicatorAnalysis:
        """Analyze MACD indicator."""
        macd = float(indicators['macd'][-1]) if not np.isnan(indicators['macd'][-1]) else 0
        signal_line = float(indicators['macd_signal'][-1]) if not np.isnan(indicators['macd_signal'][-1]) else 0
        histogram = float(indicators['macd_histogram'][-1]) if not np.isnan(indicators['macd_histogram'][-1]) else 0
        
        if macd > signal_line:
            status = "MACD線在信號線上方，買入信號"
            signal = SignalStatus.BULLISH
        else:
            status = "MACD線在信號線下方，賣出信號"
            signal = SignalStatus.BEARISH
        
        return IndicatorAnalysis(
            name="MACD (12,26,9)",
            current_value=macd,
            status=status,
            signal=signal,
            details={
                "macd": macd,
                "signal_line": signal_line,
                "histogram": histogram
            }
        )
    
    def _analyze_rsi(self, df: pd.DataFrame, indicators: Dict) -> IndicatorAnalysis:
        """Analyze RSI indicator."""
        rsi = float(indicators['rsi'][-1]) if not np.isnan(indicators['rsi'][-1]) else 50
        
        if rsi > 70:
            status = "超買區間(>70)，可能回調"
            signal = SignalStatus.BEARISH
        elif rsi < 30:
            status = "超賣區間(<30)，可能反彈"
            signal = SignalStatus.BULLISH
        elif rsi > 50:
            status = "中性偏多(50-70)"
            signal = SignalStatus.BULLISH
        else:
            status = "中性偏空(30-50)"
            signal = SignalStatus.BEARISH
        
        return IndicatorAnalysis(
            name="RSI (14)",
            current_value=rsi,
            status=status,
            signal=signal,
            details={"level": "overbought" if rsi > 70 else "oversold" if rsi < 30 else "neutral"}
        )
    
    def _analyze_atr(self, df: pd.DataFrame, indicators: Dict) -> IndicatorAnalysis:
        """Analyze ATR indicator."""
        atr = float(indicators['atr'][-1]) if not np.isnan(indicators['atr'][-1]) else 0
        current_price = float(df['close'].iloc[-1])
        atr_percent = (atr / current_price * 100) if current_price != 0 else 0
        
        if atr_percent > 3:
            status = f"高波動性({atr_percent:.2f}%)，風險較大"
        elif atr_percent < 1:
            status = f"低波動性({atr_percent:.2f}%)，盤整行情"
        else:
            status = f"正常波動性({atr_percent:.2f}%)"
        
        return IndicatorAnalysis(
            name="ATR (14)",
            current_value=atr,
            status=status,
            signal=SignalStatus.NEUTRAL,
            details={"atr_percent": atr_percent}
        )
    
    def _analyze_bollinger_bands(self, df: pd.DataFrame, indicators: Dict) -> IndicatorAnalysis:
        """Analyze Bollinger Bands."""
        upper = float(indicators['bb_upper'][-1]) if not np.isnan(indicators['bb_upper'][-1]) else 0
        middle = float(indicators['bb_middle'][-1]) if not np.isnan(indicators['bb_middle'][-1]) else 0
        lower = float(indicators['bb_lower'][-1]) if not np.isnan(indicators['bb_lower'][-1]) else 0
        current_price = float(df['close'].iloc[-1])
        
        bandwidth = ((upper - lower) / middle * 100) if middle != 0 else 0
        
        if current_price > upper:
            status = f"價格突破上軌，超買區間，帶寬{bandwidth:.2f}%"
            signal = SignalStatus.BEARISH
        elif current_price < lower:
            status = f"價格跌破下軌，超賣區間，帶寬{bandwidth:.2f}%"
            signal = SignalStatus.BULLISH
        else:
            status = f"價格在布林帶內，帶寬{bandwidth:.2f}%"
            signal = SignalStatus.NEUTRAL
        
        return IndicatorAnalysis(
            name="布林帶 (20,2)",
            current_value=middle,
            status=status,
            signal=signal,
            details={
                "upper": upper,
                "middle": middle,
                "lower": lower,
                "bandwidth": bandwidth
            }
        )
    
    def _analyze_volume(self, df: pd.DataFrame, indicators: Dict) -> VolumeAnalysis:
        """Analyze volume patterns."""
        current_volume = float(df['volume'].iloc[-1])
        avg_volume = float(indicators['volume_sma_20'][-1]) if not np.isnan(indicators['volume_sma_20'][-1]) else current_volume
        volume_ratio = (current_volume / avg_volume) if avg_volume != 0 else 1
        
        if volume_ratio > 1.5:
            volume_trend = f"成交量放大({volume_ratio:.2f}倍)，趨勢加強"
        elif volume_ratio < 0.7:
            volume_trend = f"成交量萎縮({volume_ratio:.2f}倍)，趨勢減弱"
        else:
            volume_trend = f"成交量正常({volume_ratio:.2f}倍)"
        
        # OBV trend (simple: compare recent trend)
        obv = indicators['obv']
        obv_recent = obv[-5:]
        if obv_recent[-1] > obv_recent[0]:
            obv_trend = "OBV上升，買盤積極"
        else:
            obv_trend = "OBV下降，賣盤積極"
        
        return VolumeAnalysis(
            current_volume=current_volume,
            avg_volume_20d=avg_volume,
            volume_ratio=volume_ratio,
            volume_trend=volume_trend,
            obv_trend=obv_trend
        )
    
    def _confirm_signals_3_of_4(
        self,
        supertrend: IndicatorAnalysis,
        ma20: IndicatorAnalysis,
        macd: IndicatorAnalysis,
        rsi: IndicatorAnalysis
    ) -> SignalConfirmation:
        """Apply 3/4 signal confirmation rule."""
        signals = [
            supertrend.signal,
            ma20.signal,
            macd.signal,
            rsi.signal
        ]
        
        bullish_count = sum(1 for s in signals if s == SignalStatus.BULLISH)
        bearish_count = sum(1 for s in signals if s == SignalStatus.BEARISH)
        
        confirmed_signals = max(bullish_count, bearish_count)
        confirmation_passed = confirmed_signals >= 3
        
        if confirmation_passed:
            overall_direction = SignalStatus.BULLISH if bullish_count >= 3 else SignalStatus.BEARISH
        else:
            overall_direction = SignalStatus.NEUTRAL
        
        return SignalConfirmation(
            supertrend_signal=supertrend.signal,
            price_vs_ma20=ma20.signal,
            macd_signal=macd.signal,
            rsi_signal=rsi.signal,
            confirmed_signals=confirmed_signals,
            confirmation_passed=confirmation_passed,
            overall_direction=overall_direction
        )
    
    def _assess_overall_trend(
        self,
        signal_confirmation: SignalConfirmation,
        indicators: Dict
    ) -> TrendDirection:
        """Assess overall trend strength."""
        if not signal_confirmation.confirmation_passed:
            return TrendDirection.NEUTRAL
        
        confirmed = signal_confirmation.confirmed_signals
        direction = signal_confirmation.overall_direction
        
        if direction == SignalStatus.BULLISH:
            return TrendDirection.STRONG_BULLISH if confirmed == 4 else TrendDirection.BULLISH
        elif direction == SignalStatus.BEARISH:
            return TrendDirection.STRONG_BEARISH if confirmed == 4 else TrendDirection.BEARISH
        else:
            return TrendDirection.NEUTRAL
    
    def _generate_trade_recommendation(
        self,
        signal_confirmation: SignalConfirmation,
        overall_trend: TrendDirection,
        df: pd.DataFrame,
        indicators: Dict,
        price_analysis: PriceAnalysis,
        atr_analysis: IndicatorAnalysis
    ) -> TradeRecommendation:
        """Generate detailed trade recommendation."""
        current_price = float(df['close'].iloc[-1])
        atr = atr_analysis.current_value
        
        if not signal_confirmation.confirmation_passed:
            return TradeRecommendation(
                direction=TradeSignal.HOLD,
                position_size_percent=0
            )
        
        is_bullish = signal_confirmation.overall_direction == SignalStatus.BULLISH
        
        if is_bullish:
            # Bullish trade
            entry_low = current_price * 0.99  # 1% below current
            entry_high = current_price * 1.01  # 1% above current
            stop_loss = current_price - (2 * atr)
            
            # Conservative target: next resistance or 1.5xATR
            target_conservative = (
                price_analysis.resistance_levels[0] 
                if price_analysis.resistance_levels 
                else current_price + (1.5 * atr)
            )
            
            # Aggressive target: higher resistance or 3xATR
            target_aggressive = (
                price_analysis.resistance_levels[1] 
                if len(price_analysis.resistance_levels) > 1 
                else current_price + (3 * atr)
            )
            
            r_mult_cons = (target_conservative - current_price) / (current_price - stop_loss) if (current_price - stop_loss) != 0 else 0
            r_mult_agg = (target_aggressive - current_price) / (current_price - stop_loss) if (current_price - stop_loss) != 0 else 0
            
            return TradeRecommendation(
                direction=TradeSignal.BUY,
                entry_price_low=entry_low,
                entry_price_high=entry_high,
                stop_loss=stop_loss,
                stop_loss_reasoning=f"入場價 - (2 × ATR)，約${stop_loss:.2f}",
                target_conservative=target_conservative,
                target_conservative_reasoning=f"基於阻力位${target_conservative:.2f}或1.5倍ATR投射",
                target_aggressive=target_aggressive,
                target_aggressive_reasoning=f"基於更高阻力位${target_aggressive:.2f}或3倍ATR投射",
                r_multiple_conservative=r_mult_cons,
                r_multiple_aggressive=r_mult_agg,
                position_size_percent=5.0 if r_mult_cons >= 1.5 else 3.0
            )
        else:
            # Bearish trade
            entry_low = current_price * 0.99
            entry_high = current_price * 1.01
            stop_loss = current_price + (2 * atr)
            
            target_conservative = (
                price_analysis.support_levels[-1] 
                if price_analysis.support_levels 
                else current_price - (1.5 * atr)
            )
            
            target_aggressive = (
                price_analysis.support_levels[0] 
                if price_analysis.support_levels 
                else current_price - (3 * atr)
            )
            
            r_mult_cons = (current_price - target_conservative) / (stop_loss - current_price) if (stop_loss - current_price) != 0 else 0
            r_mult_agg = (current_price - target_aggressive) / (stop_loss - current_price) if (stop_loss - current_price) != 0 else 0
            
            return TradeRecommendation(
                direction=TradeSignal.SELL,
                entry_price_low=entry_low,
                entry_price_high=entry_high,
                stop_loss=stop_loss,
                stop_loss_reasoning=f"入場價 + (2 × ATR)，約${stop_loss:.2f}",
                target_conservative=target_conservative,
                target_conservative_reasoning=f"基於支撐位${target_conservative:.2f}或1.5倍ATR投射",
                target_aggressive=target_aggressive,
                target_aggressive_reasoning=f"基於更低支撐位${target_aggressive:.2f}或3倍ATR投射",
                r_multiple_conservative=r_mult_cons,
                r_multiple_aggressive=r_mult_agg,
                position_size_percent=5.0 if r_mult_cons >= 1.5 else 3.0
            )
    
    def _assess_risks(
        self,
        df: pd.DataFrame,
        indicators: Dict,
        signal_confirmation: SignalConfirmation,
        overall_trend: TrendDirection
    ) -> RiskAssessment:
        """Assess technical risks."""
        risks = []
        current_price = float(df['close'].iloc[-1])
        
        # Check for conflicting signals
        if signal_confirmation.confirmed_signals < 4:
            risks.append("並非所有指標確認，存在背離風險")
        
        # Check RSI extremes
        rsi = float(indicators['rsi'][-1]) if not np.isnan(indicators['rsi'][-1]) else 50
        if rsi > 70:
            risks.append("RSI超買，可能短期回調")
        elif rsi < 30:
            risks.append("RSI超賣，但下跌趨勢可能持續")
        
        # Check Bollinger Band squeeze
        bb_width = ((indicators['bb_upper'][-1] - indicators['bb_lower'][-1]) / indicators['bb_middle'][-1] * 100) if not np.isnan(indicators['bb_middle'][-1]) else 0
        if bb_width < 5:
            risks.append("布林帶收窄，可能爆發性行情")
        
        # Reversal price (based on MA200)
        sma200 = float(indicators['sma_200'][-1]) if not np.isnan(indicators['sma_200'][-1]) else None
        
        failure_scenarios = []
        if overall_trend in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
            failure_scenarios.append("若跌破20日均線，多頭信號失效")
            failure_scenarios.append("若MACD出現死叉，趨勢可能反轉")
        elif overall_trend in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
            failure_scenarios.append("若突破20日均線，空頭信號失效")
            failure_scenarios.append("若MACD出現金叉，趨勢可能反轉")
        
        return RiskAssessment(
            technical_risks=risks,
            reversal_price=sma200,
            failure_scenarios=failure_scenarios,
            weekly_confirmation="待實現週線圖確認"  # TODO: Implement weekly analysis
        )
    
    def _generate_report(
        self,
        symbol: str,
        price_analysis,
        supertrend,
        ma20,
        ma50,
        ma200,
        ma_crosses,
        macd,
        rsi,
        atr,
        bb,
        volume_analysis,
        signal_confirmation,
        overall_trend,
        trade_recommendation,
        risk_assessment,
        language: str
    ) -> str:
        """Generate markdown analysis report."""
        if language == "zh":
            return self._generate_chinese_report(
                symbol, price_analysis, supertrend, ma20, ma50, ma200, ma_crosses,
                macd, rsi, atr, bb, volume_analysis, signal_confirmation, 
                overall_trend, trade_recommendation, risk_assessment
            )
        else:
            return self._generate_english_report(
                symbol, price_analysis, supertrend, ma20, ma50, ma200, ma_crosses,
                macd, rsi, atr, bb, volume_analysis, signal_confirmation,
                overall_trend, trade_recommendation, risk_assessment
            )
    
    def _generate_chinese_report(
        self, symbol, price_analysis, supertrend, ma20, ma50, ma200, ma_crosses,
        macd, rsi, atr, bb, volume_analysis, signal_confirmation, overall_trend,
        trade_recommendation, risk_assessment
    ) -> str:
        """Generate Chinese analysis report."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Map trend to Chinese
        trend_map = {
            TrendDirection.STRONG_BULLISH: "強烈看漲",
            TrendDirection.BULLISH: "看漲",
            TrendDirection.NEUTRAL: "中性",
            TrendDirection.BEARISH: "看跌",
            TrendDirection.STRONG_BEARISH: "強烈看跌"
        }
        
        signal_map = {
            TradeSignal.BUY: "進場(做多)",
            TradeSignal.SELL: "進場(做空)",
            TradeSignal.HOLD: "持有/觀望"
        }
        
        # Safe formatting helper
        sf = self._safe_format
        
        report = f"""# {symbol} 技術分析報告

分析日期: {now}

## 1. 核心價格分析
- 目前價格: {sf(price_analysis.current_price, 'price')}
- 價格變化: {sf(price_analysis.price_change, 'decimal', '0.00')} ({sf(price_analysis.price_change_percent, 'decimal', '0.00')}%)
- 趨勢概述: {price_analysis.trend_description}
- 關鍵阻力位: {', '.join([sf(r, 'price') for r in price_analysis.resistance_levels[:3]]) if price_analysis.resistance_levels else 'N/A'}
- 關鍵支撐位: {', '.join([sf(s, 'price') for s in price_analysis.support_levels[:3]]) if price_analysis.support_levels else 'N/A'}

## 2. 趨勢指標分析

### A. SuperTrend (10,3)
- 目前狀態: {supertrend.status}
- 數值: {sf(supertrend.current_value, 'price')}
- 信號: {'看漲' if supertrend.signal == SignalStatus.BULLISH else '看跌' if supertrend.signal == SignalStatus.BEARISH else '中性'}

### B. 移動平均線系統
- 20日SMA: {sf(ma20.current_value, 'price')} - {ma20.status}
- 50日SMA: {sf(ma50.current_value, 'price')} - {ma50.status}
- 200日SMA: {sf(ma200.current_value, 'price')} - {ma200.status}
- 均線交叉: {', '.join(ma_crosses.values()) if ma_crosses else '無特殊交叉'}

## 3. 確認指標分析

### A. 動能確認
- **MACD (12,26,9)**: {macd.status}
  - MACD線: {sf(macd.details.get('macd'), 'decimal')}
  - 信號線: {sf(macd.details.get('signal_line'), 'decimal')}
  - 柱狀圖: {sf(macd.details.get('histogram'), 'decimal')}

- **RSI (14)**: {rsi.status}
  - 數值: {sf(rsi.current_value, 'decimal')}

### B. 波動性分析
- **ATR (14)**: {atr.status}
  - 數值: {sf(atr.current_value, 'price')}

- **布林帶 (20,2)**: {bb.status}
  - 上軌: {sf(bb.details.get('upper'), 'price')}
  - 中軌: {sf(bb.details.get('middle'), 'price')}
  - 下軌: {sf(bb.details.get('lower'), 'price')}

### C. 成交量分析
- 目前成交量: {sf(volume_analysis.current_volume, 'int')}
- 20日平均成交量: {sf(volume_analysis.avg_volume_20d, 'int')}
- 成交量比率: {sf(volume_analysis.volume_ratio, 'decimal')}x
- 成交量趨勢: {volume_analysis.volume_trend}
- OBV趨勢: {volume_analysis.obv_trend}

## 4. 信號確認系統 (3/4規則)

以下4個信號中，有{signal_confirmation.confirmed_signals}個確認{'✓' if signal_confirmation.confirmation_passed else '✗'}:
- SuperTrend信號: {'看漲✓' if signal_confirmation.supertrend_signal == SignalStatus.BULLISH else '看跌✓' if signal_confirmation.supertrend_signal == SignalStatus.BEARISH else '中性'}
- 價格vs 20日SMA: {'上方✓' if signal_confirmation.price_vs_ma20 == SignalStatus.BULLISH else '下方✓' if signal_confirmation.price_vs_ma20 == SignalStatus.BEARISH else '持平'}
- MACD信號線交叉: {'買入✓' if signal_confirmation.macd_signal == SignalStatus.BULLISH else '賣出✓' if signal_confirmation.macd_signal == SignalStatus.BEARISH else '中性'}
- RSI相對位置: {'>50看漲✓' if signal_confirmation.rsi_signal == SignalStatus.BULLISH else '<50看跌✓' if signal_confirmation.rsi_signal == SignalStatus.BEARISH else '中性'}

**確認結果**: {'通過 - 信號有效' if signal_confirmation.confirmation_passed else '未通過 - 信號衝突，建議觀望'}

## 5. 交易建議

- **綜合趨勢判斷**: {trend_map[overall_trend]}
- **交易信號**: {signal_map[trade_recommendation.direction]}
"""
        
        if trade_recommendation.direction == TradeSignal.BUY:
            report += f"""
### 做多(Long)交易參數:
- **進場區間**: {sf(trade_recommendation.entry_price_low, 'price')} - {sf(trade_recommendation.entry_price_high, 'price')}
- **止損位**: {sf(trade_recommendation.stop_loss, 'price')}
  - 計算方式: {trade_recommendation.stop_loss_reasoning or 'N/A'}

- **獲利目標**:
  - 第一目標(保守): {sf(trade_recommendation.target_conservative, 'price')}
    - 依據: {trade_recommendation.target_conservative_reasoning or 'N/A'}
    - R倍數: {sf(trade_recommendation.r_multiple_conservative, 'decimal')}R
  
  - 第二目標(進取): {sf(trade_recommendation.target_aggressive, 'price')}
    - 依據: {trade_recommendation.target_aggressive_reasoning or 'N/A'}
    - R倍數: {sf(trade_recommendation.r_multiple_aggressive, 'decimal')}R

- **建議倉位大小**: {sf(trade_recommendation.position_size_percent, 'decimal')}% (基於固定風險管理)
"""
        elif trade_recommendation.direction == TradeSignal.SELL:
            report += f"""
### 做空(Short)交易參數:
- **進場區間**: {sf(trade_recommendation.entry_price_low, 'price')} - {sf(trade_recommendation.entry_price_high, 'price')}
- **止損位**: {sf(trade_recommendation.stop_loss, 'price')}
  - 計算方式: {trade_recommendation.stop_loss_reasoning or 'N/A'}

- **獲利目標**:
  - 第一目標(保守): {sf(trade_recommendation.target_conservative, 'price')}
    - 依據: {trade_recommendation.target_conservative_reasoning or 'N/A'}
    - R倍數: {sf(trade_recommendation.r_multiple_conservative, 'decimal')}R
  
  - 第二目標(進取): {sf(trade_recommendation.target_aggressive, 'price')}
    - 依據: {trade_recommendation.target_aggressive_reasoning or 'N/A'}
    - R倍數: {sf(trade_recommendation.r_multiple_aggressive, 'decimal')}R

- **建議倉位大小**: {sf(trade_recommendation.position_size_percent, 'decimal')}% (基於固定風險管理)
"""
        else:
            report += """
### 觀望建議:
目前指標信號不一致，建議持有現有倉位或觀望等待更明確的信號。
"""
        
        report += f"""
## 6. 風險評估

### 主要技術風險:
{chr(10).join(['- ' + risk for risk in risk_assessment.technical_risks]) if risk_assessment.technical_risks else '- 無特殊技術風險'}

### 關鍵反轉價格:
- 200日均線: {sf(risk_assessment.reversal_price, 'price')} (跌破可能轉為長期空頭)

### 潛在信號失敗情境:
{chr(10).join(['- ' + scenario for scenario in risk_assessment.failure_scenarios]) if risk_assessment.failure_scenarios else '- 無'}

### 週線圖確認:
- {risk_assessment.weekly_confirmation}

---

**免責聲明**: 本分析僅供參考，不構成投資建議。交易有風險，投資需謹慎。
"""
        
        return report
    
    def _generate_english_report(
        self, symbol, price_analysis, supertrend, ma20, ma50, ma200, ma_crosses,
        macd, rsi, atr, bb, volume_analysis, signal_confirmation, overall_trend,
        trade_recommendation, risk_assessment
    ) -> str:
        """Generate English analysis report."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Map trend to English
        trend_map = {
            TrendDirection.STRONG_BULLISH: "Strong Bullish",
            TrendDirection.BULLISH: "Bullish",
            TrendDirection.NEUTRAL: "Neutral",
            TrendDirection.BEARISH: "Bearish",
            TrendDirection.STRONG_BEARISH: "Strong Bearish"
        }
        
        signal_map = {
            TradeSignal.BUY: "BUY (Long)",
            TradeSignal.SELL: "SELL (Short)",
            TradeSignal.HOLD: "HOLD / Watch"
        }
        
        # Safe formatting helper
        sf = self._safe_format
        
        report = f"""# {symbol} Technical Analysis Report

Analysis Date: {now}

## 1. Core Price Analysis
- Current Price: {sf(price_analysis.current_price, 'price')}
- Price Change: {sf(price_analysis.price_change, 'decimal', '0.00')} ({sf(price_analysis.price_change_percent, 'decimal', '0.00')}%)
- Trend Overview: {price_analysis.trend_description}
- Key Resistance Levels: {', '.join([sf(r, 'price') for r in price_analysis.resistance_levels[:3]]) if price_analysis.resistance_levels else 'N/A'}
- Key Support Levels: {', '.join([sf(s, 'price') for s in price_analysis.support_levels[:3]]) if price_analysis.support_levels else 'N/A'}

## 2. Trend Indicator Analysis

### A. SuperTrend (10,3)
- Current Status: {supertrend.status}
- Value: {sf(supertrend.current_value, 'price')}
- Signal: {'Bullish' if supertrend.signal == SignalStatus.BULLISH else 'Bearish' if supertrend.signal == SignalStatus.BEARISH else 'Neutral'}

### B. Moving Average System
- 20-day SMA: {sf(ma20.current_value, 'price')} - {ma20.status}
- 50-day SMA: {sf(ma50.current_value, 'price')} - {ma50.status}
- 200-day SMA: {sf(ma200.current_value, 'price')} - {ma200.status}
- MA Crosses: {', '.join(ma_crosses.values()) if ma_crosses else 'No significant crosses'}

## 3. Confirmation Indicator Analysis

### A. Momentum Confirmation
- **MACD (12,26,9)**: {macd.status}
  - MACD Line: {sf(macd.details.get('macd'), 'decimal')}
  - Signal Line: {sf(macd.details.get('signal_line'), 'decimal')}
  - Histogram: {sf(macd.details.get('histogram'), 'decimal')}

- **RSI (14)**: {rsi.status}
  - Value: {sf(rsi.current_value, 'decimal')}

### B. Volatility Analysis
- **ATR (14)**: {atr.status}
  - Value: {sf(atr.current_value, 'price')}

- **Bollinger Bands (20,2)**: {bb.status}
  - Upper Band: {sf(bb.details.get('upper'), 'price')}
  - Middle Band: {sf(bb.details.get('middle'), 'price')}
  - Lower Band: {sf(bb.details.get('lower'), 'price')}

### C. Volume Analysis
- Current Volume: {sf(volume_analysis.current_volume, 'int')}
- 20-day Average Volume: {sf(volume_analysis.avg_volume_20d, 'int')}
- Volume Ratio: {sf(volume_analysis.volume_ratio, 'decimal')}x
- Volume Trend: {volume_analysis.volume_trend}
- OBV Trend: {volume_analysis.obv_trend}

## 4. Signal Confirmation System (3/4 Rule)

{signal_confirmation.confirmed_signals} out of 4 signals confirmed {'✓' if signal_confirmation.confirmation_passed else '✗'}:
- SuperTrend Signal: {'Bullish ✓' if signal_confirmation.supertrend_signal == SignalStatus.BULLISH else 'Bearish ✓' if signal_confirmation.supertrend_signal == SignalStatus.BEARISH else 'Neutral'}
- Price vs 20-day SMA: {'Above ✓' if signal_confirmation.price_vs_ma20 == SignalStatus.BULLISH else 'Below ✓' if signal_confirmation.price_vs_ma20 == SignalStatus.BEARISH else 'At level'}
- MACD Signal Line Cross: {'Buy ✓' if signal_confirmation.macd_signal == SignalStatus.BULLISH else 'Sell ✓' if signal_confirmation.macd_signal == SignalStatus.BEARISH else 'Neutral'}
- RSI Position: {'>50 Bullish ✓' if signal_confirmation.rsi_signal == SignalStatus.BULLISH else '<50 Bearish ✓' if signal_confirmation.rsi_signal == SignalStatus.BEARISH else 'Neutral'}

**Confirmation Result**: {'PASSED - Signal Valid' if signal_confirmation.confirmation_passed else 'NOT PASSED - Conflicting Signals, Recommend Watching'}

## 5. Trade Recommendation

- **Overall Trend**: {trend_map[overall_trend]}
- **Trade Signal**: {signal_map[trade_recommendation.direction]}
"""
        
        if trade_recommendation.direction == TradeSignal.BUY:
            report += f"""
### Long Position Parameters:
- **Entry Zone**: {sf(trade_recommendation.entry_price_low, 'price')} - {sf(trade_recommendation.entry_price_high, 'price')}
- **Stop Loss**: {sf(trade_recommendation.stop_loss, 'price')}
  - Calculation: {trade_recommendation.stop_loss_reasoning or 'N/A'}

- **Profit Targets**:
  - Conservative Target: {sf(trade_recommendation.target_conservative, 'price')}
    - Basis: {trade_recommendation.target_conservative_reasoning or 'N/A'}
    - R-Multiple: {sf(trade_recommendation.r_multiple_conservative, 'decimal')}R
  
  - Aggressive Target: {sf(trade_recommendation.target_aggressive, 'price')}
    - Basis: {trade_recommendation.target_aggressive_reasoning or 'N/A'}
    - R-Multiple: {sf(trade_recommendation.r_multiple_aggressive, 'decimal')}R

- **Recommended Position Size**: {sf(trade_recommendation.position_size_percent, 'decimal')}% (Based on fixed risk management)
"""
        elif trade_recommendation.direction == TradeSignal.SELL:
            report += f"""
### Short Position Parameters:
- **Entry Zone**: {sf(trade_recommendation.entry_price_low, 'price')} - {sf(trade_recommendation.entry_price_high, 'price')}
- **Stop Loss**: {sf(trade_recommendation.stop_loss, 'price')}
  - Calculation: {trade_recommendation.stop_loss_reasoning or 'N/A'}

- **Profit Targets**:
  - Conservative Target: {sf(trade_recommendation.target_conservative, 'price')}
    - Basis: {trade_recommendation.target_conservative_reasoning or 'N/A'}
    - R-Multiple: {sf(trade_recommendation.r_multiple_conservative, 'decimal')}R
  
  - Aggressive Target: {sf(trade_recommendation.target_aggressive, 'price')}
    - Basis: {trade_recommendation.target_aggressive_reasoning or 'N/A'}
    - R-Multiple: {sf(trade_recommendation.r_multiple_aggressive, 'decimal')}R

- **Recommended Position Size**: {sf(trade_recommendation.position_size_percent, 'decimal')}% (Based on fixed risk management)
"""
        else:
            report += """
### Watch Recommendation:
Current indicator signals are inconsistent. Recommend holding existing positions or watching for clearer signals.
"""
        
        report += f"""
## 6. Risk Assessment

### Main Technical Risks:
{chr(10).join(['- ' + risk for risk in risk_assessment.technical_risks]) if risk_assessment.technical_risks else '- No significant technical risks'}

### Key Reversal Price:
- 200-day MA: {sf(risk_assessment.reversal_price, 'price')} (Break below may indicate long-term bearish)

### Potential Signal Failure Scenarios:
{chr(10).join(['- ' + scenario for scenario in risk_assessment.failure_scenarios]) if risk_assessment.failure_scenarios else '- None'}

### Weekly Chart Confirmation:
- {risk_assessment.weekly_confirmation}

---

**Disclaimer**: This analysis is for reference only and does not constitute investment advice. Trading involves risk.
"""
        
        return report

