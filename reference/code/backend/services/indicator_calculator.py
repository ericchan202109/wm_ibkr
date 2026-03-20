"""
Indicator Calculator Service - Real TA-Lib indicator calculations.

Calculates technical indicators for trading strategies using TA-Lib.
"""
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
import ta

logger = logging.getLogger(__name__)


class IndicatorCalculator:
    """
    Service for calculating technical indicators using TA-Lib.
    
    Supports:
    - Moving Averages (SMA, EMA, WMA)
    - Bollinger Bands
    - SuperTrend
    - MACD
    - RSI
    - ATR
    - Stochastic
    - And more...
    """
    
    def __init__(self):
        """Initialize the indicator calculator."""
        pass
    
    async def calculate_indicators(
        self,
        market_data: Dict[str, Any],
        indicator_configs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate multiple indicators from market data.
        
        Args:
            market_data: Dictionary with OHLCV data by timeframe
            indicator_configs: List of indicator configurations
            
        Returns:
            Dictionary with calculated indicators by timeframe
        """
        try:
            results = {}
            
            for timeframe, data in market_data.items():
                if not data or len(data) == 0:
                    logger.warning(f"No data for timeframe {timeframe}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(data)
                
                # Ensure required columns
                if not all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']):
                    logger.error(f"Missing required columns in {timeframe} data")
                    continue
                
                # Calculate indicators
                timeframe_results = {}
                for config in indicator_configs:
                    indicator_name = config.get('name')
                    indicator_type = config.get('type', '').upper()
                    params = config.get('parameters', {})
                    
                    try:
                        indicator_data = await self._calculate_single_indicator(
                            df, indicator_type, params
                        )
                        if indicator_data:
                            timeframe_results[indicator_name] = indicator_data
                    except Exception as e:
                        logger.error(f"Failed to calculate {indicator_name}: {str(e)}")
                
                results[timeframe] = timeframe_results
            
            logger.info(f"Calculated indicators for {len(results)} timeframes")
            return results
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {str(e)}", exc_info=True)
            return {}
    
    async def _calculate_single_indicator(
        self,
        df: pd.DataFrame,
        indicator_type: str,
        params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate a single indicator.
        
        Args:
            df: DataFrame with OHLCV data
            indicator_type: Type of indicator (SMA, EMA, RSI, etc.)
            params: Indicator parameters
            
        Returns:
            Dictionary with indicator values and metadata
        """
        close = df['close'].astype(float)
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        volume = df['volume'].astype(float)
        
        result = {
            'type': indicator_type,
            'params': params
        }
        
        # Moving Averages
        if indicator_type in ['SMA', 'MA']:
            period = params.get('period', 20)
            values = ta.trend.sma_indicator(close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None

        elif indicator_type == 'EMA':
            period = params.get('period', 20)
            values = ta.trend.ema_indicator(close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None

        elif indicator_type == 'WMA':
            period = params.get('period', 20)
            values = ta.trend.wma_indicator(close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None
        
        # Bollinger Bands
        elif indicator_type == 'BB':
            period = params.get('period', 20)
            std_dev = params.get('std_dev', 2)
            upper = ta.volatility.bollinger_hband(close, window=period, window_dev=std_dev)
            middle = ta.volatility.bollinger_mavg(close, window=period)
            lower = ta.volatility.bollinger_lband(close, window=period, window_dev=std_dev)
            result['upper'] = upper.tolist()
            result['middle'] = middle.tolist()
            result['lower'] = lower.tolist()
            result['current_upper'] = float(upper.iloc[-1]) if not pd.isna(upper.iloc[-1]) else None
            result['current_middle'] = float(middle.iloc[-1]) if not pd.isna(middle.iloc[-1]) else None
            result['current_lower'] = float(lower.iloc[-1]) if not pd.isna(lower.iloc[-1]) else None
        
        # SuperTrend
        elif indicator_type == 'SUPERTREND':
            period = params.get('period', 10)
            multiplier = params.get('multiplier', 3)

            # Calculate ATR
            atr = ta.volatility.average_true_range(high, low, close, window=period)
            hl_avg = (high + low) / 2
            
            # Calculate bands
            upper_band = hl_avg + (multiplier * atr)
            lower_band = hl_avg - (multiplier * atr)
            
            # Calculate SuperTrend
            supertrend = np.where(close > upper_band, lower_band, upper_band)
            direction = np.where(close > supertrend, 1, -1)  # 1 = bullish, -1 = bearish
            
            result['values'] = supertrend.tolist()
            result['direction'] = direction.tolist()
            result['current'] = float(supertrend[-1]) if not np.isnan(supertrend[-1]) else None
            result['current_direction'] = int(direction[-1])
            result['is_bullish'] = direction[-1] == 1
        
        # MACD
        elif indicator_type == 'MACD':
            fast = params.get('fast_period', 12)
            slow = params.get('slow_period', 26)
            signal = params.get('signal_period', 9)

            macd = ta.trend.macd(close, window_fast=fast, window_slow=slow)
            signal_line = ta.trend.macd_signal(close, window_fast=fast, window_slow=slow, window_sign=signal)
            histogram = ta.trend.macd_diff(close, window_fast=fast, window_slow=slow, window_sign=signal)
            result['macd'] = macd.tolist()
            result['signal'] = signal_line.tolist()
            result['histogram'] = histogram.tolist()
            result['current_macd'] = float(macd.iloc[-1]) if not pd.isna(macd.iloc[-1]) else None
            result['current_signal'] = float(signal_line.iloc[-1]) if not pd.isna(signal_line.iloc[-1]) else None
            result['current_histogram'] = float(histogram.iloc[-1]) if not pd.isna(histogram.iloc[-1]) else None
            result['is_bullish'] = histogram.iloc[-1] > 0 if not pd.isna(histogram.iloc[-1]) else None
        
        # RSI
        elif indicator_type == 'RSI':
            period = params.get('period', 14)
            values = ta.momentum.rsi(close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None
            result['is_overbought'] = values.iloc[-1] > 70 if not pd.isna(values.iloc[-1]) else None
            result['is_oversold'] = values.iloc[-1] < 30 if not pd.isna(values.iloc[-1]) else None

        # ATR
        elif indicator_type == 'ATR':
            period = params.get('period', 14)
            values = ta.volatility.average_true_range(high, low, close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None
        
        # Stochastic
        elif indicator_type == 'STOCH':
            k_period = params.get('k_period', 14)
            d_period = params.get('d_period', 3)
            slowk = ta.momentum.stoch(high, low, close, window=k_period, smooth_window=d_period)
            slowd = ta.momentum.stoch_signal(high, low, close, window=k_period, smooth_window=d_period)
            result['k'] = slowk.tolist()
            result['d'] = slowd.tolist()
            result['current_k'] = float(slowk.iloc[-1]) if not pd.isna(slowk.iloc[-1]) else None
            result['current_d'] = float(slowd.iloc[-1]) if not pd.isna(slowd.iloc[-1]) else None
            result['is_overbought'] = slowk.iloc[-1] > 80 if not pd.isna(slowk.iloc[-1]) else None
            result['is_oversold'] = slowk.iloc[-1] < 20 if not pd.isna(slowk.iloc[-1]) else None
        
        # ADX (Trend Strength)
        elif indicator_type == 'ADX':
            period = params.get('period', 14)
            values = ta.trend.adx(high, low, close, window=period)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None
            result['strong_trend'] = values.iloc[-1] > 25 if not pd.isna(values.iloc[-1]) else None

        # OBV (Volume)
        elif indicator_type == 'OBV':
            values = ta.volume.on_balance_volume(close, volume)
            result['values'] = values.tolist()
            result['current'] = float(values.iloc[-1]) if not pd.isna(values.iloc[-1]) else None
        
        else:
            logger.warning(f"Unknown indicator type: {indicator_type}")
            return None
        
        return result
    
    def calculate_signal_confirmation(
        self,
        indicators: Dict[str, Any],
        current_price: float
    ) -> Dict[str, Any]:
        """
        Calculate 3/4 rule signal confirmation.
        
        Checks if at least 3 out of 4 indicators confirm the signal.
        
        Args:
            indicators: Calculated indicators
            current_price: Current price
            
        Returns:
            Dictionary with confirmation results
        """
        confirmations = {
            'supertrend': None,
            'price_vs_ma20': None,
            'macd': None,
            'rsi': None,
            'confirmed_count': 0,
            'passed': False
        }
        
        try:
            # Check SuperTrend
            for name, data in indicators.items():
                if data.get('type') == 'SUPERTREND':
                    confirmations['supertrend'] = data.get('is_bullish', False)
            
            # Check Price vs 20 SMA
            for name, data in indicators.items():
                if data.get('type') in ['SMA', 'MA'] and data.get('params', {}).get('period') == 20:
                    ma20 = data.get('current')
                    if ma20:
                        confirmations['price_vs_ma20'] = current_price > ma20
            
            # Check MACD
            for name, data in indicators.items():
                if data.get('type') == 'MACD':
                    confirmations['macd'] = data.get('is_bullish', False)
            
            # Check RSI
            for name, data in indicators.items():
                if data.get('type') == 'RSI':
                    rsi = data.get('current')
                    if rsi:
                        confirmations['rsi'] = rsi > 50
            
            # Count confirmations
            confirmations['confirmed_count'] = sum(
                1 for v in [
                    confirmations['supertrend'],
                    confirmations['price_vs_ma20'],
                    confirmations['macd'],
                    confirmations['rsi']
                ] if v is True
            )
            
            # 3/4 rule: need at least 3 confirmations
            confirmations['passed'] = confirmations['confirmed_count'] >= 3
            
            logger.info(
                f"Signal confirmation: {confirmations['confirmed_count']}/4 "
                f"({'PASSED' if confirmations['passed'] else 'FAILED'})"
            )
            
        except Exception as e:
            logger.error(f"Error calculating signal confirmation: {str(e)}")
        
        return confirmations


def get_indicator_calculator() -> IndicatorCalculator:
    """Factory function for IndicatorCalculator."""
    return IndicatorCalculator()

