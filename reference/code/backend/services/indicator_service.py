"""Indicator calculation service."""
import numpy as np
import pandas as pd
from typing import Dict, Any, List
from backend.services.ibkr_service import IBKRService
import logging

logger = logging.getLogger(__name__)


class IndicatorService:
    """Service for calculating technical indicators."""
    
    def __init__(self):
        self.ibkr = IBKRService()
    
    async def calculate(
        self,
        indicator_type: str,
        parameters: Dict[str, Any],
        symbol: str,
        timeframe: str = "1D",
        data_points: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Calculate indicator values for given symbol and timeframe.
        
        Args:
            indicator_type: Type of indicator (MA, RSI, MACD, etc.)
            parameters: Indicator parameters
            symbol: Stock symbol
            timeframe: Timeframe (1D, 1W, 1M)
            data_points: Number of historical data points
            
        Returns:
            List of {time: str, value: float/dict} dictionaries
        """
        # Fetch historical data from IBKR
        # For now, we'll use mock data - in production, fetch from IBKR
        df = await self._fetch_market_data(symbol, timeframe, data_points)
        
        if df is None or len(df) == 0:
            raise ValueError("No market data available")
        
        # Calculate based on indicator type
        if indicator_type in ['MA', 'SMA', 'EMA', 'WMA']:
            return self._calculate_moving_average(df, parameters)
        elif indicator_type == 'RSI':
            return self._calculate_rsi(df, parameters)
        elif indicator_type == 'MACD':
            return self._calculate_macd(df, parameters)
        elif indicator_type == 'BB':
            return self._calculate_bollinger_bands(df, parameters)
        elif indicator_type == 'SuperTrend':
            return self._calculate_supertrend(df, parameters)
        elif indicator_type == 'ATR':
            return self._calculate_atr(df, parameters)
        else:
            raise ValueError(f"Unsupported indicator type: {indicator_type}")
    
    async def _fetch_market_data(self, symbol: str, timeframe: str, data_points: int) -> pd.DataFrame:
        """Fetch historical market data."""
        try:
            # TODO: Implement actual IBKR data fetching
            # For now, generate mock data
            dates = pd.date_range(end=pd.Timestamp.now(), periods=data_points, freq='D')
            
            # Generate realistic OHLCV data
            base_price = 100
            data = {
                'time': dates,
                'open': np.random.uniform(base_price - 5, base_price + 5, data_points),
                'high': np.random.uniform(base_price, base_price + 10, data_points),
                'low': np.random.uniform(base_price - 10, base_price, data_points),
                'close': np.random.uniform(base_price - 5, base_price + 5, data_points),
                'volume': np.random.randint(1000000, 10000000, data_points)
            }
            
            df = pd.DataFrame(data)
            df.set_index('time', inplace=True)
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch market data: {str(e)}")
            return None
    
    def _calculate_moving_average(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate Moving Average."""
        period = params.get('period', 20)
        ma_type = params.get('ma_type', 'SMA')
        source = params.get('source', 'close')
        
        if ma_type == 'SMA':
            values = df[source].rolling(window=period).mean()
        elif ma_type == 'EMA':
            values = df[source].ewm(span=period, adjust=False).mean()
        elif ma_type == 'WMA':
            weights = np.arange(1, period + 1)
            values = df[source].rolling(window=period).apply(
                lambda x: np.dot(x, weights) / weights.sum(), raw=True
            )
        else:
            values = df[source].rolling(window=period).mean()
        
        return [
            {"time": idx.isoformat(), "value": float(val)}
            for idx, val in values.items()
            if not pd.isna(val)
        ]
    
    def _calculate_rsi(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate Relative Strength Index."""
        period = params.get('period', 14)
        
        # Calculate price changes
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return [
            {"time": idx.isoformat(), "value": float(val)}
            for idx, val in rsi.items()
            if not pd.isna(val)
        ]
    
    def _calculate_macd(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate MACD."""
        fast_period = params.get('fast_period', 12)
        slow_period = params.get('slow_period', 26)
        signal_period = params.get('signal_period', 9)
        
        ema_fast = df['close'].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow_period, adjust=False).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        histogram = macd_line - signal_line
        
        result = []
        for idx in macd_line.index:
            if not pd.isna(macd_line[idx]):
                result.append({
                    "time": idx.isoformat(),
                    "value": {
                        "macd": float(macd_line[idx]),
                        "signal": float(signal_line[idx]),
                        "histogram": float(histogram[idx])
                    }
                })
        
        return result
    
    def _calculate_bollinger_bands(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate Bollinger Bands."""
        period = params.get('period', 20)
        std_dev = params.get('std_dev', 2)
        source = params.get('source', 'close')
        
        sma = df[source].rolling(window=period).mean()
        std = df[source].rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        result = []
        for idx in sma.index:
            if not pd.isna(sma[idx]):
                result.append({
                    "time": idx.isoformat(),
                    "value": {
                        "upper": float(upper_band[idx]),
                        "middle": float(sma[idx]),
                        "lower": float(lower_band[idx])
                    }
                })
        
        return result
    
    def _calculate_atr(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate Average True Range."""
        period = params.get('period', 14)
        
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        
        return [
            {"time": idx.isoformat(), "value": float(val)}
            for idx, val in atr.items()
            if not pd.isna(val)
        ]
    
    def _calculate_supertrend(self, df: pd.DataFrame, params: Dict) -> List[Dict]:
        """Calculate SuperTrend indicator."""
        period = params.get('period', 10)
        multiplier = params.get('multiplier', 3)
        
        # Calculate ATR
        atr = self._calculate_atr(df, {'period': period})
        atr_series = pd.Series([v['value'] for v in atr], index=df.index[-len(atr):])
        
        # Calculate basic upper and lower bands
        hl2 = (df['high'] + df['low']) / 2
        basic_upper = hl2 + (multiplier * atr_series)
        basic_lower = hl2 - (multiplier * atr_series)
        
        # Initialize supertrend
        supertrend = pd.Series(index=df.index, dtype=float)
        direction = pd.Series(index=df.index, dtype=int)
        
        for i in range(len(df)):
            if i == 0:
                supertrend.iloc[i] = basic_upper.iloc[i] if pd.notna(basic_upper.iloc[i]) else 0
                direction.iloc[i] = -1
            else:
                if pd.notna(basic_upper.iloc[i]) and pd.notna(basic_lower.iloc[i]):
                    if direction.iloc[i-1] == -1:
                        supertrend.iloc[i] = basic_lower.iloc[i] if df['close'].iloc[i] > supertrend.iloc[i-1] else basic_upper.iloc[i]
                        direction.iloc[i] = 1 if df['close'].iloc[i] > supertrend.iloc[i-1] else -1
                    else:
                        supertrend.iloc[i] = basic_upper.iloc[i] if df['close'].iloc[i] < supertrend.iloc[i-1] else basic_lower.iloc[i]
                        direction.iloc[i] = -1 if df['close'].iloc[i] < supertrend.iloc[i-1] else 1
        
        return [
            {"time": idx.isoformat(), "value": {"supertrend": float(val), "direction": int(direction[idx])}}
            for idx, val in supertrend.items()
            if not pd.isna(val)
        ]

