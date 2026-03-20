"""Tests for IndicatorCalculator."""
import pytest
from unittest.mock import MagicMock
import pandas as pd
import numpy as np
from backend.services.indicator_calculator import IndicatorCalculator


class TestIndicatorCalculator:
    """Test suite for IndicatorCalculator."""
    
    @pytest.fixture
    def calculator(self):
        """Create an IndicatorCalculator instance."""
        return IndicatorCalculator()
    
    @pytest.fixture
    def sample_market_data(self):
        """Create sample OHLCV market data."""
        # Generate 100 days of sample data
        dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
        data = {
            'timestamp': dates,
            'open': np.random.uniform(100, 110, 100),
            'high': np.random.uniform(110, 120, 100),
            'low': np.random.uniform(90, 100, 100),
            'close': np.random.uniform(100, 110, 100),
            'volume': np.random.uniform(1000000, 2000000, 100)
        }
        df = pd.DataFrame(data)
        
        # Ensure high is highest and low is lowest
        df['high'] = df[['open', 'high', 'close']].max(axis=1)
        df['low'] = df[['open', 'low', 'close']].min(axis=1)
        
        return {
            '1d': df.to_dict('records')
        }
    
    @pytest.mark.asyncio
    async def test_calculate_sma(self, calculator, sample_market_data):
        """Test SMA (Simple Moving Average) calculation."""
        configs = [
            {'name': 'SMA20', 'type': 'SMA', 'parameters': {'period': 20}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'SMA20' in result['1d']
        assert 'values' in result['1d']['SMA20']
        assert 'current' in result['1d']['SMA20']
        assert result['1d']['SMA20']['current'] is not None
        assert result['1d']['SMA20']['type'] == 'SMA'
    
    @pytest.mark.asyncio
    async def test_calculate_ema(self, calculator, sample_market_data):
        """Test EMA (Exponential Moving Average) calculation."""
        configs = [
            {'name': 'EMA20', 'type': 'EMA', 'parameters': {'period': 20}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'EMA20' in result['1d']
        assert 'values' in result['1d']['EMA20']
        assert 'current' in result['1d']['EMA20']
        assert result['1d']['EMA20']['current'] is not None
    
    @pytest.mark.asyncio
    async def test_calculate_rsi(self, calculator, sample_market_data):
        """Test RSI (Relative Strength Index) calculation."""
        configs = [
            {'name': 'RSI14', 'type': 'RSI', 'parameters': {'period': 14}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'RSI14' in result['1d']
        rsi_data = result['1d']['RSI14']
        assert 'values' in rsi_data
        assert 'current' in rsi_data
        assert rsi_data['current'] is not None
        assert 0 <= rsi_data['current'] <= 100  # RSI is bounded 0-100
        assert 'is_overbought' in rsi_data
        assert 'is_oversold' in rsi_data
    
    @pytest.mark.asyncio
    async def test_calculate_macd(self, calculator, sample_market_data):
        """Test MACD calculation."""
        configs = [
            {
                'name': 'MACD',
                'type': 'MACD',
                'parameters': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9}
            }
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'MACD' in result['1d']
        macd_data = result['1d']['MACD']
        assert 'macd' in macd_data
        assert 'signal' in macd_data
        assert 'histogram' in macd_data
        assert 'current_macd' in macd_data
        assert 'current_signal' in macd_data
        assert 'is_bullish' in macd_data
    
    @pytest.mark.asyncio
    async def test_calculate_bollinger_bands(self, calculator, sample_market_data):
        """Test Bollinger Bands calculation."""
        configs = [
            {'name': 'BB', 'type': 'BB', 'parameters': {'period': 20, 'std_dev': 2}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'BB' in result['1d']
        bb_data = result['1d']['BB']
        assert 'upper' in bb_data
        assert 'middle' in bb_data
        assert 'lower' in bb_data
        assert 'current_upper' in bb_data
        assert 'current_middle' in bb_data
        assert 'current_lower' in bb_data
        
        # Upper should be > middle > lower
        assert bb_data['current_upper'] > bb_data['current_middle']
        assert bb_data['current_middle'] > bb_data['current_lower']
    
    @pytest.mark.asyncio
    async def test_calculate_supertrend(self, calculator, sample_market_data):
        """Test SuperTrend calculation."""
        configs = [
            {'name': 'SuperTrend', 'type': 'SUPERTREND', 'parameters': {'period': 10, 'multiplier': 3}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'SuperTrend' in result['1d']
        st_data = result['1d']['SuperTrend']
        assert 'values' in st_data
        assert 'direction' in st_data
        assert 'current' in st_data
        assert 'current_direction' in st_data
        assert 'is_bullish' in st_data
        assert st_data['current_direction'] in [1, -1]  # 1 = bullish, -1 = bearish
    
    @pytest.mark.asyncio
    async def test_calculate_atr(self, calculator, sample_market_data):
        """Test ATR (Average True Range) calculation."""
        configs = [
            {'name': 'ATR14', 'type': 'ATR', 'parameters': {'period': 14}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'ATR14' in result['1d']
        atr_data = result['1d']['ATR14']
        assert 'values' in atr_data
        assert 'current' in atr_data
        assert atr_data['current'] > 0  # ATR should be positive
    
    @pytest.mark.asyncio
    async def test_calculate_stochastic(self, calculator, sample_market_data):
        """Test Stochastic Oscillator calculation."""
        configs = [
            {'name': 'STOCH', 'type': 'STOCH', 'parameters': {'k_period': 14, 'd_period': 3}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'STOCH' in result['1d']
        stoch_data = result['1d']['STOCH']
        assert 'k' in stoch_data
        assert 'd' in stoch_data
        assert 'current_k' in stoch_data
        assert 'current_d' in stoch_data
        assert 'is_overbought' in stoch_data
        assert 'is_oversold' in stoch_data
        
        # Stochastic is bounded 0-100
        assert 0 <= stoch_data['current_k'] <= 100
        assert 0 <= stoch_data['current_d'] <= 100
    
    @pytest.mark.asyncio
    async def test_calculate_adx(self, calculator, sample_market_data):
        """Test ADX (Average Directional Index) calculation."""
        configs = [
            {'name': 'ADX14', 'type': 'ADX', 'parameters': {'period': 14}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'ADX14' in result['1d']
        adx_data = result['1d']['ADX14']
        assert 'values' in adx_data
        assert 'current' in adx_data
        assert 'strong_trend' in adx_data
        assert 0 <= adx_data['current'] <= 100  # ADX is 0-100
    
    @pytest.mark.asyncio
    async def test_calculate_multiple_indicators(self, calculator, sample_market_data):
        """Test calculating multiple indicators at once."""
        configs = [
            {'name': 'SMA20', 'type': 'SMA', 'parameters': {'period': 20}},
            {'name': 'RSI14', 'type': 'RSI', 'parameters': {'period': 14}},
            {'name': 'MACD', 'type': 'MACD', 'parameters': {}},
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        assert '1d' in result
        assert 'SMA20' in result['1d']
        assert 'RSI14' in result['1d']
        assert 'MACD' in result['1d']
    
    @pytest.mark.asyncio
    async def test_empty_market_data(self, calculator):
        """Test handling of empty market data."""
        empty_data = {'1d': []}
        configs = [
            {'name': 'SMA20', 'type': 'SMA', 'parameters': {'period': 20}}
        ]
        
        result = await calculator.calculate_indicators(empty_data, configs)
        
        # Should handle gracefully
        assert isinstance(result, dict)
    
    @pytest.mark.asyncio
    async def test_invalid_indicator_type(self, calculator, sample_market_data):
        """Test handling of invalid indicator type."""
        configs = [
            {'name': 'Invalid', 'type': 'INVALID_TYPE', 'parameters': {}}
        ]
        
        result = await calculator.calculate_indicators(sample_market_data, configs)
        
        # Should handle gracefully, skip invalid indicators
        assert '1d' in result
        # Invalid indicator should not be in results
        assert 'Invalid' not in result.get('1d', {})
    
    def test_signal_confirmation(self, calculator):
        """Test 3/4 signal confirmation rule."""
        # Create mock indicator data
        indicators = {
            'SuperTrend': {'type': 'SUPERTREND', 'is_bullish': True},
            'MACD': {'type': 'MACD', 'is_bullish': True},
            'RSI': {'type': 'RSI', 'current': 60},
            'SMA20': {'type': 'SMA', 'current': 100, 'params': {'period': 20}}
        }
        
        current_price = 105
        
        confirmation = calculator.calculate_signal_confirmation(indicators, current_price)
        
        assert 'supertrend' in confirmation
        assert 'price_vs_ma20' in confirmation
        assert 'macd' in confirmation
        assert 'rsi' in confirmation
        assert 'confirmed_count' in confirmation
        assert 'passed' in confirmation
        
        # With bullish signals, should pass
        assert confirmation['supertrend'] == True
        assert confirmation['macd'] == True
        assert confirmation['price_vs_ma20'] == True  # 105 > 100
        assert confirmation['rsi'] == True  # 60 > 50
        assert confirmation['confirmed_count'] == 4
        assert confirmation['passed'] == True  # 4/4 passed
    
    def test_signal_confirmation_failure(self, calculator):
        """Test 3/4 rule when not enough signals confirm."""
        # Create mock indicator data with mixed signals
        indicators = {
            'SuperTrend': {'type': 'SUPERTREND', 'is_bullish': False},
            'MACD': {'type': 'MACD', 'is_bullish': False},
            'RSI': {'type': 'RSI', 'current': 45},
            'SMA20': {'type': 'SMA', 'current': 110, 'params': {'period': 20}}
        }
        
        current_price = 105
        
        confirmation = calculator.calculate_signal_confirmation(indicators, current_price)
        
        # Should fail 3/4 rule
        assert confirmation['confirmed_count'] < 3
        assert confirmation['passed'] == False
    
    @pytest.mark.asyncio
    async def test_multi_timeframe_calculation(self, calculator):
        """Test calculating indicators across multiple timeframes."""
        # Create data for multiple timeframes
        dates_daily = pd.date_range(start='2024-01-01', periods=100, freq='D')
        dates_weekly = pd.date_range(start='2024-01-01', periods=20, freq='W')
        
        multi_timeframe_data = {
            '1d': pd.DataFrame({
                'open': np.random.uniform(100, 110, 100),
                'high': np.random.uniform(110, 120, 100),
                'low': np.random.uniform(90, 100, 100),
                'close': np.random.uniform(100, 110, 100),
                'volume': np.random.uniform(1000000, 2000000, 100)
            }).to_dict('records'),
            '1w': pd.DataFrame({
                'open': np.random.uniform(100, 110, 20),
                'high': np.random.uniform(110, 120, 20),
                'low': np.random.uniform(90, 100, 20),
                'close': np.random.uniform(100, 110, 20),
                'volume': np.random.uniform(5000000, 10000000, 20)
            }).to_dict('records')
        }
        
        configs = [
            {'name': 'SMA20', 'type': 'SMA', 'parameters': {'period': 20}}
        ]
        
        result = await calculator.calculate_indicators(multi_timeframe_data, configs)
        
        # Should have results for both timeframes
        assert '1d' in result
        assert '1w' in result
        assert 'SMA20' in result['1d']
        assert 'SMA20' in result['1w']

