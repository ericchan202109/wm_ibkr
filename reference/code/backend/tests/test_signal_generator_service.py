"""Tests for SignalGenerator."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from backend.services.signal_generator import SignalGenerator
from backend.models.trading_signal import TradingSignal


class TestSignalGenerator:
    """Test suite for SignalGenerator."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        return MagicMock()
    
    @pytest.fixture
    def signal_generator(self, mock_db):
        """Create a SignalGenerator instance with mock db."""
        return SignalGenerator(mock_db)
    
    @pytest.fixture
    def sample_market_data(self):
        """Create sample market data."""
        return {
            '1d': [
                {
                    'timestamp': '2024-01-01',
                    'open': 100,
                    'high': 105,
                    'low': 99,
                    'close': 103,
                    'volume': 1000000
                }
            ]
        }
    
    @pytest.fixture
    def sample_indicators(self):
        """Create sample indicator data."""
        return {
            '1d': {
                'SuperTrend': {
                    'type': 'SUPERTREND',
                    'is_bullish': True,
                    'current': 100,
                    'current_direction': 1
                },
                'MACD': {
                    'type': 'MACD',
                    'is_bullish': True,
                    'current_macd': 2.5,
                    'current_signal': 2.0
                },
                'RSI': {
                    'type': 'RSI',
                    'current': 65
                },
                'SMA20': {
                    'type': 'SMA',
                    'current': 100,
                    'params': {'period': 20}
                },
                'ATR': {
                    'type': 'ATR',
                    'current': 2.5
                }
            }
        }
    
    @pytest.fixture
    def sample_chart_urls(self):
        """Create sample chart URLs."""
        return {
            '1d': 'http://localhost:9000/charts/AAPL_1d.png',
            '1w': 'http://localhost:9000/charts/AAPL_1w.png'
        }
    
    @pytest.mark.asyncio
    async def test_generate_signal_basic(
        self,
        signal_generator,
        sample_market_data,
        sample_indicators,
        sample_chart_urls
    ):
        """Test basic signal generation."""
        with patch.object(signal_generator.llm_service, 'analyze_chart', new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = {
                'raw_text': 'Bullish signal',
                'parsed_signal': {
                    'signal': 'BUY',
                    'confidence': 0.85
                },
                'model_used': 'gpt-4-vision',
                'provider': 'openai',
                'prompt_template_id': 1,
                'prompt_version': 1
            }
            
            signal = await signal_generator.generate_signal(
                strategy_id=1,
                symbol='AAPL',
                conid=265598,
                market_data=sample_market_data,
                indicator_data=sample_indicators,
                chart_urls=sample_chart_urls,
                llm_enabled=True,
                llm_language='en'
            )
            
            # Verify signal was created
            assert isinstance(signal, TradingSignal)
            assert signal.symbol == 'AAPL'
            assert signal.strategy_id == 1
            assert signal.signal_type in ['BUY', 'SELL', 'HOLD']
            assert 0 <= signal.confidence <= 1
            assert signal.entry_price_low is not None
            assert signal.entry_price_high is not None
            assert signal.stop_loss is not None
    
    @pytest.mark.asyncio
    async def test_generate_signal_without_llm(
        self,
        signal_generator,
        sample_market_data,
        sample_indicators,
        sample_chart_urls
    ):
        """Test signal generation without LLM analysis."""
        signal = await signal_generator.generate_signal(
            strategy_id=1,
            symbol='AAPL',
            conid=265598,
            market_data=sample_market_data,
            indicator_data=sample_indicators,
            chart_urls=sample_chart_urls,
            llm_enabled=False
        )
        
        # Should still generate signal based on indicators only
        assert isinstance(signal, TradingSignal)
        assert signal.signal_type in ['BUY', 'SELL', 'HOLD']
        assert signal.model_used is None  # No LLM used
    
    def test_get_current_price(self, signal_generator, sample_market_data):
        """Test extracting current price from market data."""
        price = signal_generator._get_current_price(sample_market_data)
        
        assert price is not None
        assert price == 103  # Close price from sample data
    
    def test_get_current_price_empty_data(self, signal_generator):
        """Test current price extraction with empty data."""
        price = signal_generator._get_current_price({})
        
        assert price is None
    
    @pytest.mark.asyncio
    async def test_analyze_indicators_bullish(
        self,
        signal_generator,
        sample_indicators
    ):
        """Test indicator analysis with bullish signals."""
        result = await signal_generator._analyze_indicators(
            sample_indicators, current_price=105
        )
        
        assert result['signal_type'] in ['BUY', 'HOLD']
        assert 'trend' in result
        assert 'reasoning' in result
        assert isinstance(result['reasoning'], list)
        assert len(result['reasoning']) > 0
    
    @pytest.mark.asyncio
    async def test_analyze_indicators_bearish(
        self,
        signal_generator
    ):
        """Test indicator analysis with bearish signals."""
        bearish_indicators = {
            '1d': {
                'SuperTrend': {
                    'type': 'SUPERTREND',
                    'is_bullish': False
                },
                'MACD': {
                    'type': 'MACD',
                    'is_bullish': False
                },
                'RSI': {
                    'type': 'RSI',
                    'current': 30
                }
            }
        }
        
        result = await signal_generator._analyze_indicators(
            bearish_indicators, current_price=95
        )
        
        assert result['signal_type'] in ['SELL', 'HOLD']
    
    @pytest.mark.asyncio
    async def test_combine_analyses_llm_and_technical(
        self,
        signal_generator
    ):
        """Test combining LLM and technical analyses."""
        technical = {
            'signal_type': 'BUY',
            'trend': 'bullish',
            'reasoning': ['Technical bullish'],
            'summary': 'Technical analysis suggests BUY'
        }
        
        llm = {
            'parsed_signal': {
                'signal': 'BUY',
                'trend': 'strong_bullish',
                'confidence': 0.9
            }
        }
        
        result = await signal_generator._combine_analyses(
            technical, llm, current_price=105
        )
        
        assert result['signal_type'] == 'BUY'
        assert 'reasoning' in result
    
    @pytest.mark.asyncio
    async def test_combine_analyses_technical_only(
        self,
        signal_generator
    ):
        """Test combining when only technical analysis available."""
        technical = {
            'signal_type': 'HOLD',
            'trend': 'neutral',
            'reasoning': ['Mixed signals'],
            'summary': 'Neutral'
        }
        
        result = await signal_generator._combine_analyses(
            technical, {}, current_price=105
        )
        
        assert result['signal_type'] == 'HOLD'
        assert result['trend'] == 'neutral'
    
    def test_calculate_confidence_agreement(self, signal_generator):
        """Test confidence calculation when LLM and technical agree."""
        technical = {'signal_type': 'BUY'}
        llm = {
            'parsed_signal': {
                'signal': 'BUY',
                'confidence': 0.8
            }
        }
        
        confidence = signal_generator._calculate_confidence(technical, llm)
        
        # Should boost confidence when they agree
        assert confidence > 0.5
        assert confidence <= 1.0
    
    def test_calculate_confidence_disagreement(self, signal_generator):
        """Test confidence calculation when LLM and technical disagree."""
        technical = {'signal_type': 'BUY'}
        llm = {
            'parsed_signal': {
                'signal': 'SELL',
                'confidence': 0.7
            }
        }
        
        confidence = signal_generator._calculate_confidence(technical, llm)
        
        # Should not boost confidence when they disagree
        assert confidence >= 0.0
        assert confidence <= 1.0
    
    def test_calculate_trading_levels_buy(self, signal_generator, sample_indicators):
        """Test calculating trading levels for BUY signal."""
        levels = signal_generator._calculate_trading_levels(
            signal_type='BUY',
            current_price=100,
            indicator_data=sample_indicators,
            llm_recommendation={}
        )
        
        assert levels['entry_low'] < 100
        assert levels['entry_high'] > 100
        assert levels['stop_loss'] < 100  # Stop below entry for BUY
        assert levels['target_conservative'] > 100  # Target above entry
        assert levels['target_aggressive'] > levels['target_conservative']
        assert levels['r_conservative'] is not None
        assert levels['r_aggressive'] is not None
        assert levels['position_size'] > 0
    
    def test_calculate_trading_levels_sell(self, signal_generator, sample_indicators):
        """Test calculating trading levels for SELL signal."""
        levels = signal_generator._calculate_trading_levels(
            signal_type='SELL',
            current_price=100,
            indicator_data=sample_indicators,
            llm_recommendation={}
        )
        
        assert levels['stop_loss'] > 100  # Stop above entry for SELL
        assert levels['target_conservative'] < 100  # Target below entry
        assert levels['target_aggressive'] < levels['target_conservative']
        assert levels['r_conservative'] is not None
        assert levels['r_aggressive'] is not None
    
    def test_calculate_trading_levels_hold(self, signal_generator, sample_indicators):
        """Test calculating trading levels for HOLD signal."""
        levels = signal_generator._calculate_trading_levels(
            signal_type='HOLD',
            current_price=100,
            indicator_data=sample_indicators,
            llm_recommendation={}
        )
        
        # Should still return levels structure
        assert 'entry_low' in levels
        assert 'entry_high' in levels
        assert 'stop_loss' in levels
    
    def test_calculate_trading_levels_with_llm_recommendation(
        self,
        signal_generator,
        sample_indicators
    ):
        """Test that LLM recommendations override calculated levels."""
        llm_rec = {
            'entry_low': 98,
            'entry_high': 102,
            'stop_loss': 95,
            'target_conservative': 110,
            'target_aggressive': 120
        }
        
        levels = signal_generator._calculate_trading_levels(
            signal_type='BUY',
            current_price=100,
            indicator_data=sample_indicators,
            llm_recommendation=llm_rec
        )
        
        # Should use LLM recommendations
        assert levels['entry_low'] == 98
        assert levels['entry_high'] == 102
        assert levels['stop_loss'] == 95
        assert levels['target_conservative'] == 110
        assert levels['target_aggressive'] == 120
    
    @pytest.mark.asyncio
    async def test_get_llm_analysis_daily_only(
        self,
        signal_generator,
        sample_chart_urls,
        sample_indicators
    ):
        """Test LLM analysis with daily chart only."""
        with patch.object(signal_generator.llm_service, 'analyze_chart', new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = {
                'raw_text': 'Bullish trend',
                'parsed_signal': {'signal': 'BUY'},
                'model_used': 'gpt-4-vision',
                'provider': 'openai',
                'prompt_template_id': 1,
                'prompt_version': 1
            }
            
            result = await signal_generator._get_llm_analysis(
                strategy_id=1,
                symbol='AAPL',
                chart_urls={'1d': sample_chart_urls['1d']},
                language='en',
                current_price=100,
                indicators=sample_indicators
            )
            
            assert 'daily_analysis' in result
            assert 'model_used' in result
            assert 'provider' in result
    
    @pytest.mark.asyncio
    async def test_signal_with_confirmation(
        self,
        signal_generator,
        sample_market_data,
        sample_indicators,
        sample_chart_urls
    ):
        """Test that signal includes confirmation data."""
        signal = await signal_generator.generate_signal(
            strategy_id=1,
            symbol='AAPL',
            conid=265598,
            market_data=sample_market_data,
            indicator_data=sample_indicators,
            chart_urls=sample_chart_urls,
            llm_enabled=False
        )
        
        # Should have confirmation signals
        assert signal.confirmation_signals is not None
        assert 'confirmed_count' in signal.confirmation_signals
        assert 'passed' in signal.confirmation_signals

