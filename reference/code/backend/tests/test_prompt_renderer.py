"""
Unit Tests for Prompt Renderer and Context Builder
"""
import pytest
from datetime import datetime, date
from backend.services.prompt_renderer import PromptRenderer, PromptContextBuilder


class TestPromptRenderer:
    """Test PromptRenderer class."""
    
    @pytest.fixture
    def renderer(self):
        """Create a prompt renderer instance."""
        return PromptRenderer(enable_sandbox=True)
    
    def test_simple_variable_rendering(self, renderer):
        """Test rendering simple variables."""
        template = "Hello {{ name }}!"
        context = {"name": "World"}
        result = renderer.render(template, context)
        assert result == "Hello World!"
    
    def test_jinja2_conditional(self, renderer):
        """Test Jinja2 conditional rendering."""
        template = """
{% if price > 100 %}
Price is high: {{ price }}
{% else %}
Price is low: {{ price }}
{% endif %}
""".strip()
        
        context_high = {"price": 150}
        result_high = renderer.render(template, context_high)
        assert "Price is high: 150" in result_high
        
        context_low = {"price": 50}
        result_low = renderer.render(template, context_low)
        assert "Price is low: 50" in result_low
    
    def test_jinja2_loop(self, renderer):
        """Test Jinja2 loop rendering."""
        template = """
{% for item in items %}
- {{ item }}
{% endfor %}
""".strip()
        
        context = {"items": ["Apple", "Banana", "Cherry"]}
        result = renderer.render(template, context)
        assert "- Apple" in result
        assert "- Banana" in result
        assert "- Cherry" in result
    
    def test_round_filter(self, renderer):
        """Test round_decimal filter."""
        template = "Price: {{ price|round_decimal(2) }}"
        context = {"price": 123.456789}
        result = renderer.render(template, context)
        assert "123.46" in result
    
    def test_format_percent_filter(self, renderer):
        """Test format_percent filter."""
        template = "Win rate: {{ rate|format_percent }}"
        context = {"rate": 0.6543}
        result = renderer.render(template, context)
        assert "65.43%" in result
    
    def test_format_currency_filter(self, renderer):
        """Test format_currency filter."""
        template = "Total: {{ amount|format_currency }}"
        context = {"amount": 1234.56}
        result = renderer.render(template, context)
        assert "USD 1,234.56" in result
    
    def test_datetimeformat_filter(self, renderer):
        """Test datetimeformat filter."""
        template = "Date: {{ dt|datetimeformat('%Y-%m-%d') }}"
        context = {"dt": datetime(2024, 1, 15, 10, 30, 0)}
        result = renderer.render(template, context)
        assert "2024-01-15" in result
    
    def test_default_if_none_filter(self, renderer):
        """Test default_if_none filter."""
        template = "Value: {{ value|default_if_none('N/A') }}"
        
        context_none = {"value": None}
        result_none = renderer.render(template, context_none)
        assert "N/A" in result_none
        
        context_value = {"value": 123}
        result_value = renderer.render(template, context_value)
        assert "123" in result_value
    
    def test_abs_filter(self, renderer):
        """Test abs filter."""
        template = "Absolute: {{ num|abs }}"
        context = {"num": -42.5}
        result = renderer.render(template, context)
        assert "42.5" in result
    
    def test_truncate_filter(self, renderer):
        """Test truncate filter."""
        template = "Short: {{ text|truncate(10) }}"
        context = {"text": "This is a very long text that needs truncation"}
        result = renderer.render(template, context)
        assert "This is..." in result or "This is a..." in result
    
    def test_now_global_variable(self, renderer):
        """Test that 'now' is available as a global variable."""
        template = "Current year: {{ now.year }}"
        context = {}
        result = renderer.render(template, context)
        assert str(datetime.now().year) in result
    
    def test_today_global_variable(self, renderer):
        """Test that 'today' is available as a global variable."""
        template = "Today: {{ today }}"
        context = {}
        result = renderer.render(template, context)
        assert str(date.today()) in result
    
    def test_syntax_error_handling(self, renderer):
        """Test that syntax errors raise ValueError with message."""
        template = "{% if unclosed"
        context = {}
        
        with pytest.raises(ValueError) as exc_info:
            renderer.render(template, context)
        assert "syntax error" in str(exc_info.value).lower()
    
    def test_undefined_variable_error(self, renderer):
        """Test that undefined variables raise ValueError."""
        template = "Hello {{ undefined_var }}!"
        context = {}
        
        # With strict undefined
        with pytest.raises(ValueError) as exc_info:
            renderer.render(template, context)
        assert "undefined" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()
    
    def test_validate_template_success(self, renderer):
        """Test template validation for valid template."""
        template = "Hello {{ name }}!"
        is_valid, error = renderer.validate_template(template)
        assert is_valid is True
        assert error is None
    
    def test_validate_template_failure(self, renderer):
        """Test template validation for invalid template."""
        template = "{% if unclosed"
        is_valid, error = renderer.validate_template(template)
        assert is_valid is False
        assert error is not None
        assert "syntax" in error.lower() or "error" in error.lower()
    
    def test_get_undefined_variables(self, renderer):
        """Test getting undefined variables from template."""
        template = "{{ symbol }} - {{ price }} - {{ unknown }}"
        context = {"symbol": "AAPL", "price": 150}
        
        undefined = renderer.get_undefined_variables(template, context)
        assert "unknown" in undefined
        assert "symbol" not in undefined
        assert "price" not in undefined
    
    def test_complex_real_world_template(self, renderer):
        """Test rendering a complex real-world prompt template."""
        template = """
Analysis for {{ symbol }} on {{ today|dateformat('%Y-%m-%d') }}

Current Price: ${{ current_price|round_decimal(2) }}

{% if rsi > 70 %}
⚠️ RSI indicates overbought conditions ({{ rsi|round_decimal(1) }})
{% elif rsi < 30 %}
📈 RSI indicates oversold conditions ({{ rsi|round_decimal(1) }})
{% else %}
✅ RSI in neutral zone ({{ rsi|round_decimal(1) }})
{% endif %}

ATR: {{ atr|round_decimal(2) }}
Win Rate: {{ strategy.win_rate|format_percent }}
Recommendation: {{ strategy.recommendation|upper }}
""".strip()
        
        context = {
            "symbol": "AAPL",
            "current_price": 175.5034,
            "rsi": 68.5,
            "atr": 2.34567,
            "strategy": {
                "win_rate": 0.653,
                "recommendation": "buy"
            }
        }
        
        result = renderer.render(template, context)
        assert "AAPL" in result
        assert "175.50" in result
        assert "68.5" in result
        assert "2.35" in result
        assert "65.30%" in result
        assert "BUY" in result
        assert "✅ RSI in neutral zone" in result


class TestPromptContextBuilder:
    """Test PromptContextBuilder class."""
    
    @pytest.fixture
    def builder(self):
        """Create a context builder instance."""
        return PromptContextBuilder()
    
    def test_build_analysis_context(self, builder):
        """Test building analysis context."""
        context = builder.build_analysis_context(
            symbol="AAPL",
            current_price=175.50,
            trend_data={"daily": "bullish", "weekly": "neutral"},
            indicator_data={"rsi": 68.5, "atr": 2.3, "macd": 1.5},
            strategy_params={"risk_reward_ratio": 2.0, "stop_loss_pct": 0.02}
        )
        
        assert context["symbol"] == "AAPL"
        assert context["current_price"] == 175.50
        assert context["trend"]["daily"] == "bullish"
        assert context["indicators"]["rsi"] == 68.5
        assert context["strategy"]["risk_reward_ratio"] == 2.0
        assert context["previous_analyses"] == []
    
    def test_build_analysis_context_with_previous(self, builder):
        """Test building analysis context with previous analyses."""
        previous = [
            {"date": "2024-01-14", "signal": "buy"},
            {"date": "2024-01-13", "signal": "hold"}
        ]
        
        context = builder.build_analysis_context(
            symbol="MSFT",
            current_price=400.0,
            trend_data={},
            indicator_data={},
            strategy_params={},
            previous_analyses=previous
        )
        
        assert len(context["previous_analyses"]) == 2
        assert context["previous_analyses"][0]["signal"] == "buy"
    
    def test_build_analysis_context_with_kwargs(self, builder):
        """Test that extra kwargs are added to context."""
        context = builder.build_analysis_context(
            symbol="GOOGL",
            current_price=150.0,
            trend_data={},
            indicator_data={},
            strategy_params={},
            custom_field="custom_value",
            another_field=123
        )
        
        assert context["custom_field"] == "custom_value"
        assert context["another_field"] == 123
    
    def test_build_consolidation_context(self, builder):
        """Test building consolidation context."""
        context = builder.build_consolidation_context(
            daily_analysis_summary="Daily: Bullish trend with RSI at 65",
            weekly_analysis_summary="Weekly: Neutral trend, consolidating",
            symbol="TSLA",
            strategy_params={"timeframe": "daily", "risk": "medium"}
        )
        
        assert context["daily_analysis"] == "Daily: Bullish trend with RSI at 65"
        assert context["weekly_analysis"] == "Weekly: Neutral trend, consolidating"
        assert context["symbol"] == "TSLA"
        assert context["strategy"]["timeframe"] == "daily"
    
    def test_build_consolidation_context_with_kwargs(self, builder):
        """Test consolidation context with extra kwargs."""
        context = builder.build_consolidation_context(
            daily_analysis_summary="Daily summary",
            weekly_analysis_summary="Weekly summary",
            symbol="NVDA",
            strategy_params={},
            market_condition="bullish",
            volatility=0.25
        )
        
        assert context["market_condition"] == "bullish"
        assert context["volatility"] == 0.25


class TestPromptRendererIntegration:
    """Integration tests for renderer + context builder."""
    
    def test_full_analysis_workflow(self):
        """Test complete workflow: build context -> render template."""
        renderer = PromptRenderer(enable_sandbox=True)
        builder = PromptContextBuilder()
        
        # Build context
        context = builder.build_analysis_context(
            symbol="AAPL",
            current_price=175.50,
            trend_data={"daily": "bullish", "weekly": "neutral"},
            indicator_data={"rsi": 68.5, "atr": 2.3},
            strategy_params={"name": "Momentum Strategy"}
        )
        
        # Define template
        template = """
Symbol: {{ symbol }}
Price: ${{ current_price }}
Daily Trend: {{ trend.daily|upper }}
RSI: {{ indicators.rsi }}
Strategy: {{ strategy.name }}
"""
        
        # Render
        result = renderer.render(template, context)
        
        assert "AAPL" in result
        assert "175.5" in result
        assert "BULLISH" in result
        assert "68.5" in result
        assert "Momentum Strategy" in result
    
    def test_full_consolidation_workflow(self):
        """Test consolidation workflow."""
        renderer = PromptRenderer(enable_sandbox=True)
        builder = PromptContextBuilder()
        
        context = builder.build_consolidation_context(
            daily_analysis_summary="Strong bullish momentum on daily chart",
            weekly_analysis_summary="Weekly showing consolidation pattern",
            symbol="TSLA",
            strategy_params={"type": "swing"}
        )
        
        template = """
{{ symbol }} Consolidation Report

Daily: {{ daily_analysis }}
Weekly: {{ weekly_analysis }}

Strategy Type: {{ strategy.type|upper }}
"""
        
        result = renderer.render(template, context)
        
        assert "TSLA" in result
        assert "Strong bullish momentum" in result
        assert "consolidation pattern" in result
        assert "SWING" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

