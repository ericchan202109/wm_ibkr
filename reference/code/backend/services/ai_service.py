"""AI/LLM integration service."""
import openai
from typing import Dict, Any, Optional, List
from backend.config.settings import settings
import logging
import json

logger = logging.getLogger(__name__)


class AIService:
    """Service for AI-powered analysis using OpenAI-compatible APIs."""
    
    def __init__(self):
        logger.info(f"🔧 Initializing AIService with OpenAI-compatible API")
        logger.info(f"   Base URL (from .env): {settings.OPENAI_API_BASE}")
        logger.info(f"   Model: {settings.OPENAI_MODEL}")
        logger.info(f"   API Key configured: {'Yes' if settings.OPENAI_API_KEY and settings.OPENAI_API_KEY != 'your_key_here' else 'No'}")
        
        self.client = openai.OpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_API_BASE
        )
        self.model = settings.OPENAI_MODEL
        self.timeout = settings.AI_REQUEST_TIMEOUT
    
    def _get_daily_chart_prompt(self) -> str:
        """Get prompt template for daily chart analysis."""
        return """You are a professional technical analyst specialized in medium-term trading (30-180 days holding period) based on Daily candle chart.

Please analyze this daily chart and provide:

## 1. Core Price Analysis
- Current price and overall trend status
- Key support/resistance levels
- Important candlestick patterns and price structure

## 2. Trend Indicator Analysis
A. SuperTrend (10,3):
   - Current status: [uptrend/downtrend]
   - Signal color: [green/red]
   - Relative position to price and recent changes

B. Moving Average System:
   - 20-day SMA: short-term trend direction
   - 50-day SMA: medium-term trend direction
   - 200-day SMA: long-term trend direction
   - Golden/Death cross situations
   - Price relationship with MAs (support/resistance)

## 3. Confirmation Indicators
A. Momentum Confirmation:
   - MACD (12,26,9) status and signals
   - RSI (14) level and direction [overbought/oversold/neutral]

B. Volatility Analysis:
   - ATR (14) value and trend
   - Bollinger Bands (20,2) width and price position

C. Volume Analysis:
   - Current volume vs 20-day average
   - Volume trend characteristics
   - OBV trend and price confirmation

## 4. Signal Confirmation System (3/4 Rule)
At least 3 of these signals must confirm trade direction:
- SuperTrend signal direction [bullish/bearish]
- Price vs 20-day SMA relationship [above/below]
- MACD signal line cross [buy/sell]
- RSI relative position [>50 bullish/<50 bearish]

## 5. Trading Recommendation
- Overall trend judgment: [strongly bullish/bullish/neutral/bearish/strongly bearish]
- Trading signal: [enter/hold/exit]

If bullish (long):
   - Entry range: [price range]
   - Stop-loss: entry - (2 × ATR), approximately [specific value]
   - Profit targets:
     * First target (conservative): [value] - based on [technical basis]
     * Second target (aggressive): [value] - based on [technical basis]
   - R multiple calculation
   - Suggested position size: [%]

If bearish (short):
   - Entry range: [price range]
   - Stop-loss: entry + (2 × ATR), approximately [specific value]
   - Profit targets:
     * First target (conservative): [value] - based on [technical basis]
     * Second target (aggressive): [value] - based on [technical basis]
   - R multiple calculation
   - Suggested position size: [%]

## 6. Risk Assessment
- Main technical risks
- Key reversal prices
- Potential signal failure scenarios
- Weekly chart confirmation status

Provide comprehensive judgment and clearly explain reasoning, focusing on how key indicators confirm/contradict each other. Give clear entry recommendations only when multiple indicators align.
"""
    
    def _get_weekly_chart_prompt(self) -> str:
        """Get prompt template for weekly chart analysis."""
        return """You are a professional technical analyst specializing in weekly chart analysis for medium-term (30-180 days) trading decisions.

Please analyze the weekly chart and provide:

## 1. Medium-Term Trend Assessment
- 12-month weekly trend direction and strength [up/consolidating/down]
- Major trendlines and channel analysis
- Weekly trend stage [early/middle/late]
- SuperTrend weekly indicator status

## 2. Key Price Level Analysis
- Major support levels in 12 months [price range and importance]
- Major resistance levels in 12 months [price range and importance]
- Current price relative position assessment
- Medium-term value range determination

## 3. Weekly Indicator Analysis
- Weekly RSI(14): medium-term overbought/oversold status
- Weekly MACD(12,26,9): medium-term momentum
- Weekly MA system: 10-week/20-week/50-week relationship
- Weekly Bollinger Bands (20,2) position and width

## 4. Medium-Term Price Patterns
- Recent weekly chart patterns formed
- Weekly breakout/pullback situations
- Recent reversal signals
- Medium-term trend strength assessment

## 5. Daily and Weekly Coordination Analysis
- Daily and weekly trend consistency judgment
- Trend conflict analysis (if any)
- How weekly supports or contradicts daily signals
- Reliability of daily signals in weekly context

## 6. Medium-Term Outlook (30-180 days)
- Expected trend for next 1-6 months
- Key turning point identification
- Main risks for medium-term holding
- Most important weekly warning signals to watch

Provide weekly chart medium-term trend analysis, especially emphasizing consistency or differences between weekly and daily signals, to provide reference for 30-180 day trading decisions.
"""
    
    def _get_consolidation_prompt(self) -> str:
        """Get prompt template for multi-timeframe consolidation."""
        return """You are a professional technical analyst creating a comprehensive trading recommendation based primarily on daily chart analysis, with weekly chart serving as trend confirmation.

Please synthesize a detailed final analysis report focused on medium-term (30-180 days) trading opportunities.

## 1. Price and Trend Overview
- Current price and overall trend status
- Daily primary trend direction and strength
- Weekly trend confirmation situation
- Price relative to key support/resistance levels
- Cross-timeframe trend consistency assessment

## 2. Technical Analysis Summary
- SuperTrend signals on daily and weekly charts
- Moving average system cross-timeframe analysis
- Momentum indicators (MACD, RSI) key readings and confirmation
- Volatility indicators (ATR, Bollinger Bands) important values
- Volume analysis and cross-timeframe confirmation
- Key technical patterns and impact

## 3. Multi-Timeframe Signal Confirmation System
- Daily 3/4 rule signal results
- Weekly support/contradiction level for daily signals
- Cross-timeframe confirmation strength [strong/medium/weak]
- Any significant timeframe conflicts and resolutions
- Final confirmation signal determination [confirmed/partially confirmed/unconfirmed]

## 4. Detailed Trading Recommendation
- Comprehensive trend judgment: [strongly bullish/bullish/neutral/bearish/strongly bearish]
- Trading signal: [enter/hold/exit]

If bullish (long):
   - Suggested entry range: [specific price range]
   - Stop-loss: [specific price] (based on daily ATR, weekly confirmation)
   - Target prices:
     * First target (conservative): [specific price] - [technical basis]
     * Second target (aggressive): [specific price] - [technical basis]
   - R multiple: [specific value] calculation
   - Expected profit percentage: [specific value]%

If bearish (short):
   - Suggested entry range: [specific price range]
   - Stop-loss: [specific price] (based on daily ATR, weekly confirmation)
   - Target prices:
     * First target (conservative): [specific price] - [technical basis]
     * Second target (aggressive): [specific price] - [technical basis]
   - R multiple: [specific value] calculation
   - Expected profit percentage: [specific value]%

- Suggested position size: [%]
- Trading timing and execution strategy
- Staged targets and adjustment points

## 5. Risk Assessment
- Main technical risk factors
- Additional risk considerations identified from weekly chart
- Key reversal prices and alert points
- Potential signal failure scenarios
- Cross-timeframe risk monitoring priorities
- How to manage and reduce identified risks

## 6. Conclusion
- Final trading recommendation summary
- Main reasoning and technical basis
- Key monitoring indicators and timepoints
- Trading plan timeliness and follow-up update recommendations

Provide detailed and comprehensive integrated analysis, ensuring all trading parameters are clear and precise, emphasizing key technical basis. Report should highlight daily analysis as primary, while showing how weekly trend confirms or adjusts main trading view.
"""
    
    def _get_decision_prompt(self) -> str:
        """Get prompt template for trading decision generation."""
        return """You are a data analyst. Based on the technical analysis provided, generate a structured trading decision.

You MUST provide your final answer as a JSON object with the following structure:

{
  "code": "SYMBOL",
  "type": "buy" | "sell" | "hold",
  "current_price": 123.45,
  "target_price": 130.00,
  "stop_loss": 120.00,
  "profit_margin": 0.053,
  "R_coefficient": 2.17,
  "strategy_used": "two_indicator"
}

Rules:
1. When analysis shows downtrend: Check if R coefficient >= 1.0 AND expected return >= 5%
   If both conditions met, set type to "sell"

2. When analysis shows uptrend or reversal from downtrend: Check if R coefficient >= 1.0 AND expected return >= 5%
   If both conditions met, set type to "buy"

3. When no clear trend or recommendation is to wait, set type to "hold"

4. When both first and second targets exist, use first target as target_price

5. profit_margin = ABS((target_price - current_price) / current_price)

6. R_coefficient = ABS((target_price - current_price) / (stop_loss - current_price))

7. If any of target_price, R_coefficient, or profit_margin data is missing, set to 0 (do not calculate randomly)

Return ONLY the JSON object, no additional text.
"""
    
    async def analyze_daily_chart(self, symbol: str, chart_url: str) -> str:
        """
        Analyze daily chart with AI.
        
        Args:
            symbol: Stock symbol
            chart_url: URL to chart image
            
        Returns:
            Analysis text
        """
        try:
            logger.info(f"Analyzing daily chart for {symbol}")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_daily_chart_prompt()
                    },
                    {
                        "role": "user",
                        "content": f"Analyze this daily chart for {symbol}. Chart URL: {chart_url}"
                    }
                ],
                timeout=self.timeout
            )
            
            analysis = response.choices[0].message.content
            logger.info(f"Daily chart analysis completed for {symbol}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze daily chart: {e}")
            raise
    
    async def analyze_weekly_chart(self, symbol: str, chart_url: str) -> str:
        """Analyze weekly chart with AI."""
        try:
            logger.info(f"Analyzing weekly chart for {symbol}")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_weekly_chart_prompt()
                    },
                    {
                        "role": "user",
                        "content": f"Analyze this weekly chart for {symbol}. Chart URL: {chart_url}"
                    }
                ],
                timeout=self.timeout
            )
            
            analysis = response.choices[0].message.content
            logger.info(f"Weekly chart analysis completed for {symbol}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze weekly chart: {e}")
            raise
    
    async def consolidate_analysis(self, daily_analysis: str, weekly_analysis: str) -> str:
        """Consolidate daily and weekly analyses."""
        try:
            logger.info("Consolidating multi-timeframe analysis")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_consolidation_prompt()
                    },
                    {
                        "role": "user",
                        "content": f"## Daily Analysis:\n{daily_analysis}\n\n## Weekly Analysis:\n{weekly_analysis}"
                    }
                ],
                timeout=self.timeout
            )
            
            consolidated = response.choices[0].message.content
            logger.info("Multi-timeframe analysis consolidated")
            return consolidated
            
        except Exception as e:
            logger.error(f"Failed to consolidate analysis: {e}")
            raise
    
    async def generate_trading_decision(
        self,
        analysis: str,
        symbol: str,
        strategy_name: str = "two_indicator"
    ) -> Dict[str, Any]:
        """
        Generate structured trading decision from analysis.
        
        Returns:
            Dictionary with: code, type, current_price, target_price, stop_loss, 
                           profit_margin, R_coefficient, strategy_used
        """
        try:
            logger.info(f"Generating trading decision for {symbol}")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_decision_prompt()
                    },
                    {
                        "role": "user",
                        "content": f"Analysis:\n{analysis}\n\nSymbol: {symbol}\nStrategy: {strategy_name}"
                    }
                ],
                response_format={"type": "json_object"},
                timeout=self.timeout
            )
            
            decision_text = response.choices[0].message.content
            decision = json.loads(decision_text)
            
            # Validate decision structure
            required_fields = ['code', 'type', 'current_price', 'target_price', 'stop_loss', 'profit_margin', 'R_coefficient']
            if not all(field in decision for field in required_fields):
                raise ValueError(f"Decision missing required fields: {required_fields}")
            
            logger.info(f"Trading decision generated for {symbol}: {decision['type']}")
            return decision
            
        except Exception as e:
            logger.error(f"Failed to generate trading decision: {e}")
            raise

