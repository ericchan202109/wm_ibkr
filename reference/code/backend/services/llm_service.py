"""
LLM Service for Chart Vision Analysis.

Integrates with OpenAI GPT-4-Vision and Google Gemini to analyze technical charts.
Loads prompts from database with strategy-specific overrides and Jinja2 rendering.
"""
import asyncio
import base64
import logging
import httpx
import json
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
from functools import lru_cache

from backend.config.settings import settings
from backend.services.prompt_renderer import get_prompt_renderer, PromptContextBuilder

logger = logging.getLogger(__name__)


# Prompt templates based on reference workflow
PROMPT_TEMPLATES = {
    "daily_en": """Analysis Date: {now}

# Daily Chart Analysis

## 1. Core Price Analysis
- Current price and trend overview
- Key support/resistance levels
- Important candlestick patterns and price structure
- Chart timeframe: Daily chart (primary)

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
   - Price-MA relationships (support/resistance)

## 3. Confirmation Indicator Analysis
A. Momentum Confirmation:
   - MACD (12,26,9) status and signals
   - RSI (14) level and direction [overbought/oversold/neutral]

B. Volatility Analysis:
   - ATR (14) value and trend
   - Bollinger Bands (20,2) width and price position

C. Volume Analysis:
   - Current volume vs. 20-day average
   - Volume trend characteristics
   - OBV trend and price confirmation

## 4. 3/4 Signal Confirmation System
At least 3 of the following 4 signals must confirm:
- SuperTrend direction [bullish/bearish]
- Price vs 20-day SMA [above/below]
- MACD signal line cross [buy/sell]
- RSI relative position [>50 bullish/<50 bearish]

## 5. Trading Recommendation
- Overall trend: [Strong Bullish/Bullish/Neutral/Bearish/Strong Bearish]
- Trade signal: [BUY/HOLD/SELL]

If Bullish:
- Entry range: [price range]
- Stop loss: Entry - (2 × ATR), approximately [specific value]
- Profit targets:
  * Conservative: [value] - Based on [support/resistance/pattern/indicator]
  * Aggressive: [value] - Based on [support/resistance/pattern/indicator]
- R-multiples: (Target - Entry) ÷ (Entry - Stop) = ?R

If Bearish:
- Entry range: [price range]
- Stop loss: Entry + (2 × ATR), approximately [specific value]
- Profit targets:
  * Conservative: [value] - Based on [technical basis]
  * Aggressive: [value] - Based on [technical basis]
- R-multiples: (Entry - Target) ÷ (Stop - Entry) = ?R

- Recommended position size: [%]

## 6. Risk Assessment
- Main technical risks
- Key reversal prices
- Potential signal failure scenarios
- Weekly chart confirmation status (if available)

Please provide comprehensive analysis with clear reasoning, especially explaining mutual confirmation/contradiction between key indicators. Only give clear entry signals when multiple indicators confirm simultaneously. When analyzing target prices, must explain technical basis such as support/resistance levels, chart pattern measurements, Fibonacci levels, Bollinger Band targets, moving average positions, volume targets, or volatility projections.
""",
    
    "weekly_en": """Analysis Date: {now}

# Weekly Chart Analysis

## 1. Medium-Term Trend Assessment
- 12-month weekly trend direction and strength [uptrend/consolidation/downtrend]
- Main trend lines and channel analysis
- Weekly trend phase [early/mid/late]
- SuperTrend weekly indicator status

## 2. Key Price Level Analysis
- Main support levels in past 12 months [price zones and importance]
- Main resistance levels in past 12 months [price zones and importance]
- Current price relative to range position
- Medium-term value zone determination

## 3. Weekly Indicator Analysis
- Weekly RSI(14): medium-term overbought/oversold status
- Weekly MACD(12,26,9): medium-term momentum
- Weekly MA system: 10-week/20-week/50-week MA relationships
- Weekly Bollinger Bands(20,2) position and width

## 4. Medium-Term Price Patterns
- Recent weekly chart patterns formed
- Weekly breakout/retest situations
- Recent reversal signals
- Medium-term trend strength assessment

## 5. Weekly Volume Characteristics
- Weekly volume trend
- Important weekly price-volume relationships
- Weekly OBV trend
- Abnormal volume weeks analysis

## 6. Daily-Weekly Coordination Analysis
- Daily-weekly trend consistency determination
- Trend conflict analysis (if any)
- How weekly supports or contradicts daily signals
- Daily signal reliability in weekly context

## 7. Medium-Term Outlook (30-180 days)
- Expected trend for next 1-6 months
- Key turning point identification
- Main risks for medium-term holding
- Most important weekly warning signals to watch

Please provide weekly chart medium-term trend analysis, especially emphasizing consistency or differences between weekly and daily signals, providing reference for 30-180 day trading decisions. Analysis should focus on pattern completeness, trend strength, and support/resistance reliability.
""",
    
    "consolidation_en": """Analysis Date: {now}

## Daily Analysis Summary
{daily_analysis}

## Weekly Analysis Summary
{weekly_analysis}

# Integrated Technical Analysis Report

## 1. Price and Trend Overview
- Current price and overall trend status
- Daily main trend direction and strength
- Weekly trend confirmation situation
- Price relative to key support/resistance positions
- Cross-timeframe trend consistency assessment

## 2. Technical Analysis Summary
- SuperTrend signals in daily and weekly status
- Moving average system cross-timeframe analysis
- Key readings and confirmation from momentum indicators (MACD, RSI)
- Important values from volatility indicators (ATR, Bollinger Bands)
- Volume analysis and cross-timeframe confirmation
- Key technical patterns and impact

## 3. Multi-Timeframe Signal Confirmation System
- Daily 3/4 rule signal results
- Weekly support/opposition degree for daily signals
- Cross-timeframe confirmation strength [strong/medium/weak]
- Any significant timeframe conflicts and resolutions
- Final confirmation signal determination [confirmed/partially confirmed/not confirmed]

## 4. Detailed Trading Recommendation
- Overall trend judgment: [Strong Bullish/Bullish/Neutral/Bearish/Strong Bearish]
- Trade signal: [BUY/HOLD/SELL]

If Bullish:
- Recommended entry range: [specific price range]
- Stop loss: [specific price] (based on daily ATR, weekly confirmation)
- Target prices:
  * Conservative target: [specific price] - [technical basis]
  * Aggressive target: [specific price] - [technical basis]
- R-multiples: [specific calculation]
- Expected profit percentage: [specific value]%

If Bearish:
- Recommended entry range: [specific price range]
- Stop loss: [specific price] (based on daily ATR, weekly confirmation)
- Target prices:
  * Conservative target: [specific price] - [technical basis]
  * Aggressive target: [specific price] - [technical basis]
- R-multiples: [specific calculation]
- Expected profit percentage: [specific value]%

- Recommended position size: [%]
- Trade timing and execution strategy
- Staged targets and adjustment points

## 5. Risk Assessment
- Main technical risk factors
- Additional risk considerations identified from weekly chart
- Key reversal prices and warning points
- Potential signal failure scenarios
- Cross-timeframe risk monitoring priorities
- How to manage and reduce identified risks

## 6. Conclusion
- Final trading recommendation summary
- Main reasoning and technical basis
- Key monitoring indicators and timepoints
- Trading plan timeliness and follow-up update recommendations

Please provide detailed and comprehensive integrated analysis, ensuring all trading parameters are clear and explicit, emphasizing key technical basis. Report should highlight daily analysis as primary while showing how weekly trend confirms or adjusts main trading viewpoint.
"""
}


class LLMService:
    """Service for LLM vision-based chart analysis."""
    
    def __init__(self, db_session=None):
        self.provider = (settings.LLM_VISION_PROVIDER or "openai").lower()
        self.model = self._resolve_model()
        self.timeout = settings.LLM_VISION_TIMEOUT
        self.db = db_session
        self.renderer = get_prompt_renderer()
        
        # Cache for prompt templates (strategy_id, template_type, language) -> template
        self._prompt_cache = {}
        
        # Validate configuration on initialization
        self._validate_config()

    def _resolve_model(self) -> str:
        """Resolve the provider-specific model to use for chart analysis."""
        if self.provider == "gemini":
            return (settings.GEMINI_MODEL or "").strip() or settings.LLM_VISION_MODEL
        return settings.LLM_VISION_MODEL
    
    def _get_prompt_template(
        self,
        template_type: str,
        language: str = "en",
        strategy_id: Optional[int] = None
    ) -> Tuple[Optional[Any], Optional[str]]:
        """
        Load prompt template from database with strategy-specific lookup.
        
        Args:
            template_type: Type of template (analysis, consolidation, system_message)
            language: Language code (en, zh)
            strategy_id: Strategy ID for strategy-specific prompts (None for global)
        
        Returns:
            Tuple of (PromptTemplate object, rendered_prompt_text) or (None, fallback_text)
        """
        # Check cache first
        cache_key = (strategy_id, template_type, language)
        if cache_key in self._prompt_cache:
            logger.debug(f"Using cached prompt: {cache_key}")
            return self._prompt_cache[cache_key]
        
        if not self.db:
            logger.warning("No database session available, using fallback prompts")
            return None, self._get_fallback_prompt(template_type, language)
        
        try:
            from backend.models.prompt import PromptTemplate
            
            # Try strategy-specific prompt first
            prompt = None
            if strategy_id:
                prompt = self.db.query(PromptTemplate).filter(
                    PromptTemplate.strategy_id == strategy_id,
                    PromptTemplate.template_type == template_type,
                    PromptTemplate.language == language,
                    PromptTemplate.is_active == True
                ).order_by(
                    PromptTemplate.is_default.desc(),
                    PromptTemplate.created_at.desc()
                ).first()
                
                if prompt:
                    logger.info(f"Loaded strategy-specific prompt: {prompt.name} (ID: {prompt.id}) for strategy {strategy_id}")
            
            # Fallback to global prompt
            if not prompt:
                prompt = self.db.query(PromptTemplate).filter(
                    PromptTemplate.strategy_id.is_(None),
                    PromptTemplate.template_type == template_type,
                    PromptTemplate.language == language,
                    PromptTemplate.is_active == True,
                    PromptTemplate.is_default == True
                ).first()
                
                if prompt:
                    logger.info(f"Loaded global default prompt: {prompt.name} (ID: {prompt.id})")
            
            # Cache the result
            if prompt:
                self._prompt_cache[cache_key] = (prompt, prompt.prompt_text)
                return prompt, prompt.prompt_text
            else:
                logger.warning(f"No prompt found in database for {template_type}/{language}, using fallback")
                fallback = self._get_fallback_prompt(template_type, language)
                self._prompt_cache[cache_key] = (None, fallback)
                return None, fallback
                
        except Exception as e:
            logger.error(f"Error loading prompt template: {e}")
            return None, self._get_fallback_prompt(template_type, language)
    
    def _get_fallback_prompt(self, template_type: str, language: str) -> str:
        """Get hardcoded fallback prompt if database load fails."""
        # Map template_type to old template keys
        template_map = {
            "analysis": "daily",
            "consolidation": "consolidation",
        }
        
        template_name = template_map.get(template_type, "daily")
        template_key = f"{template_name}_{language}"
        
        return PROMPT_TEMPLATES.get(template_key, PROMPT_TEMPLATES.get(f"{template_name}_en", ""))
    
    def clear_prompt_cache(self):
        """Clear the prompt template cache (useful after prompt updates)."""
        self._prompt_cache.clear()
        logger.info("Prompt template cache cleared")
        
    async def analyze_chart(
        self,
        chart_url: str,
        prompt_template: str,
        symbol: str,
        strategy_id: Optional[int] = None,
        language: str = "en",
        **context
    ) -> Dict[str, Any]:
        """
        Analyze a chart image using LLM vision model.
        
        Args:
            chart_url: Public URL to chart image
            prompt_template: Template type ("analysis", "consolidation")
            symbol: Stock symbol
            strategy_id: Strategy ID for strategy-specific prompts
            language: Language code (en, zh)
            **context: Additional context for prompt formatting (current_price, indicators, etc.)
            
        Returns:
            Dict with analysis results including prompt metadata
        """
        try:
            logger.info(f"Analyzing chart for {symbol} using {prompt_template} template (strategy_id={strategy_id}, lang={language})")
            
            # 1. Download chart image
            image_data = await self._download_chart(chart_url)
            
            # 2. Load prompt template from database
            prompt_obj, prompt_text = self._get_prompt_template(
                template_type=prompt_template,
                language=language,
                strategy_id=strategy_id
            )
            
            # 3. Render prompt with Jinja2
            prompt_context = PromptContextBuilder.build_analysis_context(
                symbol=symbol,
                **context
            )
            rendered_prompt = self.renderer.render(prompt_text, prompt_context, strict=False)
            
            logger.debug(f"Rendered prompt ({len(rendered_prompt)} chars) from template: {prompt_obj.name if prompt_obj else 'fallback'}")
            
            # 4. Call LLM
            if self.provider == "openai":
                response_text = await self._call_openai_vision(rendered_prompt, image_data)
            elif self.provider == "gemini":
                response_text = await self._call_gemini_vision(rendered_prompt, image_data)
            else:
                raise ValueError(f"Unknown LLM provider: {self.provider}")
            
            # 5. Parse response
            parsed = self._parse_response(response_text)
            
            # 6. Build result with prompt metadata for traceability
            result = {
                "raw_text": response_text,
                "parsed_signal": parsed,
                "model_used": self.model,
                "provider": self.provider,
                "analyzed_at": datetime.now(),
                # Prompt metadata for traceability
                "prompt_template_id": prompt_obj.id if prompt_obj else None,
                "prompt_version": prompt_obj.template_version if prompt_obj else None,
                "prompt_type": prompt_template,
            }
            
            logger.info(f"Analysis complete for {symbol} (prompt_id={result['prompt_template_id']}, version={result['prompt_version']})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing chart: {str(e)}")
            raise
    
    async def _download_chart(self, chart_url: str) -> bytes:
        """Download chart image from URL."""
        try:
            # Convert public URL to internal Docker URL if needed
            # Public: http://localhost:9000/... → Internal: http://minio:9000/...
            internal_url = chart_url.replace("localhost", "minio").replace(settings.MINIO_PUBLIC_ENDPOINT, settings.MINIO_ENDPOINT)
            
            logger.info(f"Downloading chart from: {internal_url} (original: {chart_url})")
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(internal_url, timeout=30.0)
                response.raise_for_status()
                logger.info(f"Chart downloaded successfully: {len(response.content)} bytes")
                return response.content
        except Exception as e:
            logger.error(f"Failed to download chart from {internal_url}: {type(e).__name__}: {str(e)}")
            raise
    
    def _prepare_prompt(
        self,
        template_name: str,
        symbol: str,
        **context
    ) -> str:
        """Prepare prompt from template (English only)."""
        template_key = f"{template_name}_en"
        template = PROMPT_TEMPLATES.get(template_key, PROMPT_TEMPLATES["daily_en"])
        
        # Format template with context
        context_data = {
            "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            **context
        }
        
        return template.format(**context_data)
    
    async def _call_openai_vision(self, prompt: str, image_data: bytes) -> str:
        """Call OpenAI GPT-4-Vision API."""
        try:
            # Validate API key is configured
            if not settings.OPENAI_API_KEY or settings.OPENAI_API_KEY == "your_key_here":
                raise ValueError(
                    "OpenAI API key not configured. "
                    "Please set OPENAI_API_KEY in .env file. "
                    "Get your key from https://platform.openai.com/api-keys"
                )
            
            # Encode image to base64
            image_b64 = base64.b64encode(image_data).decode('utf-8')
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{image_b64}"
                                }
                            }
                        ]
                    }
                ],
                "max_tokens": settings.LLM_VISION_MAX_TOKENS,
                "temperature": settings.LLM_VISION_TEMPERATURE
            }
            
            # Construct full API endpoint from base URL
            api_endpoint = f"{settings.OPENAI_API_BASE}/chat/completions"
            
            async with httpx.AsyncClient(timeout=self.timeout, verify=False) as client:
                logger.info(f"🔗 OpenAI API Configuration:")
                logger.info(f"   Base URL (from .env): {settings.OPENAI_API_BASE}")
                logger.info(f"   Full Endpoint: {api_endpoint}")
                logger.info(f"   Model: {self.model}")
                
                response = await client.post(
                    api_endpoint,
                    headers=headers,
                    json=payload
                )
                
                logger.info(f"OpenAI API response status: {response.status_code}")
                response.raise_for_status()
                result = response.json()
                
                return result["choices"][0]["message"]["content"]
                
        except httpx.HTTPStatusError as e:
            logger.error(f"OpenAI API HTTP error: {e.response.status_code} - {e.response.text}")
            raise ValueError(f"OpenAI API error: {e.response.status_code} - {e.response.text[:200]}")
        except httpx.ConnectError as e:
            logger.error(f"Cannot connect to OpenAI API at {settings.OPENAI_API_BASE}: {str(e)}")
            raise ValueError(f"Cannot connect to API endpoint: {settings.OPENAI_API_BASE}. Check your network and API base URL.")
        except httpx.TimeoutException as e:
            logger.error(f"OpenAI API timeout after {self.timeout}s: {str(e)}")
            raise ValueError(f"API request timed out after {self.timeout} seconds. Try increasing LLM_VISION_TIMEOUT.")
        except Exception as e:
            logger.error(f"OpenAI API error: {type(e).__name__}: {str(e)}")
            raise
    
    def _validate_config(self):
        """Validate LLM configuration on startup."""
        if self.provider == "openai":
            if not settings.OPENAI_API_KEY or settings.OPENAI_API_KEY == "your_key_here":
                logger.warning(
                    "⚠️  OpenAI API key not configured! "
                    "Signal generation will fail. "
                    "Please set OPENAI_API_KEY in .env file. "
                    "Get your key from https://platform.openai.com/api-keys"
                )
        elif self.provider == "gemini":
            if not settings.GEMINI_API_KEY:
                logger.warning(
                    "⚠️  Gemini API key not configured! "
                    "Signal generation will fail. "
                    "Please set GEMINI_API_KEY in .env file. "
                    "Get your key from https://aistudio.google.com/app/apikey"
                )
    
    async def _call_gemini_vision(self, prompt: str, image_data: bytes) -> str:
        """Call Google Gemini Vision API."""
        if not settings.GEMINI_API_KEY:
            raise ValueError(
                "Gemini API key not configured. "
                "Please set GEMINI_API_KEY in .env file. "
                "Get your key from https://aistudio.google.com/app/apikey"
            )

        model = (self.model or "").strip()
        if not model:
            raise ValueError(
                "Gemini model not configured. "
                "Please set GEMINI_MODEL (recommended) or LLM_VISION_MODEL in .env."
            )

        image_b64 = base64.b64encode(image_data).decode("utf-8")
        mime_type = self._detect_image_mime_type(image_data)
        api_endpoint = self._gemini_generate_content_endpoint(model=model)

        headers = {"Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": mime_type,
                                "data": image_b64,
                            }
                        },
                    ],
                }
            ],
            "generationConfig": {
                "maxOutputTokens": settings.LLM_VISION_MAX_TOKENS,
                "temperature": settings.LLM_VISION_TEMPERATURE,
            },
        }

        max_attempts = max(1, int(getattr(settings, "LLM_RETRY_ATTEMPTS", 1)))
        base_delay = max(0, int(getattr(settings, "LLM_RETRY_DELAY", 0)))
        retryable_status_codes = {429, 500, 502, 503, 504}

        async with httpx.AsyncClient(timeout=self.timeout, verify=False) as client:
            logger.info("🔗 Gemini API Configuration:")
            logger.info(f"   Base URL (from .env): {settings.GEMINI_API_BASE}")
            logger.info(f"   Full Endpoint: {api_endpoint}")
            logger.info(f"   Model: {model}")

            last_error: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await client.post(
                        api_endpoint,
                        headers=headers,
                        params={"key": settings.GEMINI_API_KEY},
                        json=payload,
                    )

                    if response.status_code in retryable_status_codes and attempt < max_attempts:
                        wait_s = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Gemini API returned {response.status_code}; retrying in {wait_s}s "
                            f"(attempt {attempt}/{max_attempts})"
                        )
                        await asyncio.sleep(wait_s)
                        continue

                    logger.info(f"Gemini API response status: {response.status_code}")
                    response.raise_for_status()
                    data = response.json()
                    return self._extract_gemini_text(data)

                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    last_error = e
                    if attempt < max_attempts:
                        wait_s = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Gemini API connection/timeout error; retrying in {wait_s}s "
                            f"(attempt {attempt}/{max_attempts}): {type(e).__name__}: {e}"
                        )
                        await asyncio.sleep(wait_s)
                        continue
                    raise ValueError(
                        f"Cannot connect to Gemini API endpoint: {settings.GEMINI_API_BASE}. "
                        "Check your network and GEMINI_API_BASE."
                    ) from e
                except httpx.HTTPStatusError as e:
                    last_error = e
                    # Preserve a compact error message while avoiding leaking the API key.
                    body_preview = (e.response.text or "")[:500]
                    raise ValueError(
                        f"Gemini API error: {e.response.status_code} - {body_preview}"
                    ) from e
                except Exception as e:
                    last_error = e
                    raise

            if last_error:
                raise last_error

        raise ValueError("Gemini request failed without a response")

    def _detect_image_mime_type(self, image_data: bytes) -> str:
        """Best-effort MIME type detection for chart images."""
        if image_data.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if image_data.startswith(b"\xff\xd8"):
            return "image/jpeg"
        return "image/png"

    def _gemini_generate_content_endpoint(self, model: str) -> str:
        """Build Gemini generateContent endpoint from GEMINI_API_BASE."""
        base = (settings.GEMINI_API_BASE or "").rstrip("/")
        if base.endswith("/v1beta") or base.endswith("/v1"):
            versioned = base
        else:
            versioned = f"{base}/v1beta" if base else "https://generativelanguage.googleapis.com/v1beta"
        return f"{versioned}/models/{model}:generateContent"

    def _extract_gemini_text(self, payload: Dict[str, Any]) -> str:
        """Extract the first candidate text from a Gemini generateContent response."""
        candidates = payload.get("candidates") or []
        if not candidates:
            feedback = payload.get("promptFeedback") or {}
            block_reason = feedback.get("blockReason")
            raise ValueError(f"Gemini returned no candidates. blockReason={block_reason!r}")

        content = (candidates[0] or {}).get("content") or {}
        parts = content.get("parts") or []
        texts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
        text = "".join(texts).strip()
        if not text:
            raise ValueError("Gemini returned an empty response text")
        return text
    
    def _parse_response(self, text: str) -> Dict[str, Any]:
        """
        Parse LLM response into structured signal data.
        
        Extracts:
        - Trend direction
        - Signal (BUY/SELL/HOLD)
        - Entry/Stop/Targets
        - R-multiples
        - Confirmation details
        """
        parsed = {
            "trend": None,
            "signal": None,
            "entry_low": None,
            "entry_high": None,
            "stop_loss": None,
            "target_conservative": None,
            "target_aggressive": None,
            "r_multiple_conservative": None,
            "r_multiple_aggressive": None,
            "position_size_percent": None,
            "confirmation": {
                "supertrend": None,
                "price_vs_ma20": None,
                "macd": None,
                "rsi": None,
                "confirmed_count": 0,
                "passed": False
            }
        }
        
        # Simple parsing - can be enhanced with more sophisticated NLP
        text_lower = text.lower()
        
        # Extract signal (English only)
        if "buy" in text_lower:
            parsed["signal"] = "BUY"
        elif "sell" in text_lower:
            parsed["signal"] = "SELL"
        else:
            parsed["signal"] = "HOLD"
        
        # Extract trend (English only)
        if "strong bullish" in text_lower:
            parsed["trend"] = "strong_bullish"
        elif "bullish" in text_lower:
            parsed["trend"] = "bullish"
        elif "bearish" in text_lower:
            parsed["trend"] = "bearish"
        elif "strong bearish" in text_lower:
            parsed["trend"] = "strong_bearish"
        else:
            parsed["trend"] = "neutral"
        
        # TODO: Add more sophisticated parsing for prices, R-multiples, etc.
        # This would use regex patterns to extract numerical values
        
        logger.info(f"Parsed signal: {parsed['signal']}, Trend: {parsed['trend']}")
        
        return parsed
    
    async def consolidate_analyses(
        self,
        daily_analysis: str,
        weekly_analysis: str,
        symbol: str,
        strategy_id: Optional[int] = None,
        language: str = "en"
    ) -> Dict[str, Any]:
        """
        Consolidate daily and weekly analyses into final recommendation.
        Uses text-only LLM call (no chart image needed).
        """
        try:
            logger.info(f"Consolidating analyses for {symbol} (strategy_id={strategy_id}, lang={language})")
            
            # Load consolidation prompt template from database
            prompt_obj, prompt_text = self._get_prompt_template(
                template_type="consolidation",
                language=language,
                strategy_id=strategy_id
            )
            
            # Render prompt with Jinja2
            prompt_context = PromptContextBuilder.build_consolidation_context(
                symbol=symbol,
                analyses=[],  # Not using the analyses list here
                analysis_daily=daily_analysis,
                analysis_weekly=weekly_analysis
            )
            rendered_prompt = self.renderer.render(prompt_text, prompt_context, strict=False)
            
            logger.debug(f"Rendered consolidation prompt ({len(rendered_prompt)} chars) from template: {prompt_obj.name if prompt_obj else 'fallback'}")
            
            # Call LLM (text-only, no image)
            if self.provider == "openai":
                response_text = await self._call_openai_text(rendered_prompt)
            elif self.provider == "gemini":
                response_text = await self._call_gemini_text(rendered_prompt)
            else:
                raise ValueError(f"Unknown LLM provider: {self.provider}")
            
            # Parse response
            parsed = self._parse_response(response_text)
            
            # Build result with prompt metadata
            result = {
                "raw_text": response_text,
                "parsed_signal": parsed,
                "model_used": self.model,
                "provider": self.provider,
                "analyzed_at": datetime.now(),
                # Prompt metadata
                "prompt_template_id": prompt_obj.id if prompt_obj else None,
                "prompt_version": prompt_obj.template_version if prompt_obj else None,
                "prompt_type": "consolidation",
            }
            
            logger.info(f"Consolidation complete for {symbol} (prompt_id={result['prompt_template_id']}, version={result['prompt_version']})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error consolidating analyses: {str(e)}")
            raise
    
    async def _call_openai_text(self, prompt: str) -> str:
        """Call OpenAI API for text-only (no image) completion."""
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}"
            }
            
            payload = {
                "model": settings.OPENAI_MODEL,  # Use text model, not vision
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": settings.LLM_VISION_MAX_TOKENS,
                "temperature": settings.LLM_VISION_TEMPERATURE
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{settings.OPENAI_API_BASE}/chat/completions",
                    headers=headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                
                return result["choices"][0]["message"]["content"]
                
        except Exception as e:
            logger.error(f"OpenAI text API error: {str(e)}")
            raise
    
    async def _call_gemini_text(self, prompt: str) -> str:
        """Call Google Gemini for text-only completion."""
        if not settings.GEMINI_API_KEY:
            raise ValueError(
                "Gemini API key not configured. "
                "Please set GEMINI_API_KEY in .env file. "
                "Get your key from https://aistudio.google.com/app/apikey"
            )

        model = (self.model or "").strip()
        if not model:
            raise ValueError(
                "Gemini model not configured. "
                "Please set GEMINI_MODEL (recommended) or LLM_VISION_MODEL in .env."
            )

        api_endpoint = self._gemini_generate_content_endpoint(model=model)
        headers = {"Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": settings.LLM_VISION_MAX_TOKENS,
                "temperature": settings.LLM_VISION_TEMPERATURE,
            },
        }

        max_attempts = max(1, int(getattr(settings, "LLM_RETRY_ATTEMPTS", 1)))
        base_delay = max(0, int(getattr(settings, "LLM_RETRY_DELAY", 0)))
        retryable_status_codes = {429, 500, 502, 503, 504}

        async with httpx.AsyncClient(timeout=self.timeout, verify=False) as client:
            last_error: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await client.post(
                        api_endpoint,
                        headers=headers,
                        params={"key": settings.GEMINI_API_KEY},
                        json=payload,
                    )

                    if response.status_code in retryable_status_codes and attempt < max_attempts:
                        wait_s = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Gemini API returned {response.status_code}; retrying in {wait_s}s "
                            f"(attempt {attempt}/{max_attempts})"
                        )
                        await asyncio.sleep(wait_s)
                        continue

                    response.raise_for_status()
                    data = response.json()
                    return self._extract_gemini_text(data)

                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    last_error = e
                    if attempt < max_attempts:
                        wait_s = base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Gemini API connection/timeout error; retrying in {wait_s}s "
                            f"(attempt {attempt}/{max_attempts}): {type(e).__name__}: {e}"
                        )
                        await asyncio.sleep(wait_s)
                        continue
                    raise ValueError(
                        f"Cannot connect to Gemini API endpoint: {settings.GEMINI_API_BASE}. "
                        "Check your network and GEMINI_API_BASE."
                    ) from e
                except httpx.HTTPStatusError as e:
                    last_error = e
                    body_preview = (e.response.text or "")[:500]
                    raise ValueError(
                        f"Gemini API error: {e.response.status_code} - {body_preview}"
                    ) from e
                except Exception as e:
                    last_error = e
                    raise

            if last_error:
                raise last_error

        raise ValueError("Gemini request failed without a response")
