"""Generate trading signals by combining indicator + LLM analysis."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from backend.models.trading_signal import TradingSignal
from backend.services.llm_service import LLMService


class SignalGenerator:
    """Combines technical indicators with LLM guidance to produce trading signals."""

    def __init__(self, db_session=None):
        self.db = db_session
        self.llm_service = LLMService(db_session=db_session)

    async def generate_signal(
        self,
        strategy_id: int,
        symbol: str,
        conid: int,
        market_data: Dict[str, Any],
        indicator_data: Dict[str, Any],
        chart_urls: Optional[Dict[str, str]] = None,
        llm_enabled: bool = True,
        llm_language: str = "en",
    ) -> TradingSignal:
        """Generate a trading signal object (and persist if db session provided)."""
        current_price = self._get_current_price(market_data)
        technical_analysis = await self._analyze_indicators(indicator_data, current_price)

        llm_details: Dict[str, Any] = {}
        if llm_enabled and chart_urls:
            llm_details = await self._get_llm_analysis(
                strategy_id=strategy_id,
                symbol=symbol,
                chart_urls=chart_urls,
                language=llm_language,
                current_price=current_price,
                indicators=indicator_data,
            )

        combined = await self._combine_analyses(technical_analysis, llm_details, current_price)
        confidence = self._calculate_confidence(technical_analysis, llm_details)
        llm_recommendation = llm_details.get("parsed_signal") or {}
        levels = self._calculate_trading_levels(
            combined["signal_type"],
            current_price=current_price,
            indicator_data=indicator_data,
            llm_recommendation=llm_recommendation,
        )

        signal = TradingSignal(
            strategy_id=strategy_id,
            symbol=symbol,
            signal_type=combined["signal_type"],
            trend=combined.get("trend"),
            confidence=confidence,
            reasoning="\n".join(combined.get("reasoning", [])),
            entry_price_low=levels["entry_low"],
            entry_price_high=levels["entry_high"],
            stop_loss=levels["stop_loss"],
            target_conservative=levels["target_conservative"],
            target_aggressive=levels["target_aggressive"],
            r_multiple_conservative=levels["r_conservative"],
            r_multiple_aggressive=levels["r_aggressive"],
            position_size_percent=levels["position_size"],
            confirmation_signals=self._build_confirmation_snapshot(technical_analysis, llm_details),
            chart_url_daily=(chart_urls or {}).get("1d"),
            chart_url_weekly=(chart_urls or {}).get("1w"),
            language=llm_language,
            model_used=llm_details.get("model_used"),
            provider=llm_details.get("provider"),
            prompt_template_id=llm_details.get("prompt_template_id"),
            prompt_version=llm_details.get("prompt_version"),
            prompt_type=llm_details.get("prompt_type"),
        )

        if self.db:
            self.db.add(signal)
            try:
                self.db.commit()
                self.db.refresh(signal)
            except Exception:
                self.db.rollback()
                raise

        return signal

    def _get_current_price(self, market_data: Dict[str, Any]) -> Optional[float]:
        """Extract the latest close from market data."""
        for timeframe in ("1d", "1h", "1w"):
            candles = market_data.get(timeframe)
            if candles:
                latest = candles[-1]
                for field in ("close", "price", "last"):
                    if latest.get(field) is not None:
                        return float(latest[field])
        return None

    async def _analyze_indicators(
        self,
        indicator_data: Dict[str, Any],
        current_price: Optional[float],
    ) -> Dict[str, Any]:
        """Derive a technical stance from indicator snapshots."""
        if not indicator_data:
            return {
                "signal_type": "HOLD",
                "trend": "neutral",
                "reasoning": ["Insufficient indicator data"],
                "summary": "No data",
            }

        data = indicator_data.get("1d") or next(iter(indicator_data.values()))
        bullish = bearish = 0
        reasoning = []

        supertrend = data.get("SuperTrend")
        if supertrend:
            if supertrend.get("is_bullish"):
                bullish += 1
                reasoning.append("SuperTrend indicates bullish trend")
            else:
                bearish += 1
                reasoning.append("SuperTrend shows bearish pressure")

        macd = data.get("MACD")
        if macd and macd.get("current_macd") is not None and macd.get("current_signal") is not None:
            delta = macd["current_macd"] - macd["current_signal"]
            if delta > 0:
                bullish += 1
                reasoning.append("MACD histogram is positive")
            else:
                bearish += 1
                reasoning.append("MACD histogram is negative")

        rsi = data.get("RSI")
        if rsi and rsi.get("current") is not None:
            if rsi["current"] > 60:
                bullish += 1
                reasoning.append(f"RSI {rsi['current']:.1f} in bullish zone")
            elif rsi["current"] < 40:
                bearish += 1
                reasoning.append(f"RSI {rsi['current']:.1f} in bearish zone")
            else:
                reasoning.append(f"RSI neutral at {rsi['current']:.1f}")

        signal_type = "HOLD"
        if bullish - bearish >= 2:
            signal_type = "BUY"
        elif bearish - bullish >= 2:
            signal_type = "SELL"

        trend = "neutral"
        if signal_type == "BUY":
            trend = "bullish"
        elif signal_type == "SELL":
            trend = "bearish"

        summary = f"Technical analysis suggests {signal_type}"
        return {
            "signal_type": signal_type,
            "trend": trend,
            "reasoning": reasoning,
            "summary": summary,
            "current_price": current_price,
            "indicators": data,
        }

    async def _combine_analyses(
        self,
        technical: Dict[str, Any],
        llm_analysis: Dict[str, Any],
        current_price: Optional[float],
    ) -> Dict[str, Any]:
        """Merge technical verdict with LLM insights."""
        if not llm_analysis:
            return technical

        result = technical.copy()
        parsed = llm_analysis.get("parsed_signal") or {}
        llm_signal = parsed.get("signal")
        if llm_signal:
            # Favor LLM when technical is undecided
            if technical["signal_type"] == "HOLD":
                result["signal_type"] = llm_signal
                result["trend"] = parsed.get("trend") or result.get("trend")
            elif llm_signal == technical["signal_type"]:
                result["reasoning"].append("LLM analysis confirmed the technical signal")
            else:
                result["reasoning"].append(f"LLM suggested {llm_signal}, monitoring divergence")

        # Attach textual analysis for downstream use
        for key in ("daily_analysis", "weekly_analysis"):
            if llm_analysis.get(key):
                result.setdefault("llm_notes", {})[key] = llm_analysis[key]

        result["llm_signal"] = llm_signal
        result["llm_confidence"] = parsed.get("confidence")
        result["current_price"] = current_price
        return result

    def _calculate_confidence(
        self,
        technical: Dict[str, Any],
        llm_analysis: Dict[str, Any],
    ) -> float:
        """Blend signal confidence."""
        base = 0.5 if technical["signal_type"] == "HOLD" else 0.65
        parsed = llm_analysis.get("parsed_signal") if llm_analysis else None
        llm_signal = parsed.get("signal") if parsed else None
        llm_conf = parsed.get("confidence") if parsed else None

        if parsed:
            if llm_signal == technical["signal_type"]:
                base = min(1.0, (base + (llm_conf or 0.7)) / 2 + 0.1)
            elif llm_signal and llm_signal != technical["signal_type"]:
                base = max(0.2, (base + (llm_conf or 0.4)) / 2 - 0.1)
            elif llm_conf:
                base = min(1.0, (base + llm_conf) / 2)

        return round(max(0.0, min(1.0, base)), 4)

    def _calculate_trading_levels(
        self,
        signal_type: str,
        current_price: Optional[float],
        indicator_data: Dict[str, Any],
        llm_recommendation: Dict[str, Any],
    ) -> Dict[str, Optional[float]]:
        """Derive entry/stop/target levels with ATR heuristics."""
        price = current_price or llm_recommendation.get("entry_low") or 0.0
        atr = self._extract_indicator(indicator_data, "ATR") or 0.0
        buffer = atr if atr else price * 0.01

        entry_low = price - buffer
        entry_high = price + buffer
        if signal_type == "SELL":
            entry_low, entry_high = price + buffer, price - buffer

        stop_loss = price - 2 * buffer if signal_type != "SELL" else price + 2 * buffer
        target_conservative = price + 3 * buffer if signal_type != "SELL" else price - 3 * buffer
        target_aggressive = price + 5 * buffer if signal_type != "SELL" else price - 5 * buffer

        r_denominator = abs(price - stop_loss) or buffer
        r_conservative = abs(target_conservative - price) / r_denominator
        r_aggressive = abs(target_aggressive - price) / r_denominator

        position_size = 2.0 if signal_type != "HOLD" else 1.0

        levels = {
            "entry_low": entry_low,
            "entry_high": entry_high,
            "stop_loss": stop_loss,
            "target_conservative": target_conservative,
            "target_aggressive": target_aggressive,
            "r_conservative": round(r_conservative, 3),
            "r_aggressive": round(r_aggressive, 3),
            "position_size": position_size,
        }

        # Override with any LLM-provided numbers
        for key in ("entry_low", "entry_high", "stop_loss", "target_conservative", "target_aggressive"):
            if llm_recommendation.get(key) is not None:
                levels[key] = float(llm_recommendation[key])

        return levels

    async def _get_llm_analysis(
        self,
        strategy_id: int,
        symbol: str,
        chart_urls: Dict[str, str],
        language: str,
        current_price: Optional[float],
        indicators: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Call the LLM service for available charts."""
        analysis: Dict[str, Any] = {}
        parsed_signal: Optional[Dict[str, Any]] = None

        for timeframe, url in chart_urls.items():
            response = await self.llm_service.analyze_chart(
                chart_url=url,
                prompt_template="analysis",
                symbol=symbol,
                strategy_id=strategy_id,
                language=language,
                current_price=current_price,
                indicators=indicators,
            )
            key = "daily_analysis" if timeframe.lower() in ("1d", "daily") else "weekly_analysis"
            analysis[key] = response.get("raw_text")
            parsed_signal = response.get("parsed_signal") or parsed_signal

            for meta_key in ("model_used", "provider", "prompt_template_id", "prompt_version", "prompt_type"):
                if response.get(meta_key):
                    analysis[meta_key] = response[meta_key]

        if parsed_signal:
            analysis["parsed_signal"] = parsed_signal
        analysis.setdefault("prompt_type", "analysis")
        return analysis

    def _build_confirmation_snapshot(
        self,
        technical: Dict[str, Any],
        llm_analysis: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare a compact confirmation summary for persistence."""
        snapshot = {
            "technical_signal": technical.get("signal_type"),
            "trend": technical.get("trend"),
            "llm_signal": (llm_analysis.get("parsed_signal") or {}).get("signal") if llm_analysis else None,
        }
        confirmations = 1 if snapshot["technical_signal"] in {"BUY", "SELL"} else 0
        if snapshot["llm_signal"] == snapshot["technical_signal"] and snapshot["llm_signal"] is not None:
            confirmations += 1
        snapshot["confirmed_count"] = confirmations
        snapshot["passed"] = confirmations >= 2 or snapshot["technical_signal"] == snapshot["llm_signal"]
        return snapshot

    @staticmethod
    def _extract_indicator(indicator_data: Dict[str, Any], indicator_name: str) -> Optional[float]:
        """Helper to fetch indicator numeric values."""
        for timeframe_data in indicator_data.values():
            indicator = timeframe_data.get(indicator_name)
            if indicator and indicator.get("current") is not None:
                return float(indicator["current"])
        return None
