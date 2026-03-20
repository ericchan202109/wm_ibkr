"""Jinja2 renderer and context builders for prompt templates."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Optional, Set

from jinja2 import Environment, StrictUndefined, TemplateError, meta, Undefined
from jinja2.sandbox import SandboxedEnvironment

_renderer_singleton: Optional["PromptRenderer"] = None


class PromptRenderer:
    """Safe Jinja2 renderer with prompt-specific filters."""

    def __init__(self, enable_sandbox: bool = True):
        env_cls = SandboxedEnvironment if enable_sandbox else Environment
        self._strict_env = self._create_env(env_cls, strict=True)
        self._lenient_env = self._create_env(env_cls, strict=False)

    def _create_env(self, env_cls: type, strict: bool) -> Environment:
        undefined_cls = StrictUndefined if strict else Undefined
        env = env_cls(
            undefined=undefined_cls,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._register_filters(env)
        return env

    def _register_filters(self, env: Environment) -> None:
        env.filters["round_decimal"] = self._round_decimal
        env.filters["format_percent"] = self._format_percent
        env.filters["format_currency"] = self._format_currency
        env.filters["datetimeformat"] = self._datetimeformat
        env.filters["dateformat"] = self._datetimeformat
        env.filters["default_if_none"] = self._default_if_none
        env.filters["abs"] = abs  # Expose python's abs

    def render(self, template: str, context: Dict[str, Any], strict: bool = True) -> str:
        """Render a template string with the supplied context."""
        env = self._strict_env if strict else self._lenient_env
        env.globals["now"] = datetime.now()
        env.globals["today"] = date.today()

        try:
            jinja_template = env.from_string(template)
            return jinja_template.render(**context)
        except TemplateError as exc:
            raise ValueError(f"Template render error: {exc}") from exc

    def validate_template(self, template: str) -> (bool, Optional[str]):
        """Return (is_valid, error_message) for the template."""
        try:
            self._strict_env.parse(template)
            return True, None
        except TemplateError as exc:
            return False, str(exc)

    def get_undefined_variables(self, template: str, context: Dict[str, Any]) -> Set[str]:
        """Return undefined variables for debugging."""
        parsed = self._lenient_env.parse(template)
        referenced = meta.find_undeclared_variables(parsed)
        provided: Set[str] = set(context.keys()) | {"now", "today"}
        return referenced - provided

    @staticmethod
    def _decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @classmethod
    def _round_decimal(cls, value: Any, digits: int = 2) -> Any:
        dec = cls._decimal(value)
        if dec is None:
            return value
        quant = Decimal("1").scaleb(-digits)
        return float(dec.quantize(quant, rounding=ROUND_HALF_UP))

    @classmethod
    def _format_percent(cls, value: Any, digits: int = 2) -> str:
        dec = cls._decimal(value)
        if dec is None:
            return "N/A"
        percent = dec * Decimal(100)
        return f"{percent.quantize(Decimal('1.').scaleb(-digits), rounding=ROUND_HALF_UP)}%"

    @classmethod
    def _format_currency(cls, value: Any, currency: str = "USD") -> str:
        dec = cls._decimal(value)
        if dec is None:
            return f"{currency} 0.00"
        quantized = dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return f"{currency} {quantized:,.2f}"

    @staticmethod
    def _datetimeformat(value: Any, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
        if isinstance(value, (datetime, date)):
            dt = value if isinstance(value, datetime) else datetime.combine(value, datetime.min.time())
            return dt.strftime(fmt)
        return ""

    @staticmethod
    def _default_if_none(value: Any, fallback: Any = "") -> Any:
        return fallback if value is None else value


class PromptContextBuilder:
    """Utility helpers for building prompt rendering context."""

    @staticmethod
    def build_analysis_context(
        symbol: str,
        current_price: Optional[float],
        trend_data: Optional[Dict[str, Any]],
        indicator_data: Optional[Dict[str, Any]],
        strategy_params: Optional[Dict[str, Any]],
        previous_analyses: Optional[Iterable[Dict[str, Any]]] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "symbol": symbol,
            "current_price": current_price,
            "trend": trend_data or {},
            "indicators": indicator_data or {},
            "strategy": strategy_params or {},
            "previous_analyses": list(previous_analyses or []),
        }
        context.update(extra)
        return context

    @staticmethod
    def build_consolidation_context(
        daily_analysis_summary: str,
        weekly_analysis_summary: str,
        symbol: str,
        strategy_params: Optional[Dict[str, Any]],
        **extra: Any,
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "daily_analysis": daily_analysis_summary,
            "weekly_analysis": weekly_analysis_summary,
            "symbol": symbol,
            "strategy": strategy_params or {},
        }
        context.update(extra)
        return context


def get_prompt_renderer() -> PromptRenderer:
    """Return shared renderer instance."""
    global _renderer_singleton
    if _renderer_singleton is None:
        _renderer_singleton = PromptRenderer(enable_sandbox=True)
    return _renderer_singleton
