from __future__ import annotations

import html
import math
import re
from datetime import datetime, timezone
from typing import Any


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _fmt_num(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def _fmt_int(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def _to_billions(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 1_000_000_000.0


def _fmt_billions(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value / 1_000_000_000.0:,.2f}"


def _choose_two_years(metric_maps: dict[str, dict[int, float]]) -> tuple[int | None, int | None]:
    years = set()
    for values in metric_maps.values():
        years.update(values.keys())
    if len(years) < 2:
        return None, None
    ordered = sorted(years, reverse=True)
    return ordered[1], ordered[0]


def _yoy(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev in (None, 0):
        return None
    return ((curr - prev) / abs(prev)) * 100.0


def _technical_signal(value: float | None, low: float, high: float, label: str) -> str:
    if value is None:
        return f"{label}: unavailable."
    if value < low:
        return f"{label}: bearish/weak."
    if value > high:
        return f"{label}: strong/overheated."
    return f"{label}: neutral."


def _quality_dot(flag: bool) -> str:
    return "🟢" if flag else "🔴"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _sma(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < period:
            out.append(None)
            continue
        window = values[idx + 1 - period : idx + 1]
        out.append(sum(window) / period)
    return out


def _bollinger(values: list[float], period: int = 20, n_std: float = 2.0) -> tuple[list[float | None], list[float | None], list[float | None]]:
    mid = _sma(values, period)
    upper: list[float | None] = []
    lower: list[float | None] = []
    for idx, center in enumerate(mid):
        if center is None:
            upper.append(None)
            lower.append(None)
            continue
        window = values[idx + 1 - period : idx + 1]
        variance = sum((x - center) ** 2 for x in window) / period
        std = math.sqrt(variance)
        upper.append(center + (n_std * std))
        lower.append(center - (n_std * std))
    return upper, mid, lower


def _rsi_badge(rsi14: float | None) -> tuple[str, str]:
    if rsi14 is None:
        return "Unavailable", "badge-gray"
    if rsi14 < 30:
        return "Oversold", "badge-red"
    if rsi14 < 45:
        return "Weak", "badge-amber"
    if rsi14 < 70:
        return "Neutral", "badge-blue"
    return "Overbought", "badge-red"


def _change_class(value: float | None) -> str:
    if value is None:
        return "text-muted"
    return "text-green" if value >= 0 else "text-red"


def _to_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(max_value, parsed))


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _clip_text(value: str, limit: int = 1800) -> str:
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + " ..."


def _normalize_headline(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _dedupe_news_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicates_removed = 0

    for item in items:
        headline_key = _normalize_headline(item.get("headline"))
        provider_code = str(item.get("provider_code") or "").strip().upper()
        article_id = str(item.get("article_id") or "").strip()
        dedupe_key = headline_key or f"{provider_code}|{article_id}"
        if not dedupe_key:
            deduped.append(item)
            continue
        if dedupe_key in seen:
            duplicates_removed += 1
            continue
        seen.add(dedupe_key)
        deduped.append(item)

    return deduped, duplicates_removed


def _build_price_svg(bars: list[dict[str, Any]]) -> str:
    if len(bars) < 3:
        return "<div class='empty-chart'>Not enough bars to render chart.</div>"

    window = bars[-120:]
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    volumes: list[float] = []

    for bar in window:
        close = _safe_float(bar.get("close"))
        high = _safe_float(bar.get("high"))
        low = _safe_float(bar.get("low"))
        vol = _safe_float(bar.get("volume"))
        if close is None or high is None or low is None:
            continue
        closes.append(close)
        highs.append(high)
        lows.append(low)
        volumes.append(vol or 0.0)

    if len(closes) < 3:
        return "<div class='empty-chart'>Bars are incomplete for chart rendering.</div>"

    sma20 = _sma(closes, 20)
    bb_upper, _, bb_lower = _bollinger(closes, 20, 2.0)

    width = 1080
    height = 470
    margin_l = 52
    margin_r = 20
    margin_t = 20
    margin_b = 24
    price_h = 300
    vol_h = 100
    price_top = margin_t
    price_bottom = price_top + price_h
    vol_top = price_bottom + 26
    vol_bottom = vol_top + vol_h
    plot_w = width - margin_l - margin_r

    n = len(closes)
    step = plot_w / max(1, n)
    candle_w = max(1.0, step * 0.62)

    min_price = min(lows)
    max_price = max(highs)
    if max_price <= min_price:
        max_price = min_price + 1.0
    price_span = max_price - min_price

    max_volume = max(volumes) if volumes else 1.0
    if max_volume <= 0:
        max_volume = 1.0

    def px(index: int) -> float:
        return margin_l + (index + 0.5) * step

    def py(price: float) -> float:
        ratio = (price - min_price) / price_span
        return price_bottom - ratio * price_h

    def vy(volume: float) -> float:
        ratio = volume / max_volume
        return vol_bottom - ratio * vol_h

    wick_parts: list[str] = []
    body_parts: list[str] = []
    vol_parts: list[str] = []

    for idx in range(n):
        bar = window[idx]
        op = _safe_float(bar.get("open"))
        cl = _safe_float(bar.get("close"))
        hi = _safe_float(bar.get("high"))
        lo = _safe_float(bar.get("low"))
        vol = _safe_float(bar.get("volume")) or 0.0
        if op is None or cl is None or hi is None or lo is None:
            continue

        x = px(idx)
        y_hi = py(hi)
        y_lo = py(lo)
        y_open = py(op)
        y_close = py(cl)
        bullish = cl >= op
        color = "#22c55e" if bullish else "#ef4444"

        wick_parts.append(
            f"<line x1='{x:.2f}' y1='{y_hi:.2f}' x2='{x:.2f}' y2='{y_lo:.2f}' stroke='{color}' stroke-width='1.1'/>"
        )

        rect_y = min(y_open, y_close)
        rect_h = max(1.2, abs(y_open - y_close))
        body_parts.append(
            f"<rect x='{(x - candle_w / 2):.2f}' y='{rect_y:.2f}' width='{candle_w:.2f}' height='{rect_h:.2f}' fill='{color}' rx='1.2'/>"
        )

        y_vol = vy(vol)
        vol_parts.append(
            f"<rect x='{(x - candle_w / 2):.2f}' y='{y_vol:.2f}' width='{candle_w:.2f}' height='{(vol_bottom - y_vol):.2f}' fill='{color}' opacity='0.5'/>"
        )

    def _polyline(series: list[float | None], stroke: str, stroke_width: float = 2.0, opacity: float = 1.0) -> str:
        pts: list[str] = []
        for idx, value in enumerate(series):
            if value is None:
                continue
            pts.append(f"{px(idx):.2f},{py(value):.2f}")
        if len(pts) < 2:
            return ""
        return (
            f"<polyline fill='none' stroke='{stroke}' stroke-width='{stroke_width}' "
            f"stroke-linecap='round' stroke-linejoin='round' opacity='{opacity}' points='{' '.join(pts)}'/>"
        )

    def _band_fill(upper: list[float | None], lower: list[float | None]) -> str:
        top_pts: list[tuple[float, float]] = []
        bot_pts: list[tuple[float, float]] = []
        for idx in range(n):
            up = upper[idx]
            lo = lower[idx]
            if up is None or lo is None:
                continue
            top_pts.append((px(idx), py(up)))
            bot_pts.append((px(idx), py(lo)))
        if len(top_pts) < 2 or len(bot_pts) < 2:
            return ""
        polygon = top_pts + list(reversed(bot_pts))
        pts = " ".join(f"{x:.2f},{y:.2f}" for x, y in polygon)
        return f"<polygon points='{pts}' fill='#2563eb' opacity='0.14'/>"

    grid_lines = "".join(
        [
            f"<line x1='{margin_l}' y1='{y}' x2='{width - margin_r}' y2='{y}' stroke='#334155' stroke-width='1' opacity='0.45'/>"
            for y in (
                price_top,
                price_top + price_h * 0.25,
                price_top + price_h * 0.5,
                price_top + price_h * 0.75,
                price_bottom,
            )
        ]
    )

    labels = [
        f"<text x='{width - margin_r - 6}' y='{price_top + 14}' class='axis'>{max_price:,.2f}</text>",
        f"<text x='{width - margin_r - 6}' y='{price_bottom - 4}' class='axis'>{min_price:,.2f}</text>",
        f"<text x='{width - margin_r - 6}' y='{vol_top + 14}' class='axis'>Vol {max_volume:,.0f}</text>",
    ]

    divider = f"<line x1='{margin_l}' y1='{vol_top - 10}' x2='{width - margin_r}' y2='{vol_top - 10}' stroke='#334155' stroke-width='1'/>"

    sma_line = _polyline(sma20, "#f59e0b", 2.2, 0.95)
    bb_u = _polyline(bb_upper, "#60a5fa", 1.6, 0.8)
    bb_l = _polyline(bb_lower, "#60a5fa", 1.6, 0.8)
    bb_fill = _band_fill(bb_upper, bb_lower)

    return (
        f"<svg viewBox='0 0 {width} {height}' role='img' aria-label='Price chart'>"
        f"<rect x='0' y='0' width='{width}' height='{height}' fill='#0b1220' rx='12'/>"
        f"{grid_lines}{divider}{bb_fill}{''.join(wick_parts)}{''.join(body_parts)}"
        f"{sma_line}{bb_u}{bb_l}{''.join(vol_parts)}{''.join(labels)}"
        "</svg>"
    )


def _build_history_select(history: dict[str, Any]) -> str:
    rows = history.get("rows") if isinstance(history, dict) else []
    if not isinstance(rows, list) or not rows:
        return "<div class='muted'>No persisted history rows found.</div>"

    options: list[str] = []
    for row in rows[:50]:
        if not isinstance(row, dict):
            continue
        row_symbol = html.escape(str(row.get("symbol") or ""))
        generated_at = html.escape(str(row.get("generated_at") or ""))
        row_id = html.escape(str(row.get("id") or ""))
        options.append(f"<option value='{row_id}'>{row_symbol} | {generated_at}</option>")

    if not options:
        return "<div class='muted'>No persisted history rows found.</div>"

    return "<select class='history-select'>" + "".join(options) + "</select>"


def _top_headlines(items: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items[:limit]:
        compact.append(
            {
                "headline": str(item.get("headline") or "Untitled"),
                "published_at": item.get("published_at"),
                "provider_code": item.get("provider_code"),
                "article_id": item.get("article_id"),
                "has_full_text": bool(item.get("has_full_text")),
            }
        )
    return compact


def main(
    symbol: str,
    fetched: dict[str, Any],
    controls: dict = None,
    history: dict = None,
) -> dict[str, Any]:
    symbol = symbol.strip().upper()

    latest = fetched.get("latest", {}) if isinstance(fetched, dict) else {}
    ind = fetched.get("indicators", {}) if isinstance(fetched, dict) else {}
    bars = fetched.get("price_series", []) if isinstance(fetched, dict) else []
    fundamentals = fetched.get("fundamentals_raw", {}) if isinstance(fetched, dict) else {}
    news = fetched.get("news", {}) if isinstance(fetched, dict) else {}
    quality = fetched.get("data_quality", {}) if isinstance(fetched, dict) else {}

    controls = controls if isinstance(controls, dict) else {}
    history = history if isinstance(history, dict) else {"count": 0, "rows": []}
    news = news if isinstance(news, dict) else {}
    resolved_news_compact_mode = _coerce_bool(controls.get("news_compact_mode"), True)
    resolved_news_markdown_top_n = _to_int(controls.get("news_markdown_top_n"), 8, 1, 50)
    news_items_raw = news.get("items")
    news_items = [item for item in news_items_raw if isinstance(item, dict)] if isinstance(news_items_raw, list) else []
    deduped_news_items, duplicate_headlines_removed = _dedupe_news_items(news_items)
    news_headlines_count = len(deduped_news_items)
    news_articles_count = sum(1 for item in deduped_news_items if item.get("has_full_text"))
    news_requested_days = news.get("requested_window_days", controls.get("news_days", "N/A"))
    news_requested_limit = news.get("requested_limit", controls.get("news_limit", "N/A"))
    providers_raw = news.get("providers")
    news_providers = [p for p in providers_raw if isinstance(p, dict)] if isinstance(providers_raw, list) else []
    provider_codes = [str(p.get("code") or "").strip() for p in news_providers if str(p.get("code") or "").strip()]

    y_prev, y_curr = _choose_two_years(
        {
            "sales": fundamentals.get("sales", {}),
            "operating_income": fundamentals.get("operating_income", {}),
            "net_income": fundamentals.get("net_income", {}),
        }
    )

    sales_prev = fundamentals.get("sales", {}).get(y_prev) if y_prev else None
    sales_curr = fundamentals.get("sales", {}).get(y_curr) if y_curr else None
    op_prev = fundamentals.get("operating_income", {}).get(y_prev) if y_prev else None
    op_curr = fundamentals.get("operating_income", {}).get(y_curr) if y_curr else None
    ni_prev = fundamentals.get("net_income", {}).get(y_prev) if y_prev else None
    ni_curr = fundamentals.get("net_income", {}).get(y_curr) if y_curr else None

    fundamentals_rows = [
        {
            "metric": "Sales",
            f"{y_prev or 'prev'}_billions_usd": _to_billions(sales_prev),
            f"{y_curr or 'curr'}_billions_usd": _to_billions(sales_curr),
            "yoy_change_pct": _yoy(sales_curr, sales_prev),
        },
        {
            "metric": "Operating Income",
            f"{y_prev or 'prev'}_billions_usd": _to_billions(op_prev),
            f"{y_curr or 'curr'}_billions_usd": _to_billions(op_curr),
            "yoy_change_pct": _yoy(op_curr, op_prev),
        },
        {
            "metric": "Net Income",
            f"{y_prev or 'prev'}_billions_usd": _to_billions(ni_prev),
            f"{y_curr or 'curr'}_billions_usd": _to_billions(ni_curr),
            "yoy_change_pct": _yoy(ni_curr, ni_prev),
        },
    ]

    close = _safe_float(latest.get("close"))
    prev_close = _safe_float(latest.get("previous_close"))
    daily_abs = _safe_float(latest.get("daily_change"))
    daily_pct = _safe_float(latest.get("daily_change_pct"))
    volume = _safe_float(latest.get("volume"))

    sma20 = _safe_float(ind.get("sma20"))
    rsi14 = _safe_float(ind.get("rsi14"))
    macd = _safe_float(ind.get("macd"))
    macd_signal = _safe_float(ind.get("macd_signal"))
    macd_hist = _safe_float(ind.get("macd_hist"))
    bb_upper = _safe_float(ind.get("bollinger_upper"))
    bb_mid = _safe_float(ind.get("bollinger_mid"))
    bb_lower = _safe_float(ind.get("bollinger_lower"))

    closes = [_safe_float(row.get("close")) for row in bars if isinstance(row, dict)]
    highs = [_safe_float(row.get("high")) for row in bars if isinstance(row, dict)]
    lows = [_safe_float(row.get("low")) for row in bars if isinstance(row, dict)]
    closes = [v for v in closes if v is not None]
    highs = [v for v in highs if v is not None]
    lows = [v for v in lows if v is not None]

    nday_high = max(highs) if highs else None
    nday_low = min(lows) if lows else None

    if sma20 and close:
        if close > sma20:
            trend_position = "above 20-day SMA, indicating short-term strength"
            sma_position = "Above SMA20"
            sma_class = "badge-green"
        else:
            trend_position = "below 20-day SMA, indicating short-term weakness"
            sma_position = "Below SMA20"
            sma_class = "badge-red"
    else:
        trend_position = "trend positioning unavailable"
        sma_position = "Unavailable"
        sma_class = "badge-gray"

    if rsi14 is None:
        rsi_text = "RSI unavailable."
    elif rsi14 < 30:
        rsi_text = "RSI is in oversold territory."
    elif rsi14 < 50:
        rsi_text = "RSI is below 50, showing weak momentum."
    elif rsi14 < 70:
        rsi_text = "RSI is in a neutral-to-constructive range."
    else:
        rsi_text = "RSI is in overbought territory."

    if macd is None or macd_signal is None:
        macd_text = "MACD unavailable."
    else:
        direction = "bullish" if macd >= macd_signal else "bearish"
        macd_text = f"MACD is {direction} (MACD={macd:.3f}, Signal={macd_signal:.3f}, Hist={macd_hist or 0:.3f})."

    fundamentals_ready = bool(quality.get("fundamentals_available")) and y_prev and y_curr
    if fundamentals_ready:
        fundamentals_intro = (
            f"{symbol} shows mixed fundamentals between {y_prev} and {y_curr}. "
            f"Revenue change: {_fmt_pct(_yoy(sales_curr, sales_prev))}, "
            f"operating income change: {_fmt_pct(_yoy(op_curr, op_prev))}, "
            f"net income change: {_fmt_pct(_yoy(ni_curr, ni_prev))}."
        )
    else:
        fundamentals_intro = (
            f"IBKR fundamentals for {symbol} are unavailable or incomplete in this run. "
            "Technical analysis is still computed from historical bars."
        )

    technical_summary = (
        f"Current close: {_fmt_money(close)} ({_fmt_pct(daily_pct)} day-over-day). "
        f"Price is {trend_position}. "
        f"{rsi_text} {macd_text} "
        f"Bollinger range: {_fmt_money(bb_lower)} to {_fmt_money(bb_upper)} (mid {_fmt_money(bb_mid)})."
    )

    news_summary = (
        f"IBKR news pull returned {news_headlines_count} unique headline(s) and {news_articles_count} article text payload(s) "
        f"for the last {news_requested_days} day(s), request limit {news_requested_limit}."
    )
    if duplicate_headlines_removed:
        news_summary += f" {duplicate_headlines_removed} duplicate headline(s) were removed."
    if resolved_news_compact_mode:
        news_summary += f" Compact markdown mode is enabled (top {resolved_news_markdown_top_n} headlines)."
    else:
        news_summary += " Detailed markdown mode is enabled."

    value_view = (
        "For value investors: prioritize margin trend and earnings quality from upcoming statements."
        if fundamentals_ready
        else "For value investors: wait for a complete fundamentals pull before conviction sizing."
    )
    trader_view = "For traders: use the 20-day SMA and Bollinger bounds as tactical trigger levels."

    quality_notes = quality.get("notes", []) if isinstance(quality, dict) else []
    if not isinstance(quality_notes, list):
        quality_notes = [str(quality_notes)]
    if duplicate_headlines_removed:
        quality_notes = list(quality_notes) + [f"Removed {duplicate_headlines_removed} duplicate IBKR headline(s) in report rendering."]
        if isinstance(quality, dict):
            quality["notes"] = quality_notes

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    rsi_label, rsi_badge_class = _rsi_badge(rsi14)

    news_md_rows: list[str] = []
    if deduped_news_items:
        shown_md = min(len(deduped_news_items), resolved_news_markdown_top_n if resolved_news_compact_mode else 20)
        for item in deduped_news_items[:shown_md]:
            provider_code = str(item.get("provider_code") or "N/A")
            published_at = str(item.get("published_at") or "N/A")
            headline = str(item.get("headline") or "Untitled")
            news_md_rows.append(f"- **[{published_at}] ({provider_code})** {headline}")
        if len(deduped_news_items) > shown_md:
            news_md_rows.append(
                f"- Showing first {shown_md} items out of {len(deduped_news_items)}. Remaining headlines are omitted from the compact result."
            )
    else:
        news_md_rows.append("- No IBKR news items were available for this run.")

    report_markdown = f"""# Financial Performance Analysis ({symbol})

## Fundamentals
{fundamentals_intro}

### Financial Performance Table
| Metric | {y_prev or 'Prev'} (B USD) | {y_curr or 'Curr'} (B USD) | YoY Change (%) |
|---|---:|---:|---:|
| Sales | {_fmt_billions(sales_prev)} | {_fmt_billions(sales_curr)} | {_fmt_pct(_yoy(sales_curr, sales_prev))} |
| Operating Income | {_fmt_billions(op_prev)} | {_fmt_billions(op_curr)} | {_fmt_pct(_yoy(op_curr, op_prev))} |
| Net Income | {_fmt_billions(ni_prev)} | {_fmt_billions(ni_curr)} | {_fmt_pct(_yoy(ni_curr, ni_prev))} |

## Technical Analysis
{technical_summary}

- {_technical_signal(rsi14, 45, 70, 'RSI')}
- {_technical_signal(macd_hist, 0, 0.000001, 'MACD histogram')}

## Market News (IBKR)
{news_summary}

Providers: {", ".join(provider_codes) if provider_codes else "N/A"}

{chr(10).join(news_md_rows)}

## Summary & Investment Perspective
{value_view}

{trader_view}
"""

    return {
        "symbol": symbol,
        "latest": latest,
        "indicators": ind,
        "fundamentals_table": {
            "year_prev": y_prev,
            "year_curr": y_curr,
            "rows": fundamentals_rows,
        },
        "technical_summary": technical_summary,
        "investment_summary": {
            "value_investor_view": value_view,
            "trader_view": trader_view,
        },
        "news": {
            "requested_window_days": news_requested_days,
            "requested_limit": news_requested_limit,
            "compact_mode": resolved_news_compact_mode,
            "markdown_top_n": resolved_news_markdown_top_n,
            "provider_codes": provider_codes,
            "headlines_count": news_headlines_count,
            "articles_count": news_articles_count,
            "duplicates_removed": duplicate_headlines_removed,
            "top_headlines": _top_headlines(deduped_news_items),
        },
        "report_markdown": report_markdown,
        "data_quality": quality,
    }
