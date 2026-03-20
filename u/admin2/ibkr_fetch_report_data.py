import html
import math
import re
from datetime import datetime, timedelta
from typing import Any
from xml.etree import ElementTree as ET

from ib_insync import IB, Stock


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


def _sma(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < period:
            out.append(None)
            continue
        window = values[idx + 1 - period : idx + 1]
        out.append(sum(window) / period)
    return out


def _ema(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    k = 2.0 / (period + 1.0)
    prev: float | None = None
    for idx, value in enumerate(values):
        if idx + 1 < period:
            out.append(None)
            continue
        if prev is None:
            seed = values[idx + 1 - period : idx + 1]
            prev = sum(seed) / period
            out.append(prev)
            continue
        prev = (value * k) + (prev * (1.0 - k))
        out.append(prev)
    return out


def _rsi(values: list[float], period: int = 14) -> list[float | None]:
    if len(values) < 2:
        return [None for _ in values]
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    out: list[float | None] = [None]
    avg_gain: float | None = None
    avg_loss: float | None = None
    for idx in range(len(deltas)):
        if idx + 1 < period:
            out.append(None)
            continue
        if idx + 1 == period:
            avg_gain = sum(gains[:period]) / period
            avg_loss = sum(losses[:period]) / period
        else:
            assert avg_gain is not None and avg_loss is not None
            avg_gain = ((avg_gain * (period - 1)) + gains[idx]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[idx]) / period

        if avg_loss == 0:
            out.append(100.0)
            continue
        rs = avg_gain / avg_loss
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out


def _macd(values: list[float]) -> tuple[list[float | None], list[float | None], list[float | None]]:
    ema12 = _ema(values, 12)
    ema26 = _ema(values, 26)
    macd: list[float | None] = []
    for e12, e26 in zip(ema12, ema26):
        if e12 is None or e26 is None:
            macd.append(None)
        else:
            macd.append(e12 - e26)

    non_null_macd = [m for m in macd if m is not None]
    signal_source = _ema(non_null_macd, 9)

    signal: list[float | None] = []
    signal_idx = 0
    for m in macd:
        if m is None:
            signal.append(None)
            continue
        signal.append(signal_source[signal_idx])
        signal_idx += 1

    hist: list[float | None] = []
    for m, s in zip(macd, signal):
        if m is None or s is None:
            hist.append(None)
        else:
            hist.append(m - s)
    return macd, signal, hist


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


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(str(value).replace(",", "").strip())
    except ValueError:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _year_from_attrs(attrs: dict[str, str]) -> int | None:
    for key in ("year", "fiscalYear", "fiscal_year", "period", "endDate", "asOfDate", "date"):
        raw = attrs.get(key)
        if not raw:
            continue
        m = re.search(r"(20\d{2})", raw)
        if m:
            return int(m.group(1))
    return None


def _canonical_metric(name: str) -> str | None:
    n = name.lower()
    if any(token in n for token in ("revenue", "sales", "totalrevenue", "srev")):
        return "sales"
    if any(token in n for token in ("operatingincome", "operating income", "opincome", "sopi", "ebit")):
        return "operating_income"
    if any(token in n for token in ("netincome", "net income", "ninc", "profitloss")):
        return "net_income"
    return None


def _extract_metrics_from_xml(xml_text: str | None) -> dict[str, dict[int, float]]:
    metrics: dict[str, dict[int, float]] = {
        "sales": {},
        "operating_income": {},
        "net_income": {},
    }
    if not xml_text:
        return metrics

    try:
        root = ET.fromstring(xml_text)

        for period in root.iter():
            period_attrs = {k: str(v) for k, v in period.attrib.items()}
            period_year = _year_from_attrs(period_attrs)
            period_tag = period.tag.split("}")[-1].lower()
            if period_year is None and "period" not in period_tag:
                continue
            for child in period.iter():
                if child is period:
                    continue
                child_attrs = {k: str(v) for k, v in child.attrib.items()}
                raw_name = " ".join(
                    [
                        child.tag.split("}")[-1],
                        child_attrs.get("coaCode", ""),
                        child_attrs.get("code", ""),
                        child_attrs.get("name", ""),
                        child_attrs.get("field", ""),
                        child_attrs.get("description", ""),
                    ]
                ).strip()
                metric = _canonical_metric(raw_name)
                if metric is None:
                    continue
                value = _safe_float(child_attrs.get("value") or child.text)
                if value is None:
                    continue
                year = _year_from_attrs(child_attrs) or period_year
                if year:
                    metrics[metric][year] = value

        for elem in root.iter():
            attrs = {k: str(v) for k, v in elem.attrib.items()}
            raw_name = " ".join(
                [
                    elem.tag.split("}")[-1],
                    attrs.get("coaCode", ""),
                    attrs.get("code", ""),
                    attrs.get("name", ""),
                    attrs.get("field", ""),
                    attrs.get("description", ""),
                ]
            ).strip()
            metric = _canonical_metric(raw_name)
            if metric is None:
                continue
            if metrics[metric]:
                continue

            year = _year_from_attrs(attrs)
            value = _safe_float(attrs.get("value") or elem.text)
            if year and value is not None:
                metrics[metric][year] = value
    except ET.ParseError:
        pass

    compact = re.sub(r"\s+", " ", xml_text)
    keyword_map = {
        "sales": ["totalrevenue", "salesrevenue", "revenue", "srev"],
        "operating_income": ["operatingincome", "operating income", "sopi", "opincome", "ebit"],
        "net_income": ["netincome", "net income", "ninc", "profitloss"],
    }
    for metric, keywords in keyword_map.items():
        if metrics[metric]:
            continue
        for keyword in keywords:
            for hit in re.finditer(keyword, compact, flags=re.IGNORECASE):
                window_start = max(0, hit.start() - 2200)
                window_end = min(len(compact), hit.end() + 2200)
                window = compact[window_start:window_end]
                for m in re.finditer(r"(20\d{2})[^0-9-]{0,80}(-?\d[\d,]*(?:\.\d+)?)", window):
                    year = int(m.group(1))
                    value = _safe_float(m.group(2))
                    if value is not None:
                        metrics[metric][year] = value
                if metrics[metric]:
                    break
            if metrics[metric]:
                break

    return metrics


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


def main(
    symbol: str,
    exchange: str = "NASDAQ",
    currency: str = "USD",
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    lookback_days: int = 180,
    bar_size: str = "1 day",
    use_rth: bool = True,
    news_days: int = 7,
    news_limit: int = 50,
    include_full_articles: bool = True,
) -> dict[str, Any]:
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol is required")

    resolved_news_days = _to_int(news_days, 7, 1, 30)
    resolved_news_limit = _to_int(news_limit, 50, 1, 100)
    resolved_include_full_articles = _coerce_bool(include_full_articles, True)

    ib = IB()
    fundamental_xml: str | None = None
    data_quality = {
        "ibkr_connected": False,
        "fundamentals_available": False,
        "bars_available": False,
        "news_headlines_available": False,
        "news_articles_available": False,
        "notes": [],
    }
    news_payload: dict[str, Any] = {
        "requested_window_days": resolved_news_days,
        "requested_limit": resolved_news_limit,
        "include_full_articles": resolved_include_full_articles,
        "providers": [],
        "headlines_count": 0,
        "articles_count": 0,
        "duplicates_removed": 0,
        "items": [],
    }
    try:
        ib.connect(host, port, clientId=client_id, timeout=12.0)
        data_quality["ibkr_connected"] = True

        contract = Stock(symbol, exchange, currency)
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"Unable to qualify IBKR contract for {symbol} on {exchange}")
        contract = qualified[0]

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=f"{max(lookback_days, 30)} D",
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=use_rth,
            formatDate=1,
            keepUpToDate=False,
        )
        bar_rows = [
            {
                "date": str(b.date),
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            }
            for b in bars
        ]
        if len(bar_rows) < 35:
            raise ValueError(f"Not enough historical bars for {symbol}; got {len(bar_rows)}")
        data_quality["bars_available"] = True

        closes = [row["close"] for row in bar_rows]
        sma20 = _sma(closes, 20)
        rsi14 = _rsi(closes, 14)
        macd, macd_signal, macd_hist = _macd(closes)
        bb_upper, bb_mid, bb_lower = _bollinger(closes, 20, 2.0)

        latest = bar_rows[-1]
        prev = bar_rows[-2]
        delta = latest["close"] - prev["close"]
        delta_pct = (delta / prev["close"] * 100.0) if prev["close"] else None

        try:
            fundamental_xml = ib.reqFundamentalData(contract, "ReportsFinStatements")
        except Exception as exc:
            data_quality["notes"].append(f"fundamentals_request_error: {exc}")

        fundamentals = _extract_metrics_from_xml(fundamental_xml)
        has_any_fundamental = any(bool(v) for v in fundamentals.values())
        data_quality["fundamentals_available"] = has_any_fundamental
        if not has_any_fundamental:
            data_quality["notes"].append("No parsable fundamentals in IBKR XML payload")

        try:
            providers = ib.reqNewsProviders()
            provider_rows: list[dict[str, str]] = []
            provider_codes: list[str] = []
            for provider in providers or []:
                code = str(
                    getattr(provider, "code", None)
                    or getattr(provider, "providerCode", None)
                    or ""
                ).strip()
                name = str(
                    getattr(provider, "name", None)
                    or getattr(provider, "providerName", None)
                    or ""
                ).strip()
                if not code:
                    continue
                provider_codes.append(code)
                provider_rows.append({"code": code, "name": name or code})

            news_payload["providers"] = provider_rows
            if not provider_codes:
                data_quality["notes"].append("No IBKR news providers returned")
            else:
                end_dt = datetime.utcnow()
                start_dt = end_dt - timedelta(days=resolved_news_days)
                headlines = ib.reqHistoricalNews(
                    contract.conId,
                    "+".join(provider_codes),
                    start_dt.strftime("%Y%m%d %H:%M:%S"),
                    end_dt.strftime("%Y%m%d %H:%M:%S"),
                    resolved_news_limit,
                )

                items: list[dict[str, Any]] = []
                article_failures = 0
                for headline_row in headlines or []:
                    provider_code = str(getattr(headline_row, "providerCode", "") or "").strip()
                    article_id = str(getattr(headline_row, "articleId", "") or "").strip()
                    headline = str(getattr(headline_row, "headline", "") or "").strip()
                    time_value = getattr(headline_row, "time", None)
                    if hasattr(time_value, "isoformat"):
                        published_at = str(time_value.isoformat())
                    elif time_value is None:
                        published_at = ""
                    else:
                        published_at = str(time_value)

                    article_text = ""
                    if resolved_include_full_articles and provider_code and article_id:
                        try:
                            article = ib.reqNewsArticle(provider_code, article_id)
                            article_text = str(getattr(article, "articleText", "") or "")
                        except Exception:
                            article_failures += 1

                    items.append(
                        {
                            "provider_code": provider_code,
                            "article_id": article_id,
                            "headline": headline,
                            "published_at": published_at,
                            "article_text": article_text,
                            "has_full_text": bool(article_text),
                            "source_contract_conid": contract.conId,
                        }
                    )

                deduped_items, duplicates_removed = _dedupe_news_items(items)
                news_payload["items"] = deduped_items
                news_payload["headlines_count"] = len(deduped_items)
                news_payload["articles_count"] = sum(1 for item in deduped_items if item.get("has_full_text"))
                news_payload["duplicates_removed"] = duplicates_removed
                data_quality["news_headlines_available"] = bool(deduped_items)
                data_quality["news_articles_available"] = bool(news_payload["articles_count"])

                if not deduped_items:
                    data_quality["notes"].append("No historical IBKR news headlines returned")
                if duplicates_removed:
                    data_quality["notes"].append(f"deduped_ibkr_news_headlines: {duplicates_removed}")
                if article_failures:
                    data_quality["notes"].append(f"news_article_fetch_failed_count: {article_failures}")
        except Exception as exc:
            data_quality["notes"].append(f"news_request_error: {exc}")

        return {
            "symbol": symbol,
            "contract": {
                "symbol": contract.symbol,
                "conid": contract.conId,
                "exchange": contract.exchange,
                "currency": contract.currency,
                "secType": contract.secType,
                "primaryExchange": getattr(contract, "primaryExchange", None),
            },
            "request": {
                "host": host,
                "port": port,
                "client_id": client_id,
                "lookback_days": lookback_days,
                "bar_size": bar_size,
                "use_rth": use_rth,
                "news_days": resolved_news_days,
                "news_limit": resolved_news_limit,
                "include_full_articles": resolved_include_full_articles,
                "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            },
            "price_series": bar_rows,
            "latest": {
                "close": latest["close"],
                "previous_close": prev["close"],
                "daily_change": delta,
                "daily_change_pct": delta_pct,
                "volume": latest["volume"],
            },
            "indicators": {
                "sma20": sma20[-1],
                "rsi14": rsi14[-1],
                "macd": macd[-1],
                "macd_signal": macd_signal[-1],
                "macd_hist": macd_hist[-1],
                "bollinger_upper": bb_upper[-1],
                "bollinger_mid": bb_mid[-1],
                "bollinger_lower": bb_lower[-1],
            },
            "fundamentals_raw": fundamentals,
            "news": news_payload,
            "data_quality": data_quality,
        }
    finally:
        if ib.isConnected():
            ib.disconnect()
