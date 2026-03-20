from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
from typing import Any

import matplotlib
from minio import Minio

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import wmill
import yfinance as yf


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rolling_mean(values: list[float | None], window: int) -> list[float | None]:
    result: list[float | None] = []
    running_sum = 0.0
    queue: list[float] = []
    for value in values:
        if value is None:
            queue.append(0.0)
            result.append(None)
            if len(queue) > window:
                running_sum -= queue.pop(0)
            continue
        queue.append(value)
        running_sum += value
        if len(queue) > window:
            running_sum -= queue.pop(0)
        if len(queue) == window:
            result.append(running_sum / window)
        else:
            result.append(None)
    return result


def _parse_date(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now(tz=timezone.utc)
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(text[:10], "%Y-%m-%d")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _build_public_url(s3_resource_path: str, object_path: str) -> str | None:
    try:
        resource = wmill.get_resource(s3_resource_path)
    except Exception:
        return None

    bucket = resource.get("bucket")
    endpoint = (
        resource.get("public_endpoint")
        or resource.get("publicEndpoint")
        or resource.get("endpoint")
        or resource.get("endPoint")
        or resource.get("host")
    )
    if not bucket or not endpoint:
        return None

    endpoint_text = str(endpoint).strip()
    if endpoint_text.startswith("http://") or endpoint_text.startswith("https://"):
        base = endpoint_text.rstrip("/")
    else:
        use_ssl = resource.get("use_ssl")
        if use_ssl is None:
            use_ssl = resource.get("useSSL")
        scheme = "https" if use_ssl not in {False, "false", "False", 0, "0"} else "http"
        base = f"{scheme}://{endpoint_text.rstrip('/')}"
    return f"{base}/{bucket}/{object_path}"


def _yahoo_interval(bar_size: str, timeframe_label: str) -> str:
    normalized_bar_size = str(bar_size or "").strip().lower()
    normalized_timeframe = str(timeframe_label or "").strip().lower()
    if "month" in normalized_bar_size or normalized_timeframe == "monthly":
        return "1mo"
    if "week" in normalized_bar_size or normalized_timeframe == "weekly":
        return "1wk"
    if "hour" in normalized_bar_size:
        return "1h"
    return "1d"


def _fetch_yahoo_price_series(
    symbol: str,
    timeframe_label: str,
    bar_size: str,
    lookback_days: int,
) -> list[dict[str, Any]]:
    interval = _yahoo_interval(bar_size, timeframe_label)
    end_dt = datetime.now(tz=timezone.utc)
    warmup_days = 400 if interval == "1mo" else 140 if interval == "1wk" else 35
    start_dt = end_dt - timedelta(days=max(int(lookback_days), 30) + warmup_days)

    history = yf.Ticker(symbol).history(
        start=start_dt.date().isoformat(),
        end=(end_dt + timedelta(days=1)).date().isoformat(),
        interval=interval,
        auto_adjust=False,
        actions=False,
    )
    if history is None or history.empty:
        return []

    price_series: list[dict[str, Any]] = []
    for index, row in history.iterrows():
        open_value = _to_float(row.get("Open"))
        high_value = _to_float(row.get("High"))
        low_value = _to_float(row.get("Low"))
        close_value = _to_float(row.get("Close"))
        volume_value = _to_float(row.get("Volume")) or 0.0
        if None in (open_value, high_value, low_value, close_value):
            continue

        if hasattr(index, "to_pydatetime"):
            date_value = index.to_pydatetime()
        else:
            date_value = _parse_date(index)
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=timezone.utc)

        price_series.append(
            {
                "date": date_value.isoformat(),
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume": volume_value,
            }
        )

    return price_series


def _write_chart_to_s3(
    object_path: str,
    image_bytes: bytes,
    s3_resource_path: str,
) -> tuple[Any, str | None, str | None, list[str]]:
    warnings: list[str] = []

    if s3_resource_path:
        try:
            s3_object = wmill.write_s3_file(
                object_path,
                image_bytes,
                s3_resource_path=s3_resource_path,
                content_type="image/png",
            )
            return s3_object, object_path, _build_public_url(s3_resource_path, object_path), warnings
        except Exception as exc:
            warnings.append(f"chart_upload_resource_failed: {exc}")
            try:
                resource = wmill.get_resource(s3_resource_path)
                bucket = str(resource.get("bucket") or "").strip()
                endpoint_value = (
                    resource.get("endPoint")
                    or resource.get("endpoint")
                    or resource.get("host")
                    or ""
                )
                endpoint_text = str(endpoint_value).strip()
                if not bucket or not endpoint_text:
                    raise ValueError("bucket or endpoint is missing")

                use_ssl = resource.get("useSSL")
                if use_ssl is None:
                    use_ssl = resource.get("use_ssl")

                if endpoint_text.startswith("http://") or endpoint_text.startswith("https://"):
                    parsed = endpoint_text.split("://", 1)
                    secure = parsed[0].lower() == "https"
                    endpoint_host = parsed[1].rstrip("/")
                else:
                    secure = bool(use_ssl)
                    endpoint_host = endpoint_text.rstrip("/")

                client = Minio(
                    endpoint_host,
                    access_key=str(resource.get("accessKey") or ""),
                    secret_key=str(resource.get("secretKey") or ""),
                    secure=secure,
                    region=str(resource.get("region") or "") or None,
                )
                if not client.bucket_exists(bucket):
                    client.make_bucket(bucket)
                client.put_object(
                    bucket,
                    object_path,
                    io.BytesIO(image_bytes),
                    len(image_bytes),
                    content_type="image/png",
                )
                presigned_url = client.presigned_get_object(
                    bucket,
                    object_path,
                    expires=timedelta(hours=6),
                )
                warnings.append("chart_upload_fallback_used: minio_presigned_url")
                return None, object_path, presigned_url, warnings
            except Exception as manual_exc:
                warnings.append(f"chart_upload_minio_failed: {manual_exc}")

    try:
        s3_object = wmill.write_s3_file(
            object_path,
            image_bytes,
            content_type="image/png",
        )
        warnings.append("chart_upload_fallback_used: workspace_default_s3")
        return s3_object, object_path, None, warnings
    except Exception as exc:
        warnings.append(f"chart_upload_failed: {exc}")
        return None, None, None, warnings


def _render_placeholder(
    symbol: str,
    timeframe_label: str,
    reason: str,
    fallback_latest: dict[str, Any] | None,
) -> bytes:
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis("off")
    latest_close = None
    if isinstance(fallback_latest, dict):
        latest_close = _to_float(fallback_latest.get("close"))

    lines = [
        f"{symbol} {timeframe_label.title()} Chart",
        "",
        "Price series was unavailable for this run.",
        f"Reason: {reason}",
    ]
    if latest_close is not None:
        lines.append(f"Latest available close: ${latest_close:,.2f}")
    ax.text(
        0.5,
        0.5,
        "\n".join(lines),
        ha="center",
        va="center",
        fontsize=14,
        family="monospace",
    )
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return buffer.getvalue()


def _render_chart(
    symbol: str,
    timeframe_label: str,
    price_series: list[dict[str, Any]],
) -> tuple[bytes, dict[str, Any]]:
    dates = [_parse_date(item.get("date")) for item in price_series]
    opens = [_to_float(item.get("open")) for item in price_series]
    highs = [_to_float(item.get("high")) for item in price_series]
    lows = [_to_float(item.get("low")) for item in price_series]
    closes = [_to_float(item.get("close")) for item in price_series]
    volumes = [_to_float(item.get("volume")) or 0.0 for item in price_series]

    price_closes = [value for value in closes if value is not None]
    if len(price_closes) < 2:
        raise ValueError("Not enough price points to render chart")

    sma20 = _rolling_mean(closes, 20)
    sma50 = _rolling_mean(closes, 50)
    sma200 = _rolling_mean(closes, 200)

    latest_close = price_closes[-1]
    first_close = price_closes[0]
    window_change_pct = ((latest_close - first_close) / first_close * 100.0) if first_close else 0.0
    high_watermark = max(value for value in highs if value is not None)
    low_watermark = min(value for value in lows if value is not None)
    latest_volume = volumes[-1] if volumes else 0.0
    avg_volume20_values = [value for value in _rolling_mean([float(v) for v in volumes], 20) if value is not None]
    avg_volume20 = avg_volume20_values[-1] if avg_volume20_values else None

    fig = plt.figure(figsize=(14, 9))
    grid = fig.add_gridspec(5, 1, hspace=0.05)
    ax_price = fig.add_subplot(grid[:4, 0])
    ax_volume = fig.add_subplot(grid[4, 0], sharex=ax_price)

    ax_price.plot(dates, closes, color="#0b5fff", linewidth=2.0, label="Close")
    if any(value is not None for value in sma20):
        ax_price.plot(dates, sma20, color="#ff7f0e", linewidth=1.2, label="SMA 20")
    if any(value is not None for value in sma50):
        ax_price.plot(dates, sma50, color="#2ca02c", linewidth=1.2, label="SMA 50")
    if any(value is not None for value in sma200):
        ax_price.plot(dates, sma200, color="#8c564b", linewidth=1.2, label="SMA 200")

    for index, date_value in enumerate(dates):
        open_value = opens[index]
        close_value = closes[index]
        if open_value is None or close_value is None:
            continue
        color = "#1b9e77" if close_value >= open_value else "#d95f02"
        ax_volume.bar(date_value, volumes[index], color=color, width=3 if timeframe_label == "weekly" else 0.8)

    ax_price.set_title(
        f"{symbol} {timeframe_label.title()} Price Overview",
        fontsize=16,
        loc="left",
    )
    ax_price.set_ylabel("Price")
    ax_price.grid(alpha=0.2)
    ax_price.legend(loc="upper left")

    ax_volume.set_ylabel("Volume")
    ax_volume.grid(alpha=0.2)
    ax_volume.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax_volume.tick_params(axis="x", rotation=25)

    latest_sma20 = next((value for value in reversed(sma20) if value is not None), None)
    latest_sma50 = next((value for value in reversed(sma50) if value is not None), None)
    latest_sma200 = next((value for value in reversed(sma200) if value is not None), None)
    summary = {
        "latest_close": latest_close,
        "window_change_pct": window_change_pct,
        "range_high": high_watermark,
        "range_low": low_watermark,
        "latest_volume": latest_volume,
        "avg_volume20": avg_volume20,
        "sma20": latest_sma20,
        "sma50": latest_sma50,
        "sma200": latest_sma200,
        "point_count": len(price_series),
        "latest_date": dates[-1].date().isoformat(),
    }
    stats_text = (
        f"Close ${latest_close:,.2f} | Window {window_change_pct:+.2f}%\n"
        f"Range ${low_watermark:,.2f} - ${high_watermark:,.2f}"
    )
    ax_price.text(
        0.01,
        0.98,
        stats_text,
        transform=ax_price.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "#cccccc"},
    )

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=180, bbox_inches="tight")
    plt.close(fig)
    return buffer.getvalue(), summary


def main(
    symbol: str,
    timeframe_label: str,
    bar_size: str,
    lookback_days: int,
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    exchange: str = "NASDAQ",
    currency: str = "USD",
    use_rth: bool = True,
    s3_resource_path: str = "u/admin2/minio_s3",
    fetch_script_path: str = "u/admin2/ibkr_fetch_report_data",
    fallback_latest: dict | None = None,
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    resolved_timeframe = str(timeframe_label or "").strip().lower() or "daily"
    if not resolved_symbol:
        raise ValueError("symbol is required")

    fetched: dict[str, Any] = {}
    warnings: list[str] = []
    fetch_error: str | None = None

    try:
        raw = wmill.run_script_by_path(
            fetch_script_path,
            args={
                "symbol": resolved_symbol,
                "exchange": exchange,
                "currency": currency,
                "host": host,
                "port": int(port),
                "client_id": int(client_id),
                "lookback_days": int(lookback_days),
                "bar_size": bar_size,
                "use_rth": bool(use_rth),
                "news_days": 1,
                "news_limit": 1,
                "include_full_articles": False,
            },
        )
        fetched = raw if isinstance(raw, dict) else {}
    except Exception as exc:
        fetch_error = str(exc)
        warnings.append(f"chart_fetch_failed: {exc}")

    price_series = fetched.get("price_series") if isinstance(fetched.get("price_series"), list) else []
    yahoo_fallback_used = False
    yahoo_fetch_error: str | None = None
    if not price_series:
        try:
            price_series = _fetch_yahoo_price_series(
                symbol=resolved_symbol,
                timeframe_label=resolved_timeframe,
                bar_size=bar_size,
                lookback_days=int(lookback_days),
            )
            if price_series:
                yahoo_fallback_used = True
                warnings.append("chart_fallback_used: yahoo_finance")
        except Exception as exc:
            yahoo_fetch_error = str(exc)
            warnings.append(f"chart_yahoo_fetch_failed: {exc}")

    if price_series:
        image_bytes, chart_summary = _render_chart(
            symbol=resolved_symbol,
            timeframe_label=resolved_timeframe,
            price_series=price_series,
        )
        chart_available = True
        fetch_status = "fallback_yahoo_finance" if yahoo_fallback_used else "ok"
    else:
        image_bytes = _render_placeholder(
            symbol=resolved_symbol,
            timeframe_label=resolved_timeframe,
            reason=(
                yahoo_fetch_error
                or fetch_error
                or "No price_series returned from IBKR or Yahoo Finance."
            ),
            fallback_latest=fallback_latest,
        )
        chart_summary = {
            "latest_close": _to_float((fallback_latest or {}).get("close")),
            "point_count": 0,
        }
        chart_available = False
        fetch_status = "placeholder"

    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_path = (
        f"charts/ibkr_strategy_engine/{resolved_symbol}/{resolved_timeframe}/"
        f"{timestamp}_{bar_size.replace(' ', '_')}.png"
    )
    s3_object, object_path_value, public_url, upload_warnings = _write_chart_to_s3(
        object_path=object_path,
        image_bytes=image_bytes,
        s3_resource_path=s3_resource_path,
    )
    warnings.extend(upload_warnings)

    return {
        "symbol": resolved_symbol,
        "timeframe_label": resolved_timeframe,
        "bar_size": bar_size,
        "lookback_days": int(lookback_days),
        "chart_available": chart_available,
        "fetch_status": fetch_status,
        "warnings": warnings,
        "fetch_error": fetch_error,
        "yahoo_fallback_used": yahoo_fallback_used,
        "chart_summary": chart_summary,
        "data_quality": fetched.get("data_quality") if isinstance(fetched, dict) else None,
        "latest": fetched.get("latest") if isinstance(fetched, dict) else None,
        "indicators": fetched.get("indicators") if isinstance(fetched, dict) else None,
        "contract": fetched.get("contract") if isinstance(fetched, dict) else None,
        "price_series_count": len(price_series),
        "s3_object": s3_object,
        "object_path": object_path_value,
        "public_url": public_url,
    }
