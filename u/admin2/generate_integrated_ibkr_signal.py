from __future__ import annotations

from copy import deepcopy
import base64
import json
import re
import time
import urllib.parse
from typing import Any

import requests
import urllib3
import wmill


VALID_SIGNALS = {"BUY", "SELL", "HOLD"}
VALID_ORDER_TYPES = {"MKT", "LMT"}
DEFAULT_CUSTOMAI_RESOURCE_PATH = "u/admin2/capi_customai"
DEFAULT_CUSTOMAI_MODEL = "gpt-5.4"
DEFAULT_ANTHROPIC_MODEL = "claude-3-7-sonnet-latest"
DEFAULT_LLM_RETRY_ATTEMPTS = 3
DEFAULT_LLM_RETRY_BASE_DELAY_S = 2
RETRYABLE_LLM_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _extract_report(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("report"), dict):
        return payload["report"]
    if isinstance(payload, dict):
        return payload
    return {}


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_json(text: str) -> dict[str, Any]:
    cleaned = _strip_code_fences(text)
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            return {}
        try:
            payload = json.loads(match.group(0))
            return payload if isinstance(payload, dict) else {}
        except json.JSONDecodeError:
            return {}


def _short_report(report: dict[str, Any]) -> dict[str, Any]:
    latest = report.get("latest") if isinstance(report.get("latest"), dict) else {}
    indicators = report.get("indicators") if isinstance(report.get("indicators"), dict) else {}
    return {
        "symbol": report.get("symbol"),
        "latest": latest,
        "indicators": indicators,
        "technical_summary": report.get("technical_summary"),
        "fundamentals_source": report.get("fundamentals_source"),
        "data_quality": report.get("data_quality"),
        "report_markdown": str(report.get("report_markdown") or "")[:4000],
    }


def _short_chart(chart: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(chart, dict):
        return {}
    return {
        "fetch_status": chart.get("fetch_status"),
        "chart_available": chart.get("chart_available"),
        "chart_summary": chart.get("chart_summary"),
        "public_url": chart.get("public_url"),
        "object_path": chart.get("object_path"),
        "s3_object": chart.get("s3_object"),
        "timeframe_label": chart.get("timeframe_label"),
        "warnings": chart.get("warnings") if isinstance(chart.get("warnings"), list) else [],
    }


def _load_chart_bytes(chart_payload: dict[str, Any]) -> tuple[str | None, str | None]:
    s3_object = chart_payload.get("s3_object")
    if not s3_object:
        public_url = str(chart_payload.get("public_url") or "").strip()
        if not public_url:
            return None, None
        try:
            response = requests.get(public_url, timeout=20)
            response.raise_for_status()
        except Exception:
            return None, None
        encoded = base64.b64encode(response.content).decode("ascii")
        return response.headers.get("Content-Type", "image/png"), encoded
    try:
        content = wmill.load_s3_file(s3_object)
    except Exception:
        public_url = str(chart_payload.get("public_url") or "").strip()
        if not public_url:
            return None, None
        try:
            response = requests.get(public_url, timeout=20)
            response.raise_for_status()
        except Exception:
            return None, None
        encoded = base64.b64encode(response.content).decode("ascii")
        return response.headers.get("Content-Type", "image/png"), encoded
    if isinstance(content, str):
        raw = content.encode("utf-8")
    else:
        raw = content
    encoded = base64.b64encode(raw).decode("ascii")
    return "image/png", encoded


def _resource_api_key(resource: dict[str, Any]) -> str:
    for key in ("api_key", "apiKey"):
        value = resource.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("resource does not expose an API key")


def _resource_model(resource: dict[str, Any]) -> str:
    model = resource.get("model")
    if isinstance(model, dict):
        for key in ("id", "name"):
            value = model.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(model, str) and model.strip():
        return model.strip()
    return ""


def _resource_base_url(resource: dict[str, Any], default: str = "") -> str:
    base_url = str(resource.get("base_url") or default).rstrip("/")
    if not base_url:
        raise ValueError("resource base_url is missing")
    return base_url


def _is_customai_openai_responses_resource(resource: dict[str, Any]) -> bool:
    return (
        str(resource.get("api") or "").strip().lower() == "openai-responses"
        and str(resource.get("provider") or "").strip().lower() == "openai-code"
    )


def _extract_openai_response_text(body: dict[str, Any]) -> str:
    output_text = body.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = body.get("output")
    if not isinstance(output, list):
        return ""

    parts: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for chunk in content:
            if not isinstance(chunk, dict):
                continue
            text = chunk.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts)


def _decode_sse_response(response: requests.Response) -> dict[str, Any]:
    completed_response: dict[str, Any] | None = None
    event_name = ""
    data_lines: list[str] = []

    for raw_line in response.iter_lines(decode_unicode=True):
        line = (raw_line or "").rstrip("\r\n")
        if not line:
            if data_lines:
                data = "\n".join(data_lines)
                if data != "[DONE]":
                    payload = json.loads(data)
                    if event_name == "response.completed" and isinstance(payload, dict):
                        maybe_response = payload.get("response")
                        if isinstance(maybe_response, dict):
                            completed_response = maybe_response
                event_name = ""
                data_lines = []
            continue

        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line.partition(":")[2].strip()
            continue
        if line.startswith("data:"):
            data_lines.append(line.partition(":")[2].strip())

    if completed_response is None:
        raise ValueError("stream ended without response.completed")
    return completed_response


def _raise_for_status_with_detail(response: requests.Response, url: str) -> None:
    if response.ok:
        return

    detail = ""
    try:
        detail = (response.text or "").strip()
    except Exception:
        detail = ""
    if detail:
        detail = detail[:2000]
        raise requests.HTTPError(
            f"{response.status_code} {response.reason} for url: {url}; response={detail}",
            response=response,
        )
    response.raise_for_status()


def _retry_delay_s(attempt: int) -> int:
    return DEFAULT_LLM_RETRY_BASE_DELAY_S * (2 ** max(0, attempt - 1))


def _is_retryable_decode_error(exc: Exception) -> bool:
    return isinstance(exc, ValueError) and "response.completed" in str(exc)


def _build_prompt(
    symbol: str,
    strategy_name: str,
    strategy_context: dict[str, Any],
    workflow_config: dict[str, Any],
    symbol_config: dict[str, Any],
    daily_report: dict[str, Any],
    weekly_report: dict[str, Any],
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
) -> str:
    payload = {
        "symbol": symbol,
        "strategy_name": strategy_name,
        "strategy_context": strategy_context,
        "workflow_config": workflow_config,
        "symbol_config": symbol_config,
        "daily_report": _short_report(daily_report),
        "weekly_report": _short_report(weekly_report),
        "daily_chart": {
            "fetch_status": daily_chart.get("fetch_status"),
            "chart_summary": daily_chart.get("chart_summary"),
            "public_url": daily_chart.get("public_url"),
        },
        "weekly_chart": {
            "fetch_status": weekly_chart.get("fetch_status"),
            "chart_summary": weekly_chart.get("chart_summary"),
            "public_url": weekly_chart.get("public_url"),
        },
    }
    return (
        "You are generating a combined IBKR trading signal for a stock strategy engine.\n"
        "Return strict JSON only. Do not wrap the JSON in markdown fences.\n"
        "Use the provided daily and weekly technical data and the chart images.\n"
        "The JSON must match this shape exactly:\n"
        "{\n"
        '  "final_signal": "BUY|SELL|HOLD",\n'
        '  "stance": "Strong Bullish|Bullish|Neutral|Bearish|Strong Bearish",\n'
        '  "confidence": 0.0,\n'
        '  "timeframe_alignment": "Strong|Medium|Weak",\n'
        '  "summary_markdown": "## Integrated Technical Analysis Report - SYMBOL ...",\n'
        '  "entry_range_low": null,\n'
        '  "entry_range_high": null,\n'
        '  "stop_loss": null,\n'
        '  "target_primary": null,\n'
        '  "target_secondary": null,\n'
        '  "reversal_level": null,\n'
        '  "order_type": "LMT|MKT",\n'
        '  "time_in_force": "DAY",\n'
        '  "risk_notes": ["..."],\n'
        '  "signal_notes": ["..."]\n'
        "}\n"
        "The summary_markdown must start with exactly:\n"
        f"## Integrated Technical Analysis Report - {symbol}\n\n"
        "It must contain these sections in order and with this numbering style:\n"
        "1. Price and Trend Overview\n"
        "2. Technical Summary\n"
        "3. Multi-Timeframe Signal Confirmation\n"
        "4. Detailed Trading Recommendations\n"
        "5. Risk Assessment\n"
        "6. Conclusion\n\n"
        "If daily and weekly disagree materially, reduce confidence and prefer HOLD.\n"
        "If data quality is weak, say so explicitly.\n"
        "Context JSON:\n"
        + json.dumps(payload, ensure_ascii=True)
    )


def _call_anthropic_compatible_api(
    llm_resource_path: str,
    resource: dict[str, Any],
    model: str,
    temperature: float,
    max_tokens: int,
    prompt: str,
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any], int]:
    api_key = str(resource.get("apiKey") or "").strip()
    if not api_key:
        raise ValueError(f"Anthropic resource {llm_resource_path} does not expose apiKey")

    base_url = _resource_base_url(resource, "https://api.anthropic.com/v1")
    url = f"{base_url}/messages"
    content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]

    chart_input_count = 0
    for chart_payload in (daily_chart, weekly_chart):
        media_type, data = _load_chart_bytes(chart_payload)
        if media_type and data:
            chart_input_count += 1
            content.append(
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": data,
                    },
                }
            )

    response = requests.post(
        url,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "messages": [
                {
                    "role": "user",
                    "content": content,
                }
            ],
        },
        timeout=(15, 180),
    )
    _raise_for_status_with_detail(response, url)
    payload = response.json()
    text_parts: list[str] = []
    for item in payload.get("content", []):
        if isinstance(item, dict) and item.get("type") == "text":
            text_parts.append(str(item.get("text") or ""))
    if not text_parts:
        raise ValueError("Anthropic response did not include text content")
    raw_text = "\n".join(text_parts)
    parsed = _extract_json(raw_text)
    if not parsed:
        raise ValueError("Anthropic response was not valid JSON")
    return parsed, raw_text, payload, chart_input_count


def _call_openai_responses_api(
    llm_resource_path: str,
    resource: dict[str, Any],
    model: str,
    max_tokens: int,
    prompt: str,
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any], int]:
    api_key = _resource_api_key(resource)
    base_url = _resource_base_url(resource)
    url = f"{base_url}/responses"

    content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    chart_input_count = 0
    for chart_payload in (daily_chart, weekly_chart):
        media_type, data = _load_chart_bytes(chart_payload)
        if media_type and data:
            chart_input_count += 1
            content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:{media_type};base64,{data}",
                }
            )

    verify_tls = urllib.parse.urlsplit(base_url).hostname != "capi.quan2go.com"
    if not verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    payload = {
        "model": model,
        "input": [{"role": "user", "content": content}],
        "max_output_tokens": int(max_tokens),
        "reasoning": {"effort": "high"},
        "store": False,
        "stream": True,
    }
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_LLM_RETRY_ATTEMPTS + 1):
        response: requests.Response | None = None
        try:
            response = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "x-api-key": api_key,
                    "api-key": api_key,
                    "Accept": "text/event-stream",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=(15, 180),
                stream=True,
                verify=verify_tls,
            )
            if response.status_code in RETRYABLE_LLM_STATUS_CODES and attempt < DEFAULT_LLM_RETRY_ATTEMPTS:
                time.sleep(_retry_delay_s(attempt))
                continue
            _raise_for_status_with_detail(response, url)
            completed = _decode_sse_response(response)
            raw_text = _extract_openai_response_text(completed)
            if not raw_text:
                raise ValueError(f"OpenAI Responses resource {llm_resource_path} did not include assistant text")
            parsed = _extract_json(raw_text)
            if not parsed:
                raise ValueError("OpenAI Responses output was not valid JSON")
            return parsed, raw_text, completed, chart_input_count
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.ChunkedEncodingError,
        ) as exc:
            last_error = exc
            if attempt < DEFAULT_LLM_RETRY_ATTEMPTS:
                time.sleep(_retry_delay_s(attempt))
                continue
            raise ValueError(
                f"OpenAI Responses transport error after {attempt} attempts for {url}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        except ValueError as exc:
            last_error = exc
            if _is_retryable_decode_error(exc) and attempt < DEFAULT_LLM_RETRY_ATTEMPTS:
                time.sleep(_retry_delay_s(attempt))
                continue
            raise
        finally:
            if response is not None:
                response.close()

    if last_error is not None:
        raise last_error
    raise ValueError(f"OpenAI Responses request failed without a response for {url}")


def _call_llm_api(
    llm_resource_path: str,
    configured_model: str,
    temperature: float,
    max_tokens: int,
    prompt: str,
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any], str, str, int]:
    resource = wmill.get_resource(llm_resource_path)
    if not isinstance(resource, dict):
        raise ValueError(f"resource {llm_resource_path} did not return an object")

    if _is_customai_openai_responses_resource(resource):
        effective_model = _resource_model(resource) or configured_model or DEFAULT_CUSTOMAI_MODEL
        parsed, raw_text, payload, chart_input_count = _call_openai_responses_api(
            llm_resource_path=llm_resource_path,
            resource=resource,
            model=effective_model,
            max_tokens=max_tokens,
            prompt=prompt,
            daily_chart=daily_chart,
            weekly_chart=weekly_chart,
        )
        return parsed, raw_text, payload, "customai_openai_responses", effective_model, chart_input_count

    effective_model = configured_model or DEFAULT_ANTHROPIC_MODEL
    parsed, raw_text, payload, chart_input_count = _call_anthropic_compatible_api(
        llm_resource_path=llm_resource_path,
        resource=resource,
        model=effective_model,
        temperature=temperature,
        max_tokens=max_tokens,
        prompt=prompt,
        daily_chart=daily_chart,
        weekly_chart=weekly_chart,
    )
    return parsed, raw_text, payload, "anthropic_compatible", effective_model, chart_input_count


def _normalize_signal_payload(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(payload)
    final_signal = str(normalized.get("final_signal") or "HOLD").strip().upper()
    if final_signal not in VALID_SIGNALS:
        final_signal = "HOLD"
    normalized["final_signal"] = final_signal

    order_type = str(normalized.get("order_type") or "MKT").strip().upper()
    if order_type not in VALID_ORDER_TYPES:
        order_type = "MKT"
    normalized["order_type"] = order_type
    normalized["time_in_force"] = str(normalized.get("time_in_force") or "DAY").strip().upper()

    for key in (
        "confidence",
        "entry_range_low",
        "entry_range_high",
        "stop_loss",
        "target_primary",
        "target_secondary",
        "reversal_level",
    ):
        if key == "confidence":
            try:
                normalized[key] = float(normalized.get(key))
            except (TypeError, ValueError):
                normalized[key] = 0.5
            normalized[key] = max(0.0, min(1.0, normalized[key]))
        else:
            normalized[key] = _to_float(normalized.get(key))

    summary_markdown = str(normalized.get("summary_markdown") or "").strip()
    if not summary_markdown.startswith(f"## Integrated Technical Analysis Report - {symbol}"):
        summary_markdown = (
            f"## Integrated Technical Analysis Report - {symbol}\n\n{summary_markdown}"
            if summary_markdown
            else f"## Integrated Technical Analysis Report - {symbol}"
        )
    normalized["summary_markdown"] = summary_markdown

    risk_notes = normalized.get("risk_notes")
    normalized["risk_notes"] = [str(item).strip() for item in risk_notes] if isinstance(risk_notes, list) else []
    signal_notes = normalized.get("signal_notes")
    normalized["signal_notes"] = [str(item).strip() for item in signal_notes] if isinstance(signal_notes, list) else []
    return normalized


def _build_input_snapshot(
    symbol: str,
    strategy_key: str,
    strategy_name: str,
    strategy_context: dict[str, Any],
    workflow_config: dict[str, Any],
    symbol_config: dict[str, Any],
    daily_report: dict[str, Any],
    weekly_report: dict[str, Any],
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
    llm_resource_path: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "strategy_key": strategy_key,
        "strategy_name": strategy_name,
        "strategy_context": deepcopy(strategy_context) if isinstance(strategy_context, dict) else {},
        "workflow_config": deepcopy(workflow_config) if isinstance(workflow_config, dict) else {},
        "symbol_config": deepcopy(symbol_config) if isinstance(symbol_config, dict) else {},
        "daily_report": _short_report(daily_report),
        "weekly_report": _short_report(weekly_report),
        "daily_chart": _short_chart(daily_chart),
        "weekly_chart": _short_chart(weekly_chart),
        "llm_resource_path": llm_resource_path,
    }


def _build_output_snapshot(
    status: str,
    normalized_signal: dict[str, Any],
    latest: dict[str, Any],
    llm_provider: str,
    llm_model_used: str,
    llm_chart_image_count: int,
    warnings: list[str],
    errors: list[str],
) -> dict[str, Any]:
    return {
        "status": status,
        "decision": {
            "final_signal": normalized_signal.get("final_signal"),
            "stance": normalized_signal.get("stance"),
            "confidence": normalized_signal.get("confidence"),
            "timeframe_alignment": normalized_signal.get("timeframe_alignment"),
            "entry_range_low": normalized_signal.get("entry_range_low"),
            "entry_range_high": normalized_signal.get("entry_range_high"),
            "stop_loss": normalized_signal.get("stop_loss"),
            "target_primary": normalized_signal.get("target_primary"),
            "target_secondary": normalized_signal.get("target_secondary"),
            "reversal_level": normalized_signal.get("reversal_level"),
            "order_type": normalized_signal.get("order_type"),
            "time_in_force": normalized_signal.get("time_in_force"),
            "risk_notes": normalized_signal.get("risk_notes"),
            "signal_notes": normalized_signal.get("signal_notes"),
        },
        "latest": deepcopy(latest) if isinstance(latest, dict) else {},
        "report_markdown": normalized_signal.get("summary_markdown"),
        "llm": {
            "provider": llm_provider,
            "model_used": llm_model_used,
            "chart_image_count": int(llm_chart_image_count),
        },
        "warnings": list(warnings or []),
        "errors": list(errors or []),
    }


def main(
    symbol: str,
    strategy_key: str,
    strategy_name: str,
    strategy_context: dict,
    workflow_config: dict,
    symbol_config: dict,
    daily_report: dict,
    weekly_report: dict,
    daily_chart: dict,
    weekly_chart: dict,
    llm_resource_path: str = "u/admin2/capi_customai",
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    daily = _extract_report(daily_report)
    weekly = _extract_report(weekly_report)

    llm_config = {}
    if isinstance(workflow_config.get("llm"), dict):
        llm_config = deepcopy(workflow_config["llm"])
    strategy_params = strategy_context.get("effective_params") if isinstance(strategy_context, dict) else {}
    if isinstance(strategy_params, dict) and isinstance(strategy_params.get("llm"), dict):
        llm_config.update(strategy_params["llm"])

    model = str(llm_config.get("model") or "").strip()
    temperature = float(llm_config.get("temperature") or 0.1)
    max_tokens = int(llm_config.get("max_tokens") or 2200)

    prompt_text = _build_prompt(
        symbol=resolved_symbol,
        strategy_name=strategy_name,
        strategy_context=strategy_context if isinstance(strategy_context, dict) else {},
        workflow_config=workflow_config if isinstance(workflow_config, dict) else {},
        symbol_config=symbol_config if isinstance(symbol_config, dict) else {},
        daily_report=daily,
        weekly_report=weekly,
        daily_chart=daily_chart if isinstance(daily_chart, dict) else {},
        weekly_chart=weekly_chart if isinstance(weekly_chart, dict) else {},
    )
    llm_payload, llm_raw_text, llm_response_json, llm_provider, llm_model_used, llm_chart_image_count = _call_llm_api(
        llm_resource_path=llm_resource_path,
        configured_model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        prompt=prompt_text,
        daily_chart=daily_chart if isinstance(daily_chart, dict) else {},
        weekly_chart=weekly_chart if isinstance(weekly_chart, dict) else {},
    )
    normalized_signal = _normalize_signal_payload(resolved_symbol, llm_payload)

    final_signal = normalized_signal["final_signal"]
    side = final_signal if final_signal in {"BUY", "SELL"} else None
    latest = daily.get("latest") if isinstance(daily.get("latest"), dict) else {}
    warnings: list[str] = []
    errors: list[str] = []
    input_snapshot = _build_input_snapshot(
        symbol=resolved_symbol,
        strategy_key=strategy_key,
        strategy_name=strategy_name,
        strategy_context=strategy_context if isinstance(strategy_context, dict) else {},
        workflow_config=workflow_config if isinstance(workflow_config, dict) else {},
        symbol_config=symbol_config if isinstance(symbol_config, dict) else {},
        daily_report=daily,
        weekly_report=weekly,
        daily_chart=daily_chart if isinstance(daily_chart, dict) else {},
        weekly_chart=weekly_chart if isinstance(weekly_chart, dict) else {},
        llm_resource_path=llm_resource_path,
    )
    output_snapshot = _build_output_snapshot(
        status="success",
        normalized_signal=normalized_signal,
        latest=latest,
        llm_provider=llm_provider,
        llm_model_used=llm_model_used,
        llm_chart_image_count=llm_chart_image_count,
        warnings=warnings,
        errors=errors,
    )

    return {
        "symbol": resolved_symbol,
        "strategy_key": strategy_key,
        "strategy_name": strategy_name,
        "strategy_context": strategy_context,
        "status": "success",
        "llm_used": True,
        "llm_error": None,
        "llm_provider": llm_provider,
        "llm_model_used": llm_model_used,
        "llm_chart_image_count": llm_chart_image_count,
        "input_snapshot": input_snapshot,
        "output_snapshot": output_snapshot,
        "llm_prompt_text": prompt_text,
        "llm_raw_text": llm_raw_text,
        "llm_response_json": llm_response_json,
        "warnings": warnings,
        "errors": errors,
        "latest": latest,
        "daily_report": _short_report(daily),
        "weekly_report": _short_report(weekly),
        "daily_chart": {
            "fetch_status": daily_chart.get("fetch_status"),
            "chart_available": daily_chart.get("chart_available"),
            "chart_summary": daily_chart.get("chart_summary"),
            "public_url": daily_chart.get("public_url"),
            "object_path": daily_chart.get("object_path"),
            "s3_object": daily_chart.get("s3_object"),
        },
        "weekly_chart": {
            "fetch_status": weekly_chart.get("fetch_status"),
            "chart_available": weekly_chart.get("chart_available"),
            "chart_summary": weekly_chart.get("chart_summary"),
            "public_url": weekly_chart.get("public_url"),
            "object_path": weekly_chart.get("object_path"),
            "s3_object": weekly_chart.get("s3_object"),
        },
        "decision": {
            "final_signal": final_signal,
            "side": side,
            "stance": normalized_signal.get("stance"),
            "confidence": normalized_signal.get("confidence"),
            "timeframe_alignment": normalized_signal.get("timeframe_alignment"),
            "entry_range": {
                "low": normalized_signal.get("entry_range_low"),
                "high": normalized_signal.get("entry_range_high"),
            },
            "stop_loss": normalized_signal.get("stop_loss"),
            "targets": [
                value
                for value in (
                    normalized_signal.get("target_primary"),
                    normalized_signal.get("target_secondary"),
                )
                if value is not None
            ],
            "target_primary": normalized_signal.get("target_primary"),
            "target_secondary": normalized_signal.get("target_secondary"),
            "reversal_level": normalized_signal.get("reversal_level"),
            "order_type": normalized_signal.get("order_type"),
            "time_in_force": normalized_signal.get("time_in_force"),
            "risk_notes": normalized_signal.get("risk_notes"),
            "signal_notes": normalized_signal.get("signal_notes"),
        },
        "report_markdown": normalized_signal.get("summary_markdown"),
    }
