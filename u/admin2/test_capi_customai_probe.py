from __future__ import annotations

import json
import urllib.parse
from typing import Any

import requests
import urllib3
import wmill


DEFAULT_RESOURCE_PATH = "u/admin2/capi_customai"
DEFAULT_PROMPT = "Reply with exactly ok"


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
    raise ValueError("resource model is missing")


def _resource_base_url(resource: dict[str, Any]) -> str:
    base_url = str(resource.get("base_url") or "").rstrip("/")
    if not base_url:
        raise ValueError("resource base_url is missing")
    return base_url


def _extract_response_text(body: dict[str, Any]) -> str:
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


def main(
    prompt: str = "Reply with exactly ok",
    resource_path: str = "u/admin2/capi_customai",
    max_output_tokens: int = 32,
    timeout_s: int = 60,
) -> dict[str, Any]:
    resolved_prompt = str(prompt or DEFAULT_PROMPT)
    resolved_resource_path = str(resource_path or DEFAULT_RESOURCE_PATH)
    resolved_max_output_tokens = int(max_output_tokens or 32)
    resolved_timeout_s = int(timeout_s or 60)

    resource = wmill.get_resource(resolved_resource_path)
    if not isinstance(resource, dict):
        raise ValueError(f"resource {resolved_resource_path} did not return an object")

    configured_api = str(resource.get("api") or "")
    if configured_api != "openai-responses":
        raise ValueError(f"resource api must be 'openai-responses', got {configured_api!r}")

    configured_provider = str(resource.get("provider") or "")
    if configured_provider != "openai-code":
        raise ValueError(f"resource provider must be 'openai-code', got {configured_provider!r}")

    api_key = _resource_api_key(resource)
    configured_model = _resource_model(resource)
    base_url = _resource_base_url(resource)
    endpoint_used = f"{base_url}/responses"

    verify_tls = urllib.parse.urlsplit(base_url).hostname != "capi.quan2go.com"
    if not verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    response = requests.post(
        endpoint_used,
        headers={
            "Authorization": f"Bearer {api_key}",
            "x-api-key": api_key,
            "api-key": api_key,
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
        },
        json={
            "model": configured_model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": resolved_prompt}]}],
            "max_output_tokens": resolved_max_output_tokens,
            "reasoning": {"effort": "high"},
            "store": False,
            "stream": True,
        },
        timeout=(15, resolved_timeout_s),
        stream=True,
        verify=verify_tls,
    )
    response.raise_for_status()

    completed = _decode_sse_response(response)
    response_text = _extract_response_text(completed)
    if not response_text:
        raise ValueError("response did not include assistant text")

    return {
        "ok": response_text == "ok",
        "response_text": response_text,
        "resource_path": resolved_resource_path,
        "resource_base_url": base_url,
        "endpoint_used": endpoint_used,
        "configured_model": configured_model,
        "response_model": completed.get("model"),
        "api": configured_api,
        "status": response.status_code,
    }
