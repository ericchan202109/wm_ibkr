from __future__ import annotations

import time
from typing import Any

import wmill


TERMINAL_SUCCESS_JOB_STATUSES = {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}
TERMINAL_FAILURE_JOB_STATUSES = {
    "FAILED",
    "FAILURE",
    "ERROR",
    "CANCELLED",
    "CANCELED",
    "TIMEOUT",
    "TIMED_OUT",
}


def _normalize_job_status(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _job_failure_message(job_id: str, details: dict[str, Any] | None, fallback_status: str = "") -> str:
    payload = details if isinstance(details, dict) else {}
    status = _normalize_job_status(
        payload.get("status")
        or payload.get("type")
        or payload.get("job_status")
        or fallback_status
    )
    for key in ("error", "message", "worker_error", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            nested_error = value.get("error") or value.get("message")
            if nested_error:
                return f"job {job_id} failed with status {status or 'UNKNOWN'}: {nested_error}"
        elif value not in (None, ""):
            return f"job {job_id} failed with status {status or 'UNKNOWN'}: {value}"
    return f"job {job_id} failed with status {status or 'UNKNOWN'}"


def _extract_job_result(job_id: str, details: dict[str, Any] | None) -> Any:
    if isinstance(details, dict) and "result" in details:
        return details.get("result")
    get_result = getattr(wmill, "get_result", None)
    if callable(get_result):
        return get_result(job_id, assert_result_is_not_none=False)
    return None


def _wait_for_job_result(job_id: str, timeout: int = 1800, poll_interval_s: float = 1.0) -> Any:
    wait_job = getattr(wmill, "wait_job", None)
    if callable(wait_job):
        try:
            return wait_job(job_id, timeout=timeout, assert_result_is_not_none=False)
        except AttributeError:
            pass

    deadline = time.monotonic() + float(timeout)
    get_job_status = getattr(wmill, "get_job_status", None)
    get_job = getattr(wmill, "get_job", None)

    while time.monotonic() <= deadline:
        details = get_job(job_id) if callable(get_job) else {}

        status = ""
        if callable(get_job_status):
            status = _normalize_job_status(get_job_status(job_id))
        if not status and isinstance(details, dict):
            status = _normalize_job_status(
                details.get("status") or details.get("type") or details.get("job_status")
            )

        if isinstance(details, dict) and details.get("completed") is True:
            if details.get("success") is False or status in TERMINAL_FAILURE_JOB_STATUSES:
                raise RuntimeError(_job_failure_message(job_id, details, status))
            return _extract_job_result(job_id, details)

        if status in TERMINAL_SUCCESS_JOB_STATUSES:
            return _extract_job_result(job_id, details)
        if status in TERMINAL_FAILURE_JOB_STATUSES:
            raise RuntimeError(_job_failure_message(job_id, details, status))

        time.sleep(poll_interval_s)

    raise TimeoutError(f"Timed out waiting for job {job_id}")


def _looks_like_strategy_result(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(
        key in payload
        for key in (
            "decision",
            "report_markdown",
            "input_snapshot",
            "output_snapshot",
            "llm_prompt_text",
            "llm_raw_text",
            "llm_response_json",
        )
    ) or ("daily_chart" in payload and "weekly_chart" in payload)


def _unwrap_strategy_result(payload: Any, depth: int = 0) -> dict[str, Any]:
    if not isinstance(payload, dict) or depth > 6:
        return {}
    if _looks_like_strategy_result(payload):
        return payload

    for key in ("finalize_output", "integrated_signal", "result", "output", "value"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidate = _unwrap_strategy_result(nested, depth + 1)
            if candidate:
                return candidate

    for key, nested in payload.items():
        if key in {"daily_chart", "weekly_chart", "decision", "latest"}:
            continue
        if isinstance(nested, dict):
            candidate = _unwrap_strategy_result(nested, depth + 1)
            if candidate:
                return candidate
    return {}


def main(
    symbol: str,
    strategies: list,
    workflow_config: dict,
    symbol_config: dict,
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    exchange: str = "NASDAQ",
    currency: str = "USD",
    use_rth: bool = True,
    db_resource_path: str = "u/admin2/supabase_postgresql",
    s3_resource_path: str = "u/admin2/minio_s3",
    llm_resource_path: str = "u/admin2/capi_customai",
    data_mode: str = "run_new",
    persist_reports: bool = False,
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    active_strategies = [
        item for item in (strategies or []) if isinstance(item, dict) and item.get("runner_flow_path")
    ]
    active_strategies.sort(key=lambda item: (int(item.get("priority", 9999)), str(item.get("strategy_key") or "")))

    if not active_strategies:
        return {
            "symbol": resolved_symbol,
            "status": "no_active_strategies",
            "results": [],
            "warnings": ["No active strategies were configured for this symbol."],
        }

    jobs: list[dict[str, Any]] = []
    for strategy in active_strategies:
        args = {
            "symbol": resolved_symbol,
            "strategy_definition": strategy,
            "workflow_config": workflow_config,
            "symbol_config": symbol_config,
            "host": host,
            "port": int(port),
            "client_id": int(client_id),
            "exchange": exchange,
            "currency": currency,
            "use_rth": bool(use_rth),
            "db_resource_path": db_resource_path,
            "s3_resource_path": s3_resource_path,
            "llm_resource_path": str(strategy.get("llm_resource_path") or llm_resource_path),
            "data_mode": data_mode,
            "persist_reports": bool(persist_reports),
        }
        job_id = wmill.run_flow_async(
            path=str(strategy["runner_flow_path"]),
            args=args,
            do_not_track_in_parent=False,
        )
        jobs.append(
            {
                "strategy_key": strategy.get("strategy_key"),
                "runner_flow_path": strategy.get("runner_flow_path"),
                "priority": strategy.get("priority"),
                "job_id": job_id,
                "strategy_definition": strategy,
            }
        )

    results: list[dict[str, Any]] = []
    warnings: list[str] = []
    for job in jobs:
        try:
            flow_result = _wait_for_job_result(job["job_id"], timeout=1800)
            normalized_result = _unwrap_strategy_result(flow_result)
            if not normalized_result:
                warnings.append(
                    f"strategy_result_shape_unrecognized:{job['strategy_key']}"
                )
            results.append(
                {
                    "strategy_key": job["strategy_key"],
                    "runner_flow_path": job["runner_flow_path"],
                    "priority": job["priority"],
                    "job_id": job["job_id"],
                    "status": "success",
                    "strategy_definition": job["strategy_definition"],
                    "result": normalized_result if normalized_result else flow_result,
                }
            )
        except Exception as exc:
            warning = f"strategy_failed:{job['strategy_key']}:{exc}"
            warnings.append(warning)
            results.append(
                {
                    "strategy_key": job["strategy_key"],
                    "runner_flow_path": job["runner_flow_path"],
                    "priority": job["priority"],
                    "job_id": job["job_id"],
                    "status": "error",
                    "strategy_definition": job["strategy_definition"],
                    "error": str(exc),
                    "result": None,
                }
            )

    return {
        "symbol": resolved_symbol,
        "status": "ok",
        "results": results,
        "warnings": warnings,
    }
