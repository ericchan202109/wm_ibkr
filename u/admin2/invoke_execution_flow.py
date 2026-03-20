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


def main(
    order_candidate: dict | None,
    execution_flow_path: str = "u/admin2/execute_ibkr_order",
) -> dict[str, Any]:
    if not isinstance(order_candidate, dict):
        return {
            "status": "skipped",
            "reason": "no_order_candidate",
            "job_id": None,
            "result": None,
        }

    job_id = wmill.run_flow_async(
        path=execution_flow_path,
        args={"order_candidate": order_candidate},
        do_not_track_in_parent=False,
    )
    try:
        result = _wait_for_job_result(job_id, timeout=1800)
        return {
            "status": "completed",
            "job_id": job_id,
            "result": result,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "job_id": job_id,
            "result": {
                "status": "error",
                "error": str(exc),
            },
        }
