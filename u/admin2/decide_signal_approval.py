from __future__ import annotations

import time
from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill


VALID_DECISIONS = {"approve", "reject"}
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


def _db_connect(db_resource_path: str):
    resource = wmill.get_resource(db_resource_path)
    return psycopg2.connect(
        host=resource.get("host"),
        port=resource.get("port", 5432),
        user=resource.get("user"),
        password=resource.get("password"),
        dbname=resource.get("dbname", "postgres"),
        sslmode=resource.get("sslmode", "require"),
    )


def _extract_execution_payload(result: dict[str, Any]) -> dict[str, Any]:
    if isinstance(result.get("result"), dict):
        return result["result"]
    return result


def _ensure_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_runs (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL DEFAULT '',
            contract_status TEXT,
            engine_status TEXT,
            final_signal TEXT,
            canonical_strategy_key TEXT,
            execution_mode TEXT,
            approval_required BOOLEAN NOT NULL DEFAULT FALSE,
            approval_status TEXT NOT NULL DEFAULT 'not_required',
            order_status TEXT NOT NULL DEFAULT 'not_submitted',
            conflict_detected BOOLEAN NOT NULL DEFAULT FALSE,
            decision_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            strategy_results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            order_candidate_json JSONB,
            chart_refs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            llm_prompt_text TEXT,
            llm_response_text TEXT,
            llm_response_json JSONB,
            integrated_input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            integrated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            report_markdown TEXT,
            warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            source_flow_job_id TEXT,
            source_job_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        ALTER TABLE signal_runs
            ADD COLUMN IF NOT EXISTS integrated_input_json JSONB NOT NULL DEFAULT '{}'::jsonb;
        ALTER TABLE signal_runs
            ADD COLUMN IF NOT EXISTS integrated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb;

        CREATE TABLE IF NOT EXISTS approval_requests (
            id BIGSERIAL PRIMARY KEY,
            signal_run_id BIGINT NOT NULL REFERENCES signal_runs(id) ON DELETE CASCADE,
            symbol TEXT NOT NULL,
            final_signal TEXT NOT NULL DEFAULT 'HOLD',
            status TEXT NOT NULL DEFAULT 'pending',
            reviewer TEXT,
            review_comment TEXT,
            order_candidate_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            execution_result_json JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            decided_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS order_events (
            id BIGSERIAL PRIMARY KEY,
            signal_run_id BIGINT REFERENCES signal_runs(id) ON DELETE SET NULL,
            approval_request_id BIGINT REFERENCES approval_requests(id) ON DELETE SET NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            ibkr_order_id TEXT,
            execution_payload_json JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def main(
    approval_request_id: int,
    decision: str,
    comment: str = "",
    execution_flow_path: str = "u/admin2/execute_ibkr_order",
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    resolved_decision = str(decision or "").strip().lower()
    if resolved_decision not in VALID_DECISIONS:
        raise ValueError("decision must be approve or reject")

    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(
                """
                SELECT id, signal_run_id, symbol, status, order_candidate_json
                FROM approval_requests
                WHERE id = %s
                """,
                (int(approval_request_id),),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"approval_request_id={approval_request_id} not found")

            _, signal_run_id, symbol, current_status, order_candidate_json = row
            current_status = str(current_status or "")
            if current_status != "pending":
                return {
                    "approval_request_id": int(approval_request_id),
                    "signal_run_id": signal_run_id,
                    "status": current_status,
                    "message": "Approval request already decided.",
                }

            if resolved_decision == "reject":
                cur.execute(
                    """
                    UPDATE approval_requests
                    SET status = 'rejected',
                        reviewer = CURRENT_USER,
                        review_comment = %s,
                        decided_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (comment or None, int(approval_request_id)),
                )
                cur.execute(
                    """
                    UPDATE signal_runs
                    SET approval_status = 'rejected',
                        order_status = 'not_submitted',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (signal_run_id,),
                )
                return {
                    "approval_request_id": int(approval_request_id),
                    "signal_run_id": signal_run_id,
                    "status": "rejected",
                }

            order_candidate = order_candidate_json if isinstance(order_candidate_json, dict) else {}
            job_id = wmill.run_flow_async(
                path=execution_flow_path,
                args={"order_candidate": order_candidate},
                do_not_track_in_parent=False,
            )
            try:
                execution_result = _wait_for_job_result(job_id, timeout=1800)
                execution_payload = _extract_execution_payload(execution_result if isinstance(execution_result, dict) else {})
                execution_status = str(execution_payload.get("status") or execution_result.get("status") or "unknown")
            except Exception as exc:
                execution_result = {
                    "status": "failed",
                    "error": str(exc),
                }
                execution_payload = execution_result
                execution_status = "error"
            order_status = "submitted" if execution_status in {"placed", "completed"} else "dry_run" if execution_status == "dry_run" else execution_status
            signal_approval_status = "approved" if order_status not in {"error", "execution_failed"} else "approved_execution_failed"

            cur.execute(
                """
                UPDATE approval_requests
                SET status = 'approved',
                    reviewer = CURRENT_USER,
                    review_comment = %s,
                    execution_result_json = %s,
                    decided_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    comment or None,
                    Json(execution_payload or {}),
                    int(approval_request_id),
                ),
            )
            cur.execute(
                """
                INSERT INTO order_events (
                    signal_run_id,
                    approval_request_id,
                    symbol,
                    event_type,
                    status,
                    ibkr_order_id,
                    execution_payload_json
                )
                VALUES (%s, %s, %s, 'approval_execution', %s, %s, %s)
                """,
                (
                    signal_run_id,
                    int(approval_request_id),
                    str(symbol or "").strip().upper(),
                    order_status,
                    str(execution_payload.get("order_id") or execution_payload.get("ibkr_order_id") or "") or None,
                    Json(execution_payload or {}),
                ),
            )
            cur.execute(
                """
                UPDATE signal_runs
                SET approval_status = %s,
                    order_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (signal_approval_status, order_status, signal_run_id),
            )
            return {
                "approval_request_id": int(approval_request_id),
                "signal_run_id": signal_run_id,
                "status": signal_approval_status,
                "execution_result": execution_result,
                "job_id": job_id,
            }
    finally:
        conn.close()
