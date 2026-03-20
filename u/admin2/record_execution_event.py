from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill


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


def _extract_payload(execution_result: dict[str, Any]) -> dict[str, Any]:
    if isinstance(execution_result.get("result"), dict):
        return execution_result["result"]
    return execution_result


def _ensure_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_runs (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
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
            symbol TEXT NOT NULL DEFAULT '',
            final_signal TEXT NOT NULL DEFAULT 'HOLD',
            status TEXT NOT NULL DEFAULT 'pending',
            order_candidate_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            reviewer TEXT,
            review_comment TEXT,
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
    signal_run_id: int,
    symbol: str,
    execution_result: dict,
    approval_request_id: int | None = None,
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    payload = _extract_payload(execution_result if isinstance(execution_result, dict) else {})
    status = str(payload.get("status") or execution_result.get("status") or "unknown")
    ibkr_order_id = payload.get("order_id") or payload.get("ibkr_order_id")
    approval_status = "approved_and_executed" if approval_request_id else "not_required"
    normalized_status = status.lower()
    if "error" in normalized_status or normalized_status in {"failed", "failure"}:
        order_status = "execution_failed"
        if approval_request_id:
            approval_status = "approved_execution_failed"
    elif status == "dry_run":
        order_status = "dry_run"
    elif status in {"placed", "completed"}:
        order_status = "submitted"
    else:
        order_status = status

    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
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
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(signal_run_id),
                    approval_request_id,
                    str(symbol or "").strip().upper(),
                    "execution",
                    order_status,
                    str(ibkr_order_id) if ibkr_order_id is not None else None,
                    Json(payload or {}),
                ),
            )
            event_id = cur.fetchone()[0]
            cur.execute(
                """
                UPDATE signal_runs
                SET order_status = %s,
                    approval_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (order_status, approval_status, int(signal_run_id)),
            )
            if approval_request_id:
                cur.execute(
                    """
                    UPDATE approval_requests
                    SET execution_result_json = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (Json(payload or {}), approval_request_id),
                )
    finally:
        conn.close()

    return {
        "order_event_id": event_id,
        "signal_run_id": int(signal_run_id),
        "approval_request_id": approval_request_id,
        "order_status": order_status,
        "approval_status": approval_status,
        "ibkr_order_id": ibkr_order_id,
    }
