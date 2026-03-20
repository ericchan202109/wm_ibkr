from __future__ import annotations

import psycopg2
import wmill


def _looks_like_strategy_result(payload: dict) -> bool:
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


def _unwrap_strategy_result(payload, depth: int = 0) -> dict:
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


def _chart_ref_value_present(value) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, bool):
        return value
    if isinstance(value, list):
        return any(_chart_ref_value_present(item) for item in value)
    if isinstance(value, dict):
        return any(
            _chart_ref_value_present(value.get(key))
            for key in (
                "url",
                "public_url",
                "presigned_url",
                "object_path",
                "s3_object",
            )
        ) or any(_chart_ref_value_present(item) for item in value.values())
    return value is not None


def _chart_refs_from_charts(daily_chart: dict, weekly_chart: dict) -> dict:
    return {
        "daily": {
            "object_path": daily_chart.get("object_path"),
            "public_url": daily_chart.get("public_url"),
            "s3_object": daily_chart.get("s3_object"),
            "chart_available": daily_chart.get("chart_available"),
            "fetch_status": daily_chart.get("fetch_status"),
        },
        "weekly": {
            "object_path": weekly_chart.get("object_path"),
            "public_url": weekly_chart.get("public_url"),
            "s3_object": weekly_chart.get("s3_object"),
            "chart_available": weekly_chart.get("chart_available"),
            "fetch_status": weekly_chart.get("fetch_status"),
        },
    }


def _resolve_chart_refs(
    stored_chart_refs: dict | None,
    integrated_input: dict | None,
    strategy_results: list | None,
    canonical_strategy_key: str | None,
) -> dict:
    if isinstance(stored_chart_refs, dict) and _chart_ref_value_present(stored_chart_refs):
        return stored_chart_refs

    input_payload = integrated_input if isinstance(integrated_input, dict) else {}
    daily_chart = input_payload.get("daily_chart") if isinstance(input_payload.get("daily_chart"), dict) else {}
    weekly_chart = input_payload.get("weekly_chart") if isinstance(input_payload.get("weekly_chart"), dict) else {}
    input_chart_refs = _chart_refs_from_charts(daily_chart, weekly_chart)
    if _chart_ref_value_present(input_chart_refs):
        return input_chart_refs

    preferred_result = {}
    fallback_result = {}
    for item in strategy_results or []:
        if not isinstance(item, dict):
            continue
        candidate = _unwrap_strategy_result(item.get("result"))
        if not candidate:
            continue
        if (
            canonical_strategy_key
            and str(item.get("strategy_key") or "") == str(canonical_strategy_key)
        ):
            preferred_result = candidate
            break
        if not fallback_result:
            fallback_result = candidate

    selected_result = preferred_result or fallback_result
    if not selected_result:
        return {}
    return _chart_refs_from_charts(
        selected_result.get("daily_chart") if isinstance(selected_result.get("daily_chart"), dict) else {},
        selected_result.get("weekly_chart") if isinstance(selected_result.get("weekly_chart"), dict) else {},
    )


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
            status TEXT NOT NULL DEFAULT 'pending',
            reviewer TEXT,
            review_comment TEXT,
            execution_result_json JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            decided_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS order_events (
            id BIGSERIAL PRIMARY KEY,
            signal_run_id BIGINT REFERENCES signal_runs(id) ON DELETE SET NULL,
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
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(
                """
                SELECT
                    id,
                    symbol,
                    contract_status,
                    engine_status,
                    final_signal,
                    canonical_strategy_key,
                    execution_mode,
                    approval_required,
                    approval_status,
                    order_status,
                    conflict_detected,
                    decision_json,
                    strategy_results_json,
                    order_candidate_json,
                    chart_refs_json,
                    llm_prompt_text,
                    llm_response_text,
                    llm_response_json,
                    integrated_input_json,
                    integrated_output_json,
                    report_markdown,
                    warnings_json,
                    errors_json,
                    source_flow_job_id,
                    source_job_id,
                    created_at,
                    updated_at
                FROM signal_runs
                WHERE id = %s
                """,
                (int(signal_run_id),),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"signal_run_id={signal_run_id} not found")
            cur.execute(
                """
                SELECT id, status, reviewer, review_comment, execution_result_json, created_at, decided_at
                FROM approval_requests
                WHERE signal_run_id = %s
                ORDER BY created_at DESC
                """,
                (int(signal_run_id),),
            )
            approvals = [
                {
                    "approval_request_id": item[0],
                    "status": item[1],
                    "reviewer": item[2],
                    "review_comment": item[3],
                    "execution_result_json": item[4],
                    "created_at": item[5].isoformat() if item[5] else None,
                    "decided_at": item[6].isoformat() if item[6] else None,
                }
                for item in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT id, event_type, status, ibkr_order_id, execution_payload_json, created_at
                FROM order_events
                WHERE signal_run_id = %s
                ORDER BY created_at DESC
                """,
                (int(signal_run_id),),
            )
            order_events = [
                {
                    "order_event_id": item[0],
                    "event_type": item[1],
                    "status": item[2],
                    "ibkr_order_id": item[3],
                    "execution_payload_json": item[4],
                    "created_at": item[5].isoformat() if item[5] else None,
                }
                for item in cur.fetchall()
            ]
            resolved_chart_refs = _resolve_chart_refs(
                stored_chart_refs=row[14] if isinstance(row[14], dict) else {},
                integrated_input=row[18] if isinstance(row[18], dict) else {},
                strategy_results=row[12] if isinstance(row[12], list) else [],
                canonical_strategy_key=row[5],
            )
            return {
                "signal_run_id": row[0],
                "symbol": row[1],
                "contract_status": row[2],
                "engine_status": row[3],
                "final_signal": row[4],
                "canonical_strategy_key": row[5],
                "execution_mode": row[6],
                "approval_required": bool(row[7]),
                "approval_status": row[8],
                "order_status": row[9],
                "conflict_detected": bool(row[10]),
                "decision_json": row[11],
                "strategy_results_json": row[12],
                "order_candidate_json": row[13],
                "chart_refs_json": resolved_chart_refs,
                "llm_prompt_text": row[15],
                "llm_response_text": row[16],
                "llm_response_json": row[17],
                "integrated_input_json": row[18],
                "integrated_output_json": row[19],
                "report_markdown": row[20],
                "warnings_json": row[21],
                "errors_json": row[22],
                "source_flow_job_id": row[23],
                "source_job_id": row[24],
                "created_at": row[25].isoformat() if row[25] else None,
                "updated_at": row[26].isoformat() if row[26] else None,
                "approvals": approvals,
                "order_events": order_events,
            }
    finally:
        conn.close()
