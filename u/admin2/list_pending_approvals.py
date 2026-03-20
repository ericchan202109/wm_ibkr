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
    input_chart_refs = _chart_refs_from_charts(
        input_payload.get("daily_chart") if isinstance(input_payload.get("daily_chart"), dict) else {},
        input_payload.get("weekly_chart") if isinstance(input_payload.get("weekly_chart"), dict) else {},
    )
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
            report_markdown TEXT,
            chart_refs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            decision_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            strategy_results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            order_candidate_json JSONB,
            llm_prompt_text TEXT,
            llm_response_text TEXT,
            llm_response_json JSONB,
            integrated_input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            integrated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
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
            final_signal TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            order_candidate_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            reviewer TEXT,
            review_comment TEXT,
            execution_result_json JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def main(
    symbol: str = "",
    limit: int = 100,
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            if symbol:
                cur.execute(
                    """
                    SELECT
                        ar.id,
                        ar.signal_run_id,
                        ar.symbol,
                        ar.final_signal,
                        ar.status,
                        ar.created_at,
                        sr.canonical_strategy_key,
                        sr.report_markdown,
                        sr.chart_refs_json,
                        sr.integrated_input_json,
                        sr.strategy_results_json,
                        sr.decision_json,
                        sr.llm_response_text
                    FROM approval_requests ar
                    JOIN signal_runs sr ON sr.id = ar.signal_run_id
                    WHERE ar.status = 'pending'
                      AND ar.symbol = %s
                    ORDER BY ar.created_at DESC
                    LIMIT %s
                    """,
                    (str(symbol).strip().upper(), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        ar.id,
                        ar.signal_run_id,
                        ar.symbol,
                        ar.final_signal,
                        ar.status,
                        ar.created_at,
                        sr.canonical_strategy_key,
                        sr.report_markdown,
                        sr.chart_refs_json,
                        sr.integrated_input_json,
                        sr.strategy_results_json,
                        sr.decision_json,
                        sr.llm_response_text
                    FROM approval_requests ar
                    JOIN signal_runs sr ON sr.id = ar.signal_run_id
                    WHERE ar.status = 'pending'
                    ORDER BY ar.created_at DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
            rows = [
                {
                    "approval_request_id": row[0],
                    "signal_run_id": row[1],
                    "symbol": row[2],
                    "final_signal": row[3],
                    "status": row[4],
                    "created_at": row[5].isoformat() if row[5] else None,
                    "canonical_strategy_key": row[6],
                    "report_markdown": row[7],
                    "chart_refs": _resolve_chart_refs(
                        stored_chart_refs=row[8] if isinstance(row[8], dict) else {},
                        integrated_input=row[9] if isinstance(row[9], dict) else {},
                        strategy_results=row[10] if isinstance(row[10], list) else [],
                        canonical_strategy_key=row[6],
                    ),
                    "decision": row[11],
                    "llm_response_text": row[12],
                }
                for row in cur.fetchall()
            ]
            return {"rows": rows, "count": len(rows)}
    finally:
        conn.close()
