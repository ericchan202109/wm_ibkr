from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill


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

        CREATE INDEX IF NOT EXISTS idx_signal_runs_symbol_created_at
            ON signal_runs (symbol, created_at DESC);

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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            decided_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_approval_requests_status_created_at
            ON approval_requests (status, created_at DESC);

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

        CREATE INDEX IF NOT EXISTS idx_order_events_signal_run_id
            ON order_events (signal_run_id, created_at DESC);
        """
    )


def _select_canonical_result(
    canonical_strategy_key: str | None,
    strategy_results: list[dict[str, Any]],
) -> dict[str, Any]:
    for item in strategy_results:
        if str(item.get("strategy_key") or "") == str(canonical_strategy_key or ""):
            result = item.get("result")
            return result if isinstance(result, dict) else {}
    for item in strategy_results:
        result = item.get("result")
        if isinstance(result, dict):
            return result
    return {}


def _normalize_strategy_results(strategy_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in strategy_results:
        if not isinstance(item, dict):
            continue
        normalized_item = dict(item)
        raw_result = item.get("result")
        normalized_result = _unwrap_strategy_result(raw_result)
        if normalized_result:
            normalized_item["result"] = normalized_result
        normalized.append(normalized_item)
    return normalized


def _chart_refs_from_charts(
    daily_chart: dict[str, Any],
    weekly_chart: dict[str, Any],
) -> dict[str, Any]:
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


def _chart_refs(canonical_result: dict[str, Any]) -> dict[str, Any]:
    daily_chart = canonical_result.get("daily_chart") if isinstance(canonical_result.get("daily_chart"), dict) else {}
    weekly_chart = canonical_result.get("weekly_chart") if isinstance(canonical_result.get("weekly_chart"), dict) else {}
    return _chart_refs_from_charts(daily_chart, weekly_chart)


def _integrated_input_snapshot(
    symbol: str,
    canonical_strategy_key: str | None,
    canonical_result: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    snapshot = canonical_result.get("input_snapshot")
    if isinstance(snapshot, dict):
        return snapshot

    active_strategies = (
        context.get("active_strategies")
        if isinstance(context.get("active_strategies"), list)
        else []
    )
    strategy_context: dict[str, Any] = {}
    for item in active_strategies:
        if not isinstance(item, dict):
            continue
        if str(item.get("strategy_key") or "") == str(canonical_strategy_key or ""):
            strategy_context = item
            break

    return {
        "symbol": symbol,
        "strategy_key": canonical_strategy_key,
        "strategy_name": canonical_result.get("strategy_name"),
        "strategy_context": strategy_context,
        "workflow_config": context.get("global_config")
        if isinstance(context.get("global_config"), dict)
        else {},
        "symbol_config": context.get("symbol_config")
        if isinstance(context.get("symbol_config"), dict)
        else {},
        "daily_report": canonical_result.get("daily_report")
        if isinstance(canonical_result.get("daily_report"), dict)
        else {},
        "weekly_report": canonical_result.get("weekly_report")
        if isinstance(canonical_result.get("weekly_report"), dict)
        else {},
        "daily_chart": canonical_result.get("daily_chart")
        if isinstance(canonical_result.get("daily_chart"), dict)
        else {},
        "weekly_chart": canonical_result.get("weekly_chart")
        if isinstance(canonical_result.get("weekly_chart"), dict)
        else {},
    }


def _integrated_output_snapshot(canonical_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = canonical_result.get("output_snapshot")
    if isinstance(snapshot, dict):
        if "chart_refs" not in snapshot:
            snapshot = dict(snapshot)
            snapshot["chart_refs"] = _chart_refs(canonical_result)
        return snapshot
    return {
        "status": canonical_result.get("status"),
        "decision": canonical_result.get("decision")
        if isinstance(canonical_result.get("decision"), dict)
        else {},
        "latest": canonical_result.get("latest")
        if isinstance(canonical_result.get("latest"), dict)
        else {},
        "report_markdown": canonical_result.get("report_markdown"),
        "llm": {
            "provider": canonical_result.get("llm_provider"),
            "model_used": canonical_result.get("llm_model_used"),
            "chart_image_count": canonical_result.get("llm_chart_image_count"),
        },
        "warnings": canonical_result.get("warnings")
        if isinstance(canonical_result.get("warnings"), list)
        else [],
        "errors": canonical_result.get("errors")
        if isinstance(canonical_result.get("errors"), list)
        else [],
        "chart_refs": _chart_refs(canonical_result),
    }


def main(
    symbol: str,
    context: dict,
    strategy_run: dict,
    aggregate: dict,
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    raw_strategy_results = strategy_run.get("results") if isinstance(strategy_run.get("results"), list) else []
    strategy_results = _normalize_strategy_results(raw_strategy_results)
    aggregate_payload = aggregate if isinstance(aggregate, dict) else {}
    canonical_strategy_key = aggregate_payload.get("canonical_strategy_key")
    canonical_result = _select_canonical_result(canonical_strategy_key, strategy_results)

    approval_required = bool(aggregate_payload.get("requires_approval"))
    should_auto_place = bool(aggregate_payload.get("should_auto_place"))
    order_candidate = aggregate_payload.get("order_candidate") if isinstance(aggregate_payload.get("order_candidate"), dict) else None
    contract = context.get("contract") if isinstance(context.get("contract"), dict) else {}
    llm_response_json = canonical_result.get("llm_response_json") if isinstance(canonical_result.get("llm_response_json"), dict) else None
    persisted_warnings: list[str] = []
    for source in (
        strategy_run.get("warnings") if isinstance(strategy_run, dict) else None,
        aggregate_payload.get("warnings"),
        canonical_result.get("warnings"),
    ):
        if isinstance(source, list):
            persisted_warnings.extend(str(item) for item in source if item)
    if strategy_results and not canonical_result:
        missing_key = str(canonical_strategy_key or "").strip() or "unresolved"
        persisted_warnings.append(f"canonical_strategy_result_missing:{missing_key}")

    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    signal_run_id = None
    approval_request_id = None
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            source_flow_job_id = (
                os.environ.get("WM_ROOT_FLOW_JOB_ID")
                or os.environ.get("WM_ROOT_JOB_ID")
                or os.environ.get("WM_JOB_ID")
            )
            source_job_id = os.environ.get("WM_JOB_ID")
            cur.execute(
                """
                INSERT INTO signal_runs (
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
                    source_job_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    resolved_symbol,
                    str(contract.get("status") or aggregate_payload.get("contract_status") or "UNREGISTERED").upper(),
                    str(strategy_run.get("status") or "ok"),
                    aggregate_payload.get("final_signal"),
                    canonical_strategy_key,
                    aggregate_payload.get("execution_mode"),
                    approval_required,
                    "approval_required" if approval_required else "not_required",
                    "queued_for_auto_execution" if should_auto_place else "awaiting_approval" if approval_required else "not_submitted",
                    bool(aggregate_payload.get("conflict_detected")),
                    Json(canonical_result.get("decision") if isinstance(canonical_result.get("decision"), dict) else {}),
                    Json(strategy_results),
                    Json(order_candidate) if order_candidate else None,
                    Json(_chart_refs(canonical_result)),
                    canonical_result.get("llm_prompt_text"),
                    canonical_result.get("llm_raw_text"),
                    Json(llm_response_json) if llm_response_json else None,
                    Json(
                        _integrated_input_snapshot(
                            resolved_symbol,
                            canonical_strategy_key,
                            canonical_result,
                            context,
                        )
                    ),
                    Json(_integrated_output_snapshot(canonical_result)),
                    aggregate_payload.get("final_report_markdown"),
                    Json(persisted_warnings),
                    Json(canonical_result.get("errors") if isinstance(canonical_result.get("errors"), list) else []),
                    source_flow_job_id,
                    source_job_id,
                ),
            )
            signal_run_id = cur.fetchone()[0]

            if approval_required and order_candidate:
                cur.execute(
                    """
                    INSERT INTO approval_requests (
                        signal_run_id,
                        symbol,
                        final_signal,
                        status,
                        order_candidate_json
                    )
                    VALUES (%s, %s, %s, 'pending', %s)
                    RETURNING id
                    """,
                    (
                        signal_run_id,
                        resolved_symbol,
                        aggregate_payload.get("final_signal"),
                        Json(order_candidate),
                    ),
                )
                approval_request_id = cur.fetchone()[0]

    finally:
        conn.close()

    return {
        "signal_run_id": signal_run_id,
        "approval_request_id": approval_request_id,
        "approval_required": approval_required,
        "approval_status": "approval_required" if approval_required else "not_required",
        "order_status": "queued_for_auto_execution" if should_auto_place else "awaiting_approval" if approval_required else "not_submitted",
        "should_auto_place": should_auto_place,
    }
