from __future__ import annotations

from typing import Any


def main(
    symbol: str,
    context: dict,
    strategy_run: dict,
    aggregate: dict,
    persisted_run: dict | None = None,
    execution_event: dict | None = None,
    execution_result: dict | None = None,
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    execution_payload = execution_result if isinstance(execution_result, dict) else {}
    aggregate_payload = aggregate if isinstance(aggregate, dict) else {}
    persisted_payload = persisted_run if isinstance(persisted_run, dict) else {}
    execution_event_payload = execution_event if isinstance(execution_event, dict) else {}

    effective_approval_status = str(
        execution_event_payload.get("approval_status")
        or persisted_payload.get("approval_status")
        or ("approval_required" if aggregate_payload.get("requires_approval") else "not_required")
    )
    effective_order_status = str(
        execution_event_payload.get("order_status")
        or persisted_payload.get("order_status")
        or "not_submitted"
    )

    approval_status = effective_approval_status

    order_execution = execution_payload.get("result") if isinstance(execution_payload.get("result"), dict) else execution_payload
    warnings = []
    for source in (context, strategy_run, aggregate_payload, persisted_payload, execution_payload):
        if not isinstance(source, dict):
            continue
        source_warnings = source.get("warnings")
        if isinstance(source_warnings, list):
            warnings.extend(str(item) for item in source_warnings if item)

    errors = []
    strategy_items = strategy_run.get("results") if isinstance(strategy_run, dict) else []
    if isinstance(strategy_items, list):
        for item in strategy_items:
            if not isinstance(item, dict):
                continue
            if item.get("status") == "error" and item.get("error"):
                errors.append(str(item["error"]))

    status = "ok"
    if aggregate_payload.get("conflict_detected"):
        status = "blocked_conflict"
    elif persisted_payload.get("approval_required") and not order_execution:
        status = "approval_pending"
    elif effective_order_status == "execution_failed":
        status = "execution_failed"
    elif errors:
        status = "partial_failure"
    elif order_execution:
        status = "executed"
    elif aggregate_payload.get("final_signal") == "HOLD":
        status = "no_action"

    return {
        "symbol": resolved_symbol,
        "status": status,
        "strategy_results": strategy_run.get("results") if isinstance(strategy_run, dict) else [],
        "conflict_detected": bool(aggregate_payload.get("conflict_detected")),
        "canonical_strategy_key": aggregate_payload.get("canonical_strategy_key"),
        "final_signal": aggregate_payload.get("final_signal"),
        "final_report_markdown": aggregate_payload.get("final_report_markdown"),
        "order_candidate": aggregate_payload.get("order_candidate"),
        "approval_status": approval_status,
        "approval_required": bool(persisted_payload.get("approval_required", aggregate_payload.get("requires_approval"))),
        "signal_run_id": persisted_payload.get("signal_run_id"),
        "approval_request_id": persisted_payload.get("approval_request_id"),
        "order_status": effective_order_status,
        "order_execution": order_execution,
        "warnings": warnings,
        "errors": errors,
        "context": {
            "seeded_defaults": context.get("seeded_defaults") if isinstance(context, dict) else [],
            "active_strategy_count": len(context.get("active_strategies") or []) if isinstance(context, dict) else 0,
            "execution_mode": aggregate_payload.get("execution_mode"),
            "contract": context.get("contract") if isinstance(context, dict) else None,
        },
    }
