from __future__ import annotations

from copy import deepcopy
from math import floor
from typing import Any


VALID_EXECUTION_MODES = {"signal_only", "approval_gate", "auto_place"}
BLOCKED_CONTRACT_STATUSES = {"SUSPENDED", "ARCHIVED"}


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_signal(value: Any) -> str:
    candidate = str(value or "").strip().upper()
    if candidate in {"BUY", "SELL", "HOLD"}:
        return candidate
    return "HOLD"


def _normalize_execution_mode(value: Any, fallback: str) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in VALID_EXECUTION_MODES:
        return candidate
    return fallback


def _pick_price_anchor(result_payload: dict[str, Any]) -> float | None:
    decision = result_payload.get("decision") if isinstance(result_payload.get("decision"), dict) else {}
    entry_range = decision.get("entry_range") if isinstance(decision.get("entry_range"), dict) else {}
    low = _to_float(entry_range.get("low"))
    high = _to_float(entry_range.get("high"))
    if low is not None and high is not None:
        return (low + high) / 2.0
    if low is not None:
        return low
    if high is not None:
        return high
    latest = result_payload.get("latest") if isinstance(result_payload.get("latest"), dict) else {}
    return _to_float(latest.get("close"))


def _compute_quantity(anchor_price: float | None, stop_loss: float | None, risk_config: dict[str, Any]) -> tuple[int, str]:
    if anchor_price is None or anchor_price <= 0:
        return 0, "no_anchor_price"

    account_size = _to_float(risk_config.get("account_size_usd"))
    per_trade_risk_pct = _to_float(risk_config.get("per_trade_risk_pct"))
    max_risk_usd = _to_float(risk_config.get("max_risk_usd"))
    max_position_notional_usd = _to_float(risk_config.get("max_position_notional_usd"))

    risk_budget = None
    if account_size is not None and per_trade_risk_pct is not None and per_trade_risk_pct > 0:
        risk_budget = account_size * per_trade_risk_pct
    elif max_risk_usd is not None:
        risk_budget = max_risk_usd

    if risk_budget and stop_loss is not None:
        risk_per_share = abs(anchor_price - stop_loss)
        if risk_per_share > 0:
            quantity = floor(risk_budget / risk_per_share)
            if max_position_notional_usd and max_position_notional_usd > 0:
                quantity = min(quantity, floor(max_position_notional_usd / anchor_price))
            if quantity > 0:
                return int(quantity), "risk_based"

    buckets = risk_config.get("fixed_qty_buckets")
    if isinstance(buckets, list):
        valid_buckets = []
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            min_price = _to_float(bucket.get("min_price"))
            qty = bucket.get("qty")
            if min_price is None:
                continue
            try:
                qty_int = int(qty)
            except (TypeError, ValueError):
                continue
            valid_buckets.append((min_price, qty_int))
        valid_buckets.sort(key=lambda item: item[0], reverse=True)
        for min_price, qty in valid_buckets:
            if anchor_price >= min_price:
                return qty, "fixed_bucket"

    return 0, "no_quantity_rule"


def _normalize_strategy_result(item: dict[str, Any]) -> dict[str, Any]:
    strategy_definition = item.get("strategy_definition") if isinstance(item.get("strategy_definition"), dict) else {}
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    decision = result.get("decision") if isinstance(result.get("decision"), dict) else {}
    signal = _normalize_signal(decision.get("side") or decision.get("final_signal"))
    actionable = signal in {"BUY", "SELL"}
    return {
        "strategy_key": str(item.get("strategy_key") or strategy_definition.get("strategy_key") or ""),
        "priority": int(item.get("priority") or strategy_definition.get("priority") or 9999),
        "status": str(item.get("status") or "error"),
        "strategy_definition": strategy_definition,
        "result": result,
        "signal": signal,
        "actionable": actionable,
        "confidence": _to_float(decision.get("confidence")) or 0.0,
        "report_markdown": result.get("report_markdown"),
    }


def main(
    symbol: str,
    strategy_results: list,
    workflow_config: dict,
    symbol_config: dict,
    contract: dict | None = None,
    execution_mode_override: str = "",
    dry_run: bool = True,
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    exchange: str = "SMART",
    primary_exchange: str = "NASDAQ",
    currency: str = "USD",
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    normalized_results = [
        _normalize_strategy_result(item)
        for item in (strategy_results or [])
        if isinstance(item, dict)
    ]
    normalized_results.sort(key=lambda item: (item["priority"], item["strategy_key"]))

    successful_results = [item for item in normalized_results if item["status"] == "success"]
    actionable_results = [item for item in successful_results if item["actionable"]]
    signals = {item["signal"] for item in actionable_results}
    conflict_detected = len(signals) > 1

    canonical = None
    if conflict_detected:
        canonical = actionable_results[0] if actionable_results else None
    elif actionable_results:
        canonical = actionable_results[0]
    elif successful_results:
        canonical = successful_results[0]

    warnings: list[str] = []
    if conflict_detected:
        warnings.append("Multiple active strategies produced conflicting actionable signals. Order placement is blocked.")
    contract_status = str((contract or {}).get("status") or "UNREGISTERED").upper()
    symbol_blocked = contract_status in BLOCKED_CONTRACT_STATUSES
    if symbol_blocked:
        warnings.append(f"Symbol {resolved_symbol} is {contract_status}; order placement is blocked.")

    global_risk = workflow_config.get("risk") if isinstance(workflow_config.get("risk"), dict) else {}
    symbol_risk = symbol_config.get("risk") if isinstance(symbol_config.get("risk"), dict) else {}
    effective_risk = _deep_merge(global_risk, symbol_risk)

    final_report_markdown = None
    final_signal = "HOLD"
    canonical_strategy_key = None
    order_candidate = None
    order_reason = "no_actionable_signal"
    execution_mode = _normalize_execution_mode(
        execution_mode_override,
        _normalize_execution_mode(
            workflow_config.get("default_execution_mode"),
            "approval_gate",
        ),
    )

    if canonical:
        canonical_strategy_key = canonical["strategy_key"]
        final_report_markdown = canonical.get("report_markdown")
        decision = canonical["result"].get("decision") if isinstance(canonical["result"].get("decision"), dict) else {}
        final_signal = canonical["signal"]
        execution_mode = _normalize_execution_mode(
            execution_mode_override
            or canonical["strategy_definition"].get("execution_mode"),
            execution_mode,
        )

        if canonical["actionable"] and not conflict_detected and not symbol_blocked:
            anchor_price = _pick_price_anchor(canonical["result"])
            stop_loss = _to_float(decision.get("stop_loss"))
            strategy_risk = canonical["strategy_definition"].get("effective_risk")
            if isinstance(strategy_risk, dict):
                effective_risk = _deep_merge(effective_risk, strategy_risk)
            quantity, quantity_source = _compute_quantity(anchor_price, stop_loss, effective_risk)
            if quantity > 0:
                entry_range = decision.get("entry_range") if isinstance(decision.get("entry_range"), dict) else {}
                limit_price = None
                if _to_float(entry_range.get("low")) is not None or _to_float(entry_range.get("high")) is not None:
                    limit_price = anchor_price

                order_defaults = workflow_config.get("order_defaults") if isinstance(workflow_config.get("order_defaults"), dict) else {}
                order_type = str(decision.get("order_type") or order_defaults.get("order_type") or "MKT").upper()
                if order_type == "LMT" and limit_price is None:
                    order_type = "MKT"

                time_in_force = str(decision.get("time_in_force") or order_defaults.get("time_in_force") or "DAY").upper()
                order_candidate = {
                    "symbol": resolved_symbol,
                    "side": canonical["signal"],
                    "quantity": int(quantity),
                    "quantity_source": quantity_source,
                    "price_anchor": anchor_price,
                    "stop_loss": stop_loss,
                    "limit_price": limit_price,
                    "order_type": order_type,
                    "time_in_force": time_in_force,
                    "execution_mode": execution_mode,
                    "host": host,
                    "port": int(port),
                    "client_id": int(client_id),
                    "exchange": str(order_defaults.get("exchange") or exchange or "SMART"),
                    "primary_exchange": str(order_defaults.get("primary_exchange") or primary_exchange or ""),
                    "currency": str(order_defaults.get("currency") or currency or "USD"),
                    "outside_rth": bool(order_defaults.get("outside_rth") or False),
                    "dry_run": bool(dry_run),
                    "source_strategy_key": canonical_strategy_key,
                    "confidence": canonical["confidence"],
                }
                order_reason = "candidate_ready"
            else:
                order_reason = "quantity_unresolved"
                warnings.append("No position size could be computed from the configured risk rules.")
        elif conflict_detected:
            order_reason = "blocked_by_conflict"
        elif symbol_blocked:
            order_reason = f"blocked_by_status_{contract_status.lower()}"

    requires_approval = bool(order_candidate) and execution_mode == "approval_gate"
    should_auto_place = bool(order_candidate) and execution_mode == "auto_place"
    execution_ready = bool(order_candidate) and execution_mode == "auto_place"

    return {
        "symbol": resolved_symbol,
        "contract_status": contract_status,
        "symbol_blocked": symbol_blocked,
        "final_signal": final_signal,
        "final_report_markdown": final_report_markdown,
        "strategy_results": normalized_results,
        "canonical_strategy_key": canonical_strategy_key,
        "conflict_detected": conflict_detected,
        "signals_seen": sorted(signals),
        "order_candidate": order_candidate,
        "order_reason": order_reason,
        "execution_mode": execution_mode,
        "requires_approval": requires_approval,
        "should_auto_place": should_auto_place,
        "execution_ready": execution_ready,
        "warnings": warnings,
    }
