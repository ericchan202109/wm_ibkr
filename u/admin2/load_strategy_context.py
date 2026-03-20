from __future__ import annotations

from copy import deepcopy
from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill


VALID_EXECUTION_MODES = {"signal_only", "approval_gate", "auto_place"}
BLOCKED_CONTRACT_STATUSES = {"SUSPENDED", "ARCHIVED"}


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


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


def _default_workflow_config(
    llm_resource_path: str,
    s3_resource_path: str,
) -> dict[str, Any]:
    return {
        "default_execution_mode": "approval_gate",
        "conflict_policy": {
            "block_auto_order_on_conflict": True,
            "hold_is_conflict": False,
        },
        "llm": {
            "resource_path": llm_resource_path,
            "model": "gpt-5.4",
            "temperature": 0.1,
            "max_tokens": 2200,
        },
        "artifacts": {
            "s3_resource_path": s3_resource_path,
            "chart_prefix": "charts/ibkr_strategy_engine",
        },
        "order_defaults": {
            "exchange": "SMART",
            "primary_exchange": "NASDAQ",
            "currency": "USD",
            "order_type": "LMT",
            "time_in_force": "DAY",
            "outside_rth": False,
        },
        "risk": {
            "account_size_usd": 25000.0,
            "per_trade_risk_pct": 0.01,
            "max_risk_usd": 250.0,
            "max_position_notional_usd": 5000.0,
            "fixed_qty_buckets": [
                {"min_price": 100.0, "qty": 1},
                {"min_price": 10.0, "qty": 10},
                {"min_price": 1.0, "qty": 100},
                {"min_price": 0.0, "qty": 1000},
            ],
        },
        "strategies": {
            "daily_weekly_llm": {
                "enabled": True,
                "description": "Daily and weekly technical synthesis with LLM consolidation.",
            }
        },
        "scheduler": {
            "mode": "daily",
            "timezone": "America/New_York",
            "daily_run_time": "09:35",
        },
    }


def _default_strategy_definition(llm_resource_path: str) -> dict[str, Any]:
    return {
        "strategy_key": "daily_weekly_llm",
        "name": "Daily + Weekly Integrated LLM",
        "strategy_type": "multi_timeframe_llm",
        "runner_flow_path": "u/admin2/strategy_daily_weekly_llm",
        "is_active": True,
        "priority": 10,
        "llm_enabled": True,
        "llm_resource_path": llm_resource_path,
        "execution_mode_default": "approval_gate",
        "param_json": {
            "strategy_family": "multi_timeframe",
            "report_style": {
                "title_prefix": "Integrated Technical Analysis Report",
            },
            "timeframes": {
                "daily": {
                    "label": "daily",
                    "bar_size": "1 day",
                    "lookback_days": 365,
                },
                "weekly": {
                    "label": "weekly",
                    "bar_size": "1 week",
                    "lookback_days": 730,
                },
            },
            "llm": {
                "model": "gpt-5.4",
                "temperature": 0.1,
                "max_tokens": 2200,
            },
        },
    }


def _fetch_one_json(cur, query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    cur.execute(query, params)
    row = cur.fetchone()
    value = row[0] if row else None
    return value if isinstance(value, dict) else {}


def _normalize_execution_mode(value: Any, fallback: str) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in VALID_EXECUTION_MODES:
        return candidate
    return fallback


def main(
    symbol: str,
    db_resource_path: str = "u/admin2/supabase_postgresql",
    llm_resource_path: str = "u/admin2/capi_customai",
    s3_resource_path: str = "u/admin2/minio_s3",
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    seeded_defaults: list[str] = []

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contracts (
                    symbol TEXT PRIMARY KEY,
                    conid INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workflow_global_config (
                    config_key TEXT PRIMARY KEY,
                    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS workflow_symbol_config (
                    symbol TEXT PRIMARY KEY,
                    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS strategy_definitions (
                    strategy_key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    strategy_type TEXT NOT NULL,
                    runner_flow_path TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    priority INTEGER NOT NULL DEFAULT 100,
                    param_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    llm_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    llm_resource_path TEXT,
                    execution_mode_default TEXT NOT NULL DEFAULT 'approval_gate',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS strategy_symbol_links (
                    symbol TEXT NOT NULL,
                    strategy_key TEXT NOT NULL REFERENCES strategy_definitions(strategy_key) ON DELETE CASCADE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    priority INTEGER,
                    override_param_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    override_execution_mode TEXT,
                    override_risk_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (symbol, strategy_key)
                );

                CREATE INDEX IF NOT EXISTS idx_strategy_symbol_links_symbol
                    ON strategy_symbol_links (symbol);

                ALTER TABLE contracts ADD COLUMN IF NOT EXISTS exchange TEXT;
                ALTER TABLE contracts ADD COLUMN IF NOT EXISTS currency TEXT;
                ALTER TABLE contracts ADD COLUMN IF NOT EXISTS sec_type TEXT;
                ALTER TABLE contracts ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'ACTIVE';
                """
            )

            workflow_config = _default_workflow_config(
                llm_resource_path=llm_resource_path,
                s3_resource_path=s3_resource_path,
            )
            cur.execute(
                """
                INSERT INTO workflow_global_config (config_key, config_json)
                VALUES ('default', %s)
                ON CONFLICT (config_key) DO NOTHING;
                """,
                (Json(workflow_config),),
            )
            if cur.rowcount:
                seeded_defaults.append("workflow_global_config.default")

            strategy_definition = _default_strategy_definition(llm_resource_path)
            cur.execute(
                """
                INSERT INTO strategy_definitions (
                    strategy_key,
                    name,
                    strategy_type,
                    runner_flow_path,
                    is_active,
                    priority,
                    param_json,
                    llm_enabled,
                    llm_resource_path,
                    execution_mode_default
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (strategy_key) DO NOTHING;
                """,
                (
                    strategy_definition["strategy_key"],
                    strategy_definition["name"],
                    strategy_definition["strategy_type"],
                    strategy_definition["runner_flow_path"],
                    strategy_definition["is_active"],
                    strategy_definition["priority"],
                    Json(strategy_definition["param_json"]),
                    strategy_definition["llm_enabled"],
                    strategy_definition["llm_resource_path"],
                    strategy_definition["execution_mode_default"],
                ),
            )
            if cur.rowcount:
                seeded_defaults.append("strategy_definitions.daily_weekly_llm")

            global_config = _fetch_one_json(
                cur,
                "SELECT config_json FROM workflow_global_config WHERE config_key = %s",
                ("default",),
            )
            symbol_config = _fetch_one_json(
                cur,
                "SELECT config_json FROM workflow_symbol_config WHERE symbol = %s",
                (resolved_symbol,),
            )
            cur.execute(
                """
                SELECT symbol, conid, exchange, currency, sec_type, status
                FROM contracts
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            contract_row = cur.fetchone()
            contract = None
            if contract_row:
                contract = {
                    "symbol": contract_row[0],
                    "conid": contract_row[1],
                    "exchange": contract_row[2],
                    "currency": contract_row[3],
                    "sec_type": contract_row[4],
                    "status": str(contract_row[5] or "ACTIVE").upper(),
                }

            cur.execute(
                "SELECT COUNT(*) FROM strategy_symbol_links WHERE symbol = %s",
                (resolved_symbol,),
            )
            has_symbol_links = (cur.fetchone() or [0])[0] > 0

            if has_symbol_links:
                cur.execute(
                    """
                    SELECT
                        d.strategy_key,
                        d.name,
                        d.strategy_type,
                        d.runner_flow_path,
                        d.priority,
                        d.param_json,
                        d.llm_enabled,
                        d.llm_resource_path,
                        d.execution_mode_default,
                        l.priority,
                        l.override_param_json,
                        l.override_execution_mode,
                        l.override_risk_json
                    FROM strategy_symbol_links l
                    JOIN strategy_definitions d
                      ON d.strategy_key = l.strategy_key
                    WHERE l.symbol = %s
                      AND l.is_active = TRUE
                      AND d.is_active = TRUE
                    ORDER BY COALESCE(l.priority, d.priority) ASC, d.strategy_key ASC;
                    """,
                    (resolved_symbol,),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        d.strategy_key,
                        d.name,
                        d.strategy_type,
                        d.runner_flow_path,
                        d.priority,
                        d.param_json,
                        d.llm_enabled,
                        d.llm_resource_path,
                        d.execution_mode_default,
                        NULL::INTEGER,
                        '{}'::jsonb,
                        NULL::TEXT,
                        '{}'::jsonb
                    FROM strategy_definitions d
                    WHERE d.is_active = TRUE
                    ORDER BY d.priority ASC, d.strategy_key ASC;
                    """
                )

            active_strategies: list[dict[str, Any]] = []
            for row in cur.fetchall():
                (
                    strategy_key,
                    name,
                    strategy_type,
                    runner_flow_path,
                    base_priority,
                    param_json,
                    llm_enabled,
                    strategy_llm_resource_path,
                    execution_mode_default,
                    link_priority,
                    override_param_json,
                    override_execution_mode,
                    override_risk_json,
                ) = row

                base_params = param_json if isinstance(param_json, dict) else {}
                override_params = (
                    override_param_json if isinstance(override_param_json, dict) else {}
                )
                effective_params = _deep_merge(base_params, override_params)

                base_risk = global_config.get("risk") if isinstance(global_config, dict) else {}
                symbol_risk = symbol_config.get("risk") if isinstance(symbol_config, dict) else {}
                link_risk = override_risk_json if isinstance(override_risk_json, dict) else {}
                effective_risk = _deep_merge(base_risk or {}, symbol_risk or {})
                effective_risk = _deep_merge(effective_risk, link_risk)
                contract_status = str((contract or {}).get("status") or "UNREGISTERED").upper()
                strategy_enabled = contract_status not in BLOCKED_CONTRACT_STATUSES

                fallback_execution_mode = _normalize_execution_mode(
                    global_config.get("default_execution_mode"),
                    "approval_gate",
                )
                effective_execution_mode = _normalize_execution_mode(
                    override_execution_mode
                    or symbol_config.get("execution_mode")
                    or execution_mode_default,
                    fallback_execution_mode,
                )

                strategy_priority = int(link_priority if link_priority is not None else base_priority)
                effective_llm_resource_path = (
                    str(strategy_llm_resource_path or "").strip() or llm_resource_path
                )

                active_strategies.append(
                    {
                        "strategy_key": strategy_key,
                        "name": name,
                        "strategy_type": strategy_type,
                        "runner_flow_path": runner_flow_path,
                        "priority": strategy_priority,
                        "llm_enabled": bool(llm_enabled),
                        "llm_resource_path": effective_llm_resource_path,
                        "execution_mode": effective_execution_mode,
                        "effective_params": effective_params,
                        "effective_risk": effective_risk,
                        "symbol_enabled": strategy_enabled,
                        "contract_status": contract_status,
                    }
                )

    finally:
        conn.close()

    return {
        "symbol": resolved_symbol,
        "db_resource_path": db_resource_path,
        "global_config": global_config,
        "symbol_config": symbol_config,
        "contract": contract,
        "symbol_enabled": str((contract or {}).get("status") or "UNREGISTERED").upper() not in BLOCKED_CONTRACT_STATUSES,
        "active_strategies": active_strategies,
        "seeded_defaults": seeded_defaults,
        "has_symbol_links": has_symbol_links,
    }
