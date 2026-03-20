from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill


VALID_CONTRACT_STATUSES = {"ACTIVE", "SUSPENDED", "ARCHIVED"}
VALID_EXECUTION_MODES = {"signal_only", "approval_gate", "auto_place"}


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
        CREATE TABLE IF NOT EXISTS contracts (
            symbol TEXT PRIMARY KEY,
            conid INTEGER NOT NULL
        );
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS exchange TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS currency TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS sec_type TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'ACTIVE';

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
        """
    )


def _normalize_daily_run_time(value: str) -> str:
    text = str(value or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        raise ValueError("daily_run_time must use HH:MM format")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("daily_run_time must use a valid 24-hour HH:MM time")
    return f"{hour:02d}:{minute:02d}"


def main(
    symbol: str,
    conid: int,
    exchange: str = "NASDAQ",
    currency: str = "USD",
    sec_type: str = "STK",
    status: str = "ACTIVE",
    timezone: str = "America/New_York",
    daily_run_time: str = "09:35",
    execution_mode: str = "approval_gate",
    order_exchange: str = "SMART",
    primary_exchange: str = "NASDAQ",
    use_rth: bool = True,
    strategy_key: str = "daily_weekly_llm",
    strategy_active: bool = True,
    strategy_priority: int = 10,
    notes: str = "",
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")
    resolved_status = str(status or "ACTIVE").strip().upper()
    if resolved_status not in VALID_CONTRACT_STATUSES:
        raise ValueError("status must be ACTIVE, SUSPENDED, or ARCHIVED")
    resolved_execution_mode = str(execution_mode or "approval_gate").strip().lower()
    if resolved_execution_mode not in VALID_EXECUTION_MODES:
        raise ValueError("execution_mode must be signal_only, approval_gate, or auto_place")
    resolved_daily_run_time = _normalize_daily_run_time(daily_run_time)

    config_json = {
        "scheduler": {},
        "market": {},
    }

    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(
                "SELECT config_json FROM workflow_symbol_config WHERE symbol = %s",
                (resolved_symbol,),
            )
            existing_row = cur.fetchone()
            existing_config = existing_row[0] if existing_row and isinstance(existing_row[0], dict) else {}
            existing_scheduler = existing_config.get("scheduler") if isinstance(existing_config.get("scheduler"), dict) else {}
            config_json = dict(existing_config)
            config_json["scheduler"] = {
                "mode": "daily",
                "timezone": timezone,
                "daily_run_time": resolved_daily_run_time,
                "last_run_at": existing_scheduler.get("last_run_at"),
            }
            config_json["execution_mode"] = resolved_execution_mode
            config_json["market"] = {
                "order_exchange": order_exchange,
                "primary_exchange": primary_exchange,
                "use_rth": bool(use_rth),
            }
            config_json["notes"] = notes
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
                VALUES (%s, %s, %s, %s, TRUE, %s, '{}'::jsonb, TRUE, %s, 'approval_gate')
                ON CONFLICT (strategy_key) DO NOTHING
                """,
                (
                    strategy_key,
                    "Daily + Weekly Integrated LLM",
                    "multi_timeframe_llm",
                    "u/admin2/strategy_daily_weekly_llm",
                    int(strategy_priority),
                    "u/admin2/capi_customai",
                ),
            )
            cur.execute(
                """
                INSERT INTO contracts (symbol, conid, exchange, currency, sec_type, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    conid = EXCLUDED.conid,
                    exchange = EXCLUDED.exchange,
                    currency = EXCLUDED.currency,
                    sec_type = EXCLUDED.sec_type,
                    status = EXCLUDED.status
                """,
                (resolved_symbol, int(conid), exchange, currency, sec_type, resolved_status),
            )
            cur.execute(
                """
                INSERT INTO workflow_symbol_config (symbol, config_json, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    config_json = EXCLUDED.config_json,
                    updated_at = NOW()
                """,
                (resolved_symbol, Json(config_json)),
            )
            cur.execute(
                """
                INSERT INTO strategy_symbol_links (
                    symbol,
                    strategy_key,
                    is_active,
                    priority,
                    override_param_json,
                    override_execution_mode,
                    override_risk_json,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, '{}'::jsonb, %s, '{}'::jsonb, NOW())
                ON CONFLICT (symbol, strategy_key) DO UPDATE SET
                    is_active = EXCLUDED.is_active,
                    priority = EXCLUDED.priority,
                    override_execution_mode = EXCLUDED.override_execution_mode,
                    updated_at = NOW()
                """,
                (
                    resolved_symbol,
                    strategy_key,
                    bool(strategy_active),
                    int(strategy_priority),
                    resolved_execution_mode,
                ),
            )
    finally:
        conn.close()

    return {
        "symbol": resolved_symbol,
        "conid": int(conid),
        "status": resolved_status,
        "config": config_json,
        "daily_run_time": resolved_daily_run_time,
        "strategy_key": strategy_key,
        "strategy_active": bool(strategy_active),
        "strategy_priority": int(strategy_priority),
    }
