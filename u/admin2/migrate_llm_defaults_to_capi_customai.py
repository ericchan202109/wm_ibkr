from __future__ import annotations

from copy import deepcopy
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


def _deepcopy_dict(value: Any) -> dict[str, Any]:
    return deepcopy(value) if isinstance(value, dict) else {}


def _update_llm_block(
    config: dict[str, Any],
    old_resource_path: str,
    new_resource_path: str,
    old_model: str,
    new_model: str,
    create_if_missing: bool = False,
) -> tuple[dict[str, Any], bool]:
    updated = deepcopy(config)
    changed = False

    llm = updated.get("llm")
    if not isinstance(llm, dict):
        if not create_if_missing:
            return updated, False
        llm = {}
        updated["llm"] = llm
        changed = True

    current_resource = str(llm.get("resource_path") or "").strip()
    if not current_resource or current_resource == old_resource_path:
        if current_resource != new_resource_path:
            llm["resource_path"] = new_resource_path
            changed = True

    current_model = str(llm.get("model") or "").strip()
    if not current_model or current_model == old_model:
        if current_model != new_model:
            llm["model"] = new_model
            changed = True

    return updated, changed


def main(
    db_resource_path: str = "u/admin2/supabase_postgresql",
    old_resource_path: str = "u/admin2/successful_anthropic",
    new_resource_path: str = "u/admin2/capi_customai",
    old_model: str = "claude-3-7-sonnet-latest",
    new_model: str = "gpt-5.4",
) -> dict[str, Any]:
    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    updates = {
        "workflow_global_config": 0,
        "strategy_definitions_resource": 0,
        "strategy_definitions_model": 0,
        "workflow_symbol_config": 0,
    }
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """
            )

            cur.execute(
                "SELECT config_json FROM workflow_global_config WHERE config_key = 'default'"
            )
            row = cur.fetchone()
            if row:
                config = _deepcopy_dict(row[0])
                updated_config, changed = _update_llm_block(
                    config=config,
                    old_resource_path=old_resource_path,
                    new_resource_path=new_resource_path,
                    old_model=old_model,
                    new_model=new_model,
                    create_if_missing=True,
                )
                if changed:
                    cur.execute(
                        """
                        UPDATE workflow_global_config
                        SET config_json = %s,
                            updated_at = NOW()
                        WHERE config_key = 'default'
                        """,
                        (Json(updated_config),),
                    )
                    updates["workflow_global_config"] += cur.rowcount

            cur.execute(
                """
                SELECT strategy_key, llm_resource_path, param_json
                FROM strategy_definitions
                ORDER BY strategy_key
                """
            )
            for strategy_key, llm_resource_path, param_json in cur.fetchall():
                next_resource_path = llm_resource_path
                resource_changed = False
                if (
                    (not llm_resource_path and strategy_key == "daily_weekly_llm")
                    or str(llm_resource_path or "").strip() == old_resource_path
                ):
                    next_resource_path = new_resource_path
                    resource_changed = True

                next_param_json = _deepcopy_dict(param_json)
                llm_settings = next_param_json.get("llm")
                if not isinstance(llm_settings, dict):
                    if strategy_key != "daily_weekly_llm":
                        llm_settings = None
                    else:
                        llm_settings = {}
                        next_param_json["llm"] = llm_settings

                if isinstance(llm_settings, dict):
                    model_changed = False
                    current_model = str(llm_settings.get("model") or "").strip()
                    if not current_model or current_model == old_model:
                        if current_model != new_model:
                            llm_settings["model"] = new_model
                            model_changed = True
                else:
                    llm_settings = {}
                    model_changed = False

                if resource_changed or model_changed:
                    cur.execute(
                        """
                        UPDATE strategy_definitions
                        SET llm_resource_path = %s,
                            param_json = %s,
                            updated_at = NOW()
                        WHERE strategy_key = %s
                        """,
                        (
                            next_resource_path,
                            Json(next_param_json),
                            strategy_key,
                        ),
                    )
                    if resource_changed:
                        updates["strategy_definitions_resource"] += 1
                    if model_changed:
                        updates["strategy_definitions_model"] += 1

            cur.execute(
                """
                SELECT symbol, config_json
                FROM workflow_symbol_config
                ORDER BY symbol
                """
            )
            for symbol, config_json in cur.fetchall():
                config = _deepcopy_dict(config_json)
                updated_config, changed = _update_llm_block(
                    config=config,
                    old_resource_path=old_resource_path,
                    new_resource_path=new_resource_path,
                    old_model=old_model,
                    new_model=new_model,
                    create_if_missing=False,
                )
                if changed:
                    cur.execute(
                        """
                        UPDATE workflow_symbol_config
                        SET config_json = %s,
                            updated_at = NOW()
                        WHERE symbol = %s
                        """,
                        (Json(updated_config), symbol),
                    )
                    updates["workflow_symbol_config"] += cur.rowcount
    finally:
        conn.close()

    return {
        "status": "ok",
        "old_resource_path": old_resource_path,
        "new_resource_path": new_resource_path,
        "old_model": old_model,
        "new_model": new_model,
        "updates": updates,
    }
