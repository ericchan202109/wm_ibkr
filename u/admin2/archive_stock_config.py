from __future__ import annotations

import psycopg2
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


def main(
    symbol: str,
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")
    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE contracts SET status = 'ARCHIVED' WHERE symbol = %s",
                (resolved_symbol,),
            )
            cur.execute(
                """
                UPDATE workflow_symbol_config
                SET config_json = jsonb_set(
                    COALESCE(config_json, '{}'::jsonb),
                    '{archived_at}',
                    to_jsonb(NOW()::text),
                    true
                ),
                updated_at = NOW()
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            cur.execute(
                """
                UPDATE strategy_symbol_links
                SET is_active = FALSE,
                    updated_at = NOW()
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
    finally:
        conn.close()
    return {"symbol": resolved_symbol, "status": "ARCHIVED"}
