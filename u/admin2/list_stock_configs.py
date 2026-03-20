from __future__ import annotations

from typing import Any

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
    limit: int = 500,
    symbol: str = "",
    strategy_key: str = "daily_weekly_llm",
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            if symbol:
                cur.execute(
                    """
                    SELECT
                        c.symbol,
                        c.conid,
                        c.exchange,
                        c.currency,
                        c.sec_type,
                        c.status,
                        wsc.config_json,
                        ssl.is_active,
                        ssl.priority
                    FROM contracts c
                    LEFT JOIN workflow_symbol_config wsc ON wsc.symbol = c.symbol
                    LEFT JOIN strategy_symbol_links ssl
                      ON ssl.symbol = c.symbol
                     AND ssl.strategy_key = %s
                    WHERE c.symbol = %s
                    ORDER BY c.symbol ASC
                    LIMIT %s
                    """,
                    (strategy_key, str(symbol).strip().upper(), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        c.symbol,
                        c.conid,
                        c.exchange,
                        c.currency,
                        c.sec_type,
                        c.status,
                        wsc.config_json,
                        ssl.is_active,
                        ssl.priority
                    FROM contracts c
                    LEFT JOIN workflow_symbol_config wsc ON wsc.symbol = c.symbol
                    LEFT JOIN strategy_symbol_links ssl
                      ON ssl.symbol = c.symbol
                     AND ssl.strategy_key = %s
                    ORDER BY c.symbol ASC
                    LIMIT %s
                    """,
                    (strategy_key, int(limit)),
                )
            rows = []
            for row in cur.fetchall():
                config = row[6] if isinstance(row[6], dict) else {}
                scheduler = config.get("scheduler") if isinstance(config.get("scheduler"), dict) else {}
                market = config.get("market") if isinstance(config.get("market"), dict) else {}
                rows.append(
                    {
                        "symbol": row[0],
                        "conid": row[1],
                        "exchange": row[2],
                        "currency": row[3],
                        "sec_type": row[4],
                        "status": row[5],
                        "scheduler_mode": scheduler.get("mode", "daily"),
                        "timezone": scheduler.get("timezone", "America/New_York"),
                        "daily_run_time": scheduler.get("daily_run_time", "09:35"),
                        "last_run_at": scheduler.get("last_run_at"),
                        "execution_mode": config.get("execution_mode", "approval_gate"),
                        "order_exchange": market.get("order_exchange", "SMART"),
                        "primary_exchange": market.get("primary_exchange", "NASDAQ"),
                        "use_rth": bool(market.get("use_rth", True)),
                        "notes": config.get("notes", ""),
                        "strategy_key": strategy_key,
                        "strategy_active": bool(True if row[7] is None else row[7]),
                        "strategy_priority": 10 if row[8] is None else int(row[8]),
                    }
                )
            return {"rows": rows, "count": len(rows)}
    finally:
        conn.close()
