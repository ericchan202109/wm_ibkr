from __future__ import annotations

from typing import Any

import psycopg2
import wmill


def _to_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(max_value, parsed))


def main(
    contracts_meta_data: int = 3,
    db_resource_path: str = "u/admin2/supabase_postgresql",
    limit: int = 500,
    search: str | None = None,
) -> dict[str, Any]:
    resolved_meta = _to_int(contracts_meta_data, 3, 0, 100_000)
    resolved_limit = _to_int(limit, 500, 1, 5_000)
    resolved_search = (search or "").strip().upper()

    warnings: list[str] = []
    symbols: list[str] = []

    try:
        db_cred = wmill.get_resource(db_resource_path)
        conn = psycopg2.connect(
            host=db_cred.get("host"),
            port=db_cred.get("port", 5432),
            user=db_cred.get("user"),
            password=db_cred.get("password"),
            dbname=db_cred.get("dbname", "neondb"),
            sslmode=db_cred.get("sslmode", "require"),
        )
        conn.autocommit = True
    except Exception as exc:
        return {
            "symbols": [],
            "options": [],
            "default_symbol": "TSLA",
            "source": "fallback",
            "meta_data": resolved_meta,
            "count": 0,
            "warnings": [f"database_connection_failed: {exc}"],
        }

    filter_applied = False

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'contracts';
                """
            )
            contract_cols = {str(row[0]).lower() for row in cur.fetchall()}
            if not contract_cols:
                return {
                    "symbols": [],
                    "options": [],
                    "default_symbol": "TSLA",
                    "source": "fallback",
                    "meta_data": resolved_meta,
                    "count": 0,
                    "warnings": ["contracts_table_not_found"],
                }

            params: list[Any] = []
            where_meta = ""
            if "meta_data" in contract_cols:
                where_meta = "AND meta_data = %s"
                params.append(resolved_meta)
                filter_applied = True
            elif "metadata" in contract_cols:
                where_meta = "AND metadata = %s"
                params.append(resolved_meta)
                filter_applied = True

            where_search = ""
            if resolved_search:
                where_search = "AND UPPER(symbol) LIKE %s"
                params.append(f"%{resolved_search}%")

            params.append(resolved_limit)

            query = f"""
                SELECT symbol
                FROM contracts
                WHERE symbol IS NOT NULL
                  AND btrim(symbol) <> ''
                  {where_meta}
                  {where_search}
                ORDER BY symbol ASC
                LIMIT %s;
            """
            cur.execute(query, tuple(params))

            seen: set[str] = set()
            for row in cur.fetchall():
                value = str(row[0] or "").strip().upper()
                if value and value not in seen:
                    seen.add(value)
                    symbols.append(value)
    finally:
        conn.close()

    options = [{"value": symbol, "label": symbol} for symbol in symbols]
    if symbols:
        source = "contracts" if filter_applied else "contracts_unfiltered"
    else:
        source = "fallback"
    default_symbol = symbols[0] if symbols else "TSLA"
    if not symbols:
        warnings.append(
            f"No contracts symbols found for meta_data={resolved_meta}; defaulted to TSLA."
        )

    return {
        "symbols": symbols,
        "options": options,
        "default_symbol": default_symbol,
        "source": source,
        "meta_data": resolved_meta,
        "count": len(symbols),
        "warnings": warnings,
    }
