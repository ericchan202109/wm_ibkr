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


def _ensure_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id BIGSERIAL PRIMARY KEY,
            account_id TEXT,
            status TEXT NOT NULL,
            summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS portfolio_positions (
            id BIGSERIAL PRIMARY KEY,
            snapshot_id BIGINT NOT NULL REFERENCES portfolio_snapshots(id) ON DELETE CASCADE,
            account_id TEXT,
            conid INTEGER,
            symbol TEXT,
            quantity DOUBLE PRECISION,
            average_cost DOUBLE PRECISION,
            market_price DOUBLE PRECISION,
            market_value DOUBLE PRECISION,
            unrealized_pnl DOUBLE PRECISION,
            realized_pnl DOUBLE PRECISION,
            currency TEXT,
            raw_payload JSONB
        );

        CREATE TABLE IF NOT EXISTS portfolio_orders (
            id BIGSERIAL PRIMARY KEY,
            snapshot_id BIGINT NOT NULL REFERENCES portfolio_snapshots(id) ON DELETE CASCADE,
            account_id TEXT,
            ibkr_order_id TEXT,
            perm_id TEXT,
            symbol TEXT,
            conid INTEGER,
            side TEXT,
            quantity DOUBLE PRECISION,
            filled_quantity DOUBLE PRECISION,
            remaining_quantity DOUBLE PRECISION,
            limit_price DOUBLE PRECISION,
            order_type TEXT,
            tif TEXT,
            status TEXT,
            raw_payload JSONB
        );
        """
    )


def main(
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(
                """
                SELECT id, account_id, status, summary_json, error_message, created_at
                FROM portfolio_snapshots
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            snapshot = cur.fetchone()
            if not snapshot:
                return {"snapshot": None, "positions": [], "orders": []}
            snapshot_id = snapshot[0]
            cur.execute(
                """
                SELECT conid, symbol, quantity, average_cost, market_price, market_value, unrealized_pnl, realized_pnl, currency, raw_payload
                FROM portfolio_positions
                WHERE snapshot_id = %s
                ORDER BY symbol ASC
                """,
                (snapshot_id,),
            )
            positions = [
                {
                    "conid": row[0],
                    "symbol": row[1],
                    "quantity": row[2],
                    "average_cost": row[3],
                    "market_price": row[4],
                    "market_value": row[5],
                    "unrealized_pnl": row[6],
                    "realized_pnl": row[7],
                    "currency": row[8],
                    "raw_payload": row[9],
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT ibkr_order_id, perm_id, symbol, conid, side, quantity, filled_quantity, remaining_quantity, limit_price, order_type, tif, status, raw_payload
                FROM portfolio_orders
                WHERE snapshot_id = %s
                ORDER BY id DESC
                """,
                (snapshot_id,),
            )
            orders = [
                {
                    "ibkr_order_id": row[0],
                    "perm_id": row[1],
                    "symbol": row[2],
                    "conid": row[3],
                    "side": row[4],
                    "quantity": row[5],
                    "filled_quantity": row[6],
                    "remaining_quantity": row[7],
                    "limit_price": row[8],
                    "order_type": row[9],
                    "tif": row[10],
                    "status": row[11],
                    "raw_payload": row[12],
                }
                for row in cur.fetchall()
            ]
            return {
                "snapshot": {
                    "snapshot_id": snapshot[0],
                    "account_id": snapshot[1],
                    "status": snapshot[2],
                    "summary_json": snapshot[3],
                    "error_message": snapshot[4],
                    "created_at": snapshot[5].isoformat() if snapshot[5] else None,
                },
                "positions": positions,
                "orders": orders,
            }
    finally:
        conn.close()
