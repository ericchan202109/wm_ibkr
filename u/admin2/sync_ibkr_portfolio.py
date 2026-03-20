from __future__ import annotations

from typing import Any

from ib_insync import IB, util
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


def _safe_tree(value: Any) -> Any:
    try:
        return util.tree(value)
    except Exception:
        return str(value)


def main(
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 21,
    account_id: str = "",
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    ib = IB()
    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    snapshot_id = None
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            ib.connect(host, int(port), clientId=int(client_id), timeout=12)
            accounts = ib.managedAccounts()
            resolved_account = str(account_id or (accounts[0] if accounts else ""))
            account_summary = ib.accountSummary(account=resolved_account) if resolved_account else ib.accountSummary()
            summary_json = {
                item.tag: {
                    "value": item.value,
                    "currency": item.currency,
                    "account": item.account,
                }
                for item in account_summary
            }
            cur.execute(
                """
                INSERT INTO portfolio_snapshots (account_id, status, summary_json)
                VALUES (%s, 'ok', %s)
                RETURNING id
                """,
                (resolved_account or None, Json(summary_json)),
            )
            snapshot_id = cur.fetchone()[0]

            portfolio_items = ib.portfolio(account=resolved_account) if resolved_account else ib.portfolio()
            for item in portfolio_items:
                contract = item.contract
                raw_payload = {
                    "account": item.account,
                    "position": item.position,
                    "avgCost": item.averageCost,
                    "marketPrice": item.marketPrice,
                    "marketValue": item.marketValue,
                    "unrealizedPNL": item.unrealizedPNL,
                    "realizedPNL": item.realizedPNL,
                    "conId": contract.conId,
                    "symbol": contract.symbol,
                    "currency": contract.currency,
                    "secType": contract.secType,
                }
                cur.execute(
                    """
                    INSERT INTO portfolio_positions (
                        snapshot_id, account_id, conid, symbol, quantity, average_cost,
                        market_price, market_value, unrealized_pnl, realized_pnl, currency, raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL, %s, %s)
                    """,
                    (
                        snapshot_id,
                        item.account,
                        contract.conId,
                        contract.symbol,
                        float(item.position),
                        float(item.averageCost),
                        float(item.marketPrice),
                        float(item.marketValue),
                        float(item.unrealizedPNL),
                        float(item.realizedPNL),
                        contract.currency,
                        Json(raw_payload),
                    ),
                )

            open_trades = ib.openTrades()
            for trade in open_trades:
                order = trade.order
                contract = trade.contract
                status = trade.orderStatus.status if trade.orderStatus else None
                cur.execute(
                    """
                    INSERT INTO portfolio_orders (
                        snapshot_id, account_id, ibkr_order_id, perm_id, symbol, conid, side,
                        quantity, filled_quantity, remaining_quantity, limit_price, order_type,
                        tif, status, raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot_id,
                        getattr(order, "account", None) or resolved_account or None,
                        str(getattr(order, "orderId", "")) or None,
                        str(getattr(order, "permId", "")) or None,
                        getattr(contract, "symbol", None),
                        getattr(contract, "conId", None),
                        getattr(order, "action", None),
                        float(getattr(order, "totalQuantity", 0) or 0),
                        float(getattr(trade.orderStatus, "filled", 0) or 0),
                        float(getattr(trade.orderStatus, "remaining", 0) or 0),
                        float(getattr(order, "lmtPrice", 0) or 0) if getattr(order, "lmtPrice", None) not in (None, "") else None,
                        getattr(order, "orderType", None),
                        getattr(order, "tif", None),
                        status,
                        Json(
                            {
                                "order": _safe_tree(order),
                                "status": _safe_tree(trade.orderStatus) if trade.orderStatus else {},
                                "contract": _safe_tree(contract),
                            }
                        ),
                    ),
                )

            return {
                "snapshot_id": snapshot_id,
                "account_id": resolved_account,
                "position_count": len(portfolio_items),
                "order_count": len(open_trades),
                "status": "ok",
            }
    except Exception as exc:
        if snapshot_id is None:
            with conn.cursor() as cur:
                _ensure_schema(cur)
                cur.execute(
                    """
                    INSERT INTO portfolio_snapshots (account_id, status, summary_json, error_message)
                    VALUES (%s, 'error', '{}'::jsonb, %s)
                    RETURNING id
                    """,
                    (account_id or None, str(exc)),
                )
                snapshot_id = cur.fetchone()[0]
        return {
            "snapshot_id": snapshot_id,
            "account_id": account_id or None,
            "status": "error",
            "error": str(exc),
        }
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass
        conn.close()
