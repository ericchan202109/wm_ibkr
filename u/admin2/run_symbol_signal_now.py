from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import wmill


def main(
    symbol: str,
    data_mode: str = "run_new",
    engine_flow_path: str = "u/admin2/ibkr_strategy_engine",
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    exchange: str = "NASDAQ",
    order_exchange: str = "SMART",
    primary_exchange: str = "NASDAQ",
    currency: str = "USD",
    use_rth: bool = True,
    db_resource_path: str = "u/admin2/supabase_postgresql",
    persist_reports: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    resolved_symbol = str(symbol or "").strip().upper()
    if not resolved_symbol:
        raise ValueError("symbol is required")

    resolved_data_mode = str(data_mode or "run_new").strip().lower() or "run_new"
    if resolved_data_mode not in {"run_new", "load_latest"}:
        raise ValueError("data_mode must be run_new or load_latest")

    job_id = wmill.run_flow_async(
        path=engine_flow_path,
        args={
            "symbol": resolved_symbol,
            "host": host,
            "port": int(port),
            "client_id": int(client_id),
            "exchange": exchange,
            "order_exchange": order_exchange,
            "primary_exchange": primary_exchange,
            "currency": currency,
            "use_rth": bool(use_rth),
            "db_resource_path": db_resource_path,
            "persist_reports": bool(persist_reports),
            "dry_run": bool(dry_run),
            "data_mode": resolved_data_mode,
        },
        do_not_track_in_parent=False,
    )

    return {
        "symbol": resolved_symbol,
        "job_id": job_id,
        "flow_path": engine_flow_path,
        "data_mode": resolved_data_mode,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "status": "submitted",
    }
