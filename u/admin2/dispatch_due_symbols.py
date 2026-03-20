from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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


def _parse_hhmm(value: str, fallback: str) -> tuple[int, int]:
    text = str(value or fallback).strip()
    parts = text.split(":")
    hour = int(parts[0]) if parts else 0
    minute = int(parts[1]) if len(parts) > 1 else 0
    return hour, minute


def _safe_zoneinfo(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(str(value or "America/New_York"))
    except ZoneInfoNotFoundError:
        return ZoneInfo("America/New_York")


def _ensure_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contracts (
            symbol TEXT PRIMARY KEY,
            conid INTEGER NOT NULL,
            exchange TEXT,
            currency TEXT,
            sec_type TEXT,
            status TEXT DEFAULT 'ACTIVE'
        );

        CREATE TABLE IF NOT EXISTS workflow_symbol_config (
            symbol TEXT PRIMARY KEY,
            config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def _to_iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _dispatch_plan(config: dict, now_utc: datetime) -> tuple[bool, str, int]:
    scheduler = config.get("scheduler") if isinstance(config.get("scheduler"), dict) else {}
    timezone_name = str(scheduler.get("timezone") or "America/New_York")
    local_timezone = _safe_zoneinfo(timezone_name)
    local_now = now_utc.astimezone(local_timezone)
    if local_now.weekday() >= 5:
        return False, "weekend", 0
    run_hour, run_minute = _parse_hhmm(scheduler.get("daily_run_time"), "09:35")
    daily_run_at = local_now.replace(hour=run_hour, minute=run_minute, second=0, microsecond=0)
    last_run_at = scheduler.get("last_run_at")
    if last_run_at:
        try:
            last_run_dt = datetime.fromisoformat(str(last_run_at).replace("Z", "+00:00"))
            if last_run_dt.tzinfo is None:
                last_run_dt = last_run_dt.replace(tzinfo=timezone.utc)
            if last_run_dt.astimezone(local_timezone).date() == local_now.date():
                return False, "already_dispatched_today", 0
        except Exception:
            pass
    if local_now < daily_run_at:
        delay_seconds = int((daily_run_at - local_now).total_seconds())
        return True, "scheduled_for_later_today", max(delay_seconds, 0)
    return True, "dispatch_now", 0


def main(
    engine_flow_path: str = "u/admin2/ibkr_strategy_engine",
    db_resource_path: str = "u/admin2/supabase_postgresql",
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
) -> dict:
    conn = _db_connect(db_resource_path)
    conn.autocommit = True
    now_utc = datetime.now(timezone.utc)
    jobs = []
    skipped = []
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute(
                """
                SELECT
                    c.symbol,
                    c.exchange,
                    c.currency,
                    wsc.config_json
                FROM contracts c
                LEFT JOIN workflow_symbol_config wsc ON wsc.symbol = c.symbol
                WHERE UPPER(COALESCE(c.status, 'ACTIVE')) = 'ACTIVE'
                ORDER BY c.symbol ASC
                """
            )
            rows = cur.fetchall()
            for symbol, exchange, currency, config_json in rows:
                config = config_json if isinstance(config_json, dict) else {}
                due, reason, scheduled_in_secs = _dispatch_plan(config, now_utc)
                if not due:
                    skipped.append({"symbol": symbol, "reason": reason})
                    continue
                market = config.get("market") if isinstance(config.get("market"), dict) else {}
                scheduler = config.get("scheduler") if isinstance(config.get("scheduler"), dict) else {}
                job_id = wmill.run_flow_async(
                    path=engine_flow_path,
                    args={
                        "symbol": symbol,
                        "host": host,
                        "port": int(port),
                        "client_id": int(client_id),
                        "exchange": exchange or "NASDAQ",
                        "order_exchange": market.get("order_exchange") or "SMART",
                        "primary_exchange": market.get("primary_exchange") or exchange or "NASDAQ",
                        "currency": currency or "USD",
                        "use_rth": bool(market.get("use_rth", True)),
                        "db_resource_path": db_resource_path,
                        "persist_reports": False,
                        "dry_run": False,
                        "data_mode": "run_new",
                    },
                    scheduled_in_secs=scheduled_in_secs,
                    do_not_track_in_parent=False,
                )
                scheduler["mode"] = "daily"
                scheduler["last_run_at"] = _to_iso_z(now_utc)
                scheduler["daily_run_time"] = str(scheduler.get("daily_run_time") or "09:35")
                scheduler["timezone"] = str(scheduler.get("timezone") or "America/New_York")
                config["scheduler"] = scheduler
                cur.execute(
                    """
                    INSERT INTO workflow_symbol_config (symbol, config_json, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (symbol) DO UPDATE SET
                        config_json = EXCLUDED.config_json,
                        updated_at = NOW()
                    """,
                    (symbol, Json(config)),
                )
                jobs.append(
                    {
                        "symbol": symbol,
                        "job_id": job_id,
                        "dispatch_mode": reason,
                        "scheduled_in_secs": scheduled_in_secs,
                    }
                )
        return {
            "jobs": jobs,
            "skipped": skipped,
            "count": len(jobs),
            "dispatched_symbols": [job["symbol"] for job in jobs],
            "skipped_symbols": [item["symbol"] for item in skipped],
            "dispatched_at": _to_iso_z(now_utc),
        }
    finally:
        conn.close()
