from __future__ import annotations

from typing import Any

import psycopg2
import wmill

SCHEMA = "trump_code"


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
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")


def _table_exists(cur, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{table_name}",))
    return cur.fetchone()[0] is not None


def _object_map(rows: list[tuple[str, Any]]) -> dict[str, Any]:
    return {str(key): value for key, value in rows}


def main(
    db_resource_path: str = "u/admin2/supabase_postgresql",
    recent_post_limit: int = 20,
    market_limit: int = 25,
    prediction_limit: int = 50,
) -> dict[str, Any]:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            required_tables = [
                "daily_summary",
                "models",
                "posts",
                "markets",
                "data_catalog",
                "data_objects",
                "sync_runs",
            ]
            if not all(_table_exists(cur, table_name) for table_name in required_tables):
                return {
                    "summary": None,
                    "models": [],
                    "recent_posts": [],
                    "markets": [],
                    "data_catalog": [],
                    "reports": {},
                    "predictions_preview": [],
                    "sync_runs": [],
                }

            cur.execute(
                f"""
                SELECT
                    summary_date,
                    posts_today,
                    signals_json,
                    consensus,
                    system_health,
                    total_rules,
                    model_count,
                    source_payload_json,
                    captured_at
                FROM {SCHEMA}.daily_summary
                ORDER BY summary_date DESC
                LIMIT 1
                """
            )
            latest_summary_row = cur.fetchone()
            latest_summary = None
            if latest_summary_row:
                latest_summary = {
                    "date": latest_summary_row[0].isoformat() if latest_summary_row[0] else None,
                    "posts_today": latest_summary_row[1],
                    "signals": latest_summary_row[2] or [],
                    "consensus": latest_summary_row[3],
                    "system_health": latest_summary_row[4],
                    "total_rules": latest_summary_row[5],
                    "model_count": latest_summary_row[6],
                    "source_payload": latest_summary_row[7] or {},
                    "captured_at": latest_summary_row[8].isoformat() if latest_summary_row[8] else None,
                }

            cur.execute(
                f"""
                SELECT
                    model_key,
                    model_name,
                    win_rate,
                    avg_return,
                    total_trades,
                    source_payload_json,
                    captured_at
                FROM {SCHEMA}.models
                ORDER BY win_rate DESC NULLS LAST, total_trades DESC NULLS LAST, model_key ASC
                LIMIT 25
                """
            )
            models = [
                {
                    "model_key": row[0],
                    "name": row[1],
                    "win_rate": row[2],
                    "avg_return": row[3],
                    "total_trades": row[4],
                    "source_payload": row[5] or {},
                    "captured_at": row[6].isoformat() if row[6] else None,
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT
                    event_at,
                    event_date,
                    event_time,
                    content,
                    post_url,
                    source_name,
                    is_repost,
                    signals_json,
                    captured_at
                FROM {SCHEMA}.posts
                ORDER BY event_at DESC NULLS LAST, captured_at DESC
                LIMIT %s
                """,
                (int(recent_post_limit),),
            )
            posts = [
                {
                    "event_at": row[0].isoformat() if row[0] else None,
                    "event_date": row[1].isoformat() if row[1] else None,
                    "event_time": row[2],
                    "text": row[3],
                    "url": row[4],
                    "source": row[5],
                    "is_repost": bool(row[6]),
                    "signals": row[7] or {},
                    "captured_at": row[8].isoformat() if row[8] else None,
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT
                    market_slug,
                    question,
                    yes_price,
                    no_price,
                    liquidity,
                    volume,
                    market_url,
                    sub_markets,
                    captured_at
                FROM {SCHEMA}.markets
                ORDER BY liquidity DESC NULLS LAST, volume DESC NULLS LAST, market_slug ASC
                LIMIT %s
                """,
                (int(market_limit),),
            )
            markets = [
                {
                    "slug": row[0],
                    "question": row[1],
                    "yes_price": row[2],
                    "no_price": row[3],
                    "liquidity": row[4],
                    "volume": row[5],
                    "url": row[6],
                    "sub_markets": row[7],
                    "captured_at": row[8].isoformat() if row[8] else None,
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT file_name, size_mb, source_url, captured_at
                FROM {SCHEMA}.data_catalog
                ORDER BY file_name ASC
                """
            )
            data_catalog = [
                {
                    "file_name": row[0],
                    "size_mb": row[1],
                    "source_url": row[2],
                    "captured_at": row[3].isoformat() if row[3] else None,
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT file_name, payload_json
                FROM {SCHEMA}.data_objects
                WHERE file_name IN (
                    'daily_report.json',
                    'learning_report.json',
                    'opus_analysis.json',
                    'predictions_log.json',
                    'signal_confidence.json',
                    'surviving_rules.json',
                    'trump_playbook.json'
                )
                ORDER BY file_name ASC
                """
            )
            data_objects = _object_map(cur.fetchall())

            cur.execute(
                f"""
                SELECT
                    id,
                    sync_mode,
                    status,
                    summary_json,
                    error_text,
                    started_at,
                    completed_at
                FROM {SCHEMA}.sync_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            )
            sync_runs = [
                {
                    "id": row[0],
                    "sync_mode": row[1],
                    "status": row[2],
                    "summary": row[3] or {},
                    "error_text": row[4],
                    "started_at": row[5].isoformat() if row[5] else None,
                    "completed_at": row[6].isoformat() if row[6] else None,
                }
                for row in cur.fetchall()
            ]

            predictions_log = data_objects.get("predictions_log.json")
            if isinstance(predictions_log, list):
                predictions_preview = predictions_log[: max(int(prediction_limit), 0)]
            else:
                predictions_preview = []

            return {
                "summary": latest_summary,
                "models": models,
                "recent_posts": posts,
                "markets": markets,
                "data_catalog": data_catalog,
                "reports": {
                    "daily_report": data_objects.get("daily_report.json"),
                    "learning_report": data_objects.get("learning_report.json"),
                    "opus_analysis": data_objects.get("opus_analysis.json"),
                    "signal_confidence": data_objects.get("signal_confidence.json"),
                    "surviving_rules": data_objects.get("surviving_rules.json"),
                    "trump_playbook": data_objects.get("trump_playbook.json"),
                },
                "predictions_preview": predictions_preview,
                "sync_runs": sync_runs,
            }
    finally:
        conn.close()
