from __future__ import annotations

import hashlib
import json
import ssl
import urllib.request
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json
import wmill

BASE_URL = "https://trumpcode.washinmura.jp"
USER_AGENT = "wmill-trump-code/1.0"
SCHEMA = "trump_code"

ENDPOINTS_BY_MODE: dict[str, list[str]] = {
    "daily": [
        "dashboard",
        "signals",
        "models",
        "status",
        "recent-posts",
        "polymarket-trump",
        "playbook",
        "data",
    ],
    "realtime": [
        "signals",
        "status",
        "recent-posts",
        "polymarket-trump",
        "data",
    ],
}

DATA_FILES_BY_MODE: dict[str, list[str]] = {
    "daily": [
        "daily_report.json",
        "learning_report.json",
        "market_SP500.json",
        "opus_analysis.json",
        "polymarket_live.json",
        "predictions_log.json",
        "signal_confidence.json",
        "surviving_rules.json",
        "trump_playbook.json",
        "trump_posts_lite.json",
    ],
    "realtime": [
        "daily_report.json",
        "polymarket_live.json",
        "signal_confidence.json",
    ],
}


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
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.sync_runs (
            id BIGSERIAL PRIMARY KEY,
            sync_mode TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            error_text TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.endpoint_snapshots (
            endpoint_name TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            payload_sha256 TEXT NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.data_objects (
            file_name TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            payload_sha256 TEXT NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.data_catalog (
            file_name TEXT PRIMARY KEY,
            size_mb DOUBLE PRECISION,
            source_url TEXT NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.daily_summary (
            summary_date DATE PRIMARY KEY,
            posts_today INTEGER,
            signals_json JSONB,
            consensus TEXT,
            system_health TEXT,
            total_rules INTEGER,
            model_count INTEGER,
            source_payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.models (
            model_key TEXT PRIMARY KEY,
            model_name TEXT,
            win_rate DOUBLE PRECISION,
            avg_return DOUBLE PRECISION,
            total_trades INTEGER,
            source_payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.markets (
            market_slug TEXT PRIMARY KEY,
            question TEXT NOT NULL,
            yes_price DOUBLE PRECISION,
            no_price DOUBLE PRECISION,
            liquidity DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            market_url TEXT,
            sub_markets INTEGER,
            source_payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA}.posts (
            post_fingerprint TEXT PRIMARY KEY,
            event_at TIMESTAMPTZ,
            event_date DATE,
            event_time TEXT,
            content TEXT NOT NULL,
            post_url TEXT,
            source_name TEXT,
            is_repost BOOLEAN NOT NULL DEFAULT FALSE,
            signals_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            source_payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS trump_code_posts_event_at_idx
            ON {SCHEMA}.posts (event_at DESC NULLS LAST, captured_at DESC);
        CREATE INDEX IF NOT EXISTS trump_code_markets_liquidity_idx
            ON {SCHEMA}.markets (liquidity DESC NULLS LAST, volume DESC NULLS LAST);
        """
    )


def _sha256_json(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _fetch_json(url: str) -> Any:
    context = None
    try:
        import certifi  # type: ignore

        context = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        context = ssl.create_default_context()

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=90, context=context) as response:
        return json.loads(response.read().decode("utf-8"))


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    if "T" not in candidate and len(candidate) == 10:
        candidate = f"{candidate}T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except ValueError:
        return None


def _upsert_sync_run_start(cur, sync_mode: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.sync_runs (sync_mode)
        VALUES (%s)
        RETURNING id
        """,
        (sync_mode,),
    )
    return int(cur.fetchone()[0])


def _finish_sync_run(cur, run_id: int, status: str, summary: dict[str, Any], error_text: str | None = None) -> None:
    cur.execute(
        f"""
        UPDATE {SCHEMA}.sync_runs
        SET
            status = %s,
            summary_json = %s,
            error_text = %s,
            completed_at = NOW()
        WHERE id = %s
        """,
        (status, Json(summary), error_text, run_id),
    )


def _upsert_endpoint_snapshot(cur, endpoint_name: str, source_url: str, payload: Any) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.endpoint_snapshots (
            endpoint_name,
            source_url,
            payload_json,
            payload_sha256,
            captured_at
        )
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (endpoint_name) DO UPDATE
        SET
            source_url = EXCLUDED.source_url,
            payload_json = EXCLUDED.payload_json,
            payload_sha256 = EXCLUDED.payload_sha256,
            captured_at = NOW()
        """,
        (endpoint_name, source_url, Json(payload), _sha256_json(payload)),
    )


def _upsert_data_catalog(cur, payload: dict[str, Any]) -> int:
    files = payload.get("files")
    if not isinstance(files, list):
        return 0
    count = 0
    for item in files:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        url = str(item.get("url") or "").strip()
        if not name or not url:
            continue
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.data_catalog (
                file_name,
                size_mb,
                source_url,
                captured_at
            )
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (file_name) DO UPDATE
            SET
                size_mb = EXCLUDED.size_mb,
                source_url = EXCLUDED.source_url,
                captured_at = NOW()
            """,
            (
                name,
                float(item.get("size_mb")) if item.get("size_mb") is not None else None,
                url,
            ),
        )
        count += 1
    return count


def _upsert_data_object(cur, file_name: str, source_url: str, payload: Any) -> None:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.data_objects (
            file_name,
            source_url,
            payload_json,
            payload_sha256,
            captured_at
        )
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (file_name) DO UPDATE
        SET
            source_url = EXCLUDED.source_url,
            payload_json = EXCLUDED.payload_json,
            payload_sha256 = EXCLUDED.payload_sha256,
            captured_at = NOW()
        """,
        (file_name, source_url, Json(payload), _sha256_json(payload)),
    )


def _upsert_daily_summary(
    cur,
    *,
    summary_date: str | None,
    posts_today: int | None = None,
    signals: list[str] | None = None,
    consensus: str | None = None,
    system_health: str | None = None,
    total_rules: int | None = None,
    model_count: int | None = None,
    source_payload: dict[str, Any] | None = None,
) -> bool:
    parsed_date = _parse_date(summary_date)
    if not parsed_date:
        return False
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.daily_summary (
            summary_date,
            posts_today,
            signals_json,
            consensus,
            system_health,
            total_rules,
            model_count,
            source_payload_json,
            captured_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (summary_date) DO UPDATE
        SET
            posts_today = COALESCE(EXCLUDED.posts_today, {SCHEMA}.daily_summary.posts_today),
            signals_json = COALESCE(EXCLUDED.signals_json, {SCHEMA}.daily_summary.signals_json),
            consensus = COALESCE(EXCLUDED.consensus, {SCHEMA}.daily_summary.consensus),
            system_health = COALESCE(EXCLUDED.system_health, {SCHEMA}.daily_summary.system_health),
            total_rules = COALESCE(EXCLUDED.total_rules, {SCHEMA}.daily_summary.total_rules),
            model_count = COALESCE(EXCLUDED.model_count, {SCHEMA}.daily_summary.model_count),
            source_payload_json = COALESCE(EXCLUDED.source_payload_json, {SCHEMA}.daily_summary.source_payload_json),
            captured_at = NOW()
        """,
        (
            parsed_date.date(),
            posts_today,
            Json(signals) if signals is not None else None,
            consensus,
            system_health,
            total_rules,
            model_count,
            Json(source_payload) if source_payload is not None else None,
        ),
    )
    return True


def _upsert_models(cur, payload: dict[str, Any]) -> int:
    models = payload.get("models")
    if not isinstance(models, dict):
        return 0
    count = 0
    for model_key, item in models.items():
        if not isinstance(item, dict):
            continue
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.models (
                model_key,
                model_name,
                win_rate,
                avg_return,
                total_trades,
                source_payload_json,
                captured_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (model_key) DO UPDATE
            SET
                model_name = EXCLUDED.model_name,
                win_rate = EXCLUDED.win_rate,
                avg_return = EXCLUDED.avg_return,
                total_trades = EXCLUDED.total_trades,
                source_payload_json = EXCLUDED.source_payload_json,
                captured_at = NOW()
            """,
            (
                str(model_key),
                item.get("name"),
                float(item.get("win_rate")) if item.get("win_rate") is not None else None,
                float(item.get("avg_return")) if item.get("avg_return") is not None else None,
                int(item.get("total_trades")) if item.get("total_trades") is not None else None,
                Json(item),
            ),
        )
        count += 1
    return count


def _upsert_markets(cur, payload: dict[str, Any], max_markets: int) -> int:
    markets = payload.get("markets")
    if not isinstance(markets, list):
        return 0
    count = 0
    for item in markets[: max_markets if max_markets > 0 else len(markets)]:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or "").strip()
        question = str(item.get("question") or "").strip()
        if not slug or not question:
            continue
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.markets (
                market_slug,
                question,
                yes_price,
                no_price,
                liquidity,
                volume,
                market_url,
                sub_markets,
                source_payload_json,
                captured_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (market_slug) DO UPDATE
            SET
                question = EXCLUDED.question,
                yes_price = EXCLUDED.yes_price,
                no_price = EXCLUDED.no_price,
                liquidity = EXCLUDED.liquidity,
                volume = EXCLUDED.volume,
                market_url = EXCLUDED.market_url,
                sub_markets = EXCLUDED.sub_markets,
                source_payload_json = EXCLUDED.source_payload_json,
                captured_at = NOW()
            """,
            (
                slug,
                question,
                float(item.get("yes_price")) if item.get("yes_price") is not None else None,
                float(item.get("no_price")) if item.get("no_price") is not None else None,
                float(item.get("liquidity")) if item.get("liquidity") is not None else None,
                float(item.get("volume")) if item.get("volume") is not None else None,
                item.get("url"),
                int(item.get("sub_markets")) if item.get("sub_markets") is not None else None,
                Json(item),
            ),
        )
        count += 1
    return count


def _upsert_posts(cur, posts: list[dict[str, Any]], max_posts: int) -> int:
    count = 0
    for item in posts[: max_posts if max_posts > 0 else len(posts)]:
        if not isinstance(item, dict):
            continue
        content = str(item.get("text") or item.get("content") or "").strip()
        if not content:
            continue
        event_raw = item.get("date") or item.get("created_at")
        event_at = _parse_timestamp(str(event_raw)) if event_raw else None
        event_date = None
        event_time = item.get("time")
        if event_at:
            event_date = event_at.date()
            if not event_time:
                event_time = event_at.strftime("%H:%M")
        elif item.get("date") and str(item.get("date")).count("-") == 2:
            parsed_date = _parse_date(str(item.get("date")))
            event_date = parsed_date.date() if parsed_date else None
        fingerprint = hashlib.sha256(
            f"{event_raw}|{content[:400]}".encode("utf-8")
        ).hexdigest()
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.posts (
                post_fingerprint,
                event_at,
                event_date,
                event_time,
                content,
                post_url,
                source_name,
                is_repost,
                signals_json,
                source_payload_json,
                captured_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (post_fingerprint) DO UPDATE
            SET
                event_at = COALESCE(EXCLUDED.event_at, {SCHEMA}.posts.event_at),
                event_date = COALESCE(EXCLUDED.event_date, {SCHEMA}.posts.event_date),
                event_time = COALESCE(EXCLUDED.event_time, {SCHEMA}.posts.event_time),
                content = EXCLUDED.content,
                post_url = COALESCE(EXCLUDED.post_url, {SCHEMA}.posts.post_url),
                source_name = COALESCE(EXCLUDED.source_name, {SCHEMA}.posts.source_name),
                is_repost = EXCLUDED.is_repost,
                signals_json = EXCLUDED.signals_json,
                source_payload_json = EXCLUDED.source_payload_json,
                captured_at = NOW()
            """,
            (
                fingerprint,
                event_at,
                event_date,
                str(event_time) if event_time is not None else None,
                content,
                item.get("url"),
                item.get("source"),
                bool(item.get("rt") or item.get("is_retweet")),
                Json(item.get("signals") if isinstance(item.get("signals"), dict) else {}),
                Json(item),
            ),
        )
        count += 1
    return count


def _extract_posts_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        posts = payload.get("posts")
        if isinstance(posts, list):
            return [item for item in posts if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def main(
    sync_mode: str = "daily",
    db_resource_path: str = "u/admin2/supabase_postgresql",
    include_data_files: bool = True,
    max_posts: int = 500,
    max_markets: int = 250,
) -> dict[str, Any]:
    normalized_mode = str(sync_mode or "daily").strip().lower()
    if normalized_mode not in ENDPOINTS_BY_MODE:
        raise ValueError(f"Unsupported sync_mode: {sync_mode}")

    conn = _db_connect(db_resource_path)
    conn.autocommit = False

    summary: dict[str, Any] = {
        "sync_mode": normalized_mode,
        "endpoints": [],
        "data_files": [],
        "models_upserted": 0,
        "markets_upserted": 0,
        "posts_upserted": 0,
        "data_catalog_entries": 0,
    }

    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            run_id = _upsert_sync_run_start(cur, normalized_mode)
            conn.commit()

            try:
                cached_payloads: dict[str, Any] = {}
                for endpoint_name in ENDPOINTS_BY_MODE[normalized_mode]:
                    source_url = f"{BASE_URL}/api/{endpoint_name}"
                    payload = _fetch_json(source_url)
                    cached_payloads[endpoint_name] = payload
                    _upsert_endpoint_snapshot(cur, endpoint_name, source_url, payload)
                    summary["endpoints"].append(endpoint_name)

                    if endpoint_name == "data" and isinstance(payload, dict):
                        summary["data_catalog_entries"] = _upsert_data_catalog(cur, payload)
                    elif endpoint_name in {"dashboard", "models"} and isinstance(payload, dict):
                        summary["models_upserted"] += _upsert_models(cur, payload)
                    elif endpoint_name == "signals" and isinstance(payload, dict):
                        _upsert_daily_summary(
                            cur,
                            summary_date=payload.get("date"),
                            signals=payload.get("signals") if isinstance(payload.get("signals"), list) else None,
                            model_count=len(payload.get("models")) if isinstance(payload.get("models"), dict) else None,
                            source_payload=payload,
                        )
                    elif endpoint_name == "status" and isinstance(payload, dict):
                        _upsert_daily_summary(
                            cur,
                            summary_date=payload.get("date"),
                            posts_today=int(payload.get("posts_today")) if payload.get("posts_today") is not None else None,
                            signals=payload.get("signals") if isinstance(payload.get("signals"), list) else None,
                            consensus=payload.get("consensus"),
                            system_health=payload.get("system_health"),
                            total_rules=int(payload.get("total_rules")) if payload.get("total_rules") is not None else None,
                            model_count=int(payload.get("models")) if payload.get("models") is not None else None,
                            source_payload=payload,
                        )
                    elif endpoint_name == "recent-posts":
                        summary["posts_upserted"] += _upsert_posts(
                            cur,
                            _extract_posts_from_payload(payload),
                            max_posts=max_posts,
                        )
                    elif endpoint_name == "polymarket-trump" and isinstance(payload, dict):
                        summary["markets_upserted"] += _upsert_markets(cur, payload, max_markets=max_markets)

                if include_data_files:
                    for file_name in DATA_FILES_BY_MODE[normalized_mode]:
                        source_url = f"{BASE_URL}/api/data/{file_name}"
                        payload = _fetch_json(source_url)
                        _upsert_data_object(cur, file_name, source_url, payload)
                        summary["data_files"].append(file_name)

                        if file_name == "daily_report.json" and isinstance(payload, dict):
                            direction_summary = payload.get("direction_summary")
                            _upsert_daily_summary(
                                cur,
                                summary_date=payload.get("date"),
                                posts_today=int(payload.get("posts_today")) if payload.get("posts_today") is not None else None,
                                signals=payload.get("signals_detected") if isinstance(payload.get("signals_detected"), list) else None,
                                consensus=direction_summary.get("consensus") if isinstance(direction_summary, dict) else None,
                                source_payload=payload,
                            )
                        elif file_name == "trump_posts_lite.json":
                            summary["posts_upserted"] += _upsert_posts(
                                cur,
                                _extract_posts_from_payload(payload),
                                max_posts=max_posts,
                            )

                _finish_sync_run(cur, run_id, "succeeded", summary)
                conn.commit()
                return summary
            except Exception as exc:
                conn.rollback()
                with conn.cursor() as retry_cur:
                    _ensure_schema(retry_cur)
                    _finish_sync_run(
                        retry_cur,
                        run_id,
                        "failed",
                        summary,
                        error_text=str(exc),
                    )
                conn.commit()
                raise
    finally:
        conn.close()
