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
        CREATE TABLE IF NOT EXISTS signal_runs (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            contract_status TEXT,
            engine_status TEXT,
            final_signal TEXT,
            canonical_strategy_key TEXT,
            execution_mode TEXT,
            approval_required BOOLEAN NOT NULL DEFAULT FALSE,
            approval_status TEXT NOT NULL DEFAULT 'not_required',
            order_status TEXT NOT NULL DEFAULT 'not_submitted',
            conflict_detected BOOLEAN NOT NULL DEFAULT FALSE,
            decision_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            strategy_results_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            order_candidate_json JSONB,
            chart_refs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            integrated_input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            integrated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            llm_prompt_text TEXT,
            llm_response_text TEXT,
            llm_response_json JSONB,
            report_markdown TEXT,
            warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            source_flow_job_id TEXT,
            source_job_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        ALTER TABLE signal_runs
            ADD COLUMN IF NOT EXISTS integrated_input_json JSONB NOT NULL DEFAULT '{}'::jsonb;
        ALTER TABLE signal_runs
            ADD COLUMN IF NOT EXISTS integrated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb;
        """
    )


def main(
    symbol: str = "",
    limit: int = 200,
    final_signal: str = "",
    approval_status: str = "",
    order_status: str = "",
    canonical_strategy_key: str = "",
    created_after: str = "",
    created_before: str = "",
    source_flow_job_id: str = "",
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict:
    conn = _db_connect(db_resource_path)
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            where_clauses: list[str] = []
            params: list[Any] = []

            if symbol:
                where_clauses.append("symbol = %s")
                params.append(str(symbol).strip().upper())
            if final_signal:
                where_clauses.append("final_signal = %s")
                params.append(str(final_signal).strip().upper())
            if approval_status:
                where_clauses.append("approval_status = %s")
                params.append(str(approval_status).strip())
            if order_status:
                where_clauses.append("order_status = %s")
                params.append(str(order_status).strip())
            if canonical_strategy_key:
                where_clauses.append("canonical_strategy_key = %s")
                params.append(str(canonical_strategy_key).strip())
            if created_after:
                where_clauses.append("created_at >= %s::timestamptz")
                params.append(str(created_after).strip())
            if created_before:
                where_clauses.append("created_at <= %s::timestamptz")
                params.append(str(created_before).strip())
            if source_flow_job_id:
                where_clauses.append("source_flow_job_id = %s")
                params.append(str(source_flow_job_id).strip())

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            params.append(int(limit))
            cur.execute(
                f"""
                SELECT
                    id,
                    symbol,
                    final_signal,
                    canonical_strategy_key,
                    execution_mode,
                    approval_status,
                    order_status,
                    contract_status,
                    conflict_detected,
                    source_flow_job_id,
                    source_job_id,
                    created_at
                FROM signal_runs
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = [
                {
                    "signal_run_id": row[0],
                    "symbol": row[1],
                    "final_signal": row[2],
                    "canonical_strategy_key": row[3],
                    "execution_mode": row[4],
                    "approval_status": row[5],
                    "order_status": row[6],
                    "contract_status": row[7],
                    "conflict_detected": bool(row[8]),
                    "source_flow_job_id": row[9],
                    "source_job_id": row[10],
                    "created_at": row[11].isoformat() if row[11] else None,
                }
                for row in cur.fetchall()
            ]
            return {"rows": rows, "count": len(rows)}
    finally:
        conn.close()
