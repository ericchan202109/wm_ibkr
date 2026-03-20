from __future__ import annotations

from copy import deepcopy
import html
import os
import re
from typing import Any

import psycopg2
import wmill
import yfinance as yf


AUTO_SYMBOL_TOKENS = {"", "AUTO", "DEFAULT", "DB", "CONTRACTS"}
WARNING_HINTS = {
    "Both run_new and load_latest were true. run_new takes precedence.": (
        "Set only one execution mode to avoid ambiguity."
    ),
}
YAHOO_FUNDAMENTALS_MIN_ROWS = 4


def _is_non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip()
        return bool(normalized) and normalized.upper() not in {
            "N/A",
            "NONE",
            "NULL",
            "NAN",
            "-",
            "--",
        }
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _unwrap_market_data_value(value: Any) -> Any:
    if isinstance(value, dict):
        for key in ("raw", "fmt", "longFmt", "shortFmt"):
            candidate = value.get(key)
            if _is_non_empty(candidate):
                return candidate
        return None
    return value


def _pick_mapping_value(mapping: Any, *keys: str) -> Any:
    if not hasattr(mapping, "get"):
        return None
    for key in keys:
        try:
            value = mapping.get(key)
        except Exception:
            value = None
        value = _unwrap_market_data_value(value)
        if _is_non_empty(value):
            return value
    return None


def _pick_object_value(obj: Any, *attrs: str) -> Any:
    for attr in attrs:
        try:
            value = getattr(obj, attr)
        except Exception:
            value = None
        value = _unwrap_market_data_value(value)
        if _is_non_empty(value):
            return value
    return None


def _pick_yahoo_value(info: dict[str, Any], fast_info: Any, *keys: str) -> Any:
    value = _pick_mapping_value(info, *keys)
    if _is_non_empty(value):
        return value
    value = _pick_mapping_value(fast_info, *keys)
    if _is_non_empty(value):
        return value
    return _pick_object_value(fast_info, *keys)


def _format_decimal(value: Any, digits: int = 2) -> str | None:
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return None


def _format_large_number(value: Any) -> str | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    abs_number = abs(number)
    for threshold, suffix in (
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ):
        if abs_number >= threshold:
            return f"{number / threshold:,.2f}{suffix}"
    return f"{number:,.0f}"


def _format_percent(value: Any) -> str | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    # Yahoo dividend yields may arrive either as a fraction (0.0042) or a percent-like
    # value (0.42 for 0.42%). Values above 25% are treated as already-percent to avoid
    # inflating ordinary equity yields.
    if abs(number) <= 0.25:
        number *= 100.0
    return f"{number:.2f}%"


def _format_range(low: Any, high: Any) -> str | None:
    formatted_low = _format_decimal(low)
    formatted_high = _format_decimal(high)
    if formatted_low and formatted_high:
        return f"{formatted_low} - {formatted_high}"
    return None


def _normalize_string(value: Any) -> str | None:
    if not _is_non_empty(value):
        return None
    return str(value).strip()


def _build_yahoo_fundamentals_rows(symbol: str) -> list[dict[str, str]]:
    ticker = yf.Ticker(symbol)
    info: dict[str, Any] = {}
    try:
        raw_info = ticker.info
        if isinstance(raw_info, dict):
            info = raw_info
    except Exception:
        try:
            raw_info = ticker.get_info()
            if isinstance(raw_info, dict):
                info = raw_info
        except Exception:
            info = {}

    fast_info = None
    try:
        fast_info = ticker.fast_info
    except Exception:
        fast_info = None

    rows: list[dict[str, str]] = []

    def add_row(metric: str, value: str | None) -> None:
        if value:
            rows.append(
                {
                    "metric": metric,
                    "value": value,
                    "source": "Yahoo Finance",
                }
            )

    market_cap = _pick_yahoo_value(info, fast_info, "marketCap", "market_cap")
    trailing_pe = _pick_yahoo_value(info, fast_info, "trailingPE", "trailing_pe")
    forward_pe = _pick_yahoo_value(info, fast_info, "forwardPE", "forward_pe")
    dividend_yield = _pick_yahoo_value(
        info, fast_info, "dividendYield", "dividend_yield"
    )
    year_low = _pick_yahoo_value(info, fast_info, "fiftyTwoWeekLow", "year_low")
    year_high = _pick_yahoo_value(info, fast_info, "fiftyTwoWeekHigh", "year_high")
    sector = _pick_yahoo_value(info, fast_info, "sector", "sectorDisp")
    industry = _pick_yahoo_value(info, fast_info, "industry", "industryDisp")
    website = _pick_yahoo_value(info, fast_info, "website")
    company_name = _pick_yahoo_value(
        info,
        fast_info,
        "shortName",
        "longName",
        "displayName",
        "short_name",
        "long_name",
    )

    add_row("Company", _normalize_string(company_name))
    add_row("Market Cap", _format_large_number(market_cap))
    add_row("Trailing P/E", _format_decimal(trailing_pe))
    add_row("Forward P/E", _format_decimal(forward_pe))
    add_row("Dividend Yield", _format_percent(dividend_yield))
    add_row("52 Week Range", _format_range(year_low, year_high))
    add_row("Sector", _normalize_string(sector))
    add_row("Industry", _normalize_string(industry))
    add_row("Website", _normalize_string(website))

    return rows


def _count_fundamentals_rows(table: Any) -> int:
    if not _is_non_empty(table):
        return 0
    if isinstance(table, list):
        count = 0
        for row in table:
            if isinstance(row, dict):
                values = [value for key, value in row.items() if key != "source"]
                if any(_is_non_empty(value) for value in values):
                    count += 1
            elif isinstance(row, (list, tuple, set)):
                if any(_is_non_empty(value) for value in row):
                    count += 1
            elif _is_non_empty(row):
                count += 1
        return count
    if isinstance(table, dict):
        return sum(1 for value in table.values() if _is_non_empty(value))
    if isinstance(table, str):
        count = 0
        for line in table.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                continue
            if set(stripped) <= {"|", "-", ":", " "}:
                continue
            count += 1
        return count
    return 1


def _report_needs_yahoo_fundamentals(report: dict[str, Any]) -> bool:
    data_quality = report.get("data_quality")
    if isinstance(data_quality, dict) and data_quality.get("fundamentals_available") is False:
        return True
    table_count = _count_fundamentals_rows(report.get("fundamentals_table"))
    return table_count < YAHOO_FUNDAMENTALS_MIN_ROWS


def _append_unique_note(notes: list[str], note: str) -> list[str]:
    if note not in notes:
        notes.append(note)
    return notes[:10]


def _render_yahoo_fundamentals_markdown(rows: list[dict[str, str]]) -> str:
    lines = [
        "## Fundamentals",
        "",
        "IBKR fundamentals were unavailable or incomplete in this run. Yahoo Finance was used to fill the gap.",
        "",
    ]
    for row in rows:
        metric = str(row.get("metric") or "").strip()
        value = str(row.get("value") or "").strip()
        if metric and value:
            lines.append(f"- {metric}: {value}")
    return "\n".join(lines)


def _replace_placeholder_fundamentals_markdown(
    markdown: str,
    symbol: str,
    replacement_block: str,
) -> str:
    pattern = (
        rf"## Fundamentals\s+"
        rf"IBKR fundamentals for {re.escape(symbol)} are unavailable or incomplete in this run\..*?"
        rf"(?=\n## |\Z)"
    )
    replaced, count = re.subn(
        pattern,
        f"{replacement_block.strip()}\n",
        markdown,
        count=1,
        flags=re.DOTALL,
    )
    if count:
        return replaced.strip()
    return markdown


def _sync_report_container(container: dict[str, Any], report: dict[str, Any]) -> None:
    if container is report:
        return
    for key in (
        "fundamentals_table",
        "data_quality",
        "report_markdown",
        "investment_summary",
        "fundamentals_source",
        "yahoo_fallback_used",
    ):
        if key in container or key in report:
            container[key] = report.get(key)


def _apply_yahoo_fundamentals_fallback(
    payload: Any,
    symbol: str,
    warnings: list[str],
    warnings_detail: list[dict[str, str]],
) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized_payload = deepcopy(payload)
    report = _extract_report(normalized_payload)
    if not isinstance(report, dict):
        return normalized_payload
    if not _report_needs_yahoo_fundamentals(report):
        report["fundamentals_source"] = "ibkr"
        report["yahoo_fallback_used"] = False
        _sync_report_container(normalized_payload, report)
        return normalized_payload

    try:
        yahoo_rows = _build_yahoo_fundamentals_rows(symbol)
    except Exception as exc:
        _add_warning(
            warnings,
            warnings_detail,
            f"yahoo_fundamentals_fallback_failed: {exc}",
            "Yahoo Finance fundamentals fallback failed; the report kept the original fundamentals payload.",
        )
        return normalized_payload

    if len(yahoo_rows) < YAHOO_FUNDAMENTALS_MIN_ROWS:
        _add_warning(
            warnings,
            warnings_detail,
            "yahoo_fundamentals_fallback_incomplete",
            "Yahoo Finance returned too few fundamentals fields to replace the missing IBKR fundamentals.",
        )
        return normalized_payload

    report["fundamentals_table"] = yahoo_rows
    report["fundamentals_source"] = "yahoo_finance_fallback"
    report["yahoo_fallback_used"] = True

    existing_summary = report.get("investment_summary")
    fallback_summary = (
        "Fundamentals sourced from Yahoo Finance because the IBKR fundamentals payload "
        "was unavailable or incomplete in this run."
    )
    if isinstance(existing_summary, str) and existing_summary.strip():
        if fallback_summary not in existing_summary:
            report["investment_summary"] = f"{existing_summary.rstrip()}\n\n{fallback_summary}"
    else:
        report["investment_summary"] = fallback_summary

    markdown_block = _render_yahoo_fundamentals_markdown(yahoo_rows)
    existing_markdown = str(report.get("report_markdown") or "").strip()
    if existing_markdown:
        replaced_markdown = _replace_placeholder_fundamentals_markdown(
            existing_markdown,
            symbol,
            markdown_block,
        )
        if replaced_markdown != existing_markdown:
            report["report_markdown"] = replaced_markdown
        elif "## Fundamentals" not in existing_markdown:
            report["report_markdown"] = f"{existing_markdown}\n\n{markdown_block}"
        else:
            report["report_markdown"] = existing_markdown
    else:
        report["report_markdown"] = markdown_block

    data_quality = report.get("data_quality")
    if not isinstance(data_quality, dict):
        data_quality = {}
    notes_raw = data_quality.get("notes")
    notes = [str(note).strip() for note in notes_raw] if isinstance(notes_raw, list) else []
    data_quality["fundamentals_available"] = True
    data_quality["notes"] = _append_unique_note(
        notes,
        "Yahoo Finance fundamentals fallback used because IBKR fundamentals were unavailable or incomplete.",
    )
    report["data_quality"] = data_quality

    _sync_report_container(normalized_payload, report)
    return normalized_payload


def _norm_symbol(value: str | None, default: str = "TSLA") -> str:
    candidate = (value or default).strip().upper()
    if not candidate:
        raise ValueError("symbol is required")
    return candidate


def _to_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(max_value, parsed))


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _coerce_symbol_input(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if normalized in AUTO_SYMBOL_TOKENS:
        return None
    return normalized


def _load_contract_symbols(
    db_resource_path: str,
    meta_data: int,
) -> tuple[list[str], list[str]]:
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
    warnings: list[str] = []

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
                return [], ["contracts_table_not_found"]

            if "meta_data" in contract_cols:
                cur.execute(
                    """
                    SELECT symbol
                    FROM contracts
                    WHERE symbol IS NOT NULL
                      AND btrim(symbol) <> ''
                      AND meta_data = %s
                    ORDER BY symbol ASC;
                    """,
                    (meta_data,),
                )
            elif "metadata" in contract_cols:
                cur.execute(
                    """
                    SELECT symbol
                    FROM contracts
                    WHERE symbol IS NOT NULL
                      AND btrim(symbol) <> ''
                      AND metadata = %s
                    ORDER BY symbol ASC;
                    """,
                    (meta_data,),
                )
            else:
                cur.execute(
                    """
                    SELECT symbol
                    FROM contracts
                    WHERE symbol IS NOT NULL
                      AND btrim(symbol) <> ''
                    ORDER BY symbol ASC;
                    """
                )

            symbols: list[str] = []
            seen: set[str] = set()
            for row in cur.fetchall():
                candidate = str(row[0] or "").strip().upper()
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    symbols.append(candidate)

            return symbols, warnings
    finally:
        conn.close()


def _extract_report(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("report"), dict):
        return payload["report"]
    if "report_markdown" in payload or "report_html" in payload:
        return payload
    return None


def _safe_history(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"count": 0, "rows": [], "options": []}

    rows = payload.get("rows")
    normalized_rows = rows if isinstance(rows, list) else []
    normalized_count = payload.get("count")
    if not isinstance(normalized_count, int):
        normalized_count = len(normalized_rows)

    options: list[dict[str, str]] = []
    for row in normalized_rows:
        if not isinstance(row, dict):
            continue
        row_symbol = str(row.get("symbol") or "").upper()
        generated_at = str(row.get("generated_at") or "")
        row_id = row.get("id")
        label = f"{row_symbol} | {generated_at}" if generated_at else row_symbol
        options.append(
            {
                "value": str(row_id) if row_id is not None else label,
                "label": label,
            }
        )

    return {
        "count": normalized_count,
        "rows": normalized_rows,
        "options": options,
    }


def _compact_news_summary(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    compact: dict[str, Any] = {}
    for key in (
        "requested_window_days",
        "requested_limit",
        "compact_mode",
        "markdown_top_n",
        "headlines_count",
        "articles_count",
    ):
        value = payload.get(key)
        if value is not None:
            compact[key] = value

    provider_codes: list[str] = []
    providers_raw = payload.get("provider_codes")
    if isinstance(providers_raw, list):
        for code in providers_raw:
            value = str(code or "").strip()
            if value and value not in provider_codes:
                provider_codes.append(value)
    else:
        providers_raw = payload.get("providers")
        if isinstance(providers_raw, list):
            for provider in providers_raw:
                if not isinstance(provider, dict):
                    continue
                code = str(provider.get("code") or "").strip()
                if code and code not in provider_codes:
                    provider_codes.append(code)
    if provider_codes:
        compact["provider_codes"] = provider_codes

    items_raw = payload.get("top_headlines")
    if not isinstance(items_raw, list):
        items_raw = payload.get("items")
    top_headlines: list[dict[str, Any]] = []
    if isinstance(items_raw, list):
        for item in items_raw[:10]:
            if not isinstance(item, dict):
                continue
            compact_item = {
                "headline": str(item.get("headline") or "Untitled"),
                "published_at": item.get("published_at"),
                "provider_code": item.get("provider_code"),
                "article_id": item.get("article_id"),
                "has_full_text": bool(item.get("has_full_text")),
            }
            top_headlines.append(compact_item)
    if top_headlines:
        compact["top_headlines"] = top_headlines

    return compact or None


def _compact_report(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    compact: dict[str, Any] = {}
    for key in (
        "symbol",
        "latest",
        "indicators",
        "fundamentals_table",
        "fundamentals_source",
        "technical_summary",
        "investment_summary",
        "data_quality",
        "yahoo_fallback_used",
        "report_markdown",
        "persistence",
    ):
        value = payload.get(key)
        if value is not None:
            compact[key] = value

    news_summary = _compact_news_summary(payload.get("news"))
    if news_summary:
        compact["news"] = news_summary

    return compact or None


def _empty_html(message: str) -> str:
    escaped = html.escape(message)
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>Stock Report</title>"
        "<style>body{font-family:ui-sans-serif,system-ui;background:#0f172a;color:#e2e8f0;"
        "padding:24px} .box{max-width:900px;margin:24px auto;padding:20px;border-radius:12px;"
        "background:#1e293b;border:1px solid #334155;white-space:pre-wrap;line-height:1.4}</style>"
        "</head><body><div class='box'>"
        f"{escaped}"
        "</div></body></html>"
    )


def _add_warning(
    warnings: list[str],
    warnings_detail: list[dict[str, str]],
    code: str,
    hint: str | None = None,
) -> None:
    warnings.append(code)
    detail: dict[str, str] = {
        "code": code,
        "severity": "warning",
    }
    resolved_hint = hint or WARNING_HINTS.get(code)
    if resolved_hint:
        detail["hint"] = resolved_hint
    warnings_detail.append(detail)


def _fallback_report_markdown(
    symbol: str,
    message: str,
    warnings: list[str],
    errors: list[str],
    run_new: bool,
    load_latest: bool,
) -> str:
    if run_new:
        mode_label = "Fresh IBKR Run"
    elif load_latest:
        mode_label = "Load Latest Persisted"
    else:
        mode_label = "No Mode Selected"

    lines = [
        f"# Stock Report Unavailable ({symbol})",
        "",
        message,
        "",
        f"Mode: {mode_label}",
    ]

    if warnings:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {warning}" for warning in warnings)

    if errors:
        lines.extend(["", "## Errors"])
        lines.extend(f"- {error}" for error in errors)

    if load_latest and not errors:
        lines.extend(
            [
                "",
                "Next step: run with `run_new=true` if you want to generate and persist a fresh report.",
            ]
        )

    return "\n".join(lines)


def _fallback_data_quality(
    warnings_detail: list[dict[str, str]],
    errors: list[str],
) -> dict[str, Any]:
    notes: list[str] = []

    for detail in warnings_detail:
        if not isinstance(detail, dict):
            continue
        hint = detail.get("hint")
        code = detail.get("code")
        note = str(hint or code or "").strip()
        if note:
            notes.append(note)

    for error in errors:
        note = str(error).strip()
        if note:
            notes.append(note)

    if not notes:
        notes.append("No report payload was produced.")

    return {
        "ibkr_connected": False,
        "fundamentals_available": False,
        "bars_available": False,
        "news_headlines_available": False,
        "notes": notes[:10],
    }


def main(
    symbol: str = "AUTO",
    exchange: str = "NASDAQ",
    currency: str = "USD",
    host: str = "host.docker.internal",
    port: int = 4002,
    client_id: int = 7,
    news_days: int = 7,
    news_limit: int = 50,
    include_full_articles: bool = True,
    news_compact_mode: bool = True,
    news_markdown_top_n: int = 8,
    lookback_days: int = 180,
    bar_size: str = "1 day",
    use_rth: bool = True,
    persist: bool = True,
    run_new: bool = False,
    load_latest: bool = False,
    history_symbol: str = "AUTO",
    history_limit: int = 10,
    contracts_meta_data: int = 3,
    db_resource_path: str = "u/admin2/supabase_postgresql",
) -> dict[str, Any]:
    resolved_contracts_meta_data = _to_int(contracts_meta_data, 3, 0, 100_000)
    resolved_lookback_days = _to_int(lookback_days, 180, 30, 2000)
    resolved_history_limit = _to_int(history_limit, 10, 1, 50)
    resolved_port = _to_int(port, 4002, 1, 65535)
    resolved_client_id = _to_int(client_id, 7, 1, 9999)
    resolved_news_days = _to_int(news_days, 7, 1, 30)
    resolved_news_limit = _to_int(news_limit, 50, 1, 100)
    resolved_news_markdown_top_n = _to_int(news_markdown_top_n, 8, 1, 50)
    resolved_persist = _coerce_bool(persist, True)
    resolved_run_new = _coerce_bool(run_new, False)
    resolved_load_latest = _coerce_bool(load_latest, False)
    resolved_use_rth = _coerce_bool(use_rth, True)
    resolved_include_full_articles = _coerce_bool(include_full_articles, True)
    resolved_news_compact_mode = _coerce_bool(news_compact_mode, True)

    warnings: list[str] = []
    warnings_detail: list[dict[str, str]] = []
    errors: list[str] = []
    report_payload: dict[str, Any] | None = None
    status = "ok"
    message: str | None = None

    contract_symbols: list[str] = []
    symbol_source = "fallback"
    try:
        contract_symbols, contract_warnings = _load_contract_symbols(
            db_resource_path=db_resource_path,
            meta_data=resolved_contracts_meta_data,
        )
        for warning_code in contract_warnings:
            _add_warning(warnings, warnings_detail, warning_code)
    except Exception as exc:
        _add_warning(
            warnings,
            warnings_detail,
            f"contract_symbol_lookup_failed: {exc}",
            "contract symbol lookup failed; symbol resolution may fall back to defaults.",
        )

    requested_symbol = _coerce_symbol_input(symbol)
    if requested_symbol:
        resolved_symbol = _norm_symbol(requested_symbol, "TSLA")
        symbol_source = "user"
    elif contract_symbols:
        resolved_symbol = contract_symbols[0]
        symbol_source = "contracts"
    else:
        resolved_symbol = "TSLA"
        _add_warning(
            warnings,
            warnings_detail,
            f"No contracts symbol found for meta_data={resolved_contracts_meta_data}; defaulted to TSLA.",
        )

    requested_history_symbol = _coerce_symbol_input(history_symbol)
    resolved_history_symbol = _norm_symbol(
        requested_history_symbol or resolved_symbol,
        resolved_symbol,
    )

    if resolved_run_new and resolved_load_latest:
        _add_warning(
            warnings,
            warnings_detail,
            "Both run_new and load_latest were true. run_new takes precedence.",
        )

    history_payload: dict[str, Any] = {"count": 0, "rows": [], "options": []}
    try:
        history_raw = wmill.run_script_by_path(
            "u/admin2/get_persisted_stock_report",
            args={
                "symbol": resolved_history_symbol,
                "limit": resolved_history_limit,
                "include_report": False,
                "db_resource_path": db_resource_path,
            },
        )
        history_payload = _safe_history(history_raw)
    except Exception as exc:
        _add_warning(
            warnings,
            warnings_detail,
            f"history_load_failed: {exc}",
            "history query failed; history metadata may be empty.",
        )

    if not resolved_run_new and not resolved_load_latest:
        status = "invalid_mode"
        message = "Invalid mode: set run_new=true or load_latest=true."
        errors.append("invalid_mode: set run_new=true or load_latest=true")

    if status != "invalid_mode" and resolved_run_new:
        try:
            fetched = wmill.run_script_by_path(
                "u/admin2/ibkr_fetch_report_data",
                args={
                    "symbol": resolved_symbol,
                    "exchange": exchange,
                    "currency": currency,
                    "host": host,
                    "port": resolved_port,
                    "client_id": resolved_client_id,
                    "lookback_days": resolved_lookback_days,
                    "bar_size": bar_size,
                    "use_rth": resolved_use_rth,
                    "news_days": resolved_news_days,
                    "news_limit": resolved_news_limit,
                    "include_full_articles": resolved_include_full_articles,
                },
            )
            rendered = wmill.run_script_by_path(
                "u/admin2/render_stock_report",
                args={
                    "symbol": resolved_symbol,
                    "fetched": fetched,
                    "controls": {
                        "symbol": resolved_symbol,
                        "lookback_days": resolved_lookback_days,
                        "run_new": resolved_run_new,
                        "load_latest": resolved_load_latest,
                        "history_symbol": resolved_history_symbol,
                        "history_limit": resolved_history_limit,
                        "contracts_meta_data": resolved_contracts_meta_data,
                        "persist": resolved_persist,
                        "host": host,
                        "port": resolved_port,
                        "news_days": resolved_news_days,
                        "news_limit": resolved_news_limit,
                        "include_full_articles": resolved_include_full_articles,
                        "news_compact_mode": resolved_news_compact_mode,
                        "news_markdown_top_n": resolved_news_markdown_top_n,
                    },
                    "history": history_payload,
                },
            )
            rendered = _apply_yahoo_fundamentals_fallback(
                rendered,
                resolved_symbol,
                warnings,
                warnings_detail,
            )
            report_payload = rendered

            if resolved_persist:
                root_job_id = (
                    os.environ.get("WM_ROOT_FLOW_JOB_ID")
                    or os.environ.get("WM_ROOT_JOB_ID")
                    or os.environ.get("WM_JOB_ID")
                )
                report_payload = wmill.run_script_by_path(
                    "u/admin2/persist_stock_report",
                    args={
                        "symbol": resolved_symbol,
                        "report": rendered,
                        "source_flow_job_id": root_job_id,
                        "db_resource_path": db_resource_path,
                    },
                )
        except Exception as exc:
            errors.append(f"run_new_failed: {exc}")
    elif status != "invalid_mode" and resolved_load_latest:
        try:
            latest = wmill.run_script_by_path(
                "u/admin2/get_persisted_stock_report",
                args={
                    "symbol": resolved_symbol,
                    "limit": 1,
                    "include_report": True,
                    "db_resource_path": db_resource_path,
                },
            )
            latest_report = latest.get("latest") if isinstance(latest, dict) else None
            if isinstance(latest_report, dict):
                report_payload = _apply_yahoo_fundamentals_fallback(
                    latest_report,
                    resolved_symbol,
                    warnings,
                    warnings_detail,
                )
            else:
                _add_warning(
                    warnings,
                    warnings_detail,
                    f"No persisted report found for {resolved_symbol}.",
                    "Run with run_new=true to generate and persist a fresh report.",
                )
        except Exception as exc:
            errors.append(f"load_latest_failed: {exc}")

    extracted_report = _extract_report(report_payload)
    normalized_report = _compact_report(extracted_report)
    report_markdown: str | None = None
    data_quality: dict[str, Any] = {}

    if normalized_report:
        report_markdown_value = normalized_report.get("report_markdown")
        report_markdown = str(report_markdown_value) if report_markdown_value else None
        dq = normalized_report.get("data_quality")
        if isinstance(dq, dict):
            data_quality = dq

    if message is None:
        if normalized_report:
            message = "Report generated successfully."
        elif resolved_load_latest and not errors:
            message = (
                f"No persisted report found for {resolved_symbol}. "
                "Run with run_new=true to generate a new report."
            )
        elif resolved_run_new and errors:
            message = "Failed to generate report from fresh data."
        elif errors:
            message = "Run completed with errors."
        else:
            message = "No report produced."

    if not normalized_report:
        report_markdown = _fallback_report_markdown(
            symbol=resolved_symbol,
            message=message,
            warnings=warnings,
            errors=errors,
            run_new=resolved_run_new,
            load_latest=resolved_load_latest,
        )
        data_quality = _fallback_data_quality(
            warnings_detail=warnings_detail,
            errors=errors,
        )
        normalized_report = {
            "symbol": resolved_symbol,
            "report_markdown": report_markdown,
            "data_quality": data_quality,
        }

    if status == "ok" and errors:
        status = "error"
    elif status == "ok" and warnings and not report_payload:
        status = "warning"

    return {
        "status": status,
        "message": message,
        "symbol": resolved_symbol,
        "controls": {
            "symbol": resolved_symbol,
            "lookback_days": resolved_lookback_days,
            "run_new": resolved_run_new,
            "load_latest": resolved_load_latest,
            "history_symbol": resolved_history_symbol,
            "history_limit": resolved_history_limit,
            "contracts_meta_data": resolved_contracts_meta_data,
            "persist": resolved_persist,
            "exchange": exchange,
            "currency": currency,
            "host": host,
            "port": resolved_port,
            "client_id": resolved_client_id,
            "news_days": resolved_news_days,
            "news_limit": resolved_news_limit,
            "include_full_articles": resolved_include_full_articles,
            "news_compact_mode": resolved_news_compact_mode,
            "news_markdown_top_n": resolved_news_markdown_top_n,
            "bar_size": bar_size,
            "use_rth": resolved_use_rth,
            "db_resource_path": db_resource_path,
        },
        "contract_symbols": {
            "meta_data": resolved_contracts_meta_data,
            "count": len(contract_symbols),
            "values": contract_symbols,
            "selected_source": symbol_source,
        },
        "history": history_payload,
        "report": normalized_report,
        "report_markdown": report_markdown,
        "data_quality": data_quality,
        "warnings": warnings,
        "warnings_detail": warnings_detail,
        "errors": errors,
    }
