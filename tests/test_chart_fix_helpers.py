from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]


def _install_stubs() -> None:
    if "wmill" not in sys.modules:
        wmill = types.ModuleType("wmill")
        wmill.get_resource = lambda *_args, **_kwargs: {}
        sys.modules["wmill"] = wmill

    if "psycopg2" not in sys.modules:
        psycopg2 = types.ModuleType("psycopg2")
        psycopg2.connect = lambda **_kwargs: None
        extras = types.ModuleType("psycopg2.extras")
        extras.Json = lambda value: value
        psycopg2.extras = extras
        sys.modules["psycopg2"] = psycopg2
        sys.modules["psycopg2.extras"] = extras


def _load_module(relative_path: str, module_name: str):
    _install_stubs()
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_run_active_strategies_unwraps_integrated_signal_result():
    module = _load_module("u/admin2/run_active_strategies.py", "run_active_strategies_test")

    payload = {
        "daily_report": {"status": "ok"},
        "integrated_signal": {
            "decision": {"final_signal": "BUY"},
            "daily_chart": {"public_url": "https://example.com/daily.png"},
            "weekly_chart": {"public_url": "https://example.com/weekly.png"},
        },
    }

    unwrapped = module._unwrap_strategy_result(payload)

    assert unwrapped["decision"]["final_signal"] == "BUY"
    assert unwrapped["daily_chart"]["public_url"] == "https://example.com/daily.png"


def test_get_signal_run_detail_recovers_chart_refs_from_strategy_results():
    module = _load_module("u/admin2/get_signal_run_detail.py", "get_signal_run_detail_test")

    resolved = module._resolve_chart_refs(
        stored_chart_refs={"daily": {"chart_available": False}},
        integrated_input={},
        strategy_results=[
            {
                "strategy_key": "daily_weekly_llm",
                "result": {
                    "integrated_signal": {
                        "daily_chart": {"public_url": "https://example.com/daily.png"},
                        "weekly_chart": {"public_url": "https://example.com/weekly.png"},
                    }
                },
            }
        ],
        canonical_strategy_key="daily_weekly_llm",
    )

    assert resolved["daily"]["public_url"] == "https://example.com/daily.png"
    assert resolved["weekly"]["public_url"] == "https://example.com/weekly.png"


def test_persist_signal_run_output_snapshot_includes_chart_refs():
    module = _load_module("u/admin2/persist_signal_run.py", "persist_signal_run_test")

    snapshot = module._integrated_output_snapshot(
        {
            "status": "success",
            "decision": {"final_signal": "BUY"},
            "daily_chart": {"public_url": "https://example.com/daily.png"},
            "weekly_chart": {"public_url": "https://example.com/weekly.png"},
        }
    )

    assert snapshot["chart_refs"]["daily"]["public_url"] == "https://example.com/daily.png"
    assert snapshot["chart_refs"]["weekly"]["public_url"] == "https://example.com/weekly.png"
