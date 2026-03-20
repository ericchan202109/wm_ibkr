from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]


class WmillStub:
    def __init__(
        self,
        *,
        statuses: list[str] | None = None,
        result=None,
        wait_job=None,
        job_details: dict | None = None,
    ):
        self._statuses = list(statuses or [])
        self._result = result
        self._wait_job = wait_job
        self._job_details = job_details or {}
        self.get_result_calls = 0
        self.get_job_calls = 0

    def run_flow_async(self, *_args, **_kwargs):
        return "job-123"

    def get_job_status(self, _job_id: str):
        if self._statuses:
            if len(self._statuses) > 1:
                return self._statuses.pop(0)
            return self._statuses[0]
        return ""

    def get_result(self, _job_id: str, assert_result_is_not_none: bool = True):
        self.get_result_calls += 1
        return self._result

    def get_job(self, _job_id: str):
        self.get_job_calls += 1
        return dict(self._job_details)

    def wait_job(self, *args, **kwargs):
        if self._wait_job is None:
            raise AttributeError("wait_job unavailable")
        return self._wait_job(*args, **kwargs)


def _install_modules(wmill_stub: object) -> None:
    sys.modules["wmill"] = wmill_stub

    if "psycopg2" not in sys.modules:
        psycopg2 = types.ModuleType("psycopg2")
        psycopg2.connect = lambda **_kwargs: None
        extras = types.ModuleType("psycopg2.extras")
        extras.Json = lambda value: value
        psycopg2.extras = extras
        sys.modules["psycopg2"] = psycopg2
        sys.modules["psycopg2.extras"] = extras


def _load_module(relative_path: str, module_name: str, wmill_stub: object):
    _install_modules(wmill_stub)
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_run_active_strategies_wait_helper_uses_wait_job_when_available():
    stub = WmillStub(wait_job=lambda *_args, **_kwargs: {"decision": {"final_signal": "BUY"}})
    module = _load_module("u/admin2/run_active_strategies.py", "run_active_strategies_wait_job_test", stub)

    result = module._wait_for_job_result("job-123", timeout=1)

    assert result["decision"]["final_signal"] == "BUY"


def test_run_active_strategies_wait_helper_polls_status_and_result():
    stub = WmillStub(statuses=["RUNNING", "COMPLETED"], result={"decision": {"final_signal": "BUY"}})
    module = _load_module("u/admin2/run_active_strategies.py", "run_active_strategies_poll_test", stub)

    result = module._wait_for_job_result("job-123", timeout=1, poll_interval_s=0)

    assert result["decision"]["final_signal"] == "BUY"
    assert stub.get_result_calls == 1


def test_run_active_strategies_wait_helper_raises_on_failure_status():
    stub = WmillStub(statuses=["FAILED"], job_details={"error": "child flow exploded"})
    module = _load_module("u/admin2/run_active_strategies.py", "run_active_strategies_failure_test", stub)

    try:
        module._wait_for_job_result("job-123", timeout=1, poll_interval_s=0)
    except RuntimeError as exc:
        assert "child flow exploded" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for failed job")


def test_invoke_execution_flow_wait_helper_times_out():
    stub = WmillStub(statuses=["RUNNING"])
    module = _load_module("u/admin2/invoke_execution_flow.py", "invoke_execution_timeout_test", stub)

    try:
        module._wait_for_job_result("job-123", timeout=0, poll_interval_s=0)
    except TimeoutError as exc:
        assert "job-123" in str(exc)
    else:
        raise AssertionError("Expected TimeoutError")


def test_decide_signal_approval_wait_helper_uses_completed_job_payload():
    stub = WmillStub(
        statuses=[""],
        job_details={"completed": True, "success": True, "result": {"status": "placed"}},
    )
    module = _load_module("u/admin2/decide_signal_approval.py", "decide_signal_approval_completed_job_test", stub)

    result = module._wait_for_job_result("job-123", timeout=1, poll_interval_s=0)

    assert result["status"] == "placed"
