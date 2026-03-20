from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


def _load_dags_packages() -> None:
    dags_dir = Path(__file__).resolve().parents[2] / "dags"
    sys.path.insert(0, str(dags_dir))


_load_dags_packages()

from models.market_data import MarketData, OHLCVBar  # noqa: E402
from models.signal import SignalAction  # noqa: E402
from utils.autogen_finagent_orchestrator import AutoGenFinAgentOrchestrator  # noqa: E402
from utils.autogen_trademaster_orchestrator import AutoGenTradeMasterOrchestrator  # noqa: E402


class DummyOpenAIChatCompletionClient:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs


class DummyAssistantAgent:
    def __init__(self, name: str, model_client: Any, system_message: str = ""):
        self.name = name
        self.model_client = model_client
        self.system_message = system_message


class DummyTextMentionTermination:
    def __init__(self, text: str, sources: Optional[List[str]] = None):
        self.text = text
        self.sources = sources or []


class DummyMessage:
    def __init__(self, *, source: str, content: str):
        self.source = source
        self.content = content

    def model_dump(self) -> Dict[str, Any]:
        return {"source": self.source, "content": self.content}


class DummyResult:
    def __init__(self, *, messages: List[DummyMessage], stop_reason: str):
        self.messages = messages
        self.stop_reason = stop_reason


class DummyRoundRobinGroupChat:
    def __init__(self, *, participants: List[Any], termination_condition: Any, max_turns: int):
        self.participants = participants
        self.termination_condition = termination_condition
        self.max_turns = max_turns

    def run(self, *, task: str):
        async def _run_async() -> DummyResult:
            executor_payload = {
                "action": "HOLD",
                "confidence_score": 60,
                "reasoning": "Sample decision rationale that exceeds ten characters.",
                "key_factors": ["stubbed"],
                "trend": "neutral",
                "support_level": 100,
                "resistance_level": 110,
                "suggested_entry_price": 105,
                "suggested_stop_loss": 95,
                "suggested_take_profit": 115,
                "timeframe_analyzed": "1 day",
            }
            messages = [
                DummyMessage(source="TechnicalAnalyst", content="Technical summary."),
                DummyMessage(source="Executor", content=f"{executor_payload}\nTERMINATE".replace("'", '"')),
            ]
            return DummyResult(messages=messages, stop_reason="terminated")

        return _run_async()


def _patch_autogen(monkeypatch: pytest.MonkeyPatch, cls: type) -> None:
    monkeypatch.setattr(
        cls,
        "_lazy_import_autogen",
        staticmethod(
            lambda: (
                DummyAssistantAgent,
                DummyTextMentionTermination,
                DummyRoundRobinGroupChat,
                DummyOpenAIChatCompletionClient,
            )
        ),
    )


def _make_market_data() -> MarketData:
    start = datetime(2025, 1, 1)
    bars = [
        OHLCVBar(
            timestamp=start,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("95"),
            close=Decimal("102"),
            volume=1000,
        ),
        OHLCVBar(
            timestamp=start + timedelta(days=1),
            open=Decimal("102"),
            high=Decimal("106"),
            low=Decimal("101"),
            close=Decimal("104"),
            volume=1100,
        ),
    ]
    return MarketData(symbol="TSLA", bars=bars, timeframe="1 day")


def test_finagent_orchestrator_handles_async_team_run(monkeypatch: pytest.MonkeyPatch):
    _patch_autogen(monkeypatch, AutoGenFinAgentOrchestrator)
    orchestrator = AutoGenFinAgentOrchestrator(api_key="test")
    decision = orchestrator.run(market_data=_make_market_data(), news_items=[])

    assert decision.signal.action in {"HOLD", SignalAction.HOLD}
    assert decision.agent_outputs["stop_reason"] == "terminated"
    assert any(msg.get("source") == "Executor" for msg in decision.conversation)


@pytest.mark.asyncio
async def test_trademaster_orchestrator_handles_async_team_run_under_running_loop(monkeypatch: pytest.MonkeyPatch):
    _patch_autogen(monkeypatch, AutoGenTradeMasterOrchestrator)
    orchestrator = AutoGenTradeMasterOrchestrator(api_key="test")
    decision = orchestrator.run(market_data=_make_market_data(), trademaster_summary={"stub": True})

    assert decision.signal.action in {"HOLD", SignalAction.HOLD}
    assert decision.agent_outputs["stop_reason"] == "terminated"
    assert any(msg.get("source") == "Executor" for msg in decision.conversation)

