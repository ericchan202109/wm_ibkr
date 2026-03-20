"""Exercise generation endpoints.

This module adds a small compatibility endpoint used by some clients:

  POST /generate_exercise

Historically, some callers posted to this root path. The backend API is now
namespaced under `/api/*`, but we keep this lightweight endpoint to avoid 404s.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

router = APIRouter()


class GenerateExerciseRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    topic: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("topic", "type", "exercise_type"),
        description="Exercise topic/type.",
    )
    difficulty: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("difficulty", "level"),
    )
    count: int | str | None = Field(
        default=None,
        validation_alias=AliasChoices("count", "n", "num_exercises"),
        description="Number of exercises to generate (1-10).",
    )
    seed: int | str | None = Field(
        default=None,
        validation_alias=AliasChoices("seed", "random_seed"),
        description="Optional deterministic seed.",
    )
    include_solution: bool | str | int | None = Field(
        default=None,
        validation_alias=AliasChoices("include_solution", "include_answer", "show_solution"),
        description="Whether to include the expected answer/solution.",
    )


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_difficulty(value: Optional[str]) -> str:
    """Coerce a loosely-specified difficulty value into easy/medium/hard."""
    if not value:
        return "easy"
    normalized = str(value).strip().lower()
    if normalized in {"easy", "e", "beginner", "low", "1"}:
        return "easy"
    if normalized in {"medium", "med", "intermediate", "mid", "2"}:
        return "medium"
    if normalized in {"hard", "advanced", "high", "3"}:
        return "hard"
    return "easy"


def _parse_count(value: int | str | None) -> int:
    if value is None:
        return 1
    try:
        parsed = int(value)
    except Exception:
        return 1
    return max(1, min(10, parsed))


def _parse_seed(value: int | str | None) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_bool(value: bool | str | int | None, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _make_r_multiple_exercise(rng: random.Random, difficulty: str) -> Dict[str, Any]:
    entry = round(rng.uniform(50, 500), 2)
    stop_distance = {"easy": (1.0, 5.0), "medium": (2.0, 12.0), "hard": (1.0, 25.0)}[difficulty]
    target_distance = {"easy": (2.0, 10.0), "medium": (5.0, 30.0), "hard": (2.0, 60.0)}[difficulty]

    stop = round(entry - rng.uniform(*stop_distance), 2)
    target = round(entry + rng.uniform(*target_distance), 2)

    risk = max(entry - stop, 0.01)
    reward = max(target - entry, 0.0)
    r_multiple = round(reward / risk, 2)

    question = (
        "Compute the R-multiple for this trade idea.\n\n"
        f"- Entry: {entry}\n"
        f"- Stop: {stop}\n"
        f"- Target: {target}\n\n"
        "R = (Target - Entry) / (Entry - Stop)."
    )

    return {
        "topic": "r_multiple",
        "question": question,
        "inputs": {"entry": entry, "stop": stop, "target": target},
        "solution": {"r_multiple": r_multiple},
    }


def _make_signal_confirmation_exercise(rng: random.Random, difficulty: str) -> Dict[str, Any]:
    indicators = [
        ("supertrend", rng.choice(["bullish", "bearish"])),
        ("price_vs_sma20", rng.choice(["above", "below"])),
        ("macd_cross", rng.choice(["buy", "sell", "none"] if difficulty != "easy" else ["buy", "sell"])),
        ("rsi", rng.choice([">50", "<50", "overbought", "oversold"] if difficulty == "hard" else [">50", "<50"])),
    ]

    bullish_votes = 0
    for name, value in indicators:
        if name == "supertrend" and value == "bullish":
            bullish_votes += 1
        elif name == "price_vs_sma20" and value == "above":
            bullish_votes += 1
        elif name == "macd_cross" and value == "buy":
            bullish_votes += 1
        elif name == "rsi" and value in (">50", "oversold"):
            bullish_votes += 1

    confirmed = bullish_votes >= 3
    question = (
        "Apply the 3-of-4 confirmation rule.\n\n"
        f"- SuperTrend: {dict(indicators)['supertrend']}\n"
        f"- Price vs SMA20: {dict(indicators)['price_vs_sma20']}\n"
        f"- MACD cross: {dict(indicators)['macd_cross']}\n"
        f"- RSI: {dict(indicators)['rsi']}\n\n"
        "Is a bullish trade confirmed? (>= 3 bullish signals)"
    )

    return {
        "topic": "signal_confirmation",
        "question": question,
        "inputs": {k: v for k, v in indicators},
        "solution": {"bullish_votes": bullish_votes, "bullish_confirmed": confirmed},
    }


def _make_position_sizing_exercise(rng: random.Random, difficulty: str) -> Dict[str, Any]:
    account_size = round(rng.uniform(5_000, 250_000), 2)
    risk_pct = rng.choice([0.005, 0.01, 0.02] if difficulty != "hard" else [0.0025, 0.005, 0.01, 0.02])
    entry = round(rng.uniform(10, 400), 2)
    stop = round(entry - rng.uniform(0.25, 15.0 if difficulty != "easy" else 5.0), 2)

    risk_dollars = round(account_size * risk_pct, 2)
    per_share_risk = max(round(entry - stop, 2), 0.01)
    shares = int(risk_dollars // per_share_risk)

    question = (
        "Calculate position size using fixed-fractional risk.\n\n"
        f"- Account size: {account_size}\n"
        f"- Risk per trade: {risk_pct * 100:.2f}%\n"
        f"- Entry: {entry}\n"
        f"- Stop: {stop}\n\n"
        "Shares = floor((Account * Risk%) / (Entry - Stop))."
    )

    return {
        "topic": "position_sizing",
        "question": question,
        "inputs": {
            "account_size": account_size,
            "risk_pct": risk_pct,
            "entry": entry,
            "stop": stop,
        },
        "solution": {"risk_dollars": risk_dollars, "per_share_risk": per_share_risk, "shares": shares},
    }


def _generate_one(topic: str, difficulty: str, rng: random.Random) -> Dict[str, Any]:
    normalized_topic = (topic or "r_multiple").strip().lower()
    if normalized_topic in {"r", "r_multiple", "r-multiple"}:
        return _make_r_multiple_exercise(rng, difficulty)
    if normalized_topic in {"signals", "signal_confirmation", "3of4", "3_of_4"}:
        return _make_signal_confirmation_exercise(rng, difficulty)
    if normalized_topic in {"position_sizing", "sizing", "risk"}:
        return _make_position_sizing_exercise(rng, difficulty)

    # Unknown topic: return a simple structured placeholder rather than erroring.
    return {
        "topic": normalized_topic,
        "question": f"Unsupported topic '{topic}'. Supported: r_multiple, signal_confirmation, position_sizing.",
        "inputs": {"topic": topic, "difficulty": difficulty},
        "solution": {"supported_topics": ["r_multiple", "signal_confirmation", "position_sizing"]},
    }


async def _coerce_payload(request: Request) -> Dict[str, Any]:
    """Best-effort parsing for callers that don't send JSON."""
    payload: Any = {}

    try:
        payload = await request.json()
    except Exception:
        payload = None

    if payload is None:
        try:
            form = await request.form()
            payload = dict(form)
        except Exception:
            raw = await request.body()
            if raw:
                payload = {"raw": raw.decode("utf-8", errors="replace")}
            else:
                payload = {}

    if isinstance(payload, dict):
        return payload

    # Some callers might send a JSON scalar; wrap it so validation still works.
    return {"value": payload}


@router.post("/generate_exercise")
async def generate_exercise(request: Request) -> Dict[str, Any]:
    """
    Generate one or more lightweight trading exercises.

    This endpoint is intentionally tolerant in what it accepts to avoid 4xxs for
    legacy callers. The response always includes `exercise` (first) and
    `exercises` (list).
    """
    payload = await _coerce_payload(request)
    try:
        req = GenerateExerciseRequest.model_validate(payload)
    except Exception:
        req = GenerateExerciseRequest()

    topic = (req.topic or "r_multiple").strip()
    difficulty = _normalize_difficulty(req.difficulty)
    count = _parse_count(req.count)
    seed = _parse_seed(req.seed)
    include_solution = _parse_bool(req.include_solution, default=True)

    rng = random.Random(seed) if seed is not None else random.Random()
    exercises = []
    for _ in range(count):
        exercise = _generate_one(topic, difficulty, rng)
        exercise_id = str(uuid.uuid4())
        exercise.update(
            {
                "id": exercise_id,
                "difficulty": difficulty,
                "created_at": _iso_now(),
            }
        )
        if not include_solution:
            exercise.pop("solution", None)
        exercises.append(exercise)

    return {
        "ok": True,
        "exercise": exercises[0],
        "exercises": exercises,
        "count": len(exercises),
    }
