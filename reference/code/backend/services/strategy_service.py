"""Business logic for managing strategies."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from croniter import croniter
from pytz import timezone as pytz_timezone, UnknownTimeZoneError
from sqlalchemy.orm import Session

from backend.models.strategy import Strategy

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULE_TZ = "America/New_York"


class StrategyService:
    """Provides CRUD + scheduling utilities for trading strategies."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # CRUD helpers
    # ------------------------------------------------------------------
    def create_strategy(self, **strategy_data) -> Strategy:
        """Create a new strategy with optional schedule + metadata."""
        data = strategy_data.copy()
        schedule = data.get("schedule")
        schedule_tz = data.pop("schedule_timezone", DEFAULT_SCHEDULE_TZ)
        if schedule:
            self._ensure_valid_cron(schedule)
            data.setdefault("next_execution_at", self._calculate_next_execution(schedule, tz_name=schedule_tz))

        if data.get("param") is None:
            data["param"] = {}
        if data.get("risk_params") is None:
            data["risk_params"] = {}

        strategy = Strategy(**data)
        self.db.add(strategy)
        self.db.commit()
        self.db.refresh(strategy)
        return strategy

    def get_strategy(self, strategy_id: int) -> Optional[Strategy]:
        """Fetch a strategy or return None."""
        return self.db.query(Strategy).filter(Strategy.id == strategy_id).first()

    def list_strategies(self, active_only: bool = False, limit: int = 100, offset: int = 0) -> List[Strategy]:
        """List strategies with optional active filter/pagination."""
        query = self.db.query(Strategy)
        if active_only:
            query = query.filter(Strategy.is_active.is_(True))
        return query.offset(offset).limit(limit).all()

    def activate_strategy(self, strategy_id: int) -> Strategy:
        """Enable a disabled strategy and recalculate next run."""
        strategy = self._require_strategy(strategy_id)
        strategy.is_active = True
        if strategy.schedule:
            strategy.next_execution_at = self._calculate_next_execution(strategy.schedule, tz_name=DEFAULT_SCHEDULE_TZ)
        self.db.commit()
        self.db.refresh(strategy)
        return strategy

    def deactivate_strategy(self, strategy_id: int) -> Strategy:
        """Disable a strategy and clear future runs."""
        strategy = self._require_strategy(strategy_id)
        strategy.is_active = False
        strategy.next_execution_at = None
        self.db.commit()
        self.db.refresh(strategy)
        return strategy

    def mark_strategy_executed(self, strategy_id: int) -> Strategy:
        """Update timestamps after a workflow run completes."""
        strategy = self._require_strategy(strategy_id)
        now = self._now()
        strategy.last_executed_at = now
        if strategy.schedule and self._validate_cron(strategy.schedule):
            strategy.next_execution_at = self._calculate_next_execution(
                strategy.schedule,
                start_time=now,
                tz_name=DEFAULT_SCHEDULE_TZ,
            )
        self.db.commit()
        self.db.refresh(strategy)
        return strategy

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------
    def get_strategies_due_for_execution(self, tolerance_minutes: int = 0) -> List[Strategy]:
        """Return active strategies whose next_execution_at is within tolerance."""
        now = self._now()
        window = now + timedelta(minutes=tolerance_minutes)

        # Ensure any missing next_execution_at values are populated
        needs_schedule = (
            self.db.query(Strategy)
            .filter(
                Strategy.is_active.is_(True),
                Strategy.schedule.isnot(None),
                Strategy.next_execution_at.is_(None),
            )
            .all()
        )
        for strategy in needs_schedule:
            try:
                strategy.next_execution_at = self._calculate_next_execution(strategy.schedule)
            except ValueError:
                logger.warning("Invalid cron on strategy %s (%s)", strategy.id, strategy.schedule)
        if needs_schedule:
            self.db.commit()

        query = (
            self.db.query(Strategy)
            .filter(Strategy.is_active.is_(True))
            .filter(Strategy.next_execution_at.isnot(None))
            .filter(Strategy.next_execution_at <= window)
        )
        return query.all()

    def preview_next_runs(
        self,
        cron_expression: str,
        *,
        timezone_name: str = DEFAULT_SCHEDULE_TZ,
        count: int = 5,
        start_time: Optional[datetime] = None,
    ) -> List[datetime]:
        """Return a list of future run timestamps in the provided timezone."""
        self._ensure_valid_cron(cron_expression)
        tz = self._normalize_timezone(timezone_name)
        base = start_time or self._now()
        base_local = base.astimezone(pytz_timezone(tz))
        iterator = croniter(cron_expression, base_local)
        runs: List[datetime] = []
        for _ in range(max(1, count)):
            next_dt = iterator.get_next(datetime)
            if next_dt.tzinfo is None:
                next_dt = pytz_timezone(tz).localize(next_dt)
            runs.append(next_dt)
        return runs

    def validate_strategy_config(self, strategy: Strategy) -> Dict[str, List[str]]:
        """Perform lightweight validation of scheduling + LLM flags."""
        issues: List[str] = []
        warnings: List[str] = []

        if strategy.schedule and not self._validate_cron(strategy.schedule):
            issues.append("Invalid cron expression")

        if not strategy.codes:
            warnings.append("No symbols associated with strategy")

        if strategy.llm_enabled and not strategy.llm_model:
            warnings.append("LLM enabled but llm_model not set")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_cron(cron_expression: str) -> bool:
        """Check syntax using croniter."""
        if not cron_expression:
            return False
        return croniter.is_valid(cron_expression)

    def _ensure_valid_cron(self, cron_expression: str) -> None:
        if not self._validate_cron(cron_expression):
            raise ValueError("Invalid cron expression")

    def _calculate_next_execution(
        self,
        cron_expression: str,
        start_time: Optional[datetime] = None,
        tz_name: str = DEFAULT_SCHEDULE_TZ,
    ) -> datetime:
        """Calculate the next execution timestamp for a cron schedule."""
        tz = self._normalize_timezone(tz_name)
        base = start_time or StrategyService._now()
        base_local = base.astimezone(pytz_timezone(tz))
        iterator = croniter(cron_expression, base_local)
        next_run = iterator.get_next(datetime)
        if next_run.tzinfo is None:
            next_run = pytz_timezone(tz).localize(next_run)
        return next_run.astimezone(timezone.utc)

    def _require_strategy(self, strategy_id: int) -> Strategy:
        strategy = self.get_strategy(strategy_id)
        if not strategy:
            raise ValueError(f"Strategy {strategy_id} not found")
        return strategy

    @staticmethod
    def _normalize_timezone(tz_name: str) -> str:
        if not tz_name:
            return DEFAULT_SCHEDULE_TZ
        try:
            pytz_timezone(tz_name)
            return tz_name
        except UnknownTimeZoneError as exc:  # pragma: no cover - validation guard
            raise ValueError(f"Unknown timezone '{tz_name}'") from exc

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
