"""Helpers to resolve MLflow lineage information for artifacts."""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional, Dict

import requests

from backend.config.settings import settings

logger = logging.getLogger(__name__)


def _mlflow_api_base() -> str:
    base = settings.MLFLOW_TRACKING_URI.rstrip('/')
    prefix = settings.MLFLOW_API_PREFIX.lstrip('/')
    return f"{base}/{prefix}".rstrip('/')


@lru_cache(maxsize=4)
def _get_experiment_id(experiment_name: str) -> Optional[str]:
    """Fetch MLflow experiment id for the configured name."""
    try:
        url = f"{_mlflow_api_base()}/experiments/list"
        response = requests.get(url, params={"view_type": "ACTIVE_ONLY"}, timeout=settings.MLFLOW_REQUEST_TIMEOUT)
        response.raise_for_status()
        experiments = response.json().get('experiments', [])
        for exp in experiments:
            if exp.get('name') == experiment_name:
                return exp.get('experiment_id')
    except Exception as exc:
        logger.warning(f"Failed to resolve MLflow experiment id for '{experiment_name}': {exc}")
    return None


def _escape_filter_value(value: str) -> str:
    return value.replace("'", "\\'")


@lru_cache(maxsize=256)
def resolve_run_lineage(workflow_id: str, execution_id: str) -> Optional[Dict[str, str]]:
    """Resolve MLflow run/experiment identifiers for a workflow execution."""
    experiment_id = _get_experiment_id(settings.MLFLOW_EXPERIMENT_NAME)
    if not experiment_id:
        return None
    filter_parts = []
    if workflow_id:
        filter_parts.append(f"tags.workflow_id = '{_escape_filter_value(workflow_id)}'")
    if execution_id:
        filter_parts.append(f"tags.execution_id = '{_escape_filter_value(execution_id)}'")
    if not filter_parts:
        return None
    filter_string = " and ".join(filter_parts)
    payload = {
        "experiment_ids": [experiment_id],
        "max_results": 1,
        "order_by": ["attributes.start_time DESC"],
        "filter": filter_string
    }
    try:
        url = f"{_mlflow_api_base()}/runs/search"
        response = requests.post(url, json=payload, timeout=settings.MLFLOW_REQUEST_TIMEOUT)
        response.raise_for_status()
        runs = response.json().get('runs', [])
        if not runs:
            return None
        info = runs[0].get('info', {})
        return {
            'run_id': info.get('run_id'),
            'experiment_id': info.get('experiment_id') or experiment_id
        }
    except Exception as exc:
        logger.warning(
            "Failed to resolve MLflow lineage for workflow_id=%s execution_id=%s: %s",
            workflow_id,
            execution_id,
            exc
        )
    return None
