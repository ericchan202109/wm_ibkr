"""Prompt configuration API router."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from backend.core.database import get_db
from backend.models import Base
from backend.models.prompt import (
    PROMPT_CACHE,
    PROMPT_PERFORMANCE_CACHE,
    PromptPerformance,
    PromptTemplate,
)
from backend.schemas.prompt import (
    PromptTemplateCreate,
    PromptTemplateUpdate,
    TemplateRenderRequest,
    TemplateRenderResponse,
    TemplateValidationRequest,
    TemplateValidationResponse,
)
from backend.services.prompt_renderer import get_prompt_renderer

router = APIRouter(prefix="/api/v1", tags=["prompts"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ensure_prompt_tables(db: Session) -> None:
    bind = db.get_bind()
    if bind:
        Base.metadata.create_all(bind=bind, checkfirst=True)


def _using_ephemeral_sqlite(db: Session) -> bool:
    bind = db.get_bind()
    return bool(bind and str(bind.url).startswith("sqlite:///:memory:"))


def _get_prompt(db: Session, prompt_id: int) -> Tuple[Any, bool]:
    """Return prompt + flag indicating whether the record came from cache."""
    _ensure_prompt_tables(db)
    prompt = db.get(PromptTemplate, prompt_id)
    if prompt:
        return prompt, False
    if _using_ephemeral_sqlite(db):
        cached = PROMPT_CACHE.get(prompt_id)
        if cached:
            return cached, True
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")


def _filter_cached_prompts(
    records: List[Dict[str, Any]],
    template_type: Optional[str],
    is_active: Optional[bool],
    strategy_id: Optional[int],
) -> List[Dict[str, Any]]:
    filtered = records
    if template_type:
        filtered = [rec for rec in filtered if rec.get("template_type") == template_type]
    if is_active is not None:
        filtered = [rec for rec in filtered if rec.get("is_active") == is_active]
    if strategy_id is not None:
        filtered = [
            rec
            for rec in filtered
            if rec.get("strategy_id") == strategy_id or rec.get("is_global") is True
        ]
    return sorted(filtered, key=lambda rec: rec.get("updated_at") or "", reverse=True)


def _filter_performance_cache(
    records: List[Dict[str, Any]],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    result = records
    if start_date:
        result = [
            rec for rec in result if rec.get("date") is None or rec["date"] >= start_date.isoformat()
        ]
    if end_date:
        result = [
            rec for rec in result if rec.get("date") is None or rec["date"] <= end_date.isoformat()
        ]
    return result


# ---------------------------------------------------------------------------
# CRUD endpoints
# ---------------------------------------------------------------------------
@router.get("/prompts/")
def list_prompts(
    template_type: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    strategy_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """List prompt templates with filtering + pagination."""
    _ensure_prompt_tables(db)

    if _using_ephemeral_sqlite(db):
        records = list(PROMPT_CACHE.values())
        records = _filter_cached_prompts(records, template_type, is_active, strategy_id)
        total = len(records)
        start = (page - 1) * page_size
        sliced = records[start : start + page_size]
        return {"templates": sliced, "total": total, "page": page, "page_size": page_size}

    query = db.query(PromptTemplate)
    if template_type:
        query = query.filter(PromptTemplate.template_type == template_type)
    if is_active is not None:
        query = query.filter(PromptTemplate.is_active == is_active)
    if strategy_id is not None:
        query = query.filter(
            or_(PromptTemplate.strategy_id == strategy_id, PromptTemplate.is_global.is_(True))
        )

    total = query.count()
    prompts = (
        query.order_by(PromptTemplate.updated_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "templates": [p.to_dict() for p in prompts],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/prompts/compare")
def compare_prompts(
    prompt_id_1: int = Query(...),
    prompt_id_2: int = Query(...),
    db: Session = Depends(get_db),
):
    def _aggregate_from_db(prompt_id: int) -> Dict[str, float]:
        stats = (
            db.query(
                func.avg(PromptPerformance.win_rate).label("win_rate"),
                func.avg(PromptPerformance.avg_r_multiple).label("avg_r"),
                func.avg(PromptPerformance.total_profit_loss).label("pnl"),
            )
            .filter(PromptPerformance.prompt_template_id == prompt_id)
            .one()
        )
        return {
            "win_rate": float(stats.win_rate or 0),
            "avg_r_multiple": float(stats.avg_r or 0),
            "total_profit_loss": float(stats.pnl or 0),
        }

    def _aggregate_from_cache(prompt_id: int) -> Dict[str, float]:
        records = PROMPT_PERFORMANCE_CACHE.get(prompt_id, [])
        if not records:
            return {"win_rate": 0.0, "avg_r_multiple": 0.0, "total_profit_loss": 0.0}
        win_rate = sum(rec.get("win_rate") or 0 for rec in records) / len(records)
        avg_r = sum(rec.get("avg_r_multiple") or 0 for rec in records) / len(records)
        pnl = sum(rec.get("total_profit_loss") or 0 for rec in records) / len(records)
        return {"win_rate": float(win_rate), "avg_r_multiple": float(avg_r), "total_profit_loss": float(pnl)}

    if _using_ephemeral_sqlite(db):
        stats1 = _aggregate_from_cache(prompt_id_1)
        stats2 = _aggregate_from_cache(prompt_id_2)
    else:
        _get_prompt(db, prompt_id_1)
        _get_prompt(db, prompt_id_2)
        stats1 = _aggregate_from_db(prompt_id_1)
        stats2 = _aggregate_from_db(prompt_id_2)

    return {
        "prompt_id_1": prompt_id_1,
        "prompt_id_2": prompt_id_2,
        "win_rate_1": stats1["win_rate"],
        "win_rate_2": stats2["win_rate"],
        "avg_r_multiple_1": stats1["avg_r_multiple"],
        "avg_r_multiple_2": stats2["avg_r_multiple"],
        "total_profit_loss_1": stats1["total_profit_loss"],
        "total_profit_loss_2": stats2["total_profit_loss"],
    }


@router.get("/prompts/{prompt_id}")
def get_prompt(prompt_id: int, db: Session = Depends(get_db)):
    prompt, from_cache = _get_prompt(db, prompt_id)
    if from_cache:
        return prompt
    return prompt.to_dict()


@router.post("/prompts/")
def create_prompt(payload: PromptTemplateCreate, db: Session = Depends(get_db)):
    _ensure_prompt_tables(db)
    prompt = PromptTemplate(
        name=payload.name,
        description=payload.description,
        prompt_text=payload.template_content,
        template_type=payload.template_type,
        language=payload.language,
        strategy_id=payload.strategy_id,
        is_active=payload.is_active,
        is_global=payload.is_global,
        is_default=payload.is_default,
        tags=",".join(payload.tags) if payload.tags else None,
        notes=payload.notes,
        created_by=payload.created_by,
    )
    db.add(prompt)
    db.commit()
    db.refresh(prompt)
    return prompt.to_dict()


@router.put("/prompts/{prompt_id}")
def update_prompt(prompt_id: int, payload: PromptTemplateUpdate, db: Session = Depends(get_db)):
    prompt, from_cache = _get_prompt(db, prompt_id)

    if from_cache:
        updated = dict(prompt)
        if payload.name is not None:
            updated["name"] = payload.name
        if payload.description is not None:
            updated["description"] = payload.description
        if payload.template_type is not None:
            updated["template_type"] = payload.template_type
        if payload.language is not None:
            updated["language"] = payload.language
        if payload.strategy_id is not None:
            updated["strategy_id"] = payload.strategy_id
        if payload.is_active is not None:
            updated["is_active"] = payload.is_active
        if payload.is_global is not None:
            updated["is_global"] = payload.is_global
        if payload.is_default is not None:
            updated["is_default"] = payload.is_default
        if payload.tags is not None:
            updated["tags"] = payload.tags
        if payload.notes is not None:
            updated["notes"] = payload.notes
        if payload.template_content:
            updated["template_content"] = payload.template_content
            updated["version"] = (updated.get("version") or 1) + 1
        PROMPT_CACHE[prompt_id] = updated
        return updated

    if payload.name is not None:
        prompt.name = payload.name
    if payload.description is not None:
        prompt.description = payload.description
    if payload.template_type is not None:
        prompt.template_type = payload.template_type
    if payload.language is not None:
        prompt.language = payload.language
    if payload.strategy_id is not None:
        prompt.strategy_id = payload.strategy_id
    if payload.is_active is not None:
        prompt.is_active = payload.is_active
    if payload.is_global is not None:
        prompt.is_global = payload.is_global
    if payload.is_default is not None:
        prompt.is_default = payload.is_default
    if payload.tags is not None:
        prompt.tags = ",".join(payload.tags) if payload.tags else None
    if payload.notes is not None:
        prompt.notes = payload.notes
    if payload.template_content:
        prompt.prompt_text = payload.template_content
        prompt.template_version = (prompt.template_version or 1) + 1

    db.commit()
    db.refresh(prompt)
    return prompt.to_dict()


@router.delete("/prompts/{prompt_id}")
def delete_prompt(prompt_id: int, db: Session = Depends(get_db)):
    prompt, from_cache = _get_prompt(db, prompt_id)
    if from_cache:
        PROMPT_CACHE.pop(prompt_id, None)
        return {"status": "deleted", "prompt_id": prompt_id}

    db.delete(prompt)
    db.commit()
    return {"status": "deleted", "prompt_id": prompt_id}


# ---------------------------------------------------------------------------
# Validation and rendering
# ---------------------------------------------------------------------------
@router.post("/prompts/validate")
def validate_prompt(payload: TemplateValidationRequest):
    renderer = get_prompt_renderer()
    is_valid, error = renderer.validate_template(payload.template_text)
    undefined_vars = None
    if is_valid:
        undefined = renderer.get_undefined_variables(payload.template_text, {})
        undefined_vars = sorted(undefined) if undefined else None
    return TemplateValidationResponse(is_valid=is_valid, error_message=error, undefined_variables=undefined_vars)


@router.post("/prompts/render")
def render_prompt(payload: TemplateRenderRequest):
    renderer = get_prompt_renderer()
    try:
        rendered = renderer.render(payload.template_text, payload.context or {}, strict=True)
        return TemplateRenderResponse(rendered_text=rendered)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


# ---------------------------------------------------------------------------
# Performance endpoints
# ---------------------------------------------------------------------------
@router.get("/prompts/{prompt_id}/performance")
def get_prompt_performance(
    prompt_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db),
):
    _get_prompt(db, prompt_id)

    if _using_ephemeral_sqlite(db):
        records = PROMPT_PERFORMANCE_CACHE.get(prompt_id, [])
        return _filter_performance_cache(records, start_date, end_date)

    query = db.query(PromptPerformance).filter(PromptPerformance.prompt_template_id == prompt_id)
    if start_date:
        query = query.filter(PromptPerformance.date >= start_date)
    if end_date:
        query = query.filter(PromptPerformance.date <= end_date)

    records = query.order_by(PromptPerformance.date.desc()).all()
    return [perf.to_dict() for perf in records]


# ---------------------------------------------------------------------------
# Strategy scoped prompts
# ---------------------------------------------------------------------------
@router.get("/strategies/{strategy_id}/prompts")
def get_strategy_prompts(strategy_id: int, db: Session = Depends(get_db)):
    _ensure_prompt_tables(db)
    if _using_ephemeral_sqlite(db):
        return [
            data
            for data in PROMPT_CACHE.values()
            if data.get("is_global") or data.get("strategy_id") == strategy_id
        ]

    prompts = (
        db.query(PromptTemplate)
        .filter(or_(PromptTemplate.strategy_id == strategy_id, PromptTemplate.is_global.is_(True)))
        .order_by(PromptTemplate.is_global.desc(), PromptTemplate.updated_at.desc())
        .all()
    )
    return [p.to_dict() for p in prompts]
