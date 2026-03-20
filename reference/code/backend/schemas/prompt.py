"""
Pydantic schemas for Prompt Template API
"""
from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


# Base schemas
class PromptTemplateBase(BaseModel):
    """Base schema for prompt template."""

    name: str = Field(..., min_length=1, max_length=255, description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    template_content: str = Field(..., min_length=1, description="Jinja2 template text")
    template_type: str = Field(default="analysis", description="Template type: analysis, consolidation, decision, system_message")
    language: str = Field(default="en", max_length=5, description="Language code: en, zh")
    strategy_id: Optional[int] = Field(None, description="Strategy ID for strategy-specific prompts (NULL for global)")
    is_active: bool = Field(default=True, description="Whether template is active")
    is_global: bool = Field(default=True, description="Whether template applies globally or to a strategy")
    is_default: bool = Field(default=False, description="Whether this is the default template for its type")
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")
    notes: Optional[str] = Field(None, description="Additional notes")

    class Config:
        allow_population_by_field_name = True


class PromptTemplateCreate(PromptTemplateBase):
    """Schema for creating a new prompt template."""
    created_by: Optional[str] = Field(None, max_length=100, description="Creator username")
    
    @field_validator('template_type')
    @classmethod
    def validate_template_type(cls, v):
        allowed = ['analysis', 'consolidation', 'decision', 'system_message']
        if v not in allowed:
            raise ValueError(f"template_type must be one of: {', '.join(allowed)}")
        return v
    
    @field_validator('language')
    @classmethod
    def validate_language(cls, v):
        allowed = ['en', 'zh']
        if v not in allowed:
            raise ValueError(f"language must be one of: {', '.join(allowed)}")
        return v


class PromptTemplateUpdate(BaseModel):
    """Schema for updating an existing prompt template."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    template_content: Optional[str] = Field(None, min_length=1)
    template_type: Optional[str] = None
    language: Optional[str] = None
    strategy_id: Optional[int] = None
    is_active: Optional[bool] = None
    is_global: Optional[bool] = None
    is_default: Optional[bool] = None
    tags: Optional[List[str]] = None
    notes: Optional[str] = None


class PromptTemplateResponse(PromptTemplateBase):
    """Schema for prompt template responses."""

    id: int
    version: int = Field(..., alias="template_version")
    created_by: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True
        orm_mode = True
        allow_population_by_field_name = True


class PromptTemplateListResponse(BaseModel):
    """Schema for listing prompt templates."""
    templates: List[PromptTemplateResponse]
    total: int
    page: int
    page_size: int


# Performance schemas
class PromptPerformanceBase(BaseModel):
    """Base schema for prompt performance."""
    prompt_template_id: int
    strategy_id: Optional[int] = None
    date: date
    signals_generated: int = 0
    signals_executed: int = 0
    total_profit_loss: Optional[float] = None
    win_count: int = 0
    loss_count: int = 0
    avg_r_multiple: Optional[float] = None
    best_r_multiple: Optional[float] = None
    worst_r_multiple: Optional[float] = None
    avg_profit_pct: Optional[float] = None
    avg_loss_pct: Optional[float] = None
    avg_confidence: Optional[float] = None


class PromptPerformanceResponse(PromptPerformanceBase):
    """Schema for prompt performance responses."""
    id: int
    calculated_at: datetime
    updated_at: Optional[datetime]
    win_rate: float
    execution_rate: float
    
    class Config:
        from_attributes = True


class PromptPerformanceQuery(BaseModel):
    """Schema for querying prompt performance."""
    prompt_template_id: Optional[int] = None
    strategy_id: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    min_signals: Optional[int] = Field(None, ge=1, description="Minimum number of signals to include")


class PromptPerformanceSummary(BaseModel):
    """Schema for aggregated performance summary."""
    prompt_template_id: int
    prompt_name: str
    total_signals: int
    total_executed: int
    total_wins: int
    total_losses: int
    overall_win_rate: float
    overall_r_multiple: Optional[float]
    total_profit_loss: Optional[float]
    best_day: Optional[date]
    worst_day: Optional[date]
    days_active: int


class PromptComparisonRequest(BaseModel):
    """Schema for comparing multiple prompts."""
    prompt_template_ids: List[int] = Field(..., min_length=2, max_length=10, description="List of prompt IDs to compare")
    strategy_id: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    metric: str = Field(default="avg_r_multiple", description="Metric to compare: avg_r_multiple, win_rate, total_profit_loss")


class PromptComparisonResponse(BaseModel):
    """Schema for prompt comparison results."""
    prompts: List[PromptPerformanceSummary]
    metric: str
    best_prompt_id: Optional[int]
    best_prompt_name: Optional[str]


class PromptLeaderboardRequest(BaseModel):
    """Schema for leaderboard request."""
    strategy_id: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    metric: str = Field(default="avg_r_multiple", description="Sorting metric")
    limit: int = Field(default=10, ge=1, le=50, description="Number of results")
    template_type: Optional[str] = Field(None, description="Filter by template type")


class PromptLeaderboardResponse(BaseModel):
    """Schema for leaderboard response."""
    rankings: List[PromptPerformanceSummary]
    metric: str
    total_prompts: int


# Template validation schemas
class TemplateValidationRequest(BaseModel):
    """Schema for validating a template."""
    template_text: str = Field(..., min_length=1, description="Template text to validate")


class TemplateValidationResponse(BaseModel):
    """Schema for template validation response."""
    is_valid: bool
    error_message: Optional[str] = None
    undefined_variables: Optional[List[str]] = None


# Template rendering schemas
class TemplateRenderRequest(BaseModel):
    """Schema for rendering a template preview."""
    template_text: str = Field(..., description="Template text to render")
    context: dict = Field(default_factory=dict, description="Context variables for rendering")
    strict: bool = Field(default=False, description="Strict mode raises errors on undefined variables")


class TemplateRenderResponse(BaseModel):
    """Schema for template render response."""
    rendered_text: str
    warnings: Optional[List[str]] = None


# Signal outcome update schema (for performance tracking)
class SignalOutcomeUpdate(BaseModel):
    """Schema for updating signal outcome."""
    outcome: str = Field(..., description="Outcome: win, loss, pending, cancelled")
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    actual_r_multiple: Optional[float] = None
    profit_loss: Optional[float] = None
    
    @field_validator('outcome')
    @classmethod
    def validate_outcome(cls, v):
        allowed = ['win', 'loss', 'pending', 'cancelled']
        if v not in allowed:
            raise ValueError(f"outcome must be one of: {', '.join(allowed)}")
        return v
