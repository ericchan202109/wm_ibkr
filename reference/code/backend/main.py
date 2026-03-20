"""Main FastAPI application."""
import asyncio
import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.api import (
    artifacts,
    chart_images,
    charts,
    dashboard,
    exercises,
    frontend,
    health,
    ibkr_auth,
    indicators,
    llm_analyses,
    market_data,
    market_data_cache,
    orders,
    positions,
    prompts,
    strategies,
    workflow_symbols,
    workflows,
)
from backend.config.settings import settings
from backend.app.routes import airflow_proxy, mlflow_proxy
from backend.core.database import engine, get_db
from backend.models import Base
from backend.models.workflow_symbol import ensure_workflow_symbol_schema
from backend.services.ibkr_service import IBKRService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="IBKR Trading Platform",
    version="2.0.0",
    description="Automated trading platform for Interactive Brokers with LLM-powered decision making"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
if os.path.exists("frontend/static"):
    app.mount("/static", StaticFiles(directory="frontend/static"), name="static")

# Include Core API routers
app.include_router(health.router, tags=["health"])
app.include_router(market_data.router, tags=["market"])
app.include_router(market_data_cache.router, prefix="/api/market-data", tags=["market-data-cache"])
app.include_router(indicators.router, prefix="/api/indicators", tags=["indicators"])
app.include_router(orders.router, prefix="/api/orders", tags=["orders"])
app.include_router(positions.router, tags=["positions"])
app.include_router(ibkr_auth.router, prefix="/api/ibkr/auth", tags=["ibkr-auth"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(exercises.router, tags=["exercises"])
app.include_router(charts.router, prefix="/api", tags=["charts"])
app.include_router(llm_analyses.router, prefix="/api", tags=["llm-analyses"])
app.include_router(artifacts.router, prefix="/api/artifacts", tags=["artifacts"])
app.include_router(chart_images.router, prefix="/api/artifacts", tags=["artifacts"])
app.include_router(workflow_symbols.router, tags=["workflow-symbols"])
app.include_router(workflows.router)
app.include_router(strategies.router)
app.include_router(prompts.router)

# Frontend routes
app.include_router(frontend.router, tags=["frontend"])

# Airflow/MLflow proxy routes
app.include_router(airflow_proxy.router, prefix="/api/airflow", tags=["airflow"])
app.include_router(mlflow_proxy.router, prefix="/api/mlflow", tags=["mlflow"])

# WebSocket functionality removed (was used for workflow log streaming)

async def _attempt_ibkr_auto_login() -> None:
    """Background auto-login to Client Portal Gateway (optional, env-driven)."""
    try:
        if not settings.IBKR_AUTO_LOGIN:
            return

        username = (settings.IBKR_USERNAME or settings.IBKR_USER or "").strip()
        password = settings.IBKR_PASSWORD.get_secret_value() if settings.IBKR_PASSWORD else ""
        missing = []
        if not username:
            missing.append("IBKR_USERNAME")
        if not password:
            missing.append("IBKR_PASSWORD")
        if missing:
            logger.warning("IBKR_AUTO_LOGIN=true but %s not set; skipping auto-login", ", ".join(missing))
            return

        ibkr = IBKRService()

        # Wait for gateway to come online (compose starts backend before gateway is healthy).
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            status = await ibkr.check_gateway_connection()
            if status.get("server_online"):
                if status.get("authenticated"):
                    logger.info("IBKR Gateway already authenticated; skipping auto-login")
                    return
                break
            await asyncio.sleep(min(5, attempt))
        else:
            logger.warning("IBKR Gateway did not become reachable; skipping auto-login")
            return

        trading_mode = (settings.IBKR_TRADING_MODE or "paper").lower()
        if trading_mode not in {"paper", "live"}:
            logger.warning("Invalid IBKR_TRADING_MODE=%r; defaulting to 'paper'", settings.IBKR_TRADING_MODE)
            trading_mode = "paper"

        result = await ibkr.automated_login(
            username=username,
            password=password,
            trading_mode=trading_mode,
        )

        if result.get("success"):
            logger.info("IBKR auto-login: %s", result.get("message", "started"))
        else:
            logger.warning("IBKR auto-login failed: %s", result.get("message", "unknown error"))
    except Exception:
        logger.exception("IBKR auto-login task crashed")


@app.on_event("startup")
async def startup():
    """Initialize database on startup."""
    logger.info("Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.warning(f"Failed to create some database tables (may have FK constraints): {e}")
        logger.info("Attempting to create tables without foreign key constraints...")
        # Try to create tables ignoring foreign key errors
        # This is a workaround for models with references to non-existent tables
        try:
            with engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                # Drop all constraints and try again
                for table in Base.metadata.tables.values():
                    try:
                        table.create(bind=engine, checkfirst=True)
                    except Exception as table_err:
                        logger.warning(f"Couldn't create table {table.name}: {table_err}")
            logger.info("Table creation attempt completed")
        except Exception as fallback_err:
            logger.error(f"Fallback table creation also failed: {fallback_err}")
    finally:
        try:
            ensure_workflow_symbol_schema(engine)
        except Exception as schema_err:
            logger.warning("Failed to ensure workflow_symbols schema: %s", schema_err)
    
    # Optional: auto-login to Client Portal Gateway using env credentials.
    asyncio.create_task(_attempt_ibkr_auto_login())

    logger.info("Application started successfully")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    logger.info("Application shutdown")
