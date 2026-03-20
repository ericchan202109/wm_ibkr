"""Application configuration settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BeforeValidator, SecretStr, field_validator
from typing import Optional, Annotated


class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="allow"  # Allow extra fields from .env for flexibility
    )
    
    # Application
    APP_NAME: str = "IBKR Trading WebApp"
    VERSION: str = "1.0.0"
    DEBUG: bool = True
    ENVIRONMENT: str = "development"
    SECRET_KEY: str = "change-this-secret-key-in-production"
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    BACKEND_PORT: int = 8000
    
    # IBKR
    IBKR_ACCOUNT_ID: str = "DU1234567"  # Default demo account
    IBKR_API_BASE_URL: str = "https://127.0.0.1:5055/v1/api"
    IBKR_SSL_VERIFY: bool = False
    IBKR_USERNAME: Optional[str] = None
    IBKR_USER: Optional[str] = None  # Back-compat alias for IBKR_USERNAME
    IBKR_PASSWORD: Optional[SecretStr] = None
    IBKR_TRADING_MODE: str = "paper"  # "paper" or "live"
    IBKR_AUTO_LOGIN: bool = False

    # Database
    # NOTE: DATABASE_URL is required when running in Docker
    # Format: postgresql+psycopg2://username:password@host:port/database?sslmode=require
    DATABASE_URL: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/ibkr_trading"
    DB_ECHO: bool = False
    
    # Redis / Celery (use localhost for local dev)
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"
    
    # MinIO
    MINIO_ENDPOINT: str = "localhost:9000"  # Internal endpoint for backend connections
    MINIO_PUBLIC_ENDPOINT: str = "localhost:9000"  # Public endpoint for browser-accessible URLs
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "trading-charts"
    MINIO_BUCKET_CHARTS: str = "trading-charts"
    MINIO_SECURE: bool = False
    
    # OpenAI / LLM
    OPENAI_API_BASE: str = "https://api.openai.com/v1"
    OPENAI_API_KEY: str = "your_key_here"
    OPENAI_MODEL: str = "gpt-4-turbo-preview"
    AI_REQUEST_TIMEOUT: int = 120
    
    # Market Data Cache & Debug Mode
    DEBUG_MODE: bool = False  # Use cached data instead of live IBKR API
    CACHE_ENABLED: bool = True  # Enable market data caching
    _CACHE_SYMBOLS: str = "NVDA,TSLA"  # Internal comma-separated string
    CACHE_EXCHANGE: str = "NASDAQ"  # Default exchange for cached symbols
    CACHE_TTL_HOURS: int = 24  # Cache time-to-live in hours
    
    @property
    def CACHE_SYMBOLS(self) -> list[str]:
        """Parse CACHE_SYMBOLS from comma-separated string to list."""
        return [s.strip() for s in self._CACHE_SYMBOLS.split(',') if s.strip()]
    
    # LLM Vision for Chart Analysis
    LLM_VISION_PROVIDER: str = "openai"  # "openai" or "gemini"
    LLM_VISION_MODEL: str = "gpt-4-vision-preview"  # or "gemini-2.0-flash-exp"
    LLM_VISION_MAX_TOKENS: int = 4096
    LLM_VISION_TEMPERATURE: float = 0.1
    LLM_VISION_TIMEOUT: int = 60
    
    # Google Gemini (alternative to OpenAI)
    GEMINI_API_KEY: Optional[str] = None
    GEMINI_API_BASE: str = "https://generativelanguage.googleapis.com/v1beta"
    GEMINI_MODEL: str = "gemini-2.0-flash-exp"
    
    # LLM Signal Generation (English only)
    LLM_CONSOLIDATE_TIMEFRAMES: bool = True
    LLM_RETRY_ATTEMPTS: int = 3
    LLM_RETRY_DELAY: int = 2  # seconds

    # MLflow
    MLFLOW_TRACKING_URI: str = "http://mlflow-server:5500"
    MLFLOW_API_PREFIX: str = "/api/2.0/mlflow"
    MLFLOW_EXPERIMENT_NAME: str = "ibkr-stock-data"
    MLFLOW_REQUEST_TIMEOUT: int = 10
    
    # Risk Management
    MIN_R_COEFFICIENT: float = 1.0
    MIN_PROFIT_MARGIN: float = 0.05
    RISK_PER_TRADE: float = 0.01
    MAX_PORTFOLIO_EXPOSURE: float = 0.90
    MAX_OPEN_POSITIONS: int = 10
    MAX_POSITION_SIZE: float = 0.20
    
    # Trading
    DEFAULT_TIF: str = "DAY"
    DEFAULT_ORDER_TYPE: str = "LMT"
    WORKFLOW_DELAY_SECONDS: int = 60

    # Airflow
    AIRFLOW_API_URL: str = "http://airflow-webserver:8080/api/v1"
    AIRFLOW_USERNAME: str = "airflow"
    AIRFLOW_PASSWORD: str = "airflow"
    AIRFLOW_DEFAULT_DAG_ID: str = "ibkr_trading_strategy"
    AIRFLOW_RUN_TIMEOUT_SECONDS: int = 30

    # Chart
    CHART_WIDTH: int = 1920
    CHART_HEIGHT: int = 1080
    CHART_CACHE_TTL: int = 3600
    
    # Features
    AUTO_TRADING_ENABLED: bool = False
    PAPER_TRADING_MODE: bool = True
    
    @field_validator('DATABASE_URL')
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        """Ensure DATABASE_URL is properly formatted and uses psycopg2 driver."""
        if not v or v == "":
            raise ValueError(
                "DATABASE_URL is required. Please set it in your .env file.\n"
                "Format: postgresql+psycopg2://username:password@host:port/database?sslmode=require"
            )
        
        # Ensure we're using psycopg2 driver for compatibility
        if v.startswith("postgresql://"):
            # Upgrade to explicit psycopg2 driver
            v = v.replace("postgresql://", "postgresql+psycopg2://", 1)
        elif v.startswith("postgresql+psycopg://"):
            # Convert psycopg3 format to psycopg2 (we use psycopg2-binary in Docker)
            v = v.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
        elif not v.startswith("postgresql+psycopg2://"):
            raise ValueError(
                f"DATABASE_URL must use postgresql or postgresql+psycopg2 driver. Got: {v.split('://')[0] if '://' in v else v}"
            )
        
        return v


settings = Settings()
