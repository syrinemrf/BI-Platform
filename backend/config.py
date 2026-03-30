"""
Application configuration settings.
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "BI Platform"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = True

    # Database (change to PostgreSQL in production)
    DATABASE_URL: str = "sqlite:///./bi_warehouse.db"
    DATABASE_POOL_SIZE: int = 10
    DATABASE_MAX_OVERFLOW: int = 20

    # Redis (for Celery)
    REDIS_URL: str = "redis://localhost:6379/0"

    # Authentication
    SECRET_KEY: str = "bi-platform-secret-key-change-in-production-2024"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours

    # File Upload
    UPLOAD_DIR: str = "./uploads"
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS: list = [".csv", ".xlsx", ".xls", ".json"]

    # LLM Settings (Ollama)
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3:8b"
    LLM_TIMEOUT: int = 120

    # ETL Settings
    ETL_BATCH_SIZE: int = 10000
    ETL_MAX_WORKERS: int = 4

    # Data Quality Thresholds
    DQ_COMPLETENESS_THRESHOLD: float = 0.95
    DQ_UNIQUENESS_THRESHOLD: float = 0.99
    DQ_VALIDITY_THRESHOLD: float = 0.98

    # CORS
    CORS_ORIGINS: list = ["http://localhost:3000", "http://localhost:5173"]

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()


# Ensure upload directory exists
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
