"""
BI Platform - FastAPI Application Entry Point
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from config import settings
from core.database import init_db, engine
from core.models import Base
from api.routes import (
    datasets_router,
    etl_router,
    warehouse_router,
    dashboard_router,
    llm_router,
    auth_router,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _migrate_db():
    """Add missing columns to existing tables (lightweight migration)."""
    from sqlalchemy import text, inspect
    inspector = inspect(engine)

    migrations = [
        ("datasets", "user_id", "INTEGER"),
        ("datasets", "session_id", "VARCHAR(100)"),
        ("etl_jobs", "user_id", "INTEGER"),
        ("etl_jobs", "session_id", "VARCHAR(100)"),
        ("etl_jobs", "job_name", "VARCHAR(255)"),
    ]

    with engine.connect() as conn:
        for table, column, col_type in migrations:
            if table in inspector.get_table_names():
                existing_cols = [c["name"] for c in inspector.get_columns(table)]
                if column not in existing_cols:
                    try:
                        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                        logger.info(f"Added column {column} to {table}")
                    except Exception as e:
                        logger.warning(f"Could not add column {column} to {table}: {e}")
        conn.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting BI Platform API...")

    # Initialize database tables
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized")
        # Add new columns if they don't exist (lightweight migration)
        _migrate_db()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

    yield

    # Shutdown
    logger.info("Shutting down BI Platform API...")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    ## BI Platform API

    A complete Business Intelligence platform with:
    - **Dataset Management**: Upload and manage CSV, Excel, JSON files
    - **ETL Pipeline**: Automated data transformation with quality checks
    - **Star Schema**: Auto-generated dimensional model
    - **Data Warehouse**: PostgreSQL-based analytical storage
    - **Dashboard API**: KPIs, aggregations, and time series
    - **LLM Integration**: Natural language queries via local LLaMA

    ### Key Features
    - Automatic schema detection
    - Data quality validation
    - Star schema generation
    - Interactive dashboards
    - Natural language SQL generation
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal error occurred",
            "error": str(exc) if settings.DEBUG else "Internal Server Error"
        }
    )


# Include routers
app.include_router(auth_router, prefix="/api")
app.include_router(datasets_router, prefix="/api")
app.include_router(etl_router, prefix="/api")
app.include_router(warehouse_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(llm_router, prefix="/api")


# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    from services.llm_service import get_llm_service

    # Check database
    db_status = "healthy"
    try:
        from core.database import execute_raw_sql
        execute_raw_sql("SELECT 1")
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    # Check LLM
    llm_status = "unavailable"
    try:
        llm = get_llm_service()
        if await llm.is_available():
            llm_status = "available"
    except Exception:
        pass

    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "version": settings.APP_VERSION,
        "database": db_status,
        "llm": llm_status
    }


# API info endpoint
@app.get("/")
async def root():
    """API root - returns basic info."""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
