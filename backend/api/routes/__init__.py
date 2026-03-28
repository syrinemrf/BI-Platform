"""API routes module."""
from api.routes.datasets import router as datasets_router
from api.routes.etl import router as etl_router
from api.routes.warehouse import router as warehouse_router
from api.routes.dashboard import router as dashboard_router
from api.routes.llm import router as llm_router

__all__ = [
    "datasets_router",
    "etl_router",
    "warehouse_router",
    "dashboard_router",
    "llm_router"
]
