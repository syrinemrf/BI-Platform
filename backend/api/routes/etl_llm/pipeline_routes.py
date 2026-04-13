"""API routes for the full LLM ETL pipeline."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, UploadFile, File, Query

from services.etl_llm.orchestrator.pipeline_orchestrator import (
    ETLPipelineOrchestrator,
    PipelineResult,
)
from services.etl_llm.validation.hitl_validator import HITLValidator

router = APIRouter(prefix="/etl-llm", tags=["ETL LLM - Pipeline"])

_orchestrator: ETLPipelineOrchestrator | None = None


def _get_orchestrator() -> ETLPipelineOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = ETLPipelineOrchestrator()
    return _orchestrator


@router.post("/run")
async def run_pipeline(
    file: UploadFile = File(...),
    source_type: str = Query("csv"),
    auto_approve: bool = Query(False),
):
    """Upload a file and run the full 5-layer ETL pipeline."""
    upload_dir = os.path.join(os.path.dirname(__file__), "..", "..", "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)
    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    orch = _get_orchestrator()
    result = await orch.run_pipeline(file_path, source_type, auto_approve)
    return result.__dict__


@router.get("/status/{pipeline_id}")
async def get_pipeline_status(pipeline_id: str):
    """Get lineage information for a pipeline run."""
    orch = _get_orchestrator()
    graph = orch.lineage.get_lineage(pipeline_id)
    if not graph:
        raise HTTPException(404, "Pipeline not found")
    return orch.lineage.export_lineage_json(pipeline_id)


@router.get("/lineage/{pipeline_id}")
async def get_lineage_markdown(pipeline_id: str):
    """Export lineage as Markdown documentation."""
    orch = _get_orchestrator()
    md = orch.lineage.export_lineage_markdown(pipeline_id)
    if not md:
        raise HTTPException(404, "Pipeline not found")
    return {"markdown": md}


@router.get("/documentation/{pipeline_id}")
async def get_pipeline_documentation(pipeline_id: str):
    """Auto-generated pipeline documentation."""
    orch = _get_orchestrator()
    graph = orch.lineage.get_lineage(pipeline_id)
    if not graph:
        raise HTTPException(404, "Pipeline not found")
    return {
        "pipeline_id": pipeline_id,
        "steps": len(graph.nodes),
        "lineage": orch.lineage.export_lineage_markdown(pipeline_id),
    }
