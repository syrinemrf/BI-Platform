"""HITL review routes for the LLM ETL pipeline."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from services.etl_llm.validation.hitl_validator import HITLValidator

router = APIRouter(prefix="/etl-llm", tags=["ETL LLM - HITL"])
_validator = HITLValidator()


@router.get("/review-queue")
async def list_pending_reviews():
    """List all pending human reviews."""
    return [j.model_dump() for j in _validator.get_pending_reviews()]


@router.post("/approve/{job_id}")
async def approve_review(job_id: str):
    """Human approves a pending review — stores mapping to FAISS."""
    job = _validator.approve_review(job_id)
    if not job:
        raise HTTPException(404, "Review job not found")
    return {"status": "approved", "job_id": job_id}


@router.post("/reject/{job_id}")
async def reject_review(job_id: str, reason: str = ""):
    """Human rejects a pending review with a reason."""
    job = _validator.reject_review(job_id, reason)
    if not job:
        raise HTTPException(404, "Review job not found")
    return {"status": "rejected", "job_id": job_id}


@router.post("/modify/{job_id}")
async def modify_review(job_id: str, updated_mapping: dict):
    """Human modifies the mapping then approves."""
    job = _validator.modify_review(job_id, updated_mapping)
    if not job:
        raise HTTPException(404, "Review job not found")
    return {"status": "modified", "job_id": job_id}
