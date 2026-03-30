"""
Dataset management API routes.
"""
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Header
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import os
import uuid
from datetime import datetime
import logging
import traceback

from core.database import get_db
from core.models import Dataset, User
from core.schemas import DatasetResponse, DatasetPreview
from utils.file_handlers import FileHandler, save_upload_file, load_file, get_file_info
from utils.validators import FileValidator, sanitize_for_json
from services.schema_analyzer import analyze_schema
from services.auth_service import get_current_user
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["Datasets"])


def _get_session_id(x_session_id: Optional[str] = Header(None)) -> Optional[str]:
    """Get guest session ID from header."""
    return x_session_id


def _filter_datasets(query, user: Optional[User], session_id: Optional[str]):
    """Filter datasets based on user or guest session."""
    if user:
        return query.filter(Dataset.user_id == user.id)
    elif session_id:
        return query.filter(Dataset.session_id == session_id, Dataset.user_id == None)
    else:
        # No user and no session = return nothing
        return query.filter(Dataset.id == -1)


@router.post("/upload", response_model=DatasetResponse)
async def upload_dataset(
        file: UploadFile = File(...),
        name: Optional[str] = None,
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    Upload a new dataset file.

    - **file**: CSV, Excel, or JSON file to upload
    - **name**: Optional display name for the dataset
    """
    # Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    # Validate extension only (size check done after reading)
    if not FileValidator.validate_extension(file.filename):
        raise HTTPException(
            status_code=400,
            detail=f"File extension not allowed. Allowed: {', '.join(FileValidator.ALLOWED_EXTENSIONS)}"
        )

    # Generate unique filename
    file_ext = os.path.splitext(file.filename)[1].lower()
    unique_filename = f"{uuid.uuid4()}{file_ext}"

    try:
        # Save file
        file_path = await save_upload_file(file, unique_filename)
        file_info = get_file_info(file_path)

        # Load and analyze
        df = load_file(file_path)
        schema_info = analyze_schema(df)

        # Create dataset record
        dataset = Dataset(
            name=name or file.filename,
            original_filename=file.filename,
            file_path=file_path,
            file_type=FileHandler.get_file_type(file.filename),
            file_size=file_info['size'],
            row_count=len(df),
            column_count=len(df.columns),
            schema_info=sanitize_for_json(schema_info),
            user_id=user.id if user else None,
            session_id=session_id if not user else None,
        )

        db.add(dataset)
        db.commit()
        db.refresh(dataset)

        return dataset

    except Exception as e:
        # Log the full error
        logger.error(f"Upload failed: {str(e)}")
        logger.error(traceback.format_exc())
        # Clean up file on failure
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.get("", response_model=List[DatasetResponse])
async def list_datasets(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    List datasets for the current user or guest session.
    """
    query = db.query(Dataset)
    query = _filter_datasets(query, user, session_id)
    datasets = query.order_by(Dataset.created_at.desc()).offset(skip).limit(limit).all()
    return datasets


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
        dataset_id: int,
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    Get a specific dataset by ID.
    """
    query = db.query(Dataset).filter(Dataset.id == dataset_id)
    query = _filter_datasets(query, user, session_id)
    dataset = query.first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.get("/{dataset_id}/preview", response_model=DatasetPreview)
async def preview_dataset(
        dataset_id: int,
        rows: int = Query(100, ge=1, le=1000),
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    Preview dataset contents.
    """
    query = db.query(Dataset).filter(Dataset.id == dataset_id)
    query = _filter_datasets(query, user, session_id)
    dataset = query.first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        preview_df = df.head(rows)

        # Sanitize data for JSON serialization
        data = sanitize_for_json(preview_df.to_dict(orient='records'))

        return DatasetPreview(
            columns=list(preview_df.columns),
            data=data,
            total_rows=len(df)
        )
    except Exception as e:
        logger.error(f"Preview failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to preview: {str(e)}")


@router.get("/{dataset_id}/schema")
async def get_dataset_schema(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Get detailed schema analysis for a dataset.
    """
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.schema_info:
        return dataset.schema_info

    # Re-analyze if not cached
    try:
        df = load_file(dataset.file_path)
        schema_info = analyze_schema(df)

        # Update cache
        dataset.schema_info = sanitize_for_json(schema_info)
        db.commit()

        return schema_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema analysis failed: {str(e)}")


@router.delete("/{dataset_id}")
async def delete_dataset(
        dataset_id: int,
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    Delete a dataset and its associated file.
    """
    query = db.query(Dataset).filter(Dataset.id == dataset_id)
    query = _filter_datasets(query, user, session_id)
    dataset = query.first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        # Delete file
        if os.path.exists(dataset.file_path):
            os.remove(dataset.file_path)

        # Delete record
        db.delete(dataset)
        db.commit()

        return {"message": "Dataset deleted successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.post("/{dataset_id}/reanalyze")
async def reanalyze_dataset(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Force re-analysis of dataset schema.
    """
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        schema_info = analyze_schema(df)

        dataset.schema_info = sanitize_for_json(schema_info)
        dataset.row_count = len(df)
        dataset.column_count = len(df.columns)
        db.commit()

        return schema_info

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Re-analysis failed: {str(e)}")
