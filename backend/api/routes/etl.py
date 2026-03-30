"""
ETL Pipeline API routes.
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Body, Query, Header
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from datetime import datetime

from core.database import get_db
from core.models import Dataset, ETLJob, StarSchemaMetadata, DataQualityReport as DQReportModel, User
from core.schemas import ETLConfig, ETLJobResponse, DataQualityReportResponse, ETLProgress
from services.etl_pipeline import ETLPipeline, ETLStatus
from services.schema_analyzer import analyze_schema
from services.data_quality import check_data_quality
from services.auth_service import get_current_user
from utils.file_handlers import load_file
from utils.validators import sanitize_for_json

router = APIRouter(prefix="/etl", tags=["ETL"])

# In-memory job status tracking (in production, use Redis)
_job_status: Dict[int, Dict[str, Any]] = {}


def _get_session_id(x_session_id: Optional[str] = Header(None)) -> Optional[str]:
    return x_session_id


@router.post("/analyze/{dataset_id}")
async def analyze_dataset_schema(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Analyze dataset schema for star schema planning.

    Returns detailed schema analysis including:
    - Measures (numeric columns)
    - Dimensions (categorical columns)
    - Date columns
    - Suggested entities for dimensions
    """
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        analysis = analyze_schema(df)
        return sanitize_for_json(analysis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/quality-check/{dataset_id}")
async def run_quality_check(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Run data quality checks on a dataset.

    Checks include:
    - Completeness (missing values)
    - Uniqueness (duplicates)
    - Validity (type conformance)
    - Consistency (cross-field validation)
    """
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        report = check_data_quality(df)
        return sanitize_for_json(report)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quality check failed: {str(e)}")


@router.post("/run", response_model=ETLJobResponse)
async def run_etl_pipeline(
        config: ETLConfig,
        background_tasks: BackgroundTasks,
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    Start ETL pipeline execution.
    """
    dataset = db.query(Dataset).filter(Dataset.id == config.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Create ETL job record
    etl_job = ETLJob(
        dataset_id=config.dataset_id,
        user_id=user.id if user else None,
        session_id=session_id if not user else None,
        status="pending"
    )
    db.add(etl_job)
    db.commit()
    db.refresh(etl_job)

    # Initialize job status
    _job_status[etl_job.id] = {
        "status": "pending",
        "progress": 0,
        "current_step": "initializing",
        "message": "ETL job queued"
    }

    # Run ETL in background
    background_tasks.add_task(
        _execute_etl_job,
        etl_job.id,
        dataset.file_path,
        config.model_dump(),
        db
    )

    return etl_job


async def _execute_etl_job(
        job_id: int,
        file_path: str,
        config: dict,
        db: Session
):
    """Execute ETL job in background."""
    import logging
    logger = logging.getLogger(__name__)

    try:
        # Update status
        _job_status[job_id] = {
            "status": "running",
            "progress": 5,
            "current_step": "extracting",
            "message": "Starting ETL pipeline"
        }

        # Get fresh session
        from core.database import SessionLocal
        session = SessionLocal()

        try:
            job = session.query(ETLJob).filter(ETLJob.id == job_id).first()
            job.status = "running"
            job.started_at = datetime.now()
            session.commit()

            # Create and run pipeline
            pipeline = ETLPipeline(
                source_path=file_path,
                config=config
            )

            result = pipeline.run()

            logger.info(f"ETL pipeline result: success={result.success}, error={result.error}")

            # Update job record
            if result.success:
                job.status = "completed"
                job.quality_report = result.quality_report
                job.transformation_log = {
                    "steps": [
                        {
                            "name": s.name,
                            "status": s.status,
                            "duration": (s.completed_at - s.started_at).total_seconds() if s.completed_at and s.started_at else None
                        }
                        for s in result.steps
                    ],
                    "tables_created": result.tables_created,
                    "rows_processed": result.rows_processed
                }

                # Save star schema metadata
                if result.star_schema:
                    schema_meta = StarSchemaMetadata(
                        etl_job_id=job_id,
                        fact_table_name=result.star_schema.get('fact_table', {}).get('name', 'fact_main'),
                        dimension_tables=result.star_schema.get('dimensions', []),
                        schema_definition=result.star_schema
                    )
                    session.add(schema_meta)

                _job_status[job_id] = {
                    "status": "completed",
                    "progress": 100,
                    "current_step": "done",
                    "message": "ETL completed successfully"
                }
            else:
                job.status = "failed"
                job.error_message = result.error
                logger.error(f"ETL job {job_id} failed: {result.error}")

                _job_status[job_id] = {
                    "status": "failed",
                    "progress": 0,
                    "current_step": "error",
                    "message": result.error or "Unknown error"
                }

            job.completed_at = datetime.now()
            session.commit()

        finally:
            session.close()

    except Exception as e:
        import traceback
        logger.error(f"ETL job {job_id} exception: {str(e)}")
        logger.error(traceback.format_exc())
        _job_status[job_id] = {
            "status": "failed",
            "progress": 0,
            "current_step": "error",
            "message": str(e)
        }


@router.get("/status/{job_id}", response_model=ETLProgress)
async def get_etl_status(
        job_id: int,
        db: Session = Depends(get_db)
):
    """
    Get current status of an ETL job.
    """
    job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    status_info = _job_status.get(job_id, {
        "status": job.status,
        "progress": 100 if job.status == "completed" else 0,
        "current_step": job.status,
        "message": job.error_message or ""
    })

    return ETLProgress(
        job_id=job_id,
        status=status_info["status"],
        current_step=status_info["current_step"],
        progress_percent=status_info["progress"],
        message=status_info["message"]
    )


@router.get("/job/{job_id}", response_model=ETLJobResponse)
async def get_etl_job(
        job_id: int,
        db: Session = Depends(get_db)
):
    """
    Get ETL job details.
    """
    job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")
    return job


@router.get("/quality-report/{job_id}")
async def get_quality_report(
        job_id: int,
        db: Session = Depends(get_db)
):
    """
    Get data quality report for an ETL job.
    """
    job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    if not job.quality_report:
        raise HTTPException(status_code=404, detail="Quality report not available")

    return job.quality_report


@router.get("/jobs")
async def list_etl_jobs(
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 50,
        user: Optional[User] = Depends(get_current_user),
        session_id: Optional[str] = Depends(_get_session_id),
        db: Session = Depends(get_db)
):
    """
    List ETL jobs with optional filters, scoped to user/session.
    """
    query = db.query(ETLJob)

    # Filter by user/session
    if user:
        query = query.filter(ETLJob.user_id == user.id)
    elif session_id:
        query = query.filter(ETLJob.session_id == session_id, ETLJob.user_id == None)
    else:
        query = query.filter(ETLJob.id == -1)  # return nothing

    if dataset_id:
        query = query.filter(ETLJob.dataset_id == dataset_id)
    if status:
        query = query.filter(ETLJob.status == status)

    jobs = query.order_by(ETLJob.created_at.desc()).limit(limit).all()

    return [
        {
            "id": job.id,
            "dataset_id": job.dataset_id,
            "job_name": job.job_name,
            "status": job.status,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "error_message": job.error_message,
            "created_at": job.created_at
        }
        for job in jobs
    ]


@router.delete("/job/{job_id}")
async def cancel_etl_job(
        job_id: int,
        db: Session = Depends(get_db)
):
    """
    Cancel a pending or running ETL job.
    """
    job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    if job.status in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail="Cannot cancel completed/failed job")

    job.status = "cancelled"
    job.completed_at = datetime.now()
    db.commit()

    if job_id in _job_status:
        _job_status[job_id]["status"] = "cancelled"

    return {"message": "Job cancelled"}


@router.post("/improve-data/{dataset_id}")
async def improve_data(
        dataset_id: int,
        action: str,
        column: Optional[str] = None,
        body: Optional[Dict[str, Any]] = Body(default=None),
        db: Session = Depends(get_db)
):
    """
    Apply data improvement actions to a dataset.

    Actions:
    - drop_nulls: Remove rows with null values
    - fill_nulls: Fill null values with specified strategy
    - remove_duplicates: Remove duplicate rows
    - normalize_strings: Normalize string values
    - fix_types: Fix data type issues
    - custom: Apply custom transformation from LLM suggestion
    """
    import pandas as pd
    import numpy as np
    import logging

    logger = logging.getLogger(__name__)

    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        original_rows = len(df)
        original_nulls = int(df.isnull().sum().sum())
        original_cols = len(df.columns)

        params = body or {}

        # Convert column names to strings for comparison
        df.columns = [str(c) for c in df.columns]

        logger.info(f"Improve data action: {action}, column: {column}, dataset: {dataset_id}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")

        if action == "drop_nulls":
            if column:
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found. Available: {df.columns.tolist()[:10]}")
                df = df.dropna(subset=[column])
            else:
                df = df.dropna()

        elif action == "fill_nulls":
            strategy = params.get("strategy", "mean")
            if column:
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
                col_dtype = str(df[column].dtype)
                if strategy == "mean" and ('float' in col_dtype or 'int' in col_dtype):
                    mean_val = df[column].mean()
                    if pd.notna(mean_val):
                        df[column] = df[column].fillna(mean_val)
                elif strategy == "median" and ('float' in col_dtype or 'int' in col_dtype):
                    median_val = df[column].median()
                    if pd.notna(median_val):
                        df[column] = df[column].fillna(median_val)
                elif strategy == "mode":
                    mode_vals = df[column].mode()
                    if len(mode_vals) > 0:
                        df[column] = df[column].fillna(mode_vals.iloc[0])
                    else:
                        df[column] = df[column].fillna("")
                elif strategy == "value":
                    df[column] = df[column].fillna(params.get("value", ""))
                else:
                    # Default: fill with mean for numeric, empty string for others
                    col_dtype = str(df[column].dtype)
                    if 'float' in col_dtype or 'int' in col_dtype:
                        mean_val = df[column].mean()
                        if pd.notna(mean_val):
                            df[column] = df[column].fillna(mean_val)
                    else:
                        df[column] = df[column].fillna("")
            else:
                for col in df.columns:
                    col_dtype = str(df[col].dtype)
                    if 'float' in col_dtype or 'int' in col_dtype:
                        mean_val = df[col].mean()
                        if pd.notna(mean_val):
                            df[col] = df[col].fillna(mean_val)
                    else:
                        df[col] = df[col].fillna("")

        elif action == "remove_duplicates":
            df = df.drop_duplicates()

        elif action == "normalize_strings":
            if column:
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
                if df[column].dtype == 'object':
                    df[column] = df[column].astype(str).str.strip().str.lower()
                    df[column] = df[column].replace('nan', '')
            else:
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.strip().str.lower()
                    df[col] = df[col].replace('nan', '')

        elif action == "fix_types":
            target_type = params.get("type", "auto")
            if column:
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
                if target_type == "numeric":
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif target_type == "datetime":
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif target_type == "string":
                    df[column] = df[column].astype(str)

        elif action == "trim_outliers":
            if column:
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
                col_dtype = str(df[column].dtype)
                if 'float' in col_dtype or 'int' in col_dtype:
                    q1 = df[column].quantile(0.25)
                    q3 = df[column].quantile(0.75)
                    iqr = q3 - q1
                    if pd.notna(iqr) and iqr > 0:
                        lower = q1 - 1.5 * iqr
                        upper = q3 + 1.5 * iqr
                        df = df[(df[column] >= lower) & (df[column] <= upper)]

        elif action == "drop_column":
            if column:
                if column in df.columns:
                    df = df.drop(columns=[column])
                else:
                    raise HTTPException(status_code=400, detail=f"Column '{column}' not found")

        elif action == "drop_unnamed":
            # Remove all "Unnamed:" columns from the dataset
            unnamed_cols = [str(col) for col in df.columns if str(col).startswith('Unnamed:')]
            logger.info(f"Unnamed columns found: {unnamed_cols}")
            if unnamed_cols:
                df = df.drop(columns=unnamed_cols)

        # Save the improved data
        file_ext = dataset.file_path.split('.')[-1].lower()
        logger.info(f"Saving to {dataset.file_path} as {file_ext}")

        if file_ext == 'csv':
            df.to_csv(dataset.file_path, index=False)
        elif file_ext in ['xlsx', 'xls']:
            df.to_excel(dataset.file_path, index=False)
        elif file_ext == 'json':
            df.to_json(dataset.file_path, orient='records')
        else:
            # Default to CSV
            df.to_csv(dataset.file_path, index=False)

        # Update dataset metadata
        dataset.row_count = len(df)
        dataset.column_count = len(df.columns)
        db.commit()

        new_nulls = int(df.isnull().sum().sum())

        return {
            "success": True,
            "action": action,
            "original_rows": original_rows,
            "new_rows": len(df),
            "rows_removed": original_rows - len(df),
            "columns_removed": original_cols - len(df.columns),
            "original_nulls": original_nulls,
            "new_nulls": new_nulls,
            "nulls_fixed": original_nulls - new_nulls
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"Data improvement failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Data improvement failed: {str(e)}")


@router.post("/chat")
async def chat_with_data(
        dataset_id: int = Query(..., description="Dataset ID to chat about"),
        message: str = Query(..., description="User message to send"),
        db: Session = Depends(get_db)
):
    """
    Chat with LLM about the dataset.

    Supports questions about:
    - Data state and quality
    - Schema analysis
    - Transformation suggestions
    - SQL queries
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Chat request: dataset_id={dataset_id}, message={message[:50]}...")

    try:
        from services.llm_service import get_llm_service

        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Check LLM availability
        llm = get_llm_service()
        llm_available = await llm.is_available()

        # Load data info
        try:
            df = load_file(dataset.file_path)
            logger.info(f"Loaded dataset with {len(df)} rows and {len(df.columns)} columns")
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            return {
                "response": f"Failed to load dataset: {str(e)}",
                "available": llm_available,
                "success": False
            }

        # Build context
        try:
            # Safely build context with limited data
            columns_list = [str(c) for c in df.columns.tolist()[:50]]  # Limit columns

            context = f"""Dataset: {dataset.name}
Rows: {len(df)}
Columns ({len(df.columns)}): {', '.join(columns_list)}

Column Types:
{df.dtypes.head(30).to_string()}

Statistics (numeric columns):
{df.describe().to_string() if len(df.select_dtypes(include='number').columns) > 0 else 'No numeric columns'}

Null Values:
{df.isnull().sum().head(30).to_string()}

Sample Data (first 3 rows):
{df.head(3).to_string()}
"""
        except Exception as e:
            logger.error(f"Failed to build context: {e}")
            context = f"""Dataset: {dataset.name}
Error building context: {str(e)}
"""

        # If LLM is not available, provide a basic response
        if not llm_available:
            basic_response = f"""LLM service (Ollama) is not available. Here's what I can tell you about the data:

**Dataset Summary:**
- Name: {dataset.name}
- Rows: {len(df):,}
- Columns: {len(df.columns)}

**Column Names:** {', '.join([str(c) for c in df.columns.tolist()[:20]])}{'...' if len(df.columns) > 20 else ''}

**Null Values:**
{df.isnull().sum().to_string() if len(df.columns) < 30 else 'Too many columns to display'}

**To enable AI assistance:** Please start Ollama with `ollama serve` and ensure a model is available (e.g., `ollama pull llama3`)."""

            return {
                "response": basic_response,
                "available": False,
                "success": True
            }

        system_prompt = """You are a helpful data analyst assistant. You have access to information about a dataset.
Answer questions about the data clearly and concisely.
If asked to suggest improvements, provide specific actionable recommendations.
If asked to generate transformations, provide Python/pandas code snippets.
Always format your response in a readable way with clear sections.
Use markdown formatting for better readability."""

        prompt = f"""Dataset Context:
{context}

User Question: {message}

Please provide a helpful response."""

        logger.info("Calling LLM...")
        response = await llm.generate(prompt, system_prompt, temperature=0.7)

        if not response.success:
            logger.error(f"LLM error: {response.error}")
            return {
                "response": f"LLM Error: {response.error}. Please check that Ollama is running correctly.",
                "available": True,
                "success": False
            }

        logger.info(f"LLM response received, tokens: {response.tokens_used}")
        return {
            "response": response.text,
            "available": True,
            "success": True,
            "tokens_used": response.tokens_used
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"Chat error: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "response": f"An error occurred: {str(e)}",
            "available": False,
            "success": False
        }


@router.get("/improvement-suggestions/{dataset_id}")
async def get_improvement_suggestions(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Get automatic improvement suggestions for a dataset.

    Analyzes the data and returns actionable suggestions.
    """
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        suggestions = []

        # Check for unnamed columns (common CSV issue)
        unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed:')]
        if unnamed_cols:
            suggestions.append({
                "type": "warning",
                "column": None,
                "issue": f"{len(unnamed_cols)} unnamed columns",
                "action": "drop_unnamed",
                "description": f"Remove {len(unnamed_cols)} unnamed columns: {', '.join(unnamed_cols[:3])}..."
            })

        # Check for null values
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                pct = (count / len(df)) * 100
                if pct > 80:
                    suggestions.append({
                        "type": "critical",
                        "column": col,
                        "issue": f"{pct:.1f}% null values",
                        "action": "drop_column",
                        "description": f"Drop column '{col}' - mostly empty ({pct:.1f}% null)"
                    })
                elif pct > 50:
                    suggestions.append({
                        "type": "critical",
                        "column": col,
                        "issue": f"{pct:.1f}% null values",
                        "action": "drop_nulls",
                        "description": f"Drop rows with null values in '{col}' or fill them"
                    })
                elif pct > 10:
                    suggestions.append({
                        "type": "warning",
                        "column": col,
                        "issue": f"{pct:.1f}% null values",
                        "action": "fill_nulls",
                        "description": f"Fill null values in '{col}' using mean/median/mode"
                    })

        # Check for duplicates
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            pct = (dup_count / len(df)) * 100
            suggestions.append({
                "type": "warning" if pct < 10 else "critical",
                "column": None,
                "issue": f"{dup_count} duplicate rows ({pct:.1f}%)",
                "action": "remove_duplicates",
                "description": "Remove duplicate rows from the dataset"
            })

        # Check for outliers in numeric columns
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            # Skip unnamed columns
            if str(col).startswith('Unnamed:'):
                continue
            try:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                if pd.isna(iqr) or iqr == 0:
                    continue
                outliers = ((df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)).sum()
                if outliers > 0:
                    pct = (outliers / len(df)) * 100
                    if pct > 5:
                        suggestions.append({
                            "type": "info",
                            "column": col,
                            "issue": f"{outliers} outliers ({pct:.1f}%)",
                            "action": "trim_outliers",
                            "description": f"Consider handling outliers in '{col}'"
                        })
            except Exception:
                pass  # Skip columns that cause issues

        # Check for string normalization
        for col in df.select_dtypes(include=['object']).columns:
            # Skip unnamed columns
            if str(col).startswith('Unnamed:'):
                continue
            try:
                sample = df[col].dropna().head(100).astype(str)
                has_whitespace = any(sample.str.startswith(' ')) or any(sample.str.endswith(' '))
                if has_whitespace:
                    suggestions.append({
                        "type": "info",
                        "column": col,
                        "issue": "Contains leading/trailing whitespace",
                        "action": "normalize_strings",
                        "description": f"Normalize strings in '{col}'"
                    })
            except Exception:
                pass  # Skip columns that cause issues

        return {
            "suggestions": suggestions,
            "total_rows": len(df),
            "total_columns": len(df.columns)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/download-cleaned/{dataset_id}")
async def download_cleaned_data(dataset_id: int, db: Session = Depends(get_db)):
    """Download the cleaned version of a dataset as CSV."""
    from fastapi.responses import StreamingResponse
    import io

    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = load_file(dataset.file_path)
        # Apply basic cleaning
        df = df.drop_duplicates()
        df = df.dropna(how='all')
        # Normalize string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()

        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        clean_name = f"{dataset.name}_cleaned.csv"
        return StreamingResponse(
            io.BytesIO(buffer.getvalue().encode('utf-8')),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={clean_name}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate cleaned data: {str(e)}")
