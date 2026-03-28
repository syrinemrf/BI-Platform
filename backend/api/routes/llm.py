"""
LLM Integration API routes.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Optional

from core.database import get_db, get_table_names
from core.models import Dataset
from core.schemas import LLMQueryRequest, LLMQueryResponse, SchemaAssistRequest
from services.llm_service import get_llm_service, LLMService
from services.schema_analyzer import analyze_schema
from services.ddl_generator import DDLGenerator
from utils.file_handlers import load_file

router = APIRouter(prefix="/llm", tags=["LLM"])


@router.get("/status")
async def get_llm_status():
    """
    Check if the LLM service (Ollama) is available.
    """
    llm = get_llm_service()
    available = await llm.is_available()

    return {
        "available": available,
        "model": llm.model,
        "base_url": llm.base_url
    }


@router.post("/query", response_model=LLMQueryResponse)
async def query_llm(
        request: LLMQueryRequest,
        db: Session = Depends(get_db)
):
    """
    Generate SQL query from natural language question.

    The LLM analyzes the question and warehouse schema to generate appropriate SQL.
    """
    llm = get_llm_service()

    # Check availability
    if not await llm.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable. Make sure Ollama is running."
        )

    # Get table information
    from core.database import get_table_schema
    tables = get_table_names()

    table_info = {}
    for table in tables:
        if table.startswith("fact_") or table.startswith("dim_"):
            schema = get_table_schema(table)
            table_info[table] = {
                "type": "fact" if table.startswith("fact_") else "dimension",
                "columns": [
                    {
                        "name": col["column_name"],
                        "type": col["data_type"]
                    }
                    for col in schema
                ]
            }

    if not table_info:
        return LLMQueryResponse(
            answer="No warehouse tables found. Please run ETL first.",
            sql_query=None,
            confidence=0.0,
            suggestions=["Upload a dataset", "Run the ETL pipeline"]
        )

    # Generate SQL
    result = await llm.generate_sql_query(request.question, table_info)

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return LLMQueryResponse(
        answer=result.get("explanation", ""),
        sql_query=result.get("sql"),
        confidence=1.0 if result.get("confidence") == "high" else 0.7 if result.get("confidence") == "medium" else 0.4,
        suggestions=[]
    )


@router.post("/schema-assist")
async def schema_assistance(
        request: SchemaAssistRequest,
        db: Session = Depends(get_db)
):
    """
    Get LLM assistance for schema design.

    Analyzes the dataset and provides recommendations for:
    - Dimension table design
    - Measure selection
    - Data quality concerns
    - Optimization tips
    """
    llm = get_llm_service()

    if not await llm.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable. Make sure Ollama is running."
        )

    # Get dataset
    dataset = db.query(Dataset).filter(Dataset.id == request.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Get schema info
    schema_info = dataset.schema_info
    if not schema_info:
        df = load_file(dataset.file_path)
        schema_info = analyze_schema(df)

    # Get LLM suggestions
    result = await llm.analyze_schema_suggestion(schema_info, request.question)

    return result


@router.post("/transformation-suggest")
async def suggest_transformations(
        dataset_id: int,
        db: Session = Depends(get_db)
):
    """
    Get LLM suggestions for data transformations based on quality issues.
    """
    llm = get_llm_service()

    if not await llm.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable. Make sure Ollama is running."
        )

    # Get dataset
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Run quality check
    from services.data_quality import check_data_quality
    df = load_file(dataset.file_path)
    quality_report = check_data_quality(df)

    # Get transformation suggestions
    suggestions = await llm.suggest_transformations(quality_report)

    return {
        "quality_score": quality_report.get("overall_score"),
        "suggestions": suggestions
    }


@router.post("/explain")
async def explain_data(
        question: str,
        context: Optional[str] = None,
        db: Session = Depends(get_db)
):
    """
    Get natural language explanation of data or concepts.
    """
    llm = get_llm_service()

    if not await llm.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable. Make sure Ollama is running."
        )

    system_prompt = """You are a helpful data analyst assistant.
Explain concepts clearly and concisely.
When discussing data, be specific and provide examples.
Format your responses in a readable way."""

    prompt = question
    if context:
        prompt = f"Context: {context}\n\nQuestion: {question}"

    response = await llm.generate(prompt, system_prompt)

    if not response.success:
        raise HTTPException(status_code=500, detail=response.error)

    return {
        "explanation": response.text,
        "tokens_used": response.tokens_used
    }


@router.post("/natural-query")
async def natural_language_query(
        question: str,
        execute: bool = False,
        db: Session = Depends(get_db)
):
    """
    Convert natural language to SQL and optionally execute it.

    - **question**: Natural language question
    - **execute**: If True, execute the generated SQL and return results
    """
    llm = get_llm_service()

    if not await llm.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service unavailable. Make sure Ollama is running."
        )

    # Get table information
    from core.database import get_table_schema, execute_raw_sql
    tables = get_table_names()

    table_info = {}
    for table in tables:
        if table.startswith("fact_") or table.startswith("dim_"):
            schema = get_table_schema(table)
            table_info[table] = {
                "type": "fact" if table.startswith("fact_") else "dimension",
                "columns": [
                    {
                        "name": col["column_name"],
                        "type": col["data_type"]
                    }
                    for col in schema
                ]
            }

    # Generate SQL
    result = await llm.generate_sql_query(question, table_info)

    response = {
        "question": question,
        "sql": result.get("sql"),
        "explanation": result.get("explanation"),
        "confidence": result.get("confidence"),
        "executed": False,
        "results": None
    }

    # Execute if requested
    if execute and result.get("sql"):
        try:
            # Validate - only SELECT allowed
            sql = result["sql"].strip()
            if not sql.upper().startswith("SELECT"):
                raise HTTPException(status_code=400, detail="Only SELECT queries allowed")

            # Add limit for safety
            if "LIMIT" not in sql.upper():
                sql += " LIMIT 1000"

            data = execute_raw_sql(sql)
            response["executed"] = True
            response["results"] = data
            response["row_count"] = len(data)

        except Exception as e:
            response["execution_error"] = str(e)

    return response
