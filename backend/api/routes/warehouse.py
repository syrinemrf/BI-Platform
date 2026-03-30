"""
Data Warehouse API routes.
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import time

from core.database import (
    get_db,
    execute_raw_sql,
    get_table_names,
    get_table_schema,
    table_to_dataframe,
    table_exists
)
from core.models import StarSchemaMetadata, DimensionTable, FactTable
from core.schemas import TableInfo, QueryRequest, QueryResponse
from utils.validators import SchemaValidator, sanitize_for_json

router = APIRouter(prefix="/warehouse", tags=["Warehouse"])


@router.get("/tables")
async def list_warehouse_tables(
        db: Session = Depends(get_db)
):
    """
    List all tables in the data warehouse.

    Returns fact and dimension tables with metadata.
    """
    try:
        tables = get_table_names()

        # Filter to star schema tables
        warehouse_tables = []

        for table_name in tables:
            # Skip metadata tables
            if table_name in ['datasets', 'etl_jobs', 'star_schema_metadata',
                              'data_quality_reports', 'dimension_registry',
                              'fact_registry', 'saved_queries', 'alembic_version']:
                continue

            schema = get_table_schema(table_name)

            # Determine table type
            table_type = "dimension" if table_name.startswith("dim_") else (
                "fact" if table_name.startswith("fact_") else "other"
            )

            # Get row count
            try:
                result = execute_raw_sql(f"SELECT COUNT(*) as count FROM {table_name}")
                row_count = result[0]['count'] if result else 0
            except Exception:
                row_count = 0

            warehouse_tables.append({
                "name": table_name,
                "display_name": table_name.replace("dim_", "").replace("fact_", "").replace("_", " ").title(),
                "table_type": table_type,
                "columns": [
                    {
                        "name": col["column_name"],
                        "type": col["data_type"],
                        "nullable": col["is_nullable"] == "YES"
                    }
                    for col in schema
                ],
                "row_count": row_count
            })

        return warehouse_tables

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@router.get("/schema")
async def get_star_schema(
        job_id: Optional[int] = None,
        db: Session = Depends(get_db)
):
    """
    Get the star schema definition.

    - **job_id**: Optional ETL job ID to get specific schema
    """
    query = db.query(StarSchemaMetadata)

    if job_id:
        query = query.filter(StarSchemaMetadata.etl_job_id == job_id)

    schema_meta = query.order_by(StarSchemaMetadata.created_at.desc()).first()

    if not schema_meta:
        raise HTTPException(status_code=404, detail="No star schema found")

    return {
        "id": schema_meta.id,
        "etl_job_id": schema_meta.etl_job_id,
        "fact_table_name": schema_meta.fact_table_name,
        "dimension_tables": schema_meta.dimension_tables,
        "schema_definition": schema_meta.schema_definition,
        "created_at": schema_meta.created_at
    }


@router.get("/table/{table_name}")
async def get_table_data(
        table_name: str,
        limit: int = Query(1000, ge=1, le=10000),
        offset: int = Query(0, ge=0),
        db: Session = Depends(get_db)
):
    """
    Get data from a specific table.

    - **table_name**: Name of the table
    - **limit**: Maximum rows to return
    - **offset**: Row offset for pagination
    """
    if not table_exists(table_name):
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
        data = execute_raw_sql(query)

        # Get total count
        count_result = execute_raw_sql(f"SELECT COUNT(*) as total FROM {table_name}")
        total = count_result[0]['total'] if count_result else 0

        return {
            "table_name": table_name,
            "data": sanitize_for_json(data),
            "row_count": len(data),
            "total_rows": total,
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.post("/query", response_model=QueryResponse)
async def execute_query(
        request: QueryRequest,
        db: Session = Depends(get_db)
):
    """
    Execute a SQL query on the warehouse.

    Only SELECT queries are allowed for security.
    """
    # Validate query
    errors = SchemaValidator.validate_query(request.sql)
    if errors:
        raise HTTPException(status_code=400, detail="; ".join(errors))

    try:
        start_time = time.time()

        # Add LIMIT if not present
        query = request.sql.strip()
        if 'LIMIT' not in query.upper():
            query = f"{query} LIMIT {request.limit}"

        data = execute_raw_sql(query, request.params)
        execution_time = (time.time() - start_time) * 1000

        columns = list(data[0].keys()) if data else []

        return QueryResponse(
            columns=columns,
            data=sanitize_for_json(data),
            row_count=len(data),
            execution_time_ms=round(execution_time, 2)
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


@router.get("/dimensions/{dim_name}/values")
async def get_dimension_values(
        dim_name: str,
        column: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = Query(100, ge=1, le=1000),
        db: Session = Depends(get_db)
):
    """
    Get unique values from a dimension table.

    Useful for populating filter dropdowns.
    """
    table_name = dim_name if dim_name.startswith("dim_") else f"dim_{dim_name}"

    if not table_exists(table_name):
        raise HTTPException(status_code=404, detail="Dimension table not found")

    try:
        schema = get_table_schema(table_name)
        columns = [col["column_name"] for col in schema]

        # Use specified column or find the natural key
        if column and column in columns:
            value_column = column
        else:
            # Find non-key column for values
            value_column = None
            for col in columns:
                if not col.startswith("sk_") and not col.startswith("fk_") and col not in ['created_at', 'updated_at']:
                    value_column = col
                    break

            if not value_column:
                value_column = columns[1] if len(columns) > 1 else columns[0]

        # Build query
        query = f"SELECT DISTINCT {value_column} FROM {table_name}"
        if search:
            query += f" WHERE CAST({value_column} AS TEXT) LIKE '%{search}%'"
        query += f" ORDER BY {value_column} LIMIT {limit}"

        data = execute_raw_sql(query)
        values = [row[value_column] for row in data]

        return {
            "dimension": table_name,
            "column": value_column,
            "values": values,
            "count": len(values)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get values: {str(e)}")


@router.get("/relationships")
async def get_table_relationships(
        db: Session = Depends(get_db)
):
    """
    Get relationships between fact and dimension tables.
    """
    schema_meta = db.query(StarSchemaMetadata).order_by(
        StarSchemaMetadata.created_at.desc()
    ).first()

    if not schema_meta or not schema_meta.schema_definition:
        return {"relationships": []}

    relationships = schema_meta.schema_definition.get('relationships', [])
    return {"relationships": relationships}


@router.get("/stats")
async def get_warehouse_stats(
        db: Session = Depends(get_db)
):
    """
    Get overall warehouse statistics.
    """
    try:
        tables = get_table_names()

        fact_tables = [t for t in tables if t.startswith("fact_")]
        dim_tables = [t for t in tables if t.startswith("dim_")]

        total_rows = 0
        table_stats = []

        for table_name in fact_tables + dim_tables:
            try:
                result = execute_raw_sql(f"SELECT COUNT(*) as count FROM {table_name}")
                count = result[0]['count'] if result else 0
                total_rows += count

                table_stats.append({
                    "name": table_name,
                    "type": "fact" if table_name.startswith("fact_") else "dimension",
                    "row_count": count,
                })
            except Exception:
                pass

        return {
            "fact_table_count": len(fact_tables),
            "dimension_table_count": len(dim_tables),
            "total_rows": total_rows,
            "table_stats": table_stats
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")
