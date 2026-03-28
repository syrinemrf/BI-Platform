"""
Dashboard API routes.
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from core.database import get_db, execute_raw_sql, get_table_names, table_exists
from core.models import StarSchemaMetadata, SavedQuery
from core.schemas import (
    KPIRequest, KPIResponse, AggregateRequest,
    TimeSeriesRequest, TimeSeriesResponse, DashboardData
)
from utils.validators import sanitize_for_json

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/kpis")
async def get_kpis(
        fact_table: Optional[str] = None,
        db: Session = Depends(get_db)
):
    """
    Get key performance indicators from the fact table.

    Returns aggregated KPIs for all measures.
    """
    # Find fact table
    if not fact_table:
        tables = get_table_names()
        fact_tables = [t for t in tables if t.startswith("fact_")]
        if not fact_tables:
            return {"kpis": [], "message": "No fact table found"}
        fact_table = fact_tables[0]

    if not table_exists(fact_table):
        raise HTTPException(status_code=404, detail="Fact table not found")

    try:
        # Get measure columns (numeric, non-key columns)
        from core.database import get_table_schema
        schema = get_table_schema(fact_table)

        measure_cols = []
        for col in schema:
            col_name = col['column_name']
            col_type = col['data_type']

            # Skip keys and metadata
            if col_name.startswith('sk_') or col_name.startswith('fk_'):
                continue
            if col_name in ['created_at', 'updated_at']:
                continue

            # Include numeric columns
            if col_type in ['integer', 'bigint', 'real', 'double precision', 'numeric', 'decimal']:
                measure_cols.append(col_name)

        kpis = []

        for measure in measure_cols:
            # Calculate current value (sum)
            query = f"""
                SELECT
                    SUM({measure}) as total,
                    AVG({measure}) as average,
                    COUNT({measure}) as count,
                    MIN({measure}) as min_val,
                    MAX({measure}) as max_val
                FROM {fact_table}
            """
            result = execute_raw_sql(query)

            if result and result[0]:
                data = result[0]
                kpis.append({
                    "name": measure.replace('_', ' ').title(),
                    "column": measure,
                    "total": float(data['total']) if data['total'] else 0,
                    "average": float(data['average']) if data['average'] else 0,
                    "count": int(data['count']) if data['count'] else 0,
                    "min": float(data['min_val']) if data['min_val'] else 0,
                    "max": float(data['max_val']) if data['max_val'] else 0
                })

        return {"kpis": kpis, "fact_table": fact_table}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KPI calculation failed: {str(e)}")


@router.post("/aggregate")
async def aggregate_data(
        request: AggregateRequest,
        db: Session = Depends(get_db)
):
    """
    Aggregate measures by dimensions.

    Generates GROUP BY queries based on selected dimensions and measures.
    """
    # Find fact table
    tables = get_table_names()
    fact_tables = [t for t in tables if t.startswith("fact_")]
    if not fact_tables:
        raise HTTPException(status_code=404, detail="No fact table found")

    fact_table = fact_tables[0]

    try:
        # Build dimension joins
        joins = []
        select_dims = []
        group_cols = []

        for dim in request.dimensions:
            dim_table = dim if dim.startswith("dim_") else f"dim_{dim}"
            if table_exists(dim_table):
                fk_col = f"fk_{dim_table}"

                # Get a display column from dimension
                from core.database import get_table_schema
                dim_schema = get_table_schema(dim_table)

                display_col = None
                for col in dim_schema:
                    col_name = col['column_name']
                    if not col_name.startswith('sk_') and col_name not in ['created_at', 'updated_at']:
                        display_col = col_name
                        break

                if display_col:
                    joins.append(
                        f"LEFT JOIN {dim_table} ON {fact_table}.{fk_col} = {dim_table}.sk_{dim_table.replace('dim_', '')}"
                    )
                    select_dims.append(f"{dim_table}.{display_col} as {dim}")
                    group_cols.append(f"{dim_table}.{display_col}")

        # Build measure aggregations
        agg_selects = []
        for measure, agg_type in request.aggregations.items():
            if measure in request.measures:
                agg_func = agg_type.upper()
                agg_selects.append(f"{agg_func}({fact_table}.{measure}) as {measure}_{agg_type}")

        # Build query
        select_clause = ", ".join(select_dims + agg_selects)
        join_clause = " ".join(joins)
        group_clause = ", ".join(group_cols)

        query = f"SELECT {select_clause} FROM {fact_table} {join_clause}"
        if group_clause:
            query += f" GROUP BY {group_clause}"

        if request.order_by:
            query += f" ORDER BY {request.order_by} DESC"

        query += f" LIMIT {request.limit}"

        data = execute_raw_sql(query)

        return {
            "data": sanitize_for_json(data),
            "dimensions": request.dimensions,
            "measures": request.measures,
            "row_count": len(data)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {str(e)}")


@router.get("/timeseries")
async def get_timeseries(
        measure: str,
        aggregation: str = "sum",
        granularity: str = "day",
        db: Session = Depends(get_db)
):
    """
    Get time series data for a measure.

    - **measure**: Name of the measure column
    - **aggregation**: Aggregation function (sum, avg, count, min, max)
    - **granularity**: Time granularity (day, week, month, quarter, year)
    """
    # Find fact and time tables
    tables = get_table_names()
    fact_tables = [t for t in tables if t.startswith("fact_")]
    time_tables = [t for t in tables if t == "dim_time"]

    if not fact_tables:
        raise HTTPException(status_code=404, detail="No fact table found")

    fact_table = fact_tables[0]
    time_table = time_tables[0] if time_tables else None

    if not time_table:
        return {"message": "No time dimension found", "data": []}

    try:
        # Map granularity to time column
        granularity_map = {
            "day": "full_date",
            "week": "week",
            "month": "month",
            "quarter": "quarter",
            "year": "year"
        }

        time_col = granularity_map.get(granularity, "full_date")
        agg_func = aggregation.upper()

        # For proper time series, we need year context for week/month
        if granularity in ["week", "month", "quarter"]:
            query = f"""
                SELECT
                    {time_table}.year,
                    {time_table}.{time_col} as period,
                    {agg_func}({fact_table}.{measure}) as value
                FROM {fact_table}
                JOIN {time_table} ON {fact_table}.fk_dim_time = {time_table}.sk_time
                GROUP BY {time_table}.year, {time_table}.{time_col}
                ORDER BY {time_table}.year, {time_table}.{time_col}
            """
        else:
            query = f"""
                SELECT
                    {time_table}.{time_col} as period,
                    {agg_func}({fact_table}.{measure}) as value
                FROM {fact_table}
                JOIN {time_table} ON {fact_table}.fk_dim_time = {time_table}.sk_time
                GROUP BY {time_table}.{time_col}
                ORDER BY {time_table}.{time_col}
            """

        data = execute_raw_sql(query)

        # Format response
        labels = []
        values = []

        for row in data:
            if granularity in ["week", "month", "quarter"]:
                label = f"{row['year']}-{row['period']:02d}" if isinstance(row['period'], int) else str(row['period'])
            else:
                label = str(row['period'])

            labels.append(label)
            values.append(float(row['value']) if row['value'] else 0)

        return TimeSeriesResponse(
            labels=labels,
            values=values,
            measure_name=measure,
            granularity=granularity
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Time series query failed: {str(e)}")


@router.get("/filters")
async def get_filter_options(
        db: Session = Depends(get_db)
):
    """
    Get available filter options from all dimension tables.
    """
    tables = get_table_names()
    dim_tables = [t for t in tables if t.startswith("dim_") and t != "dim_time"]

    filters = []

    for dim_table in dim_tables:
        try:
            from core.database import get_table_schema
            schema = get_table_schema(dim_table)

            # Find display columns
            for col in schema:
                col_name = col['column_name']
                if col_name.startswith('sk_') or col_name in ['created_at', 'updated_at']:
                    continue

                # Get unique values
                query = f"SELECT DISTINCT {col_name} FROM {dim_table} ORDER BY {col_name} LIMIT 100"
                data = execute_raw_sql(query)
                values = [row[col_name] for row in data]

                filters.append({
                    "dimension": dim_table.replace("dim_", ""),
                    "column": col_name,
                    "display_name": col_name.replace("_", " ").title(),
                    "values": sanitize_for_json(values),
                    "count": len(values)
                })

        except Exception:
            continue

    return {"filters": filters}


@router.post("/filter")
async def filter_data(
        filters: Dict[str, Any],
        measures: List[str] = None,
        limit: int = Query(1000, ge=1, le=10000),
        db: Session = Depends(get_db)
):
    """
    Filter fact table data based on dimension values.
    """
    tables = get_table_names()
    fact_tables = [t for t in tables if t.startswith("fact_")]
    if not fact_tables:
        raise HTTPException(status_code=404, detail="No fact table found")

    fact_table = fact_tables[0]

    try:
        # Build WHERE clauses
        joins = []
        conditions = []

        for dim_col, value in filters.items():
            # Parse dimension.column format
            if "." in dim_col:
                dim_name, col_name = dim_col.split(".", 1)
            else:
                dim_name = dim_col
                col_name = dim_col

            dim_table = f"dim_{dim_name}" if not dim_name.startswith("dim_") else dim_name
            fk_col = f"fk_{dim_table}"

            if table_exists(dim_table):
                joins.append(
                    f"JOIN {dim_table} ON {fact_table}.{fk_col} = {dim_table}.sk_{dim_table.replace('dim_', '')}"
                )

                if isinstance(value, list):
                    values_str = ", ".join(f"'{v}'" for v in value)
                    conditions.append(f"{dim_table}.{col_name} IN ({values_str})")
                else:
                    conditions.append(f"{dim_table}.{col_name} = '{value}'")

        # Build query
        query = f"SELECT {fact_table}.* FROM {fact_table}"
        query += " " + " ".join(joins)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" LIMIT {limit}"

        data = execute_raw_sql(query)

        return {
            "data": sanitize_for_json(data),
            "row_count": len(data),
            "filters_applied": filters
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filter query failed: {str(e)}")


@router.get("/summary")
async def get_dashboard_summary(
        db: Session = Depends(get_db)
):
    """
    Get a summary for the dashboard homepage.
    """
    tables = get_table_names()
    fact_tables = [t for t in tables if t.startswith("fact_")]
    dim_tables = [t for t in tables if t.startswith("dim_")]

    summary = {
        "fact_tables": len(fact_tables),
        "dimension_tables": len(dim_tables),
        "kpis": [],
        "recent_data": None
    }

    if fact_tables:
        fact_table = fact_tables[0]

        # Get row count
        try:
            result = execute_raw_sql(f"SELECT COUNT(*) as total FROM {fact_table}")
            summary["total_records"] = result[0]["total"] if result else 0
        except Exception:
            summary["total_records"] = 0

        # Get quick KPIs
        try:
            kpis_response = await get_kpis(fact_table=fact_table, db=db)
            summary["kpis"] = kpis_response.get("kpis", [])[:5]
        except Exception:
            pass

    return summary


@router.post("/saved-query")
async def save_query(
        name: str,
        query_type: str,
        configuration: Dict[str, Any],
        description: Optional[str] = None,
        db: Session = Depends(get_db)
):
    """
    Save a dashboard query configuration for reuse.
    """
    saved = SavedQuery(
        name=name,
        description=description,
        query_type=query_type,
        configuration=configuration
    )
    db.add(saved)
    db.commit()
    db.refresh(saved)

    return {"id": saved.id, "message": "Query saved successfully"}


@router.get("/saved-queries")
async def list_saved_queries(
        db: Session = Depends(get_db)
):
    """
    List all saved dashboard queries.
    """
    queries = db.query(SavedQuery).order_by(SavedQuery.created_at.desc()).all()
    return [
        {
            "id": q.id,
            "name": q.name,
            "description": q.description,
            "query_type": q.query_type,
            "configuration": q.configuration,
            "created_at": q.created_at
        }
        for q in queries
    ]
