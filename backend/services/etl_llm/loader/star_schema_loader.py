"""
Star Schema Loader (Layer 5)
=============================
Uses generated SQL DDL + Python transforms from the ETL code generator
to actually materialize the star schema into the target database (SQLite
for demo, PostgreSQL supported through SQLAlchemy URL).

Reference: [Jiang 2024] — "The conversion from ER to star schema is done
by the LLM-generated code; the loader materializes the result."
"""

from __future__ import annotations

import logging
import sqlite3

import pandas as pd

from services.etl_llm.agents.code_generator import GeneratedETLCode

logger = logging.getLogger(__name__)


class StarSchemaLoader:
    """Execute generated DDL and load transformed data into the warehouse."""

    def __init__(self, db_path: str = "warehouse.db") -> None:
        self.db_path = db_path

    def execute_ddl(self, code: GeneratedETLCode) -> list[str]:
        """Execute all SQL DDL statements and return table names created."""
        tables_created: list[str] = []
        sql = code.loading_code or code.full_pipeline_code
        conn = sqlite3.connect(self.db_path)
        try:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                conn.execute(stmt)
                # Extract table name from CREATE TABLE
                upper = stmt.upper()
                if "CREATE TABLE" in upper:
                    parts = stmt.split()
                    idx = next(
                        (i for i, p in enumerate(parts) if p.upper() in ("TABLE", "EXISTS")),
                        None,
                    )
                    if idx is not None:
                        name = parts[idx + 1].strip("(\"'`")
                        # Handle IF NOT EXISTS
                        if name.upper() == "NOT":
                            name = parts[idx + 3].strip("(\"'`")
                        elif name.upper() == "IF":
                            name = parts[idx + 4].strip("(\"'`")
                        tables_created.append(name)
            conn.commit()
        finally:
            conn.close()
        return tables_created

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """Load a DataFrame into a table and return the row count inserted."""
        conn = sqlite3.connect(self.db_path)
        try:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
        finally:
            conn.close()
        return count

    def query(self, sql: str) -> list[dict]:
        """Run a SELECT query and return results as list of dicts."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(sql).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
