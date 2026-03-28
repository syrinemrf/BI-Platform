"""
Database connection and session management.
"""
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator
import pandas as pd

from config import settings


# Detect database type
is_sqlite = settings.DATABASE_URL.startswith("sqlite")

# Create engine with appropriate settings
if is_sqlite:
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=settings.DEBUG
    )
else:
    engine = create_engine(
        settings.DATABASE_URL,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_pre_ping=True,
        echo=settings.DEBUG
    )

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db() -> Generator:
    """Dependency for getting database sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager for database sessions."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db():
    """Initialize database tables."""
    Base.metadata.create_all(bind=engine)


def execute_raw_sql(sql: str, params: dict = None) -> list:
    """Execute raw SQL and return results."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        if result.returns_rows:
            return [dict(row._mapping) for row in result]
        conn.commit()
        return []


def execute_ddl(ddl: str) -> bool:
    """Execute DDL statements (CREATE, ALTER, DROP)."""
    with engine.connect() as conn:
        for statement in ddl.split(';'):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
        conn.commit()
    return True


def dataframe_to_table(df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> int:
    """Write pandas DataFrame to database table."""
    rows = df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        chunksize=settings.ETL_BATCH_SIZE
    )
    return rows or len(df)


def table_to_dataframe(table_name: str, limit: int = None) -> pd.DataFrame:
    """Read database table into pandas DataFrame."""
    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" LIMIT {limit}"
    return pd.read_sql(query, engine)


def get_table_names() -> list:
    """Get list of all tables in the database."""
    inspector = inspect(engine)
    return inspector.get_table_names()


def get_table_schema(table_name: str) -> list:
    """Get schema information for a table."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return [
        {
            "column_name": col["name"],
            "data_type": str(col["type"]),
            "is_nullable": "YES" if col.get("nullable", True) else "NO",
            "column_default": col.get("default")
        }
        for col in columns
    ]


def table_exists(table_name: str) -> bool:
    """Check if a table exists."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()
