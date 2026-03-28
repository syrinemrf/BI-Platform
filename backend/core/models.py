"""
SQLAlchemy models for metadata storage.
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Float, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

from core.database import Base


class Dataset(Base):
    """Metadata for uploaded datasets."""
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_type = Column(String(50), nullable=False)
    file_size = Column(Integer, nullable=False)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    schema_info = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    etl_jobs = relationship("ETLJob", back_populates="dataset")


class ETLJob(Base):
    """ETL job execution metadata."""
    __tablename__ = "etl_jobs"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    status = Column(String(50), default="pending")  # pending, running, completed, failed
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    quality_report = Column(JSON, nullable=True)
    transformation_log = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    dataset = relationship("Dataset", back_populates="etl_jobs")
    star_schema = relationship("StarSchemaMetadata", back_populates="etl_job", uselist=False)


class StarSchemaMetadata(Base):
    """Metadata for generated star schema."""
    __tablename__ = "star_schema_metadata"

    id = Column(Integer, primary_key=True, index=True)
    etl_job_id = Column(Integer, ForeignKey("etl_jobs.id"), nullable=False)
    fact_table_name = Column(String(255), nullable=False)
    dimension_tables = Column(JSON, nullable=False)  # List of dimension table info
    schema_definition = Column(JSON, nullable=False)  # Full schema structure
    ddl_script = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    etl_job = relationship("ETLJob", back_populates="star_schema")


class DataQualityReport(Base):
    """Data quality check results."""
    __tablename__ = "data_quality_reports"

    id = Column(Integer, primary_key=True, index=True)
    etl_job_id = Column(Integer, ForeignKey("etl_jobs.id"), nullable=False)

    # Overall scores
    overall_score = Column(Float, nullable=False)
    completeness_score = Column(Float, nullable=False)
    uniqueness_score = Column(Float, nullable=False)
    validity_score = Column(Float, nullable=False)
    consistency_score = Column(Float, nullable=False)

    # Detailed reports
    column_reports = Column(JSON, nullable=True)
    issues = Column(JSON, nullable=True)
    recommendations = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=func.now())


class DimensionTable(Base):
    """Registry of dimension tables."""
    __tablename__ = "dimension_registry"

    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(255), nullable=False, unique=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    columns = Column(JSON, nullable=False)
    surrogate_key = Column(String(255), nullable=False)
    natural_key = Column(String(255), nullable=True)
    row_count = Column(Integer, nullable=True)
    is_time_dimension = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class FactTable(Base):
    """Registry of fact tables."""
    __tablename__ = "fact_registry"

    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(255), nullable=False, unique=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    measures = Column(JSON, nullable=False)  # List of measure columns
    dimension_keys = Column(JSON, nullable=False)  # List of FK references
    row_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class SavedQuery(Base):
    """Saved dashboard queries."""
    __tablename__ = "saved_queries"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    query_type = Column(String(50), nullable=False)  # kpi, aggregate, timeseries
    configuration = Column(JSON, nullable=False)  # Query configuration
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
