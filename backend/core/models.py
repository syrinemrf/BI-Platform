"""
SQLAlchemy models for metadata storage.
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Float, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

from core.database import Base


class User(Base):
    """User accounts for saving work."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    projects = relationship("Project", back_populates="owner")


class Project(Base):
    """Saved user projects (datasets + ETL config + dashboard state)."""
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    config = Column(JSON, nullable=True)
    dataset_ids = Column(JSON, nullable=True)  # list of dataset IDs
    dashboard_state = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    owner = relationship("User", back_populates="projects")


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
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # null = guest
    session_id = Column(String(100), nullable=True)  # for guest isolation
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    etl_jobs = relationship("ETLJob", back_populates="dataset")
    owner = relationship("User", backref="datasets", foreign_keys=[user_id])


class ETLJob(Base):
    """ETL job execution metadata."""
    __tablename__ = "etl_jobs"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    session_id = Column(String(100), nullable=True)
    job_name = Column(String(255), nullable=True)
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
