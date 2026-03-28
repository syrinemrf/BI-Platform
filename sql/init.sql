-- ============================================
-- BI Platform - Database Initialization
-- ============================================

-- Create database extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE bi_warehouse TO postgres;

-- Create initial schema for metadata tables
-- (These will be created by SQLAlchemy, but we ensure the database is ready)

-- Performance settings for analytics
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';
ALTER SYSTEM SET work_mem = '64MB';

-- Analytics-optimized settings
ALTER SYSTEM SET default_statistics_target = 100;
ALTER SYSTEM SET random_page_cost = 1.1;

-- Logging
ALTER SYSTEM SET log_min_duration_statement = 1000;

SELECT 'Database initialized successfully' as status;
