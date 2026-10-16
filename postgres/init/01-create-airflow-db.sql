-- =============================================================================
-- 01-create-airflow-db.sql
-- Create dedicated database for Apache Airflow metadata
-- Executed automatically on initial PostgreSQL container startup
-- =============================================================================

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
