-- Tenants table
CREATE TABLE IF NOT EXISTS tenants (
    id          VARCHAR(100) PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert a default tenant for testing
INSERT INTO tenants (id, name) VALUES ('tenant_123', 'Default Tenant')
ON CONFLICT (id) DO NOTHING;

-- Pipelines table
-- The 'config' column stores the full pipeline JSON definition
CREATE TABLE IF NOT EXISTS pipelines (
    id              VARCHAR(100) PRIMARY KEY,
    pipeline_name   VARCHAR(255) NOT NULL,
    tenant_id       VARCHAR(100) NOT NULL,
    description     TEXT,
    config          JSONB        NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pipeline_tenant FOREIGN KEY (tenant_id)
        REFERENCES tenants(id)
);

-- Job runs table - tracks every execution of a pipeline
CREATE TABLE IF NOT EXISTS job_runs (
    id                  VARCHAR(100) PRIMARY KEY,
    pipeline_id         VARCHAR(100) NOT NULL,
    tenant_id           VARCHAR(100) NOT NULL,
    status              VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    BIGINT,
    records_read        BIGINT DEFAULT 0,
    records_written     BIGINT DEFAULT 0,
    records_filtered    BIGINT DEFAULT 0,
    records_failed      BIGINT DEFAULT 0,
    error_message       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_jobrun_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES pipelines(id)
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_pipelines_tenant_id ON pipelines(tenant_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_pipeline_id ON job_runs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_tenant_id ON job_runs(tenant_id);