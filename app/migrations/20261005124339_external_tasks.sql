-- Add migration script here
CREATE TABLE external_tasks (
    task_id UUID PRIMARY KEY,
    token_id VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255) NOT NULL REFERENCES process_instances(id) ON DELETE CASCADE,
    task_type VARCHAR(255) NOT NULL,
    state VARCHAR(16) NOT NULL,
    lock_owner VARCHAR(255),
    lock_expire_at TIMESTAMPTZ,
    retries INT NOT NULL,
    timeout_secs BIGINT NOT NULL,
    error_message TEXT,
    variables JSONB NOT NULL DEFAULT '{}',
    -- worker output only; merged into instance vars on advance
    result JSONB NOT NULL DEFAULT '{}',
    due_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    advanced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_external_tasks_ready ON external_tasks (task_type, due_at) WHERE state = 'READY';
CREATE INDEX idx_external_tasks_instance ON external_tasks (instance_id);
CREATE INDEX idx_external_tasks_unadvanced ON external_tasks (updated_at)
    WHERE state = 'COMPLETED' AND advanced_at IS NULL;