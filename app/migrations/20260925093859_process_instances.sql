-- Add migration script here
CREATE TABLE process_instances (
    id VARCHAR(255) PRIMARY KEY,
    process_def_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255),
    -- ProcessInstance.variables: HashMap<String, String>
    variables JSONB NOT NULL DEFAULT '{}',
    -- InstanceState: Running | Completed | Terminated
    state VARCHAR(32) NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tokens (
    -- Token.id is the engine's own id; scoped uniqueness per instance.
    id VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255) NOT NULL REFERENCES process_instances(id) ON DELETE CASCADE,
    node_id VARCHAR(255) NOT NULL,
    -- TokenStatus: Created | Ready | Executing | Waiting | Suspended | Completed | Terminated
    status VARCHAR(32) NOT NULL,
    -- TokenMode: Forward | Compensation
    mode VARCHAR(32) NOT NULL,
    version INT NOT NULL,
    attempt INT NOT NULL,
    parallel_group_id VARCHAR(255),
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (instance_id, id)
);

CREATE INDEX idx_tokens_instance_id ON tokens (instance_id);
CREATE INDEX idx_process_instances_state ON process_instances (state);