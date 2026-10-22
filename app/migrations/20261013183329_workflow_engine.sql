-- Add migration script here
DROP TABLE IF EXISTS tokens;
DROP TABLE IF EXISTS process_instances;

CREATE TABLE wf_definitions (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_instances (
    id UUID PRIMARY KEY,
    definition_key UUID NOT NULL,
    business_key TEXT NOT NULL,
    state_label TEXT NOT NULL,
    data JSONB NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_wf_instances_definition ON wf_instances (definition_key);

CREATE TABLE wf_tokens (
    instance_id UUID NOT NULL,
    token_id UUID NOT NULL,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, token_id)
);

CREATE TABLE wf_user_tasks (
    task_id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_service_tasks (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_timers (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_message_catches (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_files (
    object_key TEXT PRIMARY KEY,
    data BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wf_history (
    id UUID PRIMARY KEY,
    instance_id UUID NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    node_id TEXT,
    actor_type TEXT NOT NULL,
    data JSONB NOT NULL
);
CREATE INDEX idx_wf_history_instance_ts ON wf_history (instance_id, ts);

CREATE TABLE wf_completed_instances (
    id UUID PRIMARY KEY,
    definition_key UUID NOT NULL,
    business_key TEXT NOT NULL,
    state_filter TEXT NOT NULL,
    completed_at TIMESTAMPTZ,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);