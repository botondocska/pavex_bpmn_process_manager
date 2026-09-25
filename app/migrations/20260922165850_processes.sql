-- Add migration script here
CREATE TABLE processes (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    bpmn_xml TEXT NOT NULL,
    created_by UUID NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);