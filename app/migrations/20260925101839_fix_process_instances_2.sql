-- Add migration script here
 
-- process_instances.process_def_id stays named to match the bpmn engine's
-- ProcessInstance.process_def_id field (bpm-engine-core), even though it's
-- a VARCHAR holding a stringified UUID. This generated column casts it
-- back to UUID so Postgres can enforce a real FK against processes.id --
-- orphaned instances (referencing a non-existent process) are rejected at
-- the DB level, not just trusted from app code.
ALTER TABLE process_instances
    ADD COLUMN process_uuid UUID GENERATED ALWAYS AS (process_def_id::uuid) STORED
    REFERENCES processes(id);
 
CREATE INDEX idx_process_instances_process_uuid ON process_instances (process_uuid);