-- Add migration script here
ALTER TABLE process_instances
    DROP CONSTRAINT process_instances_process_uuid_fkey,
    ADD CONSTRAINT process_instances_process_uuid_fkey
        FOREIGN KEY (process_uuid) REFERENCES processes(id) ON DELETE CASCADE;