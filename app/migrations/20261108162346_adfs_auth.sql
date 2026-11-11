-- Add migration script here
ALTER TABLE users ADD COLUMN external_id TEXT UNIQUE;
ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL;