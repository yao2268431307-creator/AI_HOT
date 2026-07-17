-- Local Docker bootstrap only. Production identities must be provisioned by
-- the DBA/secret manager before 001_init.sql is applied.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'radar_app') THEN
    CREATE ROLE radar_app LOGIN PASSWORD 'radar-app-local-only'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'radar_deletion_worker') THEN
    CREATE ROLE radar_deletion_worker LOGIN PASSWORD 'radar-deletion-local-only'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
