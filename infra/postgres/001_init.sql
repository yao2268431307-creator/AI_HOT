CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS source_entities (
  id text PRIMARY KEY,
  entity_type text NOT NULL CHECK (entity_type IN ('person','organization','repository','publication','channel')),
  canonical_name text NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('candidate','active','blocked','deleted')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_accounts (
  id text PRIMARY KEY,
  platform text NOT NULL,
  external_id text NOT NULL,
  display_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (platform, external_id)
);

CREATE TABLE IF NOT EXISTS ownership_edges (
  account_id text NOT NULL REFERENCES source_accounts(id) ON DELETE CASCADE,
  entity_id text NOT NULL REFERENCES source_entities(id) ON DELETE CASCADE,
  confidence numeric(5,4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  review_state text NOT NULL DEFAULT 'candidate' CHECK (review_state IN ('candidate','confirmed','rejected')),
  evidence_ref text,
  valid_from timestamptz NOT NULL DEFAULT now(),
  valid_to timestamptz,
  reviewed_by text,
  PRIMARY KEY (account_id, entity_id, valid_from)
);

CREATE TABLE IF NOT EXISTS sources (
  id text PRIMARY KEY,
  platform text NOT NULL,
  display_name text NOT NULL,
  status text NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate','active','paused','blocked')),
  language text NOT NULL DEFAULT 'other',
  lead_score numeric(5,2) NOT NULL DEFAULT 0,
  hit_rate numeric(5,2) NOT NULL DEFAULT 0,
  originality numeric(5,2) NOT NULL DEFAULT 0,
  domain_focus numeric(5,2) NOT NULL DEFAULT 0,
  valid_observations integer NOT NULL DEFAULT 0,
  discovered_reason text,
  activated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observations (
  id text PRIMARY KEY,
  schema_version integer NOT NULL DEFAULT 1,
  connector text NOT NULL DEFAULT 'unknown',
  platform text NOT NULL,
  external_id text NOT NULL,
  source_id text NOT NULL,
  account_id text,
  entity_id text,
  published_at timestamptz NOT NULL,
  collected_at timestamptz NOT NULL,
  language text NOT NULL,
  title text,
  body text NOT NULL,
  url text NOT NULL,
  normalized_url text NOT NULL,
  content_fingerprint text NOT NULL,
  metrics jsonb NOT NULL DEFAULT '{}',
  raw_evidence_ref text NOT NULL,
  parser_version text NOT NULL DEFAULT 'parser-1',
  rights_policy_id text NOT NULL DEFAULT 'metadata-and-excerpt',
  deletion_state text NOT NULL DEFAULT 'active' CHECK (deletion_state IN ('active','tombstoned','deleted')),
  signal_family text NOT NULL CHECK (signal_family IN ('discussion','behavior','official','research')),
  embedding vector(1024),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (platform, external_id, collected_at)
);
CREATE INDEX IF NOT EXISTS observations_collected_idx ON observations (collected_at DESC);
CREATE INDEX IF NOT EXISTS observations_source_idx ON observations (source_id, published_at DESC);
CREATE INDEX IF NOT EXISTS observations_entity_idx ON observations (entity_id, published_at DESC);
CREATE INDEX IF NOT EXISTS observations_fingerprint_idx ON observations (content_fingerprint);
CREATE INDEX IF NOT EXISTS observations_metrics_idx ON observations USING gin (metrics);

CREATE TABLE IF NOT EXISTS observation_processing (
  observation_id text PRIMARY KEY REFERENCES observations(id) ON DELETE CASCADE,
  revision integer NOT NULL DEFAULT 1,
  processed_revision integer NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  lease_until timestamptz,
  last_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (revision >= processed_revision)
);
CREATE INDEX IF NOT EXISTS observation_processing_pending_idx ON observation_processing (updated_at)
  WHERE processed_revision < revision;

CREATE TABLE IF NOT EXISTS events (
  id text PRIMARY KEY,
  canonical_title_zh text NOT NULL,
  canonical_title_en text NOT NULL,
  event_type text NOT NULL,
  lifecycle_state text NOT NULL,
  structure_labels text[] NOT NULL DEFAULT '{}',
  first_seen_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  current_score jsonb NOT NULL DEFAULT '{}',
  version integer NOT NULL DEFAULT 1,
  cluster_version integer NOT NULL DEFAULT 1,
  parent_cluster_id text,
  superseded_by text[] NOT NULL DEFAULT '{}',
  merge_operation_id uuid,
  split_operation_id uuid,
  effective_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE events ADD COLUMN IF NOT EXISTS cluster_version integer NOT NULL DEFAULT 1;
ALTER TABLE events ADD COLUMN IF NOT EXISTS parent_cluster_id text;
ALTER TABLE events ADD COLUMN IF NOT EXISTS superseded_by text[] NOT NULL DEFAULT '{}';
ALTER TABLE events ADD COLUMN IF NOT EXISTS merge_operation_id uuid;
ALTER TABLE events ADD COLUMN IF NOT EXISTS split_operation_id uuid;
ALTER TABLE events ADD COLUMN IF NOT EXISTS effective_at timestamptz;

CREATE TABLE IF NOT EXISTS event_observations (
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  observation_id text NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
  cluster_score numeric(7,6) NOT NULL,
  assignment_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, observation_id),
  UNIQUE (observation_id)
);

CREATE TABLE IF NOT EXISTS score_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  cycle_id text NOT NULL,
  score_version text NOT NULL,
  threshold_version text NOT NULL,
  input_from timestamptz NOT NULL,
  input_to timestamptz NOT NULL,
  input_digest text NOT NULL,
  drivers jsonb NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS score_runs_event_idx ON score_runs (event_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS score_runs_event_cycle_uidx ON score_runs (event_id, cycle_id);
CREATE UNIQUE INDEX IF NOT EXISTS score_runs_event_digest_uidx ON score_runs (event_id, input_digest);

CREATE TABLE IF NOT EXISTS event_metric_snapshots (
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  captured_at timestamptz NOT NULL,
  attention numeric(5,2) NOT NULL,
  behavior numeric(5,2) NOT NULL,
  diversity numeric(5,2) NOT NULL,
  authority numeric(5,2) NOT NULL,
  coordination_risk numeric(5,2) NOT NULL,
  coverage numeric(5,2) NOT NULL,
  evidence_strength numeric(5,2) NOT NULL,
  score_run_id uuid REFERENCES score_runs(id),
  PRIMARY KEY (event_id, captured_at)
) PARTITION BY RANGE (captured_at);
CREATE TABLE IF NOT EXISTS event_metric_snapshots_default PARTITION OF event_metric_snapshots DEFAULT;

CREATE TABLE IF NOT EXISTS metric_snapshots (
  id text NOT NULL,
  subject_type text NOT NULL CHECK (subject_type IN ('content','source','repository','model','event')),
  subject_id text NOT NULL,
  metric_name text NOT NULL,
  value double precision NOT NULL,
  effective_at timestamptz NOT NULL,
  collected_at timestamptz NOT NULL,
  is_estimated boolean NOT NULL DEFAULT false,
  source_revision text,
  connector text NOT NULL,
  PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);
CREATE TABLE IF NOT EXISTS metric_snapshots_default PARTITION OF metric_snapshots DEFAULT;
CREATE INDEX IF NOT EXISTS metric_snapshots_subject_idx ON metric_snapshots (subject_id, metric_name, collected_at DESC);

CREATE TABLE IF NOT EXISTS feedback (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  actor_id text NOT NULL,
  action text NOT NULL,
  reason text NOT NULL,
  target_event_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kind text NOT NULL,
  aggregate_id text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0,
  last_error text
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS outbox_score_cycle_uidx ON outbox (kind, aggregate_id, (payload->>'cycleId')) WHERE kind='score.created';

CREATE TABLE IF NOT EXISTS connector_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  connector_id text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  status text NOT NULL,
  inserted_count integer NOT NULL DEFAULT 0,
  duplicate_count integer NOT NULL DEFAULT 0,
  latency_ms integer,
  estimated_cost_rmb numeric(12,4) NOT NULL DEFAULT 0 CHECK (estimated_cost_rmb >= 0),
  coverage numeric(5,2),
  error text
);

CREATE TABLE IF NOT EXISTS connector_status (
  id text PRIMARY KEY,
  payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS connector_checkpoints (
  connector_id text PRIMARY KEY,
  payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS raw_evidence_deletions (
  reference text PRIMARY KEY,
  rights_policy_id text NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed')),
  attempts integer NOT NULL DEFAULT 0,
  queued_at timestamptz NOT NULL DEFAULT now(),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz,
  deleted_at timestamptz,
  last_error text
);
ALTER TABLE raw_evidence_deletions ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE raw_evidence_deletions ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE connector_runs ADD COLUMN IF NOT EXISTS estimated_cost_rmb numeric(12,4) NOT NULL DEFAULT 0 CHECK (estimated_cost_rmb >= 0);

CREATE TABLE IF NOT EXISTS alert_rules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  actor_id text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rule_id uuid NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
  workspace_id text NOT NULL,
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  domain text NOT NULL,
  lifecycle_state text NOT NULL,
  evidence_strength text NOT NULL CHECK (evidence_strength IN ('low','medium','high')),
  evidence_count integer NOT NULL,
  channel text NOT NULL CHECK (channel IN ('in_app','webhook')),
  idempotency_key text NOT NULL DEFAULT gen_random_uuid()::text UNIQUE,
  status text NOT NULL DEFAULT 'delivered' CHECK (status IN ('reserved','delivered')),
  delivered_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS idempotency_key text NOT NULL DEFAULT gen_random_uuid()::text;
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'delivered';
CREATE UNIQUE INDEX IF NOT EXISTS alert_deliveries_idempotency_uidx ON alert_deliveries (idempotency_key);

CREATE TABLE IF NOT EXISTS watchlists (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  actor_id text NOT NULL,
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  note text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product_interactions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  actor_id text NOT NULL,
  event_id text REFERENCES events(id) ON DELETE SET NULL,
  session_id text NOT NULL,
  idempotency_key text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('detail_opened','evidence_opened','triage_submitted','watch_toggled','alert_acknowledged')),
  metadata jsonb NOT NULL DEFAULT '{}',
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE product_interactions ADD COLUMN IF NOT EXISTS idempotency_key text;
UPDATE product_interactions SET idempotency_key=id::text WHERE idempotency_key IS NULL;
ALTER TABLE product_interactions ALTER COLUMN idempotency_key SET NOT NULL;

CREATE TABLE IF NOT EXISTS cluster_edit_requests (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  actor_id text NOT NULL,
  operation text NOT NULL CHECK (operation IN ('merge','split')),
  target_event_id text REFERENCES events(id) ON DELETE SET NULL,
  observation_ids text[] NOT NULL DEFAULT '{}',
  reason text NOT NULL,
  status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','completed','failed','reverted')),
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  error text
);
ALTER TABLE cluster_edit_requests ADD COLUMN IF NOT EXISTS expected_versions jsonb NOT NULL DEFAULT '{}';
ALTER TABLE cluster_edit_requests ADD COLUMN IF NOT EXISTS result_event_ids text[] NOT NULL DEFAULT '{}';
ALTER TABLE cluster_edit_requests ADD COLUMN IF NOT EXISTS result_versions jsonb NOT NULL DEFAULT '{}';
ALTER TABLE cluster_edit_requests ADD COLUMN IF NOT EXISTS reverse_payload jsonb;
ALTER TABLE cluster_edit_requests ADD COLUMN IF NOT EXISTS reverted_at timestamptz;

CREATE TABLE IF NOT EXISTS event_lineage_edges (
  operation_id uuid NOT NULL REFERENCES cluster_edit_requests(id) ON DELETE RESTRICT,
  parent_event_id text NOT NULL REFERENCES events(id) ON DELETE RESTRICT,
  child_event_id text NOT NULL REFERENCES events(id) ON DELETE RESTRICT,
  effective_at timestamptz NOT NULL,
  reverted_at timestamptz,
  PRIMARY KEY (operation_id,parent_event_id,child_event_id)
);
CREATE INDEX IF NOT EXISTS event_lineage_parent_idx ON event_lineage_edges (parent_event_id,effective_at DESC);
CREATE INDEX IF NOT EXISTS event_lineage_child_idx ON event_lineage_edges (child_event_id,effective_at DESC);

CREATE INDEX IF NOT EXISTS feedback_workspace_idx ON feedback (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS alert_rules_workspace_idx ON alert_rules (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS alert_deliveries_budget_idx ON alert_deliveries (workspace_id, delivered_at DESC);
CREATE INDEX IF NOT EXISTS alert_deliveries_event_idx ON alert_deliveries (workspace_id, event_id, delivered_at DESC);
CREATE INDEX IF NOT EXISTS watchlists_workspace_idx ON watchlists (workspace_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS watchlists_workspace_event_uidx ON watchlists (workspace_id, event_id);
CREATE INDEX IF NOT EXISTS product_interactions_workspace_idx ON product_interactions (workspace_id,occurred_at DESC);
CREATE INDEX IF NOT EXISTS product_interactions_event_idx ON product_interactions (workspace_id,event_id,occurred_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS product_interactions_idempotency_uidx ON product_interactions (workspace_id,idempotency_key);
CREATE INDEX IF NOT EXISTS cluster_edit_requests_event_idx ON cluster_edit_requests (event_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cluster_edit_requests_workspace_idx ON cluster_edit_requests (workspace_id, created_at DESC);

ALTER TABLE feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedback FORCE ROW LEVEL SECURITY;
ALTER TABLE alert_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert_rules FORCE ROW LEVEL SECURITY;
ALTER TABLE alert_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert_deliveries FORCE ROW LEVEL SECURITY;
ALTER TABLE watchlists ENABLE ROW LEVEL SECURITY;
ALTER TABLE watchlists FORCE ROW LEVEL SECURITY;
ALTER TABLE product_interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE product_interactions FORCE ROW LEVEL SECURITY;
ALTER TABLE cluster_edit_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE cluster_edit_requests FORCE ROW LEVEL SECURITY;

CREATE POLICY feedback_workspace_isolation ON feedback
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
CREATE POLICY alert_rules_workspace_isolation ON alert_rules
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
CREATE POLICY alert_deliveries_workspace_isolation ON alert_deliveries
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
CREATE POLICY watchlists_workspace_isolation ON watchlists
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
CREATE POLICY product_interactions_workspace_isolation ON product_interactions
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
CREATE POLICY cluster_edit_requests_workspace_isolation ON cluster_edit_requests
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));

-- The bootstrap POSTGRES_USER owns migrations and is intentionally not used by
-- the application. Superusers bypass RLS even when policies are otherwise
-- correct, so local/runtime traffic uses a constrained role.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'radar_app') THEN
    CREATE ROLE radar_app LOGIN PASSWORD 'radar-app-local-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
GRANT CONNECT ON DATABASE ai_hot TO radar_app;
GRANT USAGE ON SCHEMA public TO radar_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO radar_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO radar_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO radar_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO radar_app;
