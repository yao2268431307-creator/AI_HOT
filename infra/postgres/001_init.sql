CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_attestations (
  key text PRIMARY KEY,
  value text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

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
  authority numeric(5,2) NOT NULL DEFAULT 0,
  marketing_matrix_overlap numeric(5,2) NOT NULL DEFAULT 0,
  valid_observations integer NOT NULL DEFAULT 0,
  early_hits integer NOT NULL DEFAULT 0,
  confirmed_hits integer NOT NULL DEFAULT 0,
  quality_calibrated boolean NOT NULL DEFAULT false,
  discovered_reason text,
  discovery_reasons text[] NOT NULL DEFAULT '{}',
  first_observed_at timestamptz NOT NULL DEFAULT now(),
  last_observed_at timestamptz NOT NULL DEFAULT now(),
  score_version text,
  activated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE sources ADD COLUMN IF NOT EXISTS authority numeric(5,2) NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS marketing_matrix_overlap numeric(5,2) NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS early_hits integer NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS confirmed_hits integer NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS quality_calibrated boolean NOT NULL DEFAULT false;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS discovery_reasons text[] NOT NULL DEFAULT '{}';
ALTER TABLE sources ADD COLUMN IF NOT EXISTS first_observed_at timestamptz;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS last_observed_at timestamptz;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS score_version text;
UPDATE sources SET
  first_observed_at=COALESCE(first_observed_at,created_at),
  last_observed_at=COALESCE(last_observed_at,updated_at,created_at),
  discovery_reasons=ARRAY(
    SELECT DISTINCT reason FROM unnest(
      discovery_reasons || CASE
        WHEN discovered_reason IS NULL OR btrim(discovered_reason)='' THEN '{}'::text[]
        ELSE ARRAY[discovered_reason]
      END
    ) reason
  );
ALTER TABLE sources ALTER COLUMN first_observed_at SET DEFAULT now();
ALTER TABLE sources ALTER COLUMN first_observed_at SET NOT NULL;
ALTER TABLE sources ALTER COLUMN last_observed_at SET DEFAULT now();
ALTER TABLE sources ALTER COLUMN last_observed_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS source_promotion_facts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id text NOT NULL,
  from_status text NOT NULL,
  to_status text NOT NULL,
  score numeric(5,2) NOT NULL,
  policy_version text NOT NULL,
  policy_digest text NOT NULL DEFAULT 'legacy:unavailable',
  promoted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE source_promotion_facts ADD COLUMN IF NOT EXISTS policy_digest text NOT NULL DEFAULT 'legacy:unavailable';
CREATE INDEX IF NOT EXISTS source_promotion_facts_time_idx
  ON source_promotion_facts (promoted_at DESC,source_id);

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
  available_at timestamptz NOT NULL,
  availability_basis text NOT NULL DEFAULT 'first_detected' CHECK (availability_basis IN ('provider_timestamp','first_detected')),
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
  provenance_level text NOT NULL DEFAULT 'provider_verified'
    CHECK (provenance_level IN ('self_authenticating','provider_verified','unverified_discovery')),
  deletion_state text NOT NULL DEFAULT 'active' CHECK (deletion_state IN ('active','tombstoned','deleted')),
  signal_family text NOT NULL CHECK (signal_family IN ('discussion','behavior','official','research')),
  embedding vector(1024),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (platform, external_id, collected_at)
);
ALTER TABLE observations ADD COLUMN IF NOT EXISTS available_at timestamptz;
UPDATE observations SET available_at=collected_at WHERE available_at IS NULL;
ALTER TABLE observations ALTER COLUMN available_at SET NOT NULL;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS availability_basis text NOT NULL DEFAULT 'first_detected';
ALTER TABLE observations DROP CONSTRAINT IF EXISTS observations_availability_basis_check;
ALTER TABLE observations ADD CONSTRAINT observations_availability_basis_check CHECK (availability_basis IN ('provider_timestamp','first_detected'));
ALTER TABLE observations ADD COLUMN IF NOT EXISTS provenance_level text NOT NULL DEFAULT 'provider_verified';
ALTER TABLE observations DROP CONSTRAINT IF EXISTS observations_provenance_level_check;
ALTER TABLE observations ADD CONSTRAINT observations_provenance_level_check
  CHECK (provenance_level IN ('self_authenticating','provider_verified','unverified_discovery'));
CREATE INDEX IF NOT EXISTS observations_collected_idx ON observations (collected_at DESC);
CREATE INDEX IF NOT EXISTS observations_source_idx ON observations (source_id, published_at DESC);
CREATE INDEX IF NOT EXISTS observations_entity_idx ON observations (entity_id, published_at DESC);
CREATE INDEX IF NOT EXISTS observations_fingerprint_idx ON observations (content_fingerprint);
CREATE INDEX IF NOT EXISTS observations_source_fingerprint_idx ON observations (source_id,content_fingerprint);
CREATE INDEX IF NOT EXISTS observations_metrics_idx ON observations USING gin (metrics);

-- Minimal immutable ingest ledger: no body/title/URL, only the facts required
-- to prove persisted duplicate leakage cannot be improved by later source purge.
CREATE TABLE IF NOT EXISTS content_ingest_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  observation_id text NOT NULL,
  connector_id text NOT NULL,
  content_fingerprint text NOT NULL,
  persisted_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE content_ingest_history ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid();
ALTER TABLE content_ingest_history ALTER COLUMN id SET NOT NULL;
ALTER TABLE content_ingest_history DROP CONSTRAINT IF EXISTS content_ingest_history_pkey;
ALTER TABLE content_ingest_history ADD CONSTRAINT content_ingest_history_pkey PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS content_ingest_history_time_idx ON content_ingest_history (persisted_at DESC);
CREATE INDEX IF NOT EXISTS content_ingest_history_fingerprint_idx ON content_ingest_history (connector_id,content_fingerprint);

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

CREATE TABLE IF NOT EXISTS observation_processing_history (
  observation_id text NOT NULL,
  revision integer NOT NULL,
  collected_at timestamptz NOT NULL,
  enqueued_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  attempts integer NOT NULL DEFAULT 0,
  last_failed_at timestamptz,
  last_error text,
  PRIMARY KEY (observation_id,revision)
);
ALTER TABLE observation_processing_history DROP CONSTRAINT IF EXISTS observation_processing_history_observation_id_fkey;
CREATE INDEX IF NOT EXISTS observation_processing_history_time_idx
  ON observation_processing_history (enqueued_at DESC);
CREATE INDEX IF NOT EXISTS observation_processing_history_pending_idx
  ON observation_processing_history (enqueued_at) WHERE completed_at IS NULL;

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
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS scoring_revision integer NOT NULL DEFAULT 1;
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS baseline_version text NOT NULL DEFAULT 'baseline-empty';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS baseline_digest text NOT NULL DEFAULT 'sha256:empty';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS feature_registry_version text NOT NULL DEFAULT 'unknown';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS feature_registry_digest text NOT NULL DEFAULT 'sha256:unknown';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS evidence_policy_version text NOT NULL DEFAULT 'unknown';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS label_policy_version text NOT NULL DEFAULT 'unknown';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS cluster_version integer NOT NULL DEFAULT 1;
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS identity_version text NOT NULL DEFAULT 'identity-account-fallback-v1';
ALTER TABLE score_runs ADD COLUMN IF NOT EXISTS input_observation_ids jsonb NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS score_runs_event_idx ON score_runs (event_id, created_at DESC);
DROP INDEX IF EXISTS score_runs_event_cycle_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS score_runs_event_cycle_revision_uidx ON score_runs (event_id, cycle_id, scoring_revision);
CREATE UNIQUE INDEX IF NOT EXISTS score_runs_event_digest_uidx ON score_runs (event_id, input_digest);

CREATE TABLE IF NOT EXISTS baseline_samples (
  fact_key text PRIMARY KEY,
  source_event_id text NOT NULL,
  event_type text NOT NULL,
  baseline_key text NOT NULL,
  observed_at timestamptz NOT NULL,
  value numeric NOT NULL CHECK (value >= 0),
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE baseline_samples ADD COLUMN IF NOT EXISTS source_event_id text NOT NULL DEFAULT 'legacy';
CREATE INDEX IF NOT EXISTS baseline_samples_lookup_idx ON baseline_samples (event_type, baseline_key, observed_at DESC);

CREATE TABLE IF NOT EXISTS score_history_erasure_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id text NOT NULL,
  event_ids text[] NOT NULL,
  reason text NOT NULL,
  erased_score_runs integer NOT NULL CHECK (erased_score_runs >= 0),
  erased_baseline_samples integer NOT NULL CHECK (erased_baseline_samples >= 0),
  actor text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE score_history_erasure_audit ADD COLUMN IF NOT EXISTS source_id text;
UPDATE score_history_erasure_audit SET source_id='legacy' WHERE source_id IS NULL;
ALTER TABLE score_history_erasure_audit ALTER COLUMN source_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_component_heartbeats (
  component_id text PRIMARY KEY,
  instance_id text NOT NULL,
  last_seen_at timestamptz NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS disaster_recovery_attestations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  performed_at timestamptz NOT NULL,
  backup_reference text NOT NULL,
  restored_instance_id text NOT NULL,
  verification_digest text NOT NULL,
  measured_rpo_seconds integer NOT NULL CHECK (measured_rpo_seconds >= 0),
  measured_rto_seconds integer NOT NULL CHECK (measured_rto_seconds >= 0),
  status text NOT NULL CHECK (status IN ('passed','failed')),
  operator_subject text NOT NULL,
  notes text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

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

CREATE TABLE IF NOT EXISTS event_metric_daily_rollups (
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  day date NOT NULL,
  samples integer NOT NULL,
  attention_avg numeric(5,2) NOT NULL,
  attention_max numeric(5,2) NOT NULL,
  behavior_avg numeric(5,2) NOT NULL,
  behavior_max numeric(5,2) NOT NULL,
  coverage_min numeric(5,2) NOT NULL,
  evidence_strength_max numeric(5,2) NOT NULL,
  PRIMARY KEY (event_id,day)
);

CREATE TABLE IF NOT EXISTS event_embeddings (
  event_id text NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  model_version text NOT NULL,
  dimensions integer NOT NULL CHECK (dimensions = 1024),
  title_hash text NOT NULL,
  embedding vector(1024) NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id,model_version)
);
CREATE INDEX IF NOT EXISTS event_embeddings_hnsw_idx
  ON event_embeddings USING hnsw (embedding vector_cosine_ops);

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
  event_id text NOT NULL,
  actor_id text NOT NULL,
  action text NOT NULL,
  reason text NOT NULL,
  target_event_id text,
  queue_eligibility_key text,
  alert_delivery_key text,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE feedback DROP CONSTRAINT IF EXISTS feedback_event_id_fkey;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS queue_eligibility_key text;
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS alert_delivery_key text;

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
DROP INDEX IF EXISTS outbox_score_cycle_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS outbox_score_revision_uidx
  ON outbox (kind, aggregate_id, (payload->>'inputDigest')) WHERE kind='score.created';

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

CREATE TABLE IF NOT EXISTS connector_budget_reservations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_token uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  month_start date NOT NULL,
  connector_id text NOT NULL,
  signal_family text NOT NULL,
  reserved_amount_rmb numeric(12,4) NOT NULL CHECK (reserved_amount_rmb >= 0),
  actual_amount_rmb numeric(12,4) CHECK (actual_amount_rmb >= 0),
  status text NOT NULL DEFAULT 'reserved' CHECK (status IN ('reserved','confirmed','reconciliation_required','reconciled_charged','released')),
  created_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz NOT NULL DEFAULT (now()+interval '1 hour'),
  confirmed_at timestamptz,
  reconciled_at timestamptz,
  reconciliation_reason text
);
ALTER TABLE connector_budget_reservations ADD COLUMN IF NOT EXISTS owner_token uuid DEFAULT gen_random_uuid();
UPDATE connector_budget_reservations SET owner_token=gen_random_uuid() WHERE owner_token IS NULL;
ALTER TABLE connector_budget_reservations ALTER COLUMN owner_token SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS connector_budget_reservations_owner_token_uidx
  ON connector_budget_reservations (owner_token);
ALTER TABLE connector_budget_reservations ADD COLUMN IF NOT EXISTS lease_until timestamptz;
UPDATE connector_budget_reservations SET lease_until=created_at+interval '1 hour' WHERE lease_until IS NULL;
ALTER TABLE connector_budget_reservations ALTER COLUMN lease_until SET DEFAULT (now()+interval '1 hour');
ALTER TABLE connector_budget_reservations ALTER COLUMN lease_until SET NOT NULL;
ALTER TABLE connector_budget_reservations ADD COLUMN IF NOT EXISTS reconciled_at timestamptz;
ALTER TABLE connector_budget_reservations ADD COLUMN IF NOT EXISTS reconciliation_reason text;
ALTER TABLE connector_budget_reservations DROP CONSTRAINT IF EXISTS connector_budget_reservations_status_check;
ALTER TABLE connector_budget_reservations ADD CONSTRAINT connector_budget_reservations_status_check
  CHECK (status IN ('reserved','confirmed','reconciliation_required','reconciled_charged','released'));
DROP INDEX IF EXISTS connector_budget_reservations_active_idx;
CREATE INDEX connector_budget_reservations_active_idx
  ON connector_budget_reservations (month_start,connector_id,signal_family)
  WHERE status IN ('reserved','reconciliation_required','reconciled_charged');

CREATE TABLE IF NOT EXISTS connector_budget_reconciliation_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  reservation_id uuid NOT NULL,
  connector_id text NOT NULL,
  resolution text NOT NULL CHECK (resolution IN ('released','reconciled_charged')),
  amount_rmb numeric(12,4) NOT NULL CHECK (amount_rmb >= 0),
  reason text NOT NULL,
  actor text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
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

CREATE TABLE IF NOT EXISTS workspace_memberships (
  workspace_id text NOT NULL,
  subject text NOT NULL,
  role text NOT NULL CHECK (role IN ('OWNER','ANALYST','VIEWER')),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','revoked')),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id,subject)
);

CREATE TABLE IF NOT EXISTS jwt_revocations (
  jti text PRIMARY KEY,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz NOT NULL DEFAULT now(),
  reason text NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_rules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  actor_id text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rule_id uuid NOT NULL,
  workspace_id text NOT NULL,
  event_id text NOT NULL,
  domain text NOT NULL,
  lifecycle_state text NOT NULL,
  evidence_strength text NOT NULL CHECK (evidence_strength IN ('low','medium','high')),
  evidence_count integer NOT NULL,
  channel text NOT NULL CHECK (channel IN ('in_app','webhook')),
  idempotency_key text NOT NULL DEFAULT gen_random_uuid()::text UNIQUE,
  status text NOT NULL DEFAULT 'delivered' CHECK (status IN ('reserved','delivered','aborted')),
  terminal_reason text,
  delivered_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE alert_deliveries DROP CONSTRAINT IF EXISTS alert_deliveries_rule_id_fkey;
ALTER TABLE alert_deliveries DROP CONSTRAINT IF EXISTS alert_deliveries_event_id_fkey;
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS idempotency_key text NOT NULL DEFAULT gen_random_uuid()::text;
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'delivered';
ALTER TABLE alert_deliveries ADD COLUMN IF NOT EXISTS terminal_reason text;
ALTER TABLE alert_deliveries DROP CONSTRAINT IF EXISTS alert_deliveries_status_check;
ALTER TABLE alert_deliveries ADD CONSTRAINT alert_deliveries_status_check CHECK (status IN ('reserved','delivered','aborted'));
ALTER TABLE alert_deliveries DROP CONSTRAINT IF EXISTS alert_deliveries_terminal_reason_check;
ALTER TABLE alert_deliveries ADD CONSTRAINT alert_deliveries_terminal_reason_check CHECK (status<>'aborted' OR length(terminal_reason)>0);
CREATE UNIQUE INDEX IF NOT EXISTS alert_deliveries_idempotency_uidx ON alert_deliveries (idempotency_key);

CREATE TABLE IF NOT EXISTS metric_incidents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id text NOT NULL,
  actor_id text NOT NULL,
  event_id text NOT NULL,
  target_key text NOT NULL,
  canonical_key text NOT NULL,
  cause text NOT NULL CHECK (cause IN ('worker_retry_after_timeout','delivery_ack_race','provider_duplicate_callback')),
  note text NOT NULL,
  fact_digest text NOT NULL CHECK (fact_digest ~ '^sha256:[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id,target_key,canonical_key)
);

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
  event_id text,
  session_id text NOT NULL,
  idempotency_key text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('detail_opened','evidence_opened','triage_submitted','review_segment_closed','review_heartbeat','watch_toggled','alert_acknowledged','queue_eligible','alert_quality_reviewed','metric_exclusion_recorded','metric_exclusion_reinstated')),
  metadata jsonb NOT NULL DEFAULT '{}',
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE product_interactions DROP CONSTRAINT IF EXISTS product_interactions_event_id_fkey;
ALTER TABLE product_interactions ADD COLUMN IF NOT EXISTS idempotency_key text;
UPDATE product_interactions SET idempotency_key=id::text WHERE idempotency_key IS NULL;
ALTER TABLE product_interactions ALTER COLUMN idempotency_key SET NOT NULL;
ALTER TABLE product_interactions DROP CONSTRAINT IF EXISTS product_interactions_kind_check;
ALTER TABLE product_interactions ADD CONSTRAINT product_interactions_kind_check CHECK (
  kind IN ('detail_opened','evidence_opened','triage_submitted','review_segment_closed','review_heartbeat','watch_toggled','alert_acknowledged','queue_eligible','alert_quality_reviewed','metric_exclusion_recorded','metric_exclusion_reinstated')
);

CREATE TABLE IF NOT EXISTS review_queue_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id text NOT NULL,
  eligibility_key text NOT NULL UNIQUE,
  eligible_at timestamptz NOT NULL,
  lifecycle_state text NOT NULL,
  cluster_version integer NOT NULL,
  score_version text NOT NULL,
  policy_version text NOT NULL,
  entry_kind text NOT NULL DEFAULT 'transition' CHECK (entry_kind IN ('transition','deployment_backfill')),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lead_threshold_crossings (
  event_id text NOT NULL,
  crossed_at timestamptz NOT NULL,
  score_run_ref text NOT NULL,
  threshold_version text NOT NULL,
  policy_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id,threshold_version,policy_version)
);
ALTER TABLE lead_threshold_crossings DROP CONSTRAINT IF EXISTS lead_threshold_crossings_pkey;
ALTER TABLE lead_threshold_crossings ADD PRIMARY KEY (event_id,threshold_version,policy_version);
ALTER TABLE review_queue_entries DROP CONSTRAINT IF EXISTS review_queue_entries_event_id_fkey;
ALTER TABLE review_queue_entries ADD COLUMN IF NOT EXISTS entry_kind text NOT NULL DEFAULT 'transition';
ALTER TABLE review_queue_entries DROP CONSTRAINT IF EXISTS review_queue_entries_entry_kind_check;
ALTER TABLE review_queue_entries ADD CONSTRAINT review_queue_entries_entry_kind_check
  CHECK (entry_kind IN ('transition','deployment_backfill'));
INSERT INTO review_queue_entries
  (event_id,eligibility_key,eligible_at,lifecycle_state,cluster_version,score_version,policy_version,entry_kind)
SELECT e.id,'qe-backfill-'||gen_random_uuid()::text,clock_timestamp(),e.lifecycle_state,e.cluster_version,
       COALESCE(NULLIF(e.current_score->>'scoreVersion',''),'unknown'),
       'product-metrics-2026-07-rc3.3','deployment_backfill'
FROM events e
WHERE e.lifecycle_state IN ('detected','emerging','accelerating','established','cooling')
  AND NOT EXISTS (
    SELECT 1 FROM review_queue_entries q
    WHERE q.event_id=e.id AND q.policy_version='product-metrics-2026-07-rc3.3'
  );

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
CREATE INDEX IF NOT EXISTS review_queue_entries_time_idx ON review_queue_entries (eligible_at DESC);
CREATE INDEX IF NOT EXISTS review_queue_entries_event_idx ON review_queue_entries (event_id,eligible_at DESC);
CREATE INDEX IF NOT EXISTS cluster_edit_requests_event_idx ON cluster_edit_requests (event_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cluster_edit_requests_workspace_idx ON cluster_edit_requests (workspace_id, created_at DESC);

ALTER TABLE feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedback FORCE ROW LEVEL SECURITY;
ALTER TABLE alert_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert_rules FORCE ROW LEVEL SECURITY;
ALTER TABLE alert_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert_deliveries FORCE ROW LEVEL SECURITY;
ALTER TABLE metric_incidents ENABLE ROW LEVEL SECURITY;
ALTER TABLE metric_incidents FORCE ROW LEVEL SECURITY;
ALTER TABLE watchlists ENABLE ROW LEVEL SECURITY;
ALTER TABLE watchlists FORCE ROW LEVEL SECURITY;
ALTER TABLE product_interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE product_interactions FORCE ROW LEVEL SECURITY;
ALTER TABLE cluster_edit_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE cluster_edit_requests FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_memberships FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS feedback_workspace_isolation ON feedback;
CREATE POLICY feedback_workspace_isolation ON feedback
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS alert_rules_workspace_isolation ON alert_rules;
CREATE POLICY alert_rules_workspace_isolation ON alert_rules
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS alert_deliveries_workspace_isolation ON alert_deliveries;
CREATE POLICY alert_deliveries_workspace_isolation ON alert_deliveries
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS metric_incidents_workspace_isolation ON metric_incidents;
CREATE POLICY metric_incidents_workspace_isolation ON metric_incidents
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS watchlists_workspace_isolation ON watchlists;
CREATE POLICY watchlists_workspace_isolation ON watchlists
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS product_interactions_workspace_isolation ON product_interactions;
CREATE POLICY product_interactions_workspace_isolation ON product_interactions
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS cluster_edit_requests_workspace_isolation ON cluster_edit_requests;
CREATE POLICY cluster_edit_requests_workspace_isolation ON cluster_edit_requests
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));
DROP POLICY IF EXISTS workspace_memberships_workspace_isolation ON workspace_memberships;
CREATE POLICY workspace_memberships_workspace_isolation ON workspace_memberships
  USING (workspace_id = current_setting('app.workspace_id', true))
  WITH CHECK (workspace_id = current_setting('app.workspace_id', true));

CREATE OR REPLACE FUNCTION reject_audit_fact_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is append-only; write a compensating audit fact instead', TG_TABLE_NAME;
END
$$;

DROP FUNCTION IF EXISTS erase_event_score_history(text[],text);
CREATE OR REPLACE FUNCTION erase_source_score_history(target_source_id text)
RETURNS TABLE(event_ids text[],erased_score_runs integer,erased_baseline_samples integer)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE affected_event_ids text[];
DECLARE score_count integer;
DECLARE baseline_count integer;
BEGIN
  IF target_source_id IS NULL OR length(trim(target_source_id))=0 THEN
    RAISE EXCEPTION 'source ID is required';
  END IF;
  SELECT COALESCE(array_agg(DISTINCT affected.event_id),'{}'::text[])
  INTO affected_event_ids
  FROM (
    SELECT memberships.event_id
    FROM public.event_observations memberships
    JOIN public.observations observations ON observations.id=memberships.observation_id
    WHERE observations.source_id=target_source_id
    UNION
    SELECT events.id FROM public.events events
    WHERE events.current_score->'evidence' @> jsonb_build_array(jsonb_build_object('source',target_source_id))
  ) affected;
  IF cardinality(affected_event_ids)=0 THEN
    RAISE EXCEPTION 'source has no score-bearing events eligible for erasure';
  END IF;
  UPDATE public.event_metric_snapshots SET score_run_id=NULL
    WHERE event_id=ANY(affected_event_ids);
  DELETE FROM public.score_runs WHERE event_id=ANY(affected_event_ids);
  GET DIAGNOSTICS score_count = ROW_COUNT;
  DELETE FROM public.baseline_samples WHERE source_event_id=ANY(affected_event_ids);
  GET DIAGNOSTICS baseline_count = ROW_COUNT;
  INSERT INTO public.score_history_erasure_audit
    (source_id,event_ids,reason,erased_score_runs,erased_baseline_samples,actor)
  VALUES (target_source_id,affected_event_ids,'source_erasure',score_count,baseline_count,session_user);
  RETURN QUERY SELECT affected_event_ids,score_count,baseline_count;
END
$$;

CREATE OR REPLACE FUNCTION resolve_connector_budget_reservation(
  target_owner_token uuid,resolution text,confirmed_cost_rmb numeric,operator_reason text
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE reservation_row public.connector_budget_reservations%ROWTYPE;
DECLARE final_amount numeric;
BEGIN
  IF resolution NOT IN ('released','reconciled_charged') OR length(trim(operator_reason))<8 THEN
    RAISE EXCEPTION 'a valid resolution and operator reason are required';
  END IF;
  SELECT * INTO reservation_row FROM public.connector_budget_reservations
    WHERE owner_token=target_owner_token AND status='reconciliation_required' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'reservation is not awaiting reconciliation';
  END IF;
  final_amount := CASE WHEN resolution='released' THEN 0 ELSE confirmed_cost_rmb END;
  IF final_amount IS NULL OR final_amount<0 OR final_amount>reservation_row.reserved_amount_rmb THEN
    RAISE EXCEPTION 'confirmed cost is outside the reserved amount';
  END IF;
  UPDATE public.connector_budget_reservations SET status=resolution,
    actual_amount_rmb=final_amount,reconciled_at=clock_timestamp(),
    reconciliation_reason=operator_reason WHERE id=reservation_row.id;
  INSERT INTO public.connector_budget_reconciliation_audit
    (reservation_id,connector_id,resolution,amount_rmb,reason,actor)
  VALUES (reservation_row.id,reservation_row.connector_id,resolution,final_amount,operator_reason,session_user);
  RETURN resolution;
END
$$;

CREATE OR REPLACE FUNCTION protect_alert_delivery_fact() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' AND OLD.status='reserved' THEN
    RETURN OLD;
  END IF;
  IF TG_OP='UPDATE' AND OLD.status='reserved' AND NEW.status='delivered'
     AND (to_jsonb(NEW)-'status')=(to_jsonb(OLD)-'status') THEN
    RETURN NEW;
  END IF;
  IF TG_OP='UPDATE' AND OLD.status='reserved' AND NEW.status='aborted'
     AND length(NEW.terminal_reason)>0
     AND (to_jsonb(NEW)-'status'-'terminal_reason')=(to_jsonb(OLD)-'status'-'terminal_reason') THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'delivered alert facts are immutable';
END
$$;

CREATE OR REPLACE FUNCTION protect_processing_history_fact() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'observation processing history cannot be deleted';
  END IF;
  IF NEW.observation_id<>OLD.observation_id OR NEW.revision<>OLD.revision
     OR NEW.collected_at<>OLD.collected_at OR NEW.enqueued_at<>OLD.enqueued_at
     OR NEW.attempts<OLD.attempts
     OR (OLD.completed_at IS NOT NULL AND NEW.completed_at IS DISTINCT FROM OLD.completed_at)
     OR (NEW.completed_at IS NOT NULL AND NEW.completed_at<NEW.enqueued_at)
     OR (OLD.completed_at IS NULL AND NEW.completed_at IS NOT NULL
         AND NEW.completed_at<clock_timestamp()-interval '1 minute') THEN
    RAISE EXCEPTION 'invalid mutation of observation processing history';
  END IF;
  RETURN NEW;
END
$$;

DO $$
DECLARE audit_table text;
BEGIN
  FOREACH audit_table IN ARRAY ARRAY['feedback','product_interactions','review_queue_entries','lead_threshold_crossings','content_ingest_history','connector_runs','metric_incidents','source_promotion_facts','score_history_erasure_audit','connector_budget_reconciliation_audit']
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_append_only ON %I',audit_table,audit_table);
    EXECUTE format('CREATE TRIGGER %I_append_only BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_audit_fact_mutation()',audit_table,audit_table);
  END LOOP;
END
$$;
DROP TRIGGER IF EXISTS alert_deliveries_immutable ON alert_deliveries;
CREATE TRIGGER alert_deliveries_immutable BEFORE UPDATE OR DELETE ON alert_deliveries
  FOR EACH ROW EXECUTE FUNCTION protect_alert_delivery_fact();
DROP TRIGGER IF EXISTS observation_processing_history_monotonic ON observation_processing_history;
CREATE TRIGGER observation_processing_history_monotonic BEFORE UPDATE OR DELETE ON observation_processing_history
  FOR EACH ROW EXECUTE FUNCTION protect_processing_history_fact();

-- The application identity is provisioned outside this schema migration. The
-- local-only Docker bootstrap creates it in 000_local_roles.sql; production
-- must use the DBA/secret manager and a rotated secret or workload identity.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='radar_app') THEN
    RAISE EXCEPTION 'radar_app must be provisioned before applying the schema migration';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='radar_deletion_worker') THEN
    RAISE EXCEPTION 'radar_deletion_worker must be provisioned before applying the schema migration';
  END IF;
END
$$;
GRANT CONNECT ON DATABASE ai_hot TO radar_app;
GRANT CONNECT ON DATABASE ai_hot TO radar_deletion_worker;
GRANT USAGE ON SCHEMA public TO radar_app;
GRANT USAGE ON SCHEMA public TO radar_deletion_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO radar_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO radar_deletion_worker;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO radar_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO radar_deletion_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO radar_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO radar_app;

REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON schema_attestations FROM radar_app;
GRANT SELECT ON schema_attestations TO radar_app;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON disaster_recovery_attestations FROM radar_app;
GRANT SELECT ON disaster_recovery_attestations TO radar_app;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON workspace_memberships,jwt_revocations FROM radar_app;
GRANT SELECT ON workspace_memberships,jwt_revocations TO radar_app;
REVOKE UPDATE,DELETE,TRUNCATE ON score_runs,baseline_samples FROM radar_app;
GRANT SELECT,INSERT ON score_runs,baseline_samples TO radar_app;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON score_history_erasure_audit FROM radar_app;
GRANT SELECT ON score_history_erasure_audit TO radar_app;
REVOKE DELETE ON events FROM radar_app;
REVOKE ALL ON FUNCTION erase_source_score_history(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION erase_source_score_history(text) FROM radar_app;
GRANT EXECUTE ON FUNCTION erase_source_score_history(text) TO radar_deletion_worker;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON schema_attestations,disaster_recovery_attestations,
  workspace_memberships,jwt_revocations,score_runs,baseline_samples,score_history_erasure_audit
  FROM radar_deletion_worker;
GRANT SELECT ON schema_attestations,disaster_recovery_attestations,score_runs,baseline_samples,
  score_history_erasure_audit TO radar_deletion_worker;
REVOKE DELETE ON events FROM radar_deletion_worker;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON connector_budget_reconciliation_audit
  FROM radar_app,radar_deletion_worker;
GRANT SELECT ON connector_budget_reconciliation_audit TO radar_app,radar_deletion_worker;
REVOKE ALL ON FUNCTION resolve_connector_budget_reservation(uuid,text,numeric,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION resolve_connector_budget_reservation(uuid,text,numeric,text)
  FROM radar_app,radar_deletion_worker;

-- Written last: an interrupted migration must never attest the target schema.
INSERT INTO schema_attestations (key,value,updated_at)
VALUES ('migration_version','001_init_rc3.1',clock_timestamp())
ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at;
