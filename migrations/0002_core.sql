-- Core schema: the job queue, its event log, and workspaces.
--
-- One database for the whole service. Tenant isolation is a column on every table and a
-- predicate on every query (explicit, not RLS: FORCE RLS makes non-leakproof text indexes
-- unusable, and this service will grow full-text search over analysis results).

CREATE TABLE jobs (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         text NOT NULL,
    kind              text NOT NULL,
    status            text NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    workspace_id      uuid,
    input             jsonb NOT NULL DEFAULT '{}'::jsonb,
    result            jsonb,
    error             jsonb,
    idempotency_key   text,
    timeout_seconds   integer NOT NULL DEFAULT 900 CHECK (timeout_seconds BETWEEN 1 AND 86400),
    attempts          integer NOT NULL DEFAULT 0,
    max_attempts      integer NOT NULL DEFAULT 3,
    lease_owner       text,
    lease_expires_at  timestamptz,
    cancel_requested  boolean NOT NULL DEFAULT false,
    queued_at         timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz
);

-- A replay of the same caller-supplied key returns the original job. NULL keys never collide
-- (Postgres treats NULLs as distinct in a unique index).
CREATE UNIQUE INDEX jobs_tenant_idempotency_uniq
    ON jobs (tenant_id, idempotency_key);

CREATE INDEX jobs_claim_idx
    ON jobs (queued_at)
    WHERE status = 'queued' AND cancel_requested = false;

CREATE INDEX jobs_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'running';

CREATE INDEX jobs_tenant_idx
    ON jobs (tenant_id, queued_at DESC);

CREATE TABLE job_events (
    job_id   uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    seq      bigint NOT NULL,
    at       timestamptz NOT NULL DEFAULT now(),
    kind     text NOT NULL,
    payload  jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (job_id, seq)
);

CREATE TABLE workspaces (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    text NOT NULL,
    repo_url     text NOT NULL,
    git_ref      text NOT NULL DEFAULT 'main',
    commit_sha   text,
    toolchain    text NOT NULL DEFAULT 'base',
    status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'ready', 'failed', 'expired')),
    pod_name     text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz
);

CREATE INDEX workspaces_tenant_idx ON workspaces (tenant_id, created_at DESC);
CREATE INDEX workspaces_expiry_idx ON workspaces (expires_at) WHERE status = 'ready';

ALTER TABLE jobs
    ADD CONSTRAINT jobs_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces (id) ON DELETE SET NULL;
