-- Uploaded source archives: the transport for a job whose code cannot ride in the 64 KiB Pod spec.
--
-- One row per upload. The blob is `bytea` deliberately: at this size (see
-- src/source_archive.rs::MAX_COMPRESSED_BYTES) object storage would add a credential, an S3
-- client in the runner and a second egress path, and buy nothing. If the cap ever has to grow,
-- the transport moves — the cap does not.
--
-- A Pod never reads this table. The worker mints a capability token when it claims a job and the
-- Pod presents it as `x-source-token` to `GET /api/sources/{id}/blob`. `token_expires_at` bounds
-- the window; the reaper deletes rows no queued or running job references.
--
-- The token is stored in the clear, deliberately. Hashing it (M2's first draft did) protects
-- nothing: the archive itself is in the same row, so anyone who can read this table already has
-- the thing the token authorises. What hashing cost was correctness — a re-mint could not hand a
-- reattached Pod the token it already holds, so the 409-reattach path would 404 its own download.
-- The mint is therefore idempotent per job while the token is live.

CREATE TABLE sources (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        text NOT NULL,
    sha256           text NOT NULL,
    bytes            bigint NOT NULL CHECK (bytes > 0),
    files            integer NOT NULL CHECK (files > 0),
    blob             bytea NOT NULL,
    token            text,
    token_job_id     uuid,
    token_expires_at timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX sources_created_at_idx ON sources (created_at);
CREATE INDEX sources_token_idx ON sources (token) WHERE token IS NOT NULL;
