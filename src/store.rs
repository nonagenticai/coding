//! Postgres access. Runtime queries only (never the compile-time macros), so the build needs no
//! database and `--locked` stays valid.
//!
//! Every read is tenant-scoped: a job created by tenant A is `404` to tenant B, never `403`
//! (existence is not leaked across tenants).

use serde_json::Value;
use sqlx::PgPool;

use crate::domain::{Job, JobError, JobKind, JobStatus, Timings};
use crate::error::AppError;

/// One row of `jobs` in wire shape. Timestamps arrive as RFC 3339 text from Postgres.
#[derive(Debug, sqlx::FromRow)]
pub struct JobRow {
    pub id: String,
    pub kind: String,
    pub status: String,
    pub workspace_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub queued_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
}

impl TryFrom<JobRow> for Job {
    type Error = AppError;

    fn try_from(row: JobRow) -> Result<Self, Self::Error> {
        let kind = JobKind::parse(&row.kind)
            .ok_or_else(|| AppError::Unavailable(format!("unknown job kind {:?}", row.kind)))?;
        let status = JobStatus::parse(&row.status)
            .ok_or_else(|| AppError::Unavailable(format!("unknown job status {:?}", row.status)))?;
        let error = row
            .error
            .map(serde_json::from_value::<JobError>)
            .transpose()
            .map_err(|e| AppError::Unavailable(format!("corrupt job error payload: {e}")))?;
        Ok(Job {
            id: row.id,
            kind,
            status,
            workspace_id: row.workspace_id,
            result: row.result,
            error,
            timings: Timings {
                queued_at: row.queued_at,
                started_at: row.started_at,
                finished_at: row.finished_at,
                duration_ms: row.duration_ms,
            },
        })
    }
}

/// The select list every read shares; `$1` is always the tenant.
const JOB_COLUMNS: &str = r#"
    id::text AS id,
    kind,
    status,
    workspace_id::text AS workspace_id,
    result,
    error,
    to_char(queued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS queued_at,
    to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS started_at,
    to_char(finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS finished_at,
    CASE
        WHEN started_at IS NOT NULL AND finished_at IS NOT NULL
        THEN (EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000)::bigint
    END AS duration_ms
"#;

/// Create a job. A repeated `idempotency_key` for the same tenant returns the original job.
///
/// # Errors
///
/// `AppError::NotFound` when `workspace_id` is not owned by the tenant, `AppError::Conflict`
/// when the insert conflicts but no `idempotency_key` was supplied, `AppError::Unavailable`
/// when the stored row cannot be decoded, and `AppError::Database` when Postgres fails.
pub async fn create_job(
    pool: &PgPool,
    tenant: &str,
    kind: JobKind,
    workspace_id: Option<&str>,
    input: &Value,
    idempotency_key: Option<&str>,
    timeout_seconds: u32,
) -> Result<Job, AppError> {
    if let Some(ws) = workspace_id {
        let owned: Option<(String,)> = sqlx::query_as(
            "SELECT id::text FROM workspaces WHERE id = $1::uuid AND tenant_id = $2",
        )
        .bind(ws)
        .bind(tenant)
        .fetch_optional(pool)
        .await?;
        if owned.is_none() {
            return Err(AppError::NotFound(format!("workspace {ws}")));
        }
    }

    let sql = format!(
        r"
        INSERT INTO jobs (tenant_id, kind, workspace_id, input, idempotency_key, timeout_seconds)
        VALUES ($1, $2, $3::uuid, $4, $5, $6)
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
        RETURNING {JOB_COLUMNS}
        "
    );
    let inserted: Option<JobRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(kind.as_str())
        .bind(workspace_id)
        .bind(input)
        .bind(idempotency_key)
        .bind(i64::from(timeout_seconds))
        .fetch_optional(pool)
        .await?;

    let row = if let Some(row) = inserted {
        row
    } else {
        let key = idempotency_key.ok_or_else(|| {
            AppError::Conflict("insert conflicted but no idempotency_key was supplied".into())
        })?;
        let sql =
            format!("SELECT {JOB_COLUMNS} FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2");
        sqlx::query_as::<_, JobRow>(sqlx::AssertSqlSafe(sql))
            .bind(tenant)
            .bind(key)
            .fetch_one(pool)
            .await?
    };
    Job::try_from(row)
}

/// Read one job, tenant-scoped.
///
/// # Errors
///
/// `AppError::BadRequest` for an id that is not a UUID, `AppError::NotFound` when the tenant
/// does not own the job, `AppError::Unavailable` when the row cannot be decoded, and
/// `AppError::Database` when the query fails.
pub async fn get_job(pool: &PgPool, tenant: &str, id: &str) -> Result<Job, AppError> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $2::uuid AND tenant_id = $1");
    let row = sqlx::query_as::<_, JobRow>(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref db) if db.code().as_deref() == Some("22P02") => {
                AppError::BadRequest(format!("invalid job id: {id}"))
            }
            other => AppError::Database(other),
        })?
        .ok_or_else(|| AppError::NotFound(format!("job {id}")))?;
    Job::try_from(row)
}

/// Ask a queued job to stop. Terminal jobs are returned unchanged.
///
/// # Errors
///
/// `AppError::Database` when the update fails, plus every error [`get_job`] returns for a job
/// that is already terminal or unknown.
pub async fn cancel_job(pool: &PgPool, tenant: &str, id: &str) -> Result<Job, AppError> {
    let sql = format!(
        r"
        UPDATE jobs
        SET cancel_requested = true,
            status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
            finished_at = CASE WHEN status = 'queued' THEN now() ELSE finished_at END
        WHERE id = $2::uuid AND tenant_id = $1 AND status IN ('queued', 'running')
        RETURNING {JOB_COLUMNS}
        "
    );
    let updated: Option<JobRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    match updated {
        Some(row) => Job::try_from(row),
        // Already terminal or unknown: return the current state, still tenant-scoped.
        None => get_job(pool, tenant, id).await,
    }
}

/// The fencing token for one claim: `<worker_id>/<uuid>`, minted by [`claim_job`].
///
/// A newtype rather than a bare `String`, so a call site that still passes the process
/// `worker_id` — identical across every slot and every retry — does not compile. Every lease
/// mutation is scoped to exactly one claim, and a stale slot can never write through a token
/// that a re-lease has already replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseOwner(String);

impl LeaseOwner {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LeaseOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A claimed job, everything the worker needs to start.
#[derive(Debug, Clone)]
pub struct JobClaim {
    pub id: String,
    pub tenant_id: String,
    pub kind: JobKind,
    pub workspace_id: Option<String>,
    pub input: Value,
    pub timeout_seconds: i64,
    pub attempts: i32,
    /// The fencing token for this claim; every lease mutation presents it.
    pub lease_owner: LeaseOwner,
}

/// Row shape of the claim statement, before the kind is validated.
#[derive(Debug, sqlx::FromRow)]
struct ClaimRow {
    id: String,
    tenant_id: String,
    kind: String,
    workspace_id: Option<String>,
    input: Value,
    /// The column is `integer`; the claim casts it to `bigint`, because sqlx rejects a decode
    /// whose Rust type widens the SQL column type.
    timeout_seconds: i64,
    attempts: i32,
    lease_owner: String,
}

/// Claim one queued job. The lock is released on commit, so the lease (not the row lock) is what
/// keeps two workers off the same job.
///
/// The owner is minted in the same statement — `worker_id/gen_random_uuid()` — so two claims by
/// the same process never share a fencing token, and a stale slot cannot heartbeat, complete,
/// fail or cancel a job after the reaper has handed it to a new claim.
///
/// # Errors
///
/// `AppError::Database` when the claim query fails, and `AppError::Unavailable` when the
/// claimed row carries a job kind this build does not know.
pub async fn claim_job(
    pool: &PgPool,
    worker_id: &str,
    lease_seconds: i64,
) -> Result<Option<JobClaim>, AppError> {
    let row: Option<ClaimRow> = sqlx::query_as(
        r"
        UPDATE jobs
        SET status = 'running',
            lease_owner = $1 || '/' || gen_random_uuid()::text,
            lease_expires_at = now() + make_interval(secs => $2),
            started_at = COALESCE(started_at, now()),
            attempts = attempts + 1
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = 'queued' AND cancel_requested = false
            ORDER BY queued_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id::text, tenant_id, kind, workspace_id::text, input, timeout_seconds::bigint,
                  attempts, lease_owner
        ",
    )
    .bind(worker_id)
    .bind(lease_seconds)
    .fetch_optional(pool)
    .await?;

    row.map(
        |ClaimRow {
             id,
             tenant_id,
             kind,
             workspace_id,
             input,
             timeout_seconds,
             attempts,
             lease_owner,
         }| {
            let kind = JobKind::parse(&kind)
                .ok_or_else(|| AppError::Unavailable(format!("unknown job kind {kind:?}")))?;
            Ok(JobClaim {
                id,
                tenant_id,
                kind,
                workspace_id,
                input,
                timeout_seconds,
                attempts,
                lease_owner: LeaseOwner::new(lease_owner),
            })
        },
    )
    .transpose()
}

/// Extend the lease. Returns false when this claim no longer owns the job.
///
/// # Errors
///
/// `AppError::Database` when the update fails.
pub async fn heartbeat(
    pool: &PgPool,
    owner: &LeaseOwner,
    id: &str,
    lease_seconds: i64,
) -> Result<bool, AppError> {
    let result = sqlx::query(
        r"
        UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3)
        WHERE id = $1::uuid AND lease_owner = $2 AND status = 'running'
        ",
    )
    .bind(id)
    .bind(owner.as_str())
    .bind(lease_seconds)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Mark a claimed job succeeded. Only its current owner may do so.
///
/// # Errors
///
/// `AppError::Database` when the update fails.
pub async fn complete_job(
    pool: &PgPool,
    owner: &LeaseOwner,
    id: &str,
    result: &Value,
) -> Result<bool, AppError> {
    let outcome = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'succeeded', result = $3, error = NULL,
            finished_at = now(), lease_owner = NULL, lease_expires_at = NULL
        WHERE id = $1::uuid AND lease_owner = $2 AND status = 'running'
        ",
    )
    .bind(id)
    .bind(owner.as_str())
    .bind(result)
    .execute(pool)
    .await?;
    Ok(outcome.rows_affected() == 1)
}

/// Mark a claimed job failed.
///
/// # Errors
///
/// `AppError::Unavailable` when the error payload cannot be serialized, and
/// `AppError::Database` when the update fails.
pub async fn fail_job(
    pool: &PgPool,
    owner: &LeaseOwner,
    id: &str,
    error: &JobError,
) -> Result<bool, AppError> {
    let payload = serde_json::to_value(error)
        .map_err(|e| AppError::Unavailable(format!("cannot serialize job error: {e}")))?;
    let outcome = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'failed', error = $3,
            finished_at = now(), lease_owner = NULL, lease_expires_at = NULL
        WHERE id = $1::uuid AND lease_owner = $2 AND status = 'running'
        ",
    )
    .bind(id)
    .bind(owner.as_str())
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(outcome.rows_affected() == 1)
}

/// Has this claim been asked to cancel? Losing the lease reads as `false`: the completion call
/// is rejected anyway, so the answer cannot decide an outcome by itself.
///
/// # Errors
///
/// `AppError::Database` when the read fails.
pub async fn cancel_requested(
    pool: &PgPool,
    owner: &LeaseOwner,
    id: &str,
) -> Result<bool, AppError> {
    let row: Option<(bool,)> = sqlx::query_as(
        r"
        SELECT cancel_requested FROM jobs
        WHERE id = $1::uuid AND lease_owner = $2 AND status = 'running'
        ",
    )
    .bind(id)
    .bind(owner.as_str())
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some_and(|(flag,)| flag))
}

/// Finish a claimed job as `cancelled`. Only its current owner may do so.
///
/// # Errors
///
/// `AppError::Database` when the update fails.
pub async fn cancel_claimed(pool: &PgPool, owner: &LeaseOwner, id: &str) -> Result<bool, AppError> {
    let outcome = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'cancelled', finished_at = now(),
            lease_owner = NULL, lease_expires_at = NULL
        WHERE id = $1::uuid AND lease_owner = $2 AND status = 'running'
        ",
    )
    .bind(id)
    .bind(owner.as_str())
    .execute(pool)
    .await?;
    Ok(outcome.rows_affected() == 1)
}

/// Requeue jobs whose lease expired with attempts left; fail the rest. Returns (requeued, failed).
///
/// # Errors
///
/// `AppError::Database` when either update fails.
pub async fn reap_expired(pool: &PgPool) -> Result<(u64, u64), AppError> {
    let requeued = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL
        WHERE status = 'running' AND lease_expires_at < now() AND attempts < max_attempts
        ",
    )
    .execute(pool)
    .await?
    .rows_affected();

    let failed = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'failed', finished_at = now(), lease_owner = NULL, lease_expires_at = NULL,
            error = jsonb_build_object(
                'code', 'lease_expired',
                'message', 'the worker stopped heartbeating and the attempt budget is spent',
                'retryable', false)
        WHERE status = 'running' AND lease_expires_at < now() AND attempts >= max_attempts
        ",
    )
    .execute(pool)
    .await?
    .rows_affected();

    Ok((requeued, failed))
}

/// Turn expired running jobs whose cancel was requested into terminal `cancelled` rows.
///
/// Run this BEFORE [`reap_expired`]. Otherwise the requeue would put such a job back to
/// `queued`, and [`claim_job`] skips `cancel_requested` rows forever — the job would never be
/// claimed and never reach a terminal state.
///
/// # Errors
///
/// `AppError::Database` when the update fails.
pub async fn reap_cancelled(pool: &PgPool) -> Result<u64, AppError> {
    let outcome = sqlx::query(
        r"
        UPDATE jobs
        SET status = 'cancelled', finished_at = now(),
            lease_owner = NULL, lease_expires_at = NULL
        WHERE status = 'running' AND lease_expires_at < now() AND cancel_requested = true
        ",
    )
    .execute(pool)
    .await?;
    Ok(outcome.rows_affected())
}

/// Append an event. `seq` is allocated per job; the SSE reader tails by `seq`.
///
/// # Errors
///
/// `AppError::Database` when the insert fails.
pub async fn append_event(
    pool: &PgPool,
    job_id: &str,
    kind: &str,
    payload: &Value,
) -> Result<(), AppError> {
    sqlx::query(
        r"
        INSERT INTO job_events (job_id, seq, kind, payload)
        VALUES (
            $1::uuid,
            COALESCE((SELECT max(seq) + 1 FROM job_events WHERE job_id = $1::uuid), 0),
            $2, $3)
        ",
    )
    .bind(job_id)
    .bind(kind)
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

/// Events after `after_seq`, oldest first, tenant-scoped through the job.
///
/// # Errors
///
/// `AppError::Database` when the read fails.
pub async fn events_since(
    pool: &PgPool,
    tenant: &str,
    job_id: &str,
    after_seq: i64,
    limit: i64,
) -> Result<Vec<(i64, String, Value)>, AppError> {
    let rows: Vec<(i64, String, Value)> = sqlx::query_as(
        r"
        SELECT e.seq, e.kind, e.payload
        FROM job_events e
        JOIN jobs j ON j.id = e.job_id
        WHERE e.job_id = $2::uuid AND j.tenant_id = $1 AND e.seq > $3
        ORDER BY e.seq
        LIMIT $4
        ",
    )
    .bind(tenant)
    .bind(job_id)
    .bind(after_seq)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Store one uploaded source archive. Returns its id.
///
/// # Errors
///
/// `AppError::Database` when the insert fails.
pub async fn create_source(
    pool: &PgPool,
    tenant: &str,
    sha256: &str,
    bytes: i64,
    files: i32,
    blob: &[u8],
) -> Result<String, AppError> {
    let (id,): (String,) = sqlx::query_as(
        r"
        INSERT INTO sources (tenant_id, sha256, bytes, files, blob)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id::text
        ",
    )
    .bind(tenant)
    .bind(sha256)
    .bind(bytes)
    .bind(files)
    .bind(blob)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Does this tenant own this source? A job may only name a source its own tenant uploaded.
///
/// # Errors
///
/// `AppError::BadRequest` for an id that is not a UUID, and `AppError::Database` when the
/// query fails.
pub async fn owns_source(pool: &PgPool, tenant: &str, id: &str) -> Result<bool, AppError> {
    let row: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM sources WHERE id = $2::uuid AND tenant_id = $1")
            .bind(tenant)
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db) if db.code().as_deref() == Some("22P02") => {
                    AppError::BadRequest(format!("invalid source id: {id}"))
                }
                other => AppError::Database(other),
            })?;
    Ok(row.is_some())
}

/// Mint a capability token for one job's download of one source.
///
/// The token is generated by Postgres (two v4 UUIDs = 244 bits). The mint is **idempotent per
/// job**: a live token already bound to this job is handed back unchanged, because a worker that
/// re-encounters its own Pod (409 reattach) must give that Pod the token it is holding, not a
/// fresh one. Returns `None` when the source is gone, and `(token, sha256-of-the-archive)` when it
/// is not — the Pod verifies the archive it downloads against that hash.
///
/// # Errors
///
/// `AppError::Database` when the mint fails.
pub async fn mint_source_token(
    pool: &PgPool,
    tenant: &str,
    job_id: &str,
    source_id: &str,
) -> Result<Option<(String, String)>, AppError> {
    let row: Option<(String, String)> = sqlx::query_as(
        r"
        WITH fresh AS (
            SELECT replace(gen_random_uuid()::text, '-', '')
                || replace(gen_random_uuid()::text, '-', '') AS raw
        ), live AS (
            SELECT token, token_expires_at > now() AND token_job_id = $1::uuid AS reusable
            FROM sources WHERE id = $3::uuid AND tenant_id = $2
        )
        UPDATE sources s
        SET token = CASE WHEN l.reusable THEN s.token ELSE (SELECT raw FROM fresh) END,
            token_expires_at = CASE
                WHEN l.reusable THEN s.token_expires_at
                ELSE now() + interval '30 minutes'
            END,
            token_job_id = $1::uuid
        FROM live l
        WHERE s.id = $3::uuid AND s.tenant_id = $2
        RETURNING s.token, s.sha256
        ",
    )
    .bind(job_id)
    .bind(tenant)
    .bind(source_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// The archive behind a live token, or `None` when the id, the token or the window is wrong.
///
/// A wrong token and an unknown source answer identically on purpose: the id alone must not be
/// an oracle for what exists.
///
/// # Errors
///
/// `AppError::BadRequest` for an id that is not a UUID, and `AppError::Database` when the
/// query fails.
pub async fn source_blob(
    pool: &PgPool,
    id: &str,
    token: &str,
) -> Result<Option<Vec<u8>>, AppError> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        r"
        SELECT blob FROM sources
        WHERE id = $1::uuid AND token = $2 AND token_expires_at > now()
        ",
    )
    .bind(id)
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(ref db) if db.code().as_deref() == Some("22P02") => {
            AppError::BadRequest(format!("invalid source id: {id}"))
        }
        other => AppError::Database(other),
    })?;
    Ok(row.map(|(blob,)| blob))
}

/// Delete uploads older than `older_than_seconds` that no queued or running job references.
///
/// This is the only deletion path: deleting on a job's terminal state would race both the
/// 409-reattach retry and a lost create response, so a blob lives until it is neither old nor
/// wanted. The caller's period is what bounds the window in practice.
///
/// # Errors
///
/// `AppError::Database` when the delete fails.
pub async fn reap_sources(pool: &PgPool, older_than_seconds: i64) -> Result<u64, AppError> {
    let outcome = sqlx::query(
        r"
        DELETE FROM sources s
        WHERE s.created_at < now() - make_interval(secs => $1::int)
          AND NOT EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.status IN ('queued', 'running')
                AND (
                    -- The single-source form.
                    j.input->'source'->>'id' = s.id::text
                    -- The multi-source form: every entry's own source object.
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                                 COALESCE(j.input->'sources', '[]'::jsonb)
                             ) AS entry
                        WHERE entry->'source'->>'id' = s.id::text
                    )
                )
          )
        ",
    )
    .bind(older_than_seconds)
    .execute(pool)
    .await?;
    Ok(outcome.rows_affected())
}
