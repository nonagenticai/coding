//! The worker loop: claim, heartbeat, execute, finish — plus the reaper that recovers leases
//! abandoned by a worker that died.
//!
//! M1's production executor is the kata sandbox: one Pod per claimed job, polled to a terminal
//! state, logged once, and always deleted. Lease discipline stays in this module; the Pod is
//! only ever touched through [`Sandbox`], so the loop's tests never need a cluster.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;

use crate::domain::JobError;
use crate::error::AppError;
use crate::github_app::GitHubApp;
use crate::sandbox::{PodState, Sandbox, SandboxError};
use crate::store::{self, JobClaim, LeaseOwner};

/// What an executor resolves to. Boxed so the loop can be driven with a substitute in tests and
/// so each `JobKind` can dispatch to its own implementation from M1 on.
pub type ExecutorFuture = Pin<Box<dyn Future<Output = Result<Value, JobError>> + Send + 'static>>;

/// A job executor. `Fn` (not `FnMut`) so one instance can be shared without a lock.
pub type JobExecutor = dyn Fn(JobClaim) -> ExecutorFuture + Send + Sync;

/// How the loop paces itself.
#[derive(Debug, Clone)]
pub struct WorkerSettings {
    /// A claim stays valid without a heartbeat for this long.
    pub lease_seconds: i64,
    /// How often the owner renews its lease; must stay well under `lease_seconds`.
    pub heartbeat_interval: Duration,
    /// Sleep between empty polls.
    pub poll_interval: Duration,
    /// How often the reaper looks for abandoned leases.
    pub reap_interval: Duration,
    /// Age after which the orphan-Pod reaper deletes a runner Pod that carries no
    /// `coding.io/reap-at` annotation (an older build). A Pod created by this version is reaped
    /// by its own job deadline instead, so a long job is never killed by this fallback.
    pub pod_deadline_seconds: u64,
    /// How many jobs this process runs at once. Each slot has its own fencing token.
    pub concurrency: usize,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            lease_seconds: 60,
            heartbeat_interval: Duration::from_secs(15),
            poll_interval: Duration::from_secs(1),
            reap_interval: Duration::from_secs(30),
            pod_deadline_seconds: 900,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }
}

/// The default number of concurrent jobs. Four 1-CPU/2-GiB Pods fit the `coding-runs` quota
/// (8 CPU requested, 24 GiB requested) with room for a second job later.
const DEFAULT_CONCURRENCY: usize = 4;
/// Upper bound on `CODING_WORKER_CONCURRENCY`: the kata quota is 8 CPU and the worker's pool is
/// 10 connections. Raising this is a deliberate, measured change, not an env tweak.
const MAX_CONCURRENCY: usize = 8;
/// `CODING_WORKER_CONCURRENCY`: how many jobs this worker process runs at once.
const CONCURRENCY_ENV: &str = "CODING_WORKER_CONCURRENCY";

/// Parse `CODING_WORKER_CONCURRENCY`. Unset/empty keeps the default; zero, a non-integer or an
/// oversized value is a warning and a fallback, never a refusal to start — a mistyped env var
/// must not take the worker down.
fn concurrency_from(raw: Option<&str>) -> usize {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return DEFAULT_CONCURRENCY;
    };
    match raw.parse::<usize>() {
        Ok(0) => {
            tracing::warn!(
                env = CONCURRENCY_ENV,
                value = raw,
                "must be positive; using the default"
            );
            DEFAULT_CONCURRENCY
        }
        Ok(value) if value > MAX_CONCURRENCY => {
            tracing::warn!(
                env = CONCURRENCY_ENV,
                value = raw,
                max = MAX_CONCURRENCY,
                "above the supported maximum; clamped"
            );
            MAX_CONCURRENCY
        }
        Ok(value) => value,
        Err(_) => {
            tracing::warn!(
                env = CONCURRENCY_ENV,
                value = raw,
                "not an integer; using the default"
            );
            DEFAULT_CONCURRENCY
        }
    }
}

/// How often the executor polls a running Pod. Nothing streams: the log is read once, after
/// the Pod reaches a terminal state.
const STATE_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// How many trailing log lines the executor asks the API for. The byte cap is enforced by
/// [`Sandbox::logs`] regardless.
const LOG_TAIL_LINES: u32 = 2000;
/// Where this service's own API answers from inside the cluster. Only an `Upload` source needs
/// it, and only that kind fails without it — a worker that refuses to start because a later
/// feature is unconfigured would take the git and inline jobs down with it.
const API_URL_ENV: &str = "CODING_API_URL";
/// An upload older than this is deleted unless a queued or running job still references it.
const SOURCE_RETENTION_SECONDS: i64 = 2 * 60 * 60;

/// The production executor, boxed for [`run_with`]. The sandbox is captured, so a test can
/// inject one pointed at a local stub and the worker loop never learns the difference.
///
/// The pool rides along for one reason: a cancel must delete the running Pod while the job
/// runs, not after it finishes. The identity for that check is the claim's own fencing token,
/// never the process `worker_id`: a stale slot must not read the cancel flag of a re-lease.
pub fn sandbox_executor(
    sandbox: Sandbox,
    pool: PgPool,
    api_url: Option<String>,
    github_app: Option<Arc<GitHubApp>>,
) -> Box<JobExecutor> {
    Box::new(move |claim: JobClaim| -> ExecutorFuture {
        let sandbox = sandbox.clone();
        let pool = pool.clone();
        let api_url = api_url.clone();
        let github_app = github_app.clone();
        Box::pin(async move {
            execute_sandbox(
                &sandbox,
                &pool,
                api_url.as_deref(),
                github_app.as_deref(),
                claim,
            )
            .await
        })
    })
}

/// Resolve the source a job names into the source the Pod will see.
///
/// Only an uploaded archive needs anything done here: the id is turned into a fetch URL, a
/// capability token minted for this attempt, and the archive's sha256 for the Pod to verify.
/// Everything the request supplied is preserved, and nothing is fetched by this process.
async fn prepare_input(
    pool: &PgPool,
    api_url: Option<&str>,
    github_app: Option<&GitHubApp>,
    claim: &JobClaim,
) -> Result<Value, JobError> {
    let Some(source) = claim.input.get("source") else {
        return Ok(claim.input.clone());
    };
    match source.get("kind").and_then(Value::as_str) {
        Some("upload") => {}
        Some("git") => return prepare_git_input(github_app, claim, source).await,
        _ => return Ok(claim.input.clone()),
    }
    let id = source
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_spec("an upload source must name its id".to_owned()))?;
    let Some(base) = api_url else {
        return Err(invalid_spec(format!(
            "{API_URL_ENV} is not set, so an uploaded source cannot be fetched"
        )));
    };
    let minted = store::mint_source_token(pool, &claim.tenant_id, &claim.id, id).await;
    let (token, sha256) = match minted {
        Ok(Some(minted)) => minted,
        Ok(None) => {
            return Err(invalid_spec(format!(
                "source {id} does not exist for this tenant (it may have been reaped)"
            )));
        }
        Err(error) => {
            return Err(JobError {
                code: "source_unavailable".to_owned(),
                message: format!("could not mint a token for source {id}: {error}"),
                details: None,
                retryable: true,
            });
        }
    };
    let mut input = claim.input.clone();
    if let Some(object) = input.get_mut("source").and_then(Value::as_object_mut) {
        object.insert(
            "url".to_owned(),
            Value::String(format!(
                "{}/api/sources/{id}/blob",
                base.trim_end_matches('/')
            )),
        );
        object.insert("token".to_owned(), Value::String(token));
        object.insert("sha256".to_owned(), Value::String(sha256));
    }
    Ok(input)
}

/// Give a github.com clone an installation token minted by the estate's GitHub App.
///
/// A URL that is not github.com is returned untouched: the token is a credential, and the request
/// chooses the URL. No App configured (or no token needed) leaves the clone anonymous, exactly as
/// before GitHub support existed.
async fn prepare_git_input(
    github_app: Option<&GitHubApp>,
    claim: &JobClaim,
    source: &Value,
) -> Result<Value, JobError> {
    let Some(app) = github_app else {
        return Ok(claim.input.clone());
    };
    let url = source
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !crate::github_app::is_github_url(url) {
        return Ok(claim.input.clone());
    }
    let token = app.installation_token().await.map_err(|message| JobError {
        code: "github_app_error".to_owned(),
        message,
        details: None,
        retryable: true,
    })?;
    let mut input = claim.input.clone();
    if let Some(object) = input.get_mut("source").and_then(Value::as_object_mut) {
        object.insert("token".to_owned(), Value::String(token));
    }
    Ok(input)
}

/// A spec the server itself cannot satisfy. Permanent: retrying changes nothing.
fn invalid_spec(message: String) -> JobError {
    JobError {
        code: "invalid_job_spec".to_owned(),
        message,
        details: None,
        retryable: false,
    }
}

/// Run one job in the sandbox: create the Pod, poll it, read its log once, always delete it.
///
/// Outcomes (the Pod is deleted on every path, including timeout):
/// - `Succeeded` -> the runner's last `{"coding_result": ...}` NDJSON line, or an exit code and
///   the bounded log tail.
/// - `Failed` -> `runner_failed`, non-retryable, with the bounded log as details.
/// - deadline exceeded -> `timeout`, retryable.
pub async fn execute_sandbox(
    sandbox: &Sandbox,
    pool: &PgPool,
    api_url: Option<&str>,
    github_app: Option<&GitHubApp>,
    claim: JobClaim,
) -> Result<Value, JobError> {
    let input = prepare_input(pool, api_url, github_app, &claim).await?;
    let spec = json!({
        "job_id": &claim.id,
        "tenant_id": &claim.tenant_id,
        "kind": claim.kind.as_str(),
        "workspace_id": &claim.workspace_id,
        "attempt": claim.attempts,
        "input": input,
        // The runner self-times-out just before the worker's own poll deadline, so its final
        // result line is still written and readable before the Pod is deleted.
        "deadline_seconds": claim.timeout_seconds.saturating_sub(5).max(1),
    });

    let pod = sandbox
        .create_run(&claim.id, &claim.tenant_id, claim.kind, &spec)
        .await
        .map_err(sandbox_job_error)?;

    let outcome = poll_pod(sandbox, pool, &claim, &pod).await;

    if let Err(error) = sandbox.delete(&pod).await {
        tracing::warn!(
            job_id = %claim.id,
            pod = %pod,
            error = %error,
            "sandbox: could not delete the runner pod"
        );
    }
    outcome
}

/// Poll a Pod to a terminal state. A state-read failure is logged and retried until the
/// deadline: a transient API blip must not turn a running job into a failure.
async fn poll_pod(
    sandbox: &Sandbox,
    pool: &PgPool,
    claim: &JobClaim,
    pod: &str,
) -> Result<Value, JobError> {
    let timeout_seconds = claim.timeout_seconds;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds.max(0) as u64);
    loop {
        // A cancel is checked on every tick, so the Pod dies within one interval rather than
        // running to its deadline. The caller sees `cancelled` and `execute_claim` records the
        // terminal state from the row.
        match store::cancel_requested(pool, &claim.lease_owner, &claim.id).await {
            Ok(true) => {
                tracing::info!(
                    job_id = %claim.id,
                    pod = %pod,
                    "sandbox: cancel requested; deleting the runner pod"
                );
                return Err(JobError {
                    code: "cancelled".to_owned(),
                    message: "the job was cancelled while it ran".to_owned(),
                    details: None,
                    retryable: false,
                });
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(job_id = %claim.id, error = %error, "sandbox: cancel check failed");
            }
        }
        match sandbox.state(pod).await {
            Ok(PodState::Succeeded { exit_code }) => {
                let log = read_log(sandbox, pod).await;
                tracing::info!(
                    pod = %pod,
                    exit_code = ?exit_code,
                    "sandbox: runner pod succeeded"
                );
                return Ok(result_from_log(&log, exit_code));
            }
            Ok(PodState::Failed { reason }) => {
                let log = read_log(sandbox, pod).await;
                return Err(JobError {
                    code: "runner_failed".to_owned(),
                    message: reason,
                    details: Some(Value::String(log)),
                    retryable: false,
                });
            }
            Ok(PodState::Gone) => {
                return Err(JobError {
                    code: "pod_gone".to_owned(),
                    message: "the runner pod disappeared before reaching a terminal state"
                        .to_owned(),
                    details: None,
                    retryable: true,
                });
            }
            Ok(PodState::Pending | PodState::Running) => {}
            Err(error) => {
                tracing::warn!(pod = %pod, error = %error, "sandbox: pod state poll failed");
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(JobError {
                code: "timeout".to_owned(),
                message: format!("the runner pod did not finish within {timeout_seconds}s"),
                details: None,
                retryable: true,
            });
        }
        tokio::time::sleep(STATE_POLL_INTERVAL.min(deadline - now)).await;
    }
}

/// Read the log once, at the end. A log failure must never turn a finished job into a retry
/// loop, so it degrades to an empty tail.
async fn read_log(sandbox: &Sandbox, pod: &str) -> String {
    match sandbox.logs(pod, LOG_TAIL_LINES).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(pod = %pod, error = %error, "sandbox: log read failed");
            String::new()
        }
    }
}

/// The job result. The runner's last NDJSON `{"coding_result": ...}` line wins; otherwise the
/// result is explicit about what it is — an exit status, the container's exit code, and the
/// bounded log tail.
fn result_from_log(log: &str, exit_code: Option<i32>) -> Value {
    for line in log.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(Value::Object(object)) = serde_json::from_str::<Value>(trimmed)
            && let Some(result) = object.get("coding_result")
        {
            return result.clone();
        }
    }
    json!({ "exit": "succeeded", "exit_code": exit_code, "log_tail": log })
}

/// Map a sandbox failure onto the job envelope. Manifest and configuration errors are
/// permanent; transport and API failures may succeed on a retry.
fn sandbox_job_error(error: SandboxError) -> JobError {
    let (code, retryable) = match &error {
        SandboxError::Missing(_)
        | SandboxError::Invalid { .. }
        | SandboxError::Credential(_)
        | SandboxError::ProfileNotEnabled { .. } => ("sandbox_not_configured", false),
        SandboxError::Manifest(_) | SandboxError::SpecTooLarge { .. } => {
            ("invalid_job_spec", false)
        }
        SandboxError::ForeignPod { .. } | SandboxError::ForeignPodName { .. } => {
            ("sandbox_conflict", false)
        }
        SandboxError::NotFound(_) => ("sandbox_not_found", true),
        SandboxError::Transport(_) | SandboxError::Api { .. } => ("sandbox_error", true),
    };
    JobError {
        code: code.to_owned(),
        message: error.to_string(),
        details: None,
        retryable,
    }
}

/// Run until `shutdown` resolves. Every in-flight job is allowed to finish; no new one is
/// claimed. `CODING_WORKER_CONCURRENCY` sets how many run at once (default 4).
///
/// The sandbox is built before the loop starts: an unset namespace or a non-digest-pinned
/// image is a startup failure, never a stream of failing jobs.
pub async fn run(
    pool: PgPool,
    worker_id: impl Into<String>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), AppError> {
    let sandbox =
        Sandbox::from_env().map_err(|error| AppError::Unavailable(format!("sandbox: {error}")))?;
    let worker_id = worker_id.into();
    let github_app = GitHubApp::from_env().map_err(|error| AppError::Unavailable(error.clone()))?;
    if github_app.is_none() {
        tracing::warn!(
            "worker: no GitHub App configured; git jobs can clone public repositories only"
        );
    }
    let github_app = github_app.map(Arc::new);
    let api_url = std::env::var(API_URL_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if api_url.is_none() {
        tracing::warn!(
            env = API_URL_ENV,
            "worker: no API url; jobs with an uploaded source will fail closed"
        );
    }
    let settings = WorkerSettings {
        concurrency: concurrency_from(std::env::var(CONCURRENCY_ENV).ok().as_deref()),
        ..WorkerSettings::default()
    };
    run_with_sandbox(
        pool.clone(),
        worker_id,
        shutdown,
        settings,
        sandbox_executor(sandbox.clone(), pool, api_url, github_app),
        sandbox,
    )
    .await
}

/// The loop with explicit timings and a substitutable executor (the integration tests use both).
pub async fn run_with(
    pool: PgPool,
    worker_id: impl Into<String>,
    shutdown: impl Future<Output = ()> + Send,
    settings: WorkerSettings,
    executor: Box<JobExecutor>,
) -> Result<(), AppError> {
    run_inner(pool, worker_id, shutdown, settings, executor, None).await
}

/// [`run_with`] plus the orphan-Pod reaper. A Pod outlives its job when the worker dies between
/// creating it and deleting it, so the production path must reap by age as well as by outcome.
pub async fn run_with_sandbox(
    pool: PgPool,
    worker_id: impl Into<String>,
    shutdown: impl Future<Output = ()> + Send,
    settings: WorkerSettings,
    executor: Box<JobExecutor>,
    sandbox: Sandbox,
) -> Result<(), AppError> {
    run_inner(pool, worker_id, shutdown, settings, executor, Some(sandbox)).await
}

async fn run_inner(
    pool: PgPool,
    worker_id: impl Into<String>,
    shutdown: impl Future<Output = ()> + Send,
    settings: WorkerSettings,
    executor: Box<JobExecutor>,
    sandbox: Option<Sandbox>,
) -> Result<(), AppError> {
    let worker_id = worker_id.into();
    tracing::info!(
        worker_id = %worker_id,
        lease_seconds = settings.lease_seconds,
        concurrency = settings.concurrency,
        "worker started"
    );

    let reaper = tokio::spawn(reap_loop(pool.clone(), settings.reap_interval));
    let pod_reaper = sandbox.map(|sandbox| {
        tokio::spawn(pod_reap_loop(
            sandbox,
            settings.reap_interval,
            Duration::from_secs(settings.pod_deadline_seconds),
        ))
    });
    let mut shutdown = std::pin::pin!(shutdown);
    let executor: Arc<JobExecutor> = Arc::from(executor);
    let mut in_flight: tokio::task::JoinSet<Result<(), AppError>> = tokio::task::JoinSet::new();
    let mut stopping = false;

    loop {
        // Poll shutdown once, without consuming it: the select below polls it again only while
        // it has not fired. A completed future is never polled twice.
        if !stopping && shutdown_requested(&mut shutdown).await {
            stopping = true;
        }
        if stopping {
            // Drain: every claimed job keeps its Pod and its lease until its executor returns.
            // Dropping the whole worker future still aborts these tasks (JoinSet's Drop), which
            // is the killed-worker path the lease reaper covers.
            if in_flight.is_empty() {
                break;
            }
            if let Some(joined) = in_flight.join_next().await {
                log_slot(joined);
            }
            continue;
        }

        if in_flight.len() < settings.concurrency {
            // The claim is awaited alone, never raced against shutdown: dropping a sqlx query
            // mid-flight can leave its pooled connection unusable.
            if let Some(claim) = claim_or_backoff(&pool, &worker_id, settings.lease_seconds).await {
                let pool = pool.clone();
                let worker_id = worker_id.clone();
                let settings = settings.clone();
                let executor = Arc::clone(&executor);
                in_flight.spawn(async move {
                    execute_claim(&pool, &worker_id, claim, &settings, executor.as_ref()).await
                });
                continue;
            }
        }

        tokio::select! {
            biased;
            () = &mut shutdown, if !stopping => {
                stopping = true;
            }
            joined = in_flight.join_next(), if !in_flight.is_empty() => {
                if let Some(joined) = joined {
                    log_slot(joined);
                }
            }
            () = tokio::time::sleep(settings.poll_interval) => {}
        }
    }

    reaper.abort();
    if let Some(pod_reaper) = pod_reaper {
        pod_reaper.abort();
    }
    tracing::info!(
        worker_id = %worker_id,
        concurrency = settings.concurrency,
        "worker stopped: no longer claiming"
    );
    Ok(())
}

/// Report a finished slot. A panicked slot is a `JoinError`; its job's lease then lapses and the
/// reaper retries it, so the loop must keep running.
fn log_slot(joined: Result<Result<(), AppError>, tokio::task::JoinError>) {
    match joined {
        Ok(Ok(())) => {}
        // A transient database error must not take the worker down: the lease will lapse and the
        // reaper will hand the job to the next attempt.
        Ok(Err(error)) => tracing::warn!(error = %error, "job execution failed; continuing"),
        Err(error) => tracing::warn!(error = %error, "job slot panicked; the lease will lapse"),
    }
}

/// Has shutdown fired? Polls once; never blocks.
async fn shutdown_requested<F>(shutdown: &mut Pin<&mut F>) -> bool
where
    F: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown => true,
        () = std::future::ready(()) => false,
    }
}

/// Claim one job, or log and back off. A database blip must not terminate the worker: the
/// in-flight lease lapses and the reaper retries it.
async fn claim_or_backoff(pool: &PgPool, worker_id: &str, lease_seconds: i64) -> Option<JobClaim> {
    match store::claim_job(pool, worker_id, lease_seconds).await {
        Ok(claim) => claim,
        Err(error) => {
            tracing::warn!(error = %error, "claim failed; backing off");
            tokio::time::sleep(Duration::from_secs(5)).await;
            None
        }
    }
}

/// Run one claimed job to an outcome. Every write is owner-scoped in the store, so a slot that
/// lost its lease cannot overwrite whoever owns the job now.
async fn execute_claim(
    pool: &PgPool,
    worker_id: &str,
    claim: JobClaim,
    settings: &WorkerSettings,
    executor: &JobExecutor,
) -> Result<(), AppError> {
    let owner = claim.lease_owner.clone();
    store::append_event(
        pool,
        &claim.id,
        "started",
        &json!({
            "worker_id": worker_id,
            "lease_owner": owner.as_str(),
            "attempt": claim.attempts,
        }),
    )
    .await?;

    // The request may have arrived between the claim and now; do not start work that was
    // already called off.
    if store::cancel_requested(pool, &owner, &claim.id).await? {
        store::cancel_claimed(pool, &owner, &claim.id).await?;
        tracing::info!(job_id = %claim.id, "job cancelled before it ran");
        return Ok(());
    }

    // The guard aborts the renewal on **every** exit from this scope: normal return, `?`, a
    // panic unwind, or a JoinSet abort of this future. A detached heartbeat would keep a dead
    // slot's lease alive forever, so the reaper could never reclaim the job.
    let _heartbeat = AbortOnDrop::new(tokio::spawn(heartbeat_loop(
        pool.clone(),
        owner.clone(),
        claim.id.clone(),
        settings.lease_seconds,
        settings.heartbeat_interval,
    )));

    let outcome = executor(claim.clone()).await;

    // Re-check before recording an outcome: a cancel that arrived while the job ran wins, and
    // the executor's result is discarded rather than recorded as success.
    if store::cancel_requested(pool, &owner, &claim.id).await? {
        store::cancel_claimed(pool, &owner, &claim.id).await?;
        tracing::info!(job_id = %claim.id, "job cancelled while it ran; result discarded");
        return Ok(());
    }

    match outcome {
        Ok(result) => {
            if !store::complete_job(pool, &owner, &claim.id, &result).await? {
                tracing::warn!(
                    job_id = %claim.id,
                    "lease lost before completion; the reaper will retry"
                );
            }
        }
        Err(error) => {
            if !store::fail_job(pool, &owner, &claim.id, &error).await? {
                tracing::warn!(job_id = %claim.id, "lease lost before failure; the reaper will retry");
            }
        }
    }
    Ok(())
}

/// Aborts a spawned task when dropped. The heartbeat must never outlive the claim it renews.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Renew the lease until the job is finished or this claim is no longer the owner.
async fn heartbeat_loop(
    pool: PgPool,
    owner: LeaseOwner,
    job_id: String,
    lease_seconds: i64,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick is immediate
    loop {
        ticker.tick().await;
        match store::heartbeat(&pool, &owner, &job_id, lease_seconds).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(job_id = %job_id, "lease lost; stop heartbeating");
                return;
            }
            Err(error) => tracing::warn!(job_id = %job_id, error = %error, "heartbeat failed"),
        }
    }
}

/// Sweep abandoned leases every `period`. `reap_cancelled` goes first: it turns a
/// cancel-requested job whose lease lapsed into a terminal row, which the requeue must never
/// pick up (`claim_job` skips `cancel_requested` and would strand it queued forever).
async fn reap_loop(pool: PgPool, period: Duration) {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match store::reap_cancelled(&pool).await {
            Ok(count) if count > 0 => {
                tracing::info!(count, "reaper: expired cancelled jobs marked terminal")
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "reaper: cancelled sweep failed"),
        }
        match store::reap_expired(&pool).await {
            Ok((requeued, failed)) if requeued + failed > 0 => {
                tracing::info!(requeued, failed, "reaper: reclaimed abandoned leases")
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "reaper: lease sweep failed"),
        }
        // Uploads are large rows: sweep them on the same tick, behind the lease sweeps so a
        // recovered job never loses the archive its retry still needs.
        match store::reap_sources(&pool, SOURCE_RETENTION_SECONDS).await {
            Ok(count) if count > 0 => {
                tracing::info!(count, "reaper: deleted unreferenced source uploads")
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "reaper: source sweep failed"),
        }
    }
}

/// Delete runner Pods that outlived the deadline. The executor deletes its own Pod on every
/// outcome, so anything this finds belongs to a worker that died mid-run.
async fn pod_reap_loop(sandbox: Sandbox, period: Duration, older_than: Duration) {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match sandbox.reap(older_than).await {
            Ok(0) => {}
            Ok(count) => tracing::info!(count, "reaper: deleted orphan runner Pods"),
            Err(error) => tracing::warn!(error = %error, "reaper: pod sweep failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A local stand-in for api.github.com that hands out one installation token.
    async fn github_api(token: &'static str) -> String {
        let app = axum::Router::new().route(
            "/app/installations/{id}/access_tokens",
            axum::routing::post(move || async move {
                (
                    axum::http::StatusCode::CREATED,
                    axum::Json(json!({"token": token, "expires_at": "2030-01-01T00:00:00Z"})),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn claim_with(source: serde_json::Value) -> JobClaim {
        JobClaim {
            id: "11111111-1111-4111-8111-111111111111".to_owned(),
            tenant_id: "t".to_owned(),
            kind: crate::domain::JobKind::Test,
            workspace_id: None,
            input: json!({"source": source}),
            timeout_seconds: 60,
            attempts: 1,
            lease_owner: LeaseOwner::new("test-worker/unit"),
        }
    }

    #[test]
    fn concurrency_comes_from_the_env_with_a_safe_fallback() {
        assert_eq!(concurrency_from(None), 4);
        assert_eq!(concurrency_from(Some("")), 4);
        assert_eq!(concurrency_from(Some("  ")), 4);
        assert_eq!(concurrency_from(Some("3")), 3);
        assert_eq!(concurrency_from(Some("8")), 8);
        // A value we cannot honour is a warning, never a refusal to start.
        assert_eq!(concurrency_from(Some("0")), 4);
        assert_eq!(concurrency_from(Some("-2")), 4);
        assert_eq!(concurrency_from(Some("many")), 4);
        assert_eq!(concurrency_from(Some("99")), 8);
    }

    #[tokio::test]
    async fn a_github_clone_gets_an_installation_token() {
        let api = github_api("ghs_from_the_app").await;
        let app = GitHubApp::with_signer("2333879", "12345678", &api, |_| Ok(vec![1u8; 256]));
        let claim = claim_with(json!({"kind": "git", "url": "https://github.com/o/r.git"}));
        let input = prepare_git_input(Some(&app), &claim, &claim.input["source"])
            .await
            .expect("mints");
        assert_eq!(input["source"]["token"], "ghs_from_the_app");
        // The mint is cached: a second job does not call the API again.
        let again = prepare_git_input(Some(&app), &claim, &claim.input["source"])
            .await
            .expect("cached");
        assert_eq!(again["source"]["token"], "ghs_from_the_app");
    }

    #[tokio::test]
    async fn a_token_never_goes_to_a_host_the_request_chose() {
        let api = github_api("ghs_from_the_app").await;
        let app = GitHubApp::with_signer("2333879", "12345678", &api, |_| Ok(vec![1u8; 256]));
        for url in [
            "https://gitlab.com/o/r.git",
            "https://evil.example/github.com/o/r.git",
            "https://github.com.evil.example/o/r.git",
        ] {
            let claim = claim_with(json!({"kind": "git", "url": url}));
            let input = prepare_git_input(Some(&app), &claim, &claim.input["source"])
                .await
                .expect("passes through");
            assert!(
                input["source"].get("token").is_none(),
                "{url} must not receive a token"
            );
        }
    }

    #[tokio::test]
    async fn no_app_means_an_anonymous_clone_not_a_failure() {
        let claim = claim_with(json!({"kind": "git", "url": "https://github.com/o/r.git"}));
        let input = prepare_git_input(None, &claim, &claim.input["source"])
            .await
            .expect("unchanged");
        assert_eq!(input, claim.input);
    }
}
