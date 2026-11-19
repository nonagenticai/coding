//! Worker integration tests against a real Postgres 18.
//!
//! `TEST_DATABASE_URL` is REQUIRED. These tests fail, never skip, when it is absent. Each test
//! creates its own schema and points its pool's `search_path` at it, so the suite can run in
//! parallel against one database without one test reaping another's leases.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use coding::db;
use coding::domain::{JobError, JobKind, JobStatus};
use coding::error::AppError;
use coding::store::{self, JobClaim};
use coding::worker::{self, ExecutorFuture, JobExecutor, WorkerSettings};

const TENANT: &str = "worker-test";

/// The lease tests only need an executor that fails without a cluster; M1's production
/// executor is the sandbox and is tested separately.
fn failing_executor() -> Box<JobExecutor> {
    Box::new(|_claim: JobClaim| -> ExecutorFuture {
        Box::pin(async {
            Err(JobError {
                code: "not_implemented".to_owned(),
                message: "lease tests run without a sandbox".to_owned(),
                details: None,
                retryable: false,
            })
        })
    })
}

type WorkerHandle = JoinHandle<Result<(), AppError>>;

/// One test's private schema in the shared test database.
struct TestDb {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

// The clock is not fine-grained enough on every OS for two tests starting in parallel to get
// distinct nanos, so add a process-local sequence as well.
static SCHEMA_SEQ: AtomicU64 = AtomicU64::new(0);

impl TestDb {
    async fn new() -> Self {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must point at a real Postgres 18: worker tests never skip");
        let options = PgConnectOptions::from_str(&url).expect("parse TEST_DATABASE_URL");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .expect("connect TEST_DATABASE_URL");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let seq = SCHEMA_SEQ.fetch_add(1, Ordering::Relaxed);
        let schema = format!("worker_{}_{nanos}_{seq}", std::process::id());
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await
            .expect("create test schema");
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options.options([("search_path", schema.clone())]))
            .await
            .expect("connect test schema");
        db::migrate(&pool).await.expect("migrate test schema");
        Self {
            pool,
            admin,
            schema,
        }
    }

    async fn finish(self) {
        let Self {
            pool,
            admin,
            schema,
        } = self;
        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .execute(&admin)
            .await
            .expect("drop test schema");
        admin.close().await;
    }
}

fn fast_settings() -> WorkerSettings {
    WorkerSettings {
        lease_seconds: 60,
        heartbeat_interval: Duration::from_secs(15),
        poll_interval: Duration::from_millis(10),
        reap_interval: Duration::from_secs(30),
        pod_deadline_seconds: 900,
        concurrency: 4,
    }
}

async fn enqueue(pool: &PgPool, count: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for index in 0..count {
        let job = store::create_job(
            pool,
            TENANT,
            JobKind::Test,
            None,
            &json!({ "index": index }),
            None,
            900,
        )
        .await
        .expect("enqueue job");
        ids.push(job.id);
    }
    ids
}

/// Spawn a worker that stops when the returned sender fires.
fn spawn_worker(
    pool: &PgPool,
    worker_id: &str,
    settings: WorkerSettings,
    executor: Box<worker::JobExecutor>,
) -> (WorkerHandle, oneshot::Sender<()>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let handle = tokio::spawn(worker::run_with(
        pool.clone(),
        worker_id.to_owned(),
        async move {
            let _ = stopped.await;
        },
        settings,
        executor,
    ));
    (handle, stop)
}

async fn wait_terminal(pool: &PgPool, ids: &[String], timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let terminal: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs
             WHERE id::text = ANY($1) AND status IN ('succeeded', 'failed', 'cancelled')",
        )
        .bind(ids.to_vec())
        .fetch_one(pool)
        .await
        .expect("count terminal jobs");
        if terminal == i64::try_from(ids.len()).expect("id count") {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {terminal}/{} jobs reached a terminal state; states: {}",
            ids.len(),
            job_summary(pool, ids).await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `status:attempts` per job, for a failure message that says what actually happened.
async fn job_summary(pool: &PgPool, ids: &[String]) -> String {
    sqlx::query_scalar(
        "SELECT COALESCE(string_agg(status || ':' || attempts, ',' ORDER BY queued_at), 'none')
         FROM jobs WHERE id::text = ANY($1)",
    )
    .bind(ids.to_vec())
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| format!("<summary failed: {e}>"))
}

/// Block until the job's lease has lapsed (robust against a slow test machine).
async fn wait_lease_expired(pool: &PgPool, id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let expired: Option<bool> = sqlx::query_scalar(
            "SELECT lease_expires_at IS NOT NULL AND lease_expires_at < now()
             FROM jobs WHERE id = $1::uuid",
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read lease");
        if expired == Some(true) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lease never expired"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn twenty_five_jobs_complete_exactly_once() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 25).await;
    let (mut handle, stop) = spawn_worker(&db.pool, "w25", fast_settings(), failing_executor());

    tokio::select! {
        result = &mut handle => panic!("worker exited before finishing the jobs: {result:?}"),
        () = wait_terminal(&db.pool, &ids, Duration::from_secs(30)) => {}
    }
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");

    let failed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE id::text = ANY($1) AND status = 'failed'",
    )
    .bind(ids.clone())
    .fetch_one(&db.pool)
    .await
    .expect("count failed");
    assert_eq!(
        failed, 25,
        "M0 has no executor: every job must end failed, never succeeded"
    );

    let once: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE id::text = ANY($1) AND attempts = 1")
            .bind(ids.clone())
            .fetch_one(&db.pool)
            .await
            .expect("count attempts");
    assert_eq!(once, 25, "a job was executed more than once");

    let started: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_events WHERE kind = 'started' AND job_id::text = ANY($1)",
    )
    .bind(ids.clone())
    .fetch_one(&db.pool)
    .await
    .expect("count started events");
    assert_eq!(started, 25, "a job got more than one started event");

    let not_executed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs
         WHERE id::text = ANY($1)
           AND error ->> 'code' = 'not_implemented'
           AND error ->> 'retryable' = 'false'",
    )
    .bind(ids)
    .fetch_one(&db.pool)
    .await
    .expect("count placeholder outcomes");
    assert_eq!(
        not_executed, 25,
        "M0 must report not_implemented, never a fake success"
    );

    db.finish().await;
}

#[tokio::test]
async fn a_killed_worker_lease_is_reaped_and_the_job_completes_on_the_second_attempt() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;
    let id = ids[0].clone();

    // An executor that reports it started and then never resolves: dropping the worker future
    // below is exactly a worker dying mid-flight.
    let started = Arc::new(Notify::new());
    let executor: Box<worker::JobExecutor> = {
        let started = Arc::clone(&started);
        Box::new(move |_claim: JobClaim| -> ExecutorFuture {
            let started = Arc::clone(&started);
            Box::pin(async move {
                started.notify_one();
                std::future::pending::<Result<Value, JobError>>().await
            })
        })
    };
    let settings = WorkerSettings {
        lease_seconds: 1,
        heartbeat_interval: Duration::from_secs(60),
        poll_interval: Duration::from_millis(10),
        reap_interval: Duration::from_secs(60),
        pod_deadline_seconds: 900,
        concurrency: 1,
    };

    let mut worker_fut = Box::pin(worker::run_with(
        db.pool.clone(),
        "w-killed",
        std::future::pending::<()>(),
        settings,
        executor,
    ));
    tokio::select! {
        biased;
        () = started.notified() => {}
        result = &mut worker_fut => panic!("worker finished before it could be killed: {result:?}"),
    }
    drop(worker_fut);

    let mid_flight: (String, i32) =
        sqlx::query_as("SELECT status, attempts FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .expect("read mid-flight job");
    assert_eq!(mid_flight, ("running".to_owned(), 1));

    wait_lease_expired(&db.pool, &id).await;
    let (requeued, failed) = store::reap_expired(&db.pool).await.expect("reap");
    assert_eq!(
        (requeued, failed),
        (1, 0),
        "an abandoned lease with attempts left must be requeued"
    );

    let reclaimed: (String, Option<String>, i32) =
        sqlx::query_as("SELECT status, lease_owner, attempts FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .expect("read reclaimed job");
    assert_eq!(reclaimed.0, "queued");
    assert_eq!(reclaimed.1, None);
    assert_eq!(reclaimed.2, 1, "the first attempt stays counted");

    let (handle, stop) = spawn_worker(&db.pool, "w-second", fast_settings(), failing_executor());
    wait_terminal(&db.pool, &ids, Duration::from_secs(30)).await;
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");

    let finished: (String, i32) =
        sqlx::query_as("SELECT status, attempts FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .expect("read finished job");
    assert_eq!(
        finished,
        ("failed".to_owned(), 2),
        "the reclaimed job was attempted a second time and reached a terminal state"
    );

    db.finish().await;
}

#[tokio::test]
async fn attempts_never_exceed_max_attempts_and_an_exhausted_job_fails_as_lease_expired() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;

    // One attempt is the whole budget for this job.
    sqlx::query("UPDATE jobs SET max_attempts = 1 WHERE id = $1::uuid")
        .bind(&ids[0])
        .execute(&db.pool)
        .await
        .expect("lower the attempt budget");

    let claim = store::claim_job(&db.pool, "w-dead", 1)
        .await
        .expect("claim")
        .expect("one job is queued");
    assert_eq!(claim.attempts, 1);

    wait_lease_expired(&db.pool, &ids[0]).await;
    let (requeued, failed) = store::reap_expired(&db.pool).await.expect("reap");
    assert_eq!(
        (requeued, failed),
        (0, 1),
        "an exhausted lease must fail, not requeue"
    );

    let row: (String, i32, i32, Option<String>) = sqlx::query_as(
        "SELECT status, attempts, max_attempts, error ->> 'code'
         FROM jobs WHERE id = $1::uuid",
    )
    .bind(&ids[0])
    .fetch_one(&db.pool)
    .await
    .expect("read exhausted job");
    assert_eq!(row.0, "failed");
    assert_eq!(row.1, 1);
    assert_eq!(row.2, 1);
    assert_eq!(row.3.as_deref(), Some("lease_expired"));

    let over: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE attempts > max_attempts")
        .fetch_one(&db.pool)
        .await
        .expect("count over-budget jobs");
    assert_eq!(over, 0, "an attempt budget was exceeded");

    // A worker must not pick an exhausted job up again.
    let (handle, stop) = spawn_worker(&db.pool, "w-after", fast_settings(), failing_executor());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");
    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM jobs WHERE id = $1::uuid")
        .bind(&ids[0])
        .fetch_one(&db.pool)
        .await
        .expect("read attempts");
    assert_eq!(attempts, 1, "a failed job was claimed again");

    db.finish().await;
}

#[tokio::test]
async fn a_job_cancelled_while_queued_never_runs() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;
    let cancelled = store::cancel_job(&db.pool, TENANT, &ids[0])
        .await
        .expect("cancel queued job");
    assert_eq!(cancelled.status, JobStatus::Cancelled);

    let (handle, stop) = spawn_worker(&db.pool, "w-cancel", fast_settings(), failing_executor());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");

    let row: (String, i32, Option<Value>) =
        sqlx::query_as("SELECT status, attempts, result FROM jobs WHERE id = $1::uuid")
            .bind(&ids[0])
            .fetch_one(&db.pool)
            .await
            .expect("read cancelled job");
    assert_eq!(row.0, "cancelled");
    assert_eq!(row.1, 0, "a cancelled queued job must never be claimed");
    assert!(row.2.is_none(), "a cancelled job has no result");

    let started: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_events WHERE job_id = $1::uuid AND kind = 'started'",
    )
    .bind(&ids[0])
    .fetch_one(&db.pool)
    .await
    .expect("count started events");
    assert_eq!(started, 0, "a cancelled queued job must never start");

    db.finish().await;
}

#[tokio::test]
async fn a_job_cancelled_while_running_ends_cancelled_and_discards_its_result() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;
    let id = ids[0].clone();

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let executor: Box<worker::JobExecutor> = {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        Box::new(move |_claim: JobClaim| -> ExecutorFuture {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                // If the worker recorded this, the cancellation check would be broken.
                Ok(json!({"executed": true, "must_be": "discarded"}))
            })
        })
    };

    let (handle, stop) = spawn_worker(&db.pool, "w-run-cancel", fast_settings(), executor);
    started.notified().await;

    let running = store::cancel_job(&db.pool, TENANT, &id)
        .await
        .expect("cancel running job");
    assert_eq!(running.status, JobStatus::Running, "cancel is a request");
    release.notify_one();

    wait_terminal(&db.pool, &ids, Duration::from_secs(10)).await;
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");

    let row: (String, Option<Value>) =
        sqlx::query_as("SELECT status, result FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .expect("read cancelled job");
    assert_eq!(row.0, "cancelled");
    assert!(
        row.1.is_none(),
        "a job cancelled while running must not record its result"
    );

    db.finish().await;
}

#[tokio::test]
async fn jobs_overlap_up_to_the_concurrency_limit() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 4).await;

    // Four jobs, two slots: the jobs must arrive in pairs. A barrier of two makes the limit
    // observable without timing: with one slot the lone waiter times out, with four slots all
    // four are in flight and the maximum reaches four.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let executor: Box<worker::JobExecutor> = {
        let barrier = Arc::clone(&barrier);
        let in_flight = Arc::clone(&in_flight);
        let peak = Arc::clone(&peak);
        Box::new(move |_claim: JobClaim| -> ExecutorFuture {
            let barrier = Arc::clone(&barrier);
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            Box::pin(async move {
                peak.fetch_max(
                    in_flight.fetch_add(1, Ordering::SeqCst) + 1,
                    Ordering::SeqCst,
                );
                let paired = tokio::time::timeout(Duration::from_secs(5), barrier.wait()).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                paired.map_err(|_| JobError {
                    code: "no_overlap".to_owned(),
                    message: "a second slot never arrived".to_owned(),
                    details: None,
                    retryable: false,
                })?;
                Ok(json!({"ok": true}))
            })
        })
    };

    let mut settings = fast_settings();
    settings.concurrency = 2;
    let (mut handle, stop) = spawn_worker(&db.pool, "w-overlap", settings, executor);

    tokio::select! {
        result = &mut handle => panic!("worker exited before finishing the jobs: {result:?}"),
        () = wait_terminal(&db.pool, &ids, Duration::from_secs(20)) => {}
    }
    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");

    assert_eq!(
        peak.load(Ordering::SeqCst),
        2,
        "the concurrency limit is exactly two (one = serial, four = the limit was ignored)"
    );
    let succeeded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE id::text = ANY($1) AND status = 'succeeded'",
    )
    .bind(ids)
    .fetch_one(&db.pool)
    .await
    .expect("count succeeded jobs");
    assert_eq!(
        succeeded, 4,
        "all four jobs must pass through the barrier in pairs"
    );

    db.finish().await;
}

#[tokio::test]
async fn a_stale_lease_token_cannot_finish_a_reclaimed_job() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;
    let id = ids[0].clone();

    let first = store::claim_job(&db.pool, "w-fence", 60)
        .await
        .expect("claim")
        .expect("one job is queued");
    sqlx::query(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1::uuid",
    )
    .bind(&id)
    .execute(&db.pool)
    .await
    .expect("expire the lease");
    assert_eq!(
        store::reap_expired(&db.pool).await.expect("reap"),
        (1, 0),
        "the abandoned lease is requeued"
    );

    let second = store::claim_job(&db.pool, "w-fence", 60)
        .await
        .expect("re-claim")
        .expect("the requeued job is claimable");
    assert_ne!(
        first.lease_owner, second.lease_owner,
        "each claim mints its own fencing token"
    );

    assert!(
        !store::heartbeat(&db.pool, &first.lease_owner, &id, 60)
            .await
            .expect("stale heartbeat"),
        "a stale token must not renew the new claim's lease"
    );
    assert!(
        !store::complete_job(&db.pool, &first.lease_owner, &id, &json!({"stale": true}))
            .await
            .expect("stale complete"),
        "a stale slot must not complete a job it no longer owns"
    );
    assert!(
        store::complete_job(&db.pool, &second.lease_owner, &id, &json!({"ok": true}))
            .await
            .expect("owner complete"),
        "the current owner completes the job"
    );

    let row: (String, Option<Value>) =
        sqlx::query_as("SELECT status, result FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .expect("read finished job");
    assert_eq!(row.0, "succeeded");
    assert_eq!(row.1, Some(json!({"ok": true})));

    db.finish().await;
}

#[tokio::test]
async fn a_panicking_slot_stops_its_heartbeat_and_the_job_is_reclaimed() {
    let db = TestDb::new().await;
    let ids = enqueue(&db.pool, 1).await;
    let id = ids[0].clone();

    let executor: Box<worker::JobExecutor> = Box::new(|_claim: JobClaim| -> ExecutorFuture {
        Box::pin(async { panic!("the executor died mid-run") })
    });
    let settings = WorkerSettings {
        lease_seconds: 1,
        // Faster than the lease on purpose: a detached heartbeat would renew it forever.
        heartbeat_interval: Duration::from_millis(200),
        poll_interval: Duration::from_millis(10),
        reap_interval: Duration::from_secs(60),
        pod_deadline_seconds: 900,
        concurrency: 1,
    };
    let (handle, stop) = spawn_worker(&db.pool, "w-panic", settings, executor);

    // The slot panics after the claim. The loop must survive it and the lease must lapse.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "a panicked slot must not stop the worker loop"
    );

    wait_lease_expired(&db.pool, &id).await;
    let (requeued, failed) = store::reap_expired(&db.pool).await.expect("reap");
    assert_eq!(
        (requeued, failed),
        (1, 0),
        "the panicked slot's job must be requeued, not left running forever"
    );

    let _ = stop.send(());
    handle.await.expect("worker task").expect("worker loop");
    db.finish().await;
}
