//! Worker-side `github_op` delivery: concurrency, retry and idempotency, against a stub GitHub
//! API and a real Postgres 18.
//!
//! `TEST_DATABASE_URL` is REQUIRED, exactly as in `tests/worker.rs`: these tests fail, never skip.
//! Each test gets its own schema so the suite can run in parallel against one database.

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};

use coding::db;
use coding::deliver::execute_with_allowlist;
use coding::domain::JobKind;
use coding::github_app::GitHubApp;
use coding::source_archive::{self, PackFile};
use coding::store::{self, JobClaim, LeaseOwner};

const TENANT: &str = "deliver-test";
const REPO: &str = "fixture/deliver";
const FILES: usize = 24;
/// The worker always claims a UUID job id; `mint_source_token` casts it to `uuid`.
const JOB_ID: &str = "00000000-0000-0000-0000-000000000001";

// ─────────────────────────────────────────────────────────────────────────────
// A private schema in the shared test database
// ─────────────────────────────────────────────────────────────────────────────

struct TestDb {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

static SCHEMA_SEQ: AtomicU64 = AtomicU64::new(0);

impl TestDb {
    async fn new() -> Self {
        let url = std::env::var("TEST_DATABASE_URL").expect(
            "TEST_DATABASE_URL must point at a real Postgres 18: delivery tests never skip",
        );
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
        let schema = format!("deliver_{}_{nanos}_{seq}", std::process::id());
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

// ─────────────────────────────────────────────────────────────────────────────
// The stub GitHub API
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Stub {
    inner: Arc<Mutex<StubInner>>,
}

#[derive(Default)]
struct StubInner {
    /// Every blob request, including the one the stub refuses.
    blob_requests: usize,
    blob_successes: usize,
    blobs_in_flight: usize,
    max_blobs_in_flight: usize,
    /// Refuse the first blob with a 503 and `Retry-After: 0`, once.
    fail_first_blob: bool,
    failed_once: bool,
    /// Whether the job's branch already exists (a retried attempt).
    ref_exists: bool,
    ref_updates: usize,
    pull_number: Option<u64>,
    pull_gets: usize,
    pull_posts: usize,
}

async fn serve(stub: Stub) -> String {
    let app = Router::new().fallback(api).with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let address = listener.local_addr().expect("stub address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{address}")
}

async fn api(State(stub): State<Stub>, request: Request) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();

    if path.starts_with("/app/installations/") {
        return Json(json!({"token": "ghs_delivery"})).into_response();
    }
    let base = format!("/repos/{REPO}");
    if method == Method::GET && path == base {
        return Json(json!({})).into_response();
    }
    if method == Method::GET && path == format!("{base}/git/ref/heads/main") {
        return Json(json!({"object": {"sha": "base-sha"}})).into_response();
    }
    if method == Method::POST && path == format!("{base}/git/blobs") {
        return blob(&stub).await;
    }
    if method == Method::POST && path == format!("{base}/git/trees") {
        return Json(json!({"sha": "tree-sha"})).into_response();
    }
    if method == Method::POST && path == format!("{base}/git/commits") {
        return Json(json!({"sha": "head-sha"})).into_response();
    }
    if method == Method::POST && path == format!("{base}/git/refs") {
        let mut inner = stub.inner.lock().expect("stub lock");
        if inner.ref_exists {
            return (StatusCode::UNPROCESSABLE_ENTITY, "reference already exists").into_response();
        }
        inner.ref_exists = true;
        return (StatusCode::CREATED, Json(json!({"ref": "refs/heads/…"}))).into_response();
    }
    if method == Method::GET && path.starts_with(&format!("{base}/git/ref/heads/")) {
        let inner = stub.inner.lock().expect("stub lock");
        return if inner.ref_exists {
            Json(json!({"object": {"sha": "head-sha"}})).into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        };
    }
    if method == Method::PATCH && path.starts_with(&format!("{base}/git/refs/heads/")) {
        stub.inner.lock().expect("stub lock").ref_updates += 1;
        return Json(json!({"object": {"sha": "head-sha"}})).into_response();
    }
    if path == format!("{base}/pulls") {
        let mut inner = stub.inner.lock().expect("stub lock");
        if method == Method::GET {
            inner.pull_gets += 1;
            return Json(match inner.pull_number {
                Some(number) => json!([{ "number": number, "html_url": pr_url(number) }]),
                None => json!([]),
            })
            .into_response();
        }
        inner.pull_posts += 1;
        inner.pull_number = Some(7);
        return (
            StatusCode::CREATED,
            Json(json!({"number": 7, "html_url": pr_url(7)})),
        )
            .into_response();
    }
    (StatusCode::NOT_FOUND, format!("unexpected {method} {path}")).into_response()
}

fn pr_url(number: u64) -> String {
    format!("https://example.test/{REPO}/pull/{number}")
}

async fn blob(stub: &Stub) -> Response {
    enum Step {
        Fail,
        Respond(usize),
    }
    let step = {
        let mut inner = stub.inner.lock().expect("stub lock");
        inner.blob_requests += 1;
        if inner.fail_first_blob && !inner.failed_once {
            inner.failed_once = true;
            Step::Fail
        } else {
            inner.blobs_in_flight += 1;
            inner.max_blobs_in_flight = inner.max_blobs_in_flight.max(inner.blobs_in_flight);
            Step::Respond(inner.blob_requests)
        }
    };
    match step {
        Step::Fail => {
            // Retry-After: 0 keeps the test fast; the retry path is what is under test.
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("retry-after", "0")],
                "later",
            )
                .into_response()
        }
        Step::Respond(number) => {
            // Hold the request so several uploads are genuinely in flight together.
            tokio::time::sleep(Duration::from_millis(30)).await;
            let mut inner = stub.inner.lock().expect("stub lock");
            inner.blobs_in_flight -= 1;
            inner.blob_successes += 1;
            Json(json!({"sha": format!("blob-{number}")})).into_response()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The test
// ─────────────────────────────────────────────────────────────────────────────

fn claim(source_id: &str) -> JobClaim {
    JobClaim {
        id: JOB_ID.to_owned(),
        tenant_id: TENANT.to_owned(),
        kind: JobKind::GithubOp,
        workspace_id: None,
        input: json!({
            "source": {"kind": "upload", "id": source_id},
            "output": {
                "repo": REPO,
                "base": "main",
                "branch_prefix": "coding/deliver",
                "title": "Deliver the tree",
                "body": "Body",
            },
        }),
        timeout_seconds: 60,
        attempts: 1,
        lease_owner: LeaseOwner::new("test-worker/unit"),
    }
}

async fn source(db: &TestDb) -> String {
    let files: Vec<PackFile> = (0..FILES)
        .map(|index| PackFile {
            path: format!("f{index:02}.txt"),
            data: format!("contents {index}").into_bytes(),
            executable: false,
        })
        .collect();
    let blob = source_archive::pack(&files).expect("pack");
    let sha = source_archive::sha256_hex(&blob);
    let bytes = i64::try_from(blob.len()).expect("archive length");
    let count = i32::try_from(files.len()).expect("file count");
    store::create_source(&db.pool, TENANT, &sha, bytes, count, &blob)
        .await
        .expect("store the source archive")
}

#[tokio::test]
async fn delivery_is_concurrent_retried_and_idempotent() {
    let db = TestDb::new().await;
    let stub = Stub::default();
    stub.inner.lock().expect("stub lock").fail_first_blob = true;
    let api = serve(stub.clone()).await;
    let app = GitHubApp::with_signer("1", "2", &api, |_| Ok(vec![1u8; 256]));
    let allow = vec![REPO.to_owned()];
    let source_id = source(&db).await;
    let claim = claim(&source_id);

    let first = execute_with_allowlist(&db.pool, Some(&app), claim.clone(), &allow)
        .await
        .expect("the first delivery succeeds");
    assert_eq!(first["pr_number"], 7);
    assert_eq!(first["branch"], format!("coding/deliver/{JOB_ID}"));
    let after_first = {
        let inner = stub.inner.lock().expect("stub lock");
        (
            inner.blob_successes,
            inner.blob_requests,
            inner.max_blobs_in_flight,
            inner.pull_posts,
        )
    };
    assert_eq!(after_first.0, FILES, "every file is one blob");
    assert_eq!(
        after_first.1,
        FILES + 1,
        "the refused blob was retried exactly once"
    );
    assert!(
        after_first.2 > 1,
        "blobs must upload concurrently, saw at most {}",
        after_first.2
    );
    assert!(
        after_first.2 <= 8,
        "concurrency is bounded, saw {}",
        after_first.2
    );
    assert_eq!(after_first.3, 1);

    // The same claim again is a lease retry: the branch exists, so it is moved to the new
    // commit and the open PR is reused instead of opening a twin.
    let second = execute_with_allowlist(&db.pool, Some(&app), claim, &allow)
        .await
        .expect("the retried delivery reuses the branch and PR");
    assert_eq!(second["pr_number"], 7);
    assert_eq!(second["branch"], first["branch"]);
    let after_second = {
        let inner = stub.inner.lock().expect("stub lock");
        (inner.ref_updates, inner.pull_posts, inner.pull_gets)
    };
    assert_eq!(after_second.0, 1, "the existing branch was moved once");
    assert_eq!(after_second.1, 1, "a retry must not open a second PR");
    assert_eq!(after_second.2, 2, "each attempt looked first");

    db.finish().await;
}
