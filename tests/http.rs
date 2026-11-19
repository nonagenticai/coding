//! HTTP-level tests.
//!
//! Validation and routing tests need no database: the pool is lazy and never connects. The one
//! create-job happy path must reach the store, so it runs against `TEST_DATABASE_URL` and fails
//! closed when that is absent (never skips) — the same rule as `tests/worker.rs`.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use http_body_util::BodyExt;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

use coding::tenant::TenantId;
use coding::{AppState, app, openapi};

fn state_with(default_tenant: Option<&str>) -> AppState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("lazy pool");
    AppState {
        pool,
        default_tenant: default_tenant.map(str::to_owned),
    }
}

fn state() -> AppState {
    state_with(None)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).expect("json body")
}

#[tokio::test]
async fn health_returns_ok() {
    for path in ["/health", "/api/health"] {
        let resp = app(state())
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
        assert_eq!(body_json(resp).await, serde_json::json!({ "status": "ok" }));
    }
}

#[tokio::test]
async fn unknown_route_is_404() {
    let resp = app(state())
        .oneshot(Request::get("/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn openapi_documents_the_job_contract() {
    let spec = serde_json::to_value(openapi()).unwrap();
    assert!(spec["openapi"].as_str().unwrap().starts_with("3."));
    assert_eq!(spec["info"]["title"], env!("CARGO_PKG_NAME"));
    for (path, method) in [
        ("/health", "get"),
        ("/api/health", "get"),
        ("/api/health/ready", "get"),
        ("/api/jobs", "post"),
        ("/api/jobs/status", "post"),
        ("/api/jobs/{id}", "get"),
        ("/api/jobs/{id}/cancel", "post"),
        ("/api/jobs/{id}/events", "get"),
        ("/api/sources", "post"),
        ("/api/sources/{id}/blob", "get"),
    ] {
        assert!(
            spec["paths"][path][method].is_object(),
            "missing {method} {path} in OpenAPI: {spec:#}"
        );
    }
}

#[tokio::test]
async fn job_routes_reject_a_missing_tenant() {
    for path in ["/api/jobs", "/api/jobs/status"] {
        let resp = app(state())
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"kind":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{path}");
        let body = body_json(resp).await;
        assert_eq!(body["error"]["code"], "unauthorized", "{body}");
    }
}

fn tenant_app(default: Option<&'static str>) -> Router {
    Router::new()
        .route("/t", get(|t: TenantId| async move { t.0 }))
        .with_state(state_with(default))
}

async fn tenant_call(default: Option<&'static str>, header: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut req = Request::get("/t");
    if let Some(h) = header {
        req = req.header("x-tenant-id", h);
    }
    let resp = tenant_app(default)
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, body)
}

#[tokio::test]
async fn tenant_extractor_accepts_trusted_header() {
    let (status, body) = tenant_call(None, Some("5f868d42-aaaa-bbbb")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"5f868d42-aaaa-bbbb");
}

#[tokio::test]
async fn tenant_extractor_rejects_missing_or_invalid_header() {
    for h in [None, Some(""), Some("  "), Some("a b"), Some("../x")] {
        let (status, body) = tenant_call(None, h).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "header {h:?}");
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("X-Tenant-Id"),
            "{v}"
        );
    }
    let long = "a".repeat(129);
    assert_eq!(
        tenant_call(None, Some(&long)).await.0,
        StatusCode::UNAUTHORIZED
    );
}

/// A deployment that names a default tenant accepts in-cluster callers with no header; an
/// explicit header still wins.
#[tokio::test]
async fn configured_default_tenant_is_used_only_when_the_header_is_absent() {
    let (status, body) = tenant_call(Some("platform"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"platform");

    let (status, body) = tenant_call(Some("platform"), Some("explicit")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"explicit");
}

async fn post_job(
    state: AppState,
    tenant: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    app(state)
        .oneshot(
            Request::post("/api/jobs")
                .header("content-type", "application/json")
                .header("x-tenant-id", tenant)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn create_job_without_a_source_is_400_for_test_lint_build() {
    for kind in ["test", "lint", "build"] {
        let resp = post_job(
            state(),
            "contract-tenant",
            serde_json::json!({ "kind": kind }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{kind}");
        let body = body_json(resp).await;
        assert_eq!(body["error"]["code"], "invalid_job_input", "{kind}: {body}");
        assert_eq!(
            body["error"]["message"],
            format!("kind {kind} requires input.source or input.sources")
        );
        assert_eq!(body["error"]["retryable"], false);
    }
}

#[tokio::test]
async fn create_job_with_an_escaping_path_is_400() {
    let resp = post_job(
        state(),
        "contract-tenant",
        serde_json::json!({
            "kind": "test",
            "input": {"source": {"kind": "inline", "files": [
                {"path": "../escape.rs", "text": "fn main() {}"}
            ]}}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "invalid_job_input");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("escapes the source tree"),
        "{body}"
    );
}

#[tokio::test]
async fn create_job_with_an_oversize_inline_source_is_400() {
    let text = "x".repeat(48 * 1024 + 1);
    let resp = post_job(
        state(),
        "contract-tenant",
        serde_json::json!({
            "kind": "build",
            "input": {"source": {"kind": "inline", "files": [
                {"path": "src/lib.rs", "text": text}
            ]}}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "invalid_job_input");
    assert_eq!(
        body["error"]["message"],
        "inline source is 49153 bytes, over the 49152-byte cap"
    );
}

#[tokio::test]
async fn create_job_with_both_source_forms_is_400() {
    let resp = post_job(
        state(),
        "contract-tenant",
        serde_json::json!({
            "kind": "check",
            "input": {
                "source": {"kind": "git", "url": "https://github.com/o/one"},
                "sources": [{"name": "target", "profile": "rust",
                             "source": {"kind": "git", "url": "https://github.com/o/two"}}]
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "invalid_job_input");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("mutually exclusive")),
        "{body}"
    );
}

#[tokio::test]
async fn create_job_with_an_unsafe_source_name_is_400() {
    for name in ["Target", "../target", "target/one", ""] {
        let resp = post_job(
            state(),
            "contract-tenant",
            serde_json::json!({
                "kind": "check",
                "input": {"sources": [
                    {"name": name, "profile": "rust",
                     "source": {"kind": "git", "url": "https://github.com/o/r"}}
                ]}
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{name:?}");
        let body = body_json(resp).await;
        assert_eq!(body["error"]["code"], "invalid_job_input", "{name:?}");
    }
}

/// A tenant id unique to this process and run, so the DB-backed test cannot collide with another
/// test binary using the same Postgres.
fn unique_tenant(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

/// State backed by a real Postgres 18. Fails closed: a store test must never pass by skipping.
async fn state_with_db() -> (AppState, sqlx::PgPool) {
    let url = std::env::var("TEST_DATABASE_URL").expect(
        "TEST_DATABASE_URL must point at a real Postgres 18: the create-job HTTP test never skips",
    );
    let pool = coding::db::connect(&url)
        .await
        .expect("connect TEST_DATABASE_URL");
    coding::db::migrate(&pool)
        .await
        .expect("migrate TEST_DATABASE_URL");
    (
        AppState {
            pool: pool.clone(),
            default_tenant: None,
        },
        pool,
    )
}

#[tokio::test]
async fn create_job_with_an_inline_source_is_202_and_stores_the_typed_input() {
    let (state, pool) = state_with_db().await;
    let tenant = unique_tenant("http-inline");

    let resp = post_job(
        state,
        &tenant,
        serde_json::json!({
            "kind": "test",
            "input": {"source": {"kind": "inline", "files": [
                {"path": "Cargo.toml", "text": "[package]\nname = \"x\"\n"}
            ]}}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(body["kind"], "test");
    assert_eq!(body["status"], "queued");
    assert!(
        body["id"].as_str().is_some_and(|id| !id.is_empty()),
        "{body}"
    );

    // The source survives the trip to jsonb: the route serialized the typed input, it did not
    // drop it or store the raw request body.
    let stored: serde_json::Value =
        sqlx::query_scalar("SELECT input FROM jobs WHERE tenant_id = $1")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("stored job");
    assert_eq!(stored["source"]["kind"], "inline");
    assert_eq!(stored["source"]["files"][0]["path"], "Cargo.toml");
    assert_eq!(
        stored["source"]["files"][0]["text"],
        serde_json::Value::String("[package]\nname = \"x\"\n".to_owned())
    );

    sqlx::query("DELETE FROM jobs WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}

#[tokio::test]
async fn a_multi_source_job_is_202_and_stores_every_named_source() {
    let (state, pool) = state_with_db().await;
    let tenant = unique_tenant("http-sources");

    let resp = post_job(
        state,
        &tenant,
        serde_json::json!({
            "kind": "check",
            "input": {"sources": [
                {"name": "target", "profile": "rust",
                 "source": {"kind": "git", "url": "https://github.com/o/target-rs", "ref": "abc"}},
                {"name": "source-1", "profile": "python",
                 "source": {"kind": "git", "url": "https://github.com/o/source-py", "ref": "def"}}
            ]}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "queued");

    let stored: serde_json::Value =
        sqlx::query_scalar("SELECT input FROM jobs WHERE tenant_id = $1")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("stored job");
    assert!(stored.get("source").is_none());
    assert_eq!(stored["sources"][0]["name"], "target");
    assert_eq!(stored["sources"][0]["profile"], "rust");
    assert_eq!(stored["sources"][1]["profile"], "python");
    assert_eq!(stored["sources"][1]["source"]["ref"], "def");

    sqlx::query("DELETE FROM jobs WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}

#[tokio::test]
async fn a_python_job_stores_its_profile_and_a_rust_job_omits_the_default() {
    let (state, pool) = state_with_db().await;
    let tenant = unique_tenant("http-profile");

    let resp = post_job(
        state.clone(),
        &tenant,
        serde_json::json!({
            "kind": "lint",
            "input": {
                "profile": "python",
                "source": {"kind": "inline", "files": [
                    {"path": "pyproject.toml", "text": "[project]\nname = \"x\"\n"}
                ]}
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let python: serde_json::Value = sqlx::query_scalar(
        "SELECT input FROM jobs WHERE tenant_id = $1 AND input->>'profile' = 'python'",
    )
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("the python job's stored input");
    assert_eq!(python["source"]["kind"], "inline");

    let resp = post_job(
        state,
        &tenant,
        serde_json::json!({
            "kind": "lint",
            "input": {"source": {"kind": "inline", "files": [
                {"path": "Cargo.toml", "text": "[package]\nname = \"x\"\n"}
            ]}}
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    // The default is not serialized: a runner image built before profiles existed denies
    // unknown fields, so a Rust job must store exactly the input it stored before.
    let rust_with_a_profile: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs
          WHERE tenant_id = $1 AND input ? 'profile' AND input->>'profile' <> 'python'",
    )
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("count rust jobs carrying a profile");
    assert_eq!(
        rust_with_a_profile, 0,
        "a rust job must not carry a profile key"
    );

    sqlx::query("DELETE FROM jobs WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}

#[tokio::test]
async fn an_unknown_profile_is_rejected_at_the_edge() {
    let resp = post_job(
        state(),
        "contract-tenant",
        serde_json::json!({
            "kind": "lint",
            "input": {
                "profile": "golang",
                "source": {"kind": "inline", "files": [{"path": "go.mod", "text": "module x\n"}]}
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

/// A small, valid archive: the same packer the client uses.
fn sample_archive() -> Vec<u8> {
    coding::source_archive::pack(&[
        coding::source_archive::PackFile {
            path: "Cargo.toml".to_owned(),
            data: b"[package]\nname = \"sample\"\n".to_vec(),
            executable: false,
        },
        coding::source_archive::PackFile {
            path: "src/lib.rs".to_owned(),
            data: b"pub fn f() {}\n".to_vec(),
            executable: false,
        },
    ])
    .expect("pack")
}

async fn post_source(state: AppState, tenant: &str, body: Vec<u8>) -> axum::response::Response {
    app(state)
        .oneshot(
            Request::post("/api/sources")
                .header("content-type", "application/gzip")
                .header("x-tenant-id", tenant)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn upload_rejects_a_missing_tenant_a_bad_archive_and_an_oversize_body() {
    let resp = app(state())
        .oneshot(
            Request::post("/api/sources")
                .header("content-type", "application/gzip")
                .body(Body::from(sample_archive()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = post_source(state(), "contract-tenant", b"not a gzip tar".to_vec()).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "invalid_request", "{body}");

    // One byte over the cap, and the route refuses before storing anything.
    let mut oversize = sample_archive();
    oversize.resize(coding::source_archive::MAX_COMPRESSED_BYTES + 1, 0);
    let resp = post_source(state(), "contract-tenant", oversize).await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "payload_too_large", "{body}");
}

/// An archive the client would never build: a real gzip tar whose single entry escapes the tree.
fn escaping_archive() -> Vec<u8> {
    use std::io::Write;
    let mut tar = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(1);
    header.set_mode(0o644);
    header.set_mtime(0);
    let name = b"../evil";
    let gnu = header.as_gnu_mut().expect("gnu header");
    gnu.name[..name.len()].copy_from_slice(name);
    header.set_cksum();
    tar.append(&header, &b"x"[..]).expect("hostile entry");
    let raw = tar.into_inner().expect("tar");
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&raw).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn upload_rejects_a_path_that_escapes_the_tree() {
    let resp = post_source(state(), "contract-tenant", escaping_archive()).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("escapes"),
        "{body}"
    );
}

#[tokio::test]
async fn an_uploaded_source_round_trips_and_needs_its_token() {
    let (state, pool) = state_with_db().await;
    let tenant = unique_tenant("http-source");
    let blob = sample_archive();
    let sha = coding::source_archive::sha256_hex(&blob);

    let resp = post_source(state.clone(), &tenant, blob.clone()).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp).await;
    let id = created["id"].as_str().expect("id").to_owned();
    assert_eq!(created["sha256"], sha.as_str());
    assert_eq!(created["files"], 2);
    assert_eq!(created["bytes"], blob.len());

    // Nobody can read it without the token the worker mints — and the id alone must not say
    // whether it exists, so a bad token is the same answer as an unknown id.
    let resp = app(state.clone())
        .oneshot(
            Request::get(format!("/api/sources/{id}/blob"))
                .header("x-source-token", "f".repeat(64))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // The worker's mint is the only way in.
    let job_for_token = "11111111-1111-4111-8111-111111111111";
    let (token, minted_sha) = coding::store::mint_source_token(&pool, &tenant, job_for_token, &id)
        .await
        .expect("mint")
        .expect("a live source");
    assert_eq!(minted_sha, sha);

    let resp = app(state.clone())
        .oneshot(
            Request::get(format!("/api/sources/{id}/blob"))
                .header("x-source-token", &token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/gzip")
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.to_vec(), blob);

    // A re-mint for the same job hands back the same token: a worker that re-encounters its own
    // Pod (409 reattach) must give that Pod the token it is already holding.
    let (again, _) = coding::store::mint_source_token(&pool, &tenant, job_for_token, &id)
        .await
        .expect("mint again")
        .expect("a live source");
    assert_eq!(again, token, "a live token for the same job is reused");

    // Another job gets its own token, which rotates the row.
    let (other, _) = coding::store::mint_source_token(
        &pool,
        &tenant,
        "22222222-2222-4222-8222-222222222222",
        &id,
    )
    .await
    .expect("mint for another job")
    .expect("a live source");
    assert_ne!(other, token);
    let resp = app(state)
        .oneshot(
            Request::get(format!("/api/sources/{id}/blob"))
                .header("x-source-token", &token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "the previous job's token is revoked by the rotate"
    );

    sqlx::query("DELETE FROM sources WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}

#[tokio::test]
async fn a_job_cannot_name_another_tenants_source() {
    let (state, pool) = state_with_db().await;
    let owner = unique_tenant("http-source-owner");
    let other = unique_tenant("http-source-other");
    let created = body_json(post_source(state.clone(), &owner, sample_archive()).await).await;
    let id = created["id"].as_str().expect("id").to_owned();

    let resp = post_job(
        state.clone(),
        &other,
        serde_json::json!({
            "kind": "check",
            "input": {"source": {"kind": "upload", "id": id}},
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "not_found", "{body}");

    // The same rule inside the multi-source form: a foreign id is refused at create time, not
    // accepted 202 and failed later by the worker.
    let resp = post_job(
        state.clone(),
        &other,
        serde_json::json!({
            "kind": "check",
            "input": {"sources": [
                {"name": "target", "profile": "rust",
                 "source": {"kind": "upload", "id": id}}
            ]},
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "not_found", "{body}");

    sqlx::query("DELETE FROM sources WHERE tenant_id = $1")
        .bind(&owner)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}

#[tokio::test]
// reason: `state` is the app state and `stale` is an upload's age; both names are earned.
#[allow(clippy::similar_names)]
async fn the_source_reaper_deletes_only_unreferenced_old_uploads() {
    async fn store(pool: &sqlx::PgPool, tenant: &str, age_hours: i64) -> String {
        let blob = sample_archive();
        let id = coding::store::create_source(
            pool,
            tenant,
            &coding::source_archive::sha256_hex(&blob),
            i64::try_from(blob.len()).expect("the sample archive fits in i64"),
            2,
            &blob,
        )
        .await
        .expect("store");
        sqlx::query("UPDATE sources SET created_at = now() - make_interval(hours => $2::int) WHERE id = $1::uuid")
            .bind(&id)
            .bind(age_hours)
            .execute(pool)
            .await
            .expect("age");
        id
    }

    let (state, pool) = state_with_db().await;
    let tenant = unique_tenant("http-source-reap");

    let stale = store(&pool, &tenant, 3).await;
    let referenced = store(&pool, &tenant, 3).await;
    // The multi-source form references an upload through `sources[].source.id`, not `source.id`.
    let referenced_multi = store(&pool, &tenant, 3).await;
    let fresh = store(&pool, &tenant, 0).await;

    // A queued job still wants its archive, however old it is.
    let _ = post_job(
        state.clone(),
        &tenant,
        serde_json::json!({
            "kind": "check",
            "input": {"source": {"kind": "upload", "id": referenced}},
        }),
    )
    .await;
    let _ = post_job(
        state.clone(),
        &tenant,
        serde_json::json!({
            "kind": "check",
            "input": {"sources": [
                {"name": "target", "profile": "rust",
                 "source": {"kind": "upload", "id": referenced_multi}}
            ]},
        }),
    )
    .await;

    let reaped = coding::store::reap_sources(&pool, 2 * 60 * 60)
        .await
        .expect("reap");
    assert!(reaped >= 1, "the stale upload must be gone");

    for (id, expected) in [
        (&stale, false),
        (&referenced, true),
        (&referenced_multi, true),
        (&fresh, true),
    ] {
        let present: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM sources WHERE id = $1::uuid")
            .bind(id)
            .fetch_optional(&pool)
            .await
            .expect("read");
        assert_eq!(present.is_some(), expected, "source {id}");
    }

    sqlx::query("DELETE FROM jobs WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    sqlx::query("DELETE FROM sources WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}
