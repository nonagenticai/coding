//! Sandbox behaviour without a cluster: a local axum stub speaks the five Kubernetes API
//! calls the client makes (`create`, `state`, `logs`, `delete`, `reap`). Nothing here needs
//! `kube`, a service account, or a running Pod.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use coding::domain::JobKind;
use coding::sandbox::{
    API_URL_ENV, CREATED_AT_ANNOTATION, LOG_BYTE_CAP, LOG_TRUNCATION_MARKER, NAMESPACE_ENV,
    PodState, REAP_AT_ANNOTATION, REAP_SLACK_SECONDS, RUNNER_IMAGE_ENV, Sandbox, SandboxConfig,
    SandboxError, TOKEN_ENV,
};
use coding::store::{JobClaim, LeaseOwner};
use coding::worker::execute_sandbox;

/// A digest-pinned image; the tag itself is irrelevant, the `@sha256:` is not.
const IMAGE: &str = "example.com/coding/runner@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

// ─────────────────────────────────────────────────────────────────────────────
// The stub API
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Stub {
    inner: Arc<Mutex<StubInner>>,
}

#[derive(Default)]
struct StubInner {
    /// `0` means `201 Created`; any other value is used for a canned response body.
    create_status: u16,
    create_body: Option<Value>,
    /// The Pod `GET .../pods/{name}` returns; `None` is `404`.
    pod: Option<Value>,
    /// The Pods `GET .../pods` lists, for the reaper.
    pods: Vec<Value>,
    log: String,
    deletes: Vec<String>,
    /// Every Pod manifest the client created, in order — what the worker actually sent.
    created: Vec<Value>,
}

fn stub(inner: StubInner) -> Stub {
    Stub {
        inner: Arc::new(Mutex::new(inner)),
    }
}

fn stub_app(stub: Stub) -> Router {
    Router::new()
        .route(
            "/api/v1/namespaces/{namespace}/pods",
            post(create_pod).get(list_pods),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}",
            get(get_pod).delete(delete_pod),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/log",
            get(get_log),
        )
        .with_state(stub)
}

async fn create_pod(State(stub): State<Stub>, Json(manifest): Json<Value>) -> Response {
    let inner = stub.inner.lock().expect("stub lock");
    if inner.create_status == StatusCode::CONFLICT.as_u16() {
        return (StatusCode::CONFLICT, "already exists").into_response();
    }
    if let Some(body) = &inner.create_body {
        let status = StatusCode::from_u16(inner.create_status).unwrap_or(StatusCode::OK);
        return (status, Json(body.clone())).into_response();
    }
    let name = manifest["metadata"]["name"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let mut inner = inner;
    inner.created.push(manifest);
    (
        StatusCode::CREATED,
        Json(json!({ "metadata": { "name": name } })),
    )
        .into_response()
}

async fn list_pods(State(stub): State<Stub>) -> Response {
    let inner = stub.inner.lock().expect("stub lock");
    (StatusCode::OK, Json(json!({ "items": inner.pods }))).into_response()
}

async fn get_pod(
    State(stub): State<Stub>,
    Path((_namespace, _name)): Path<(String, String)>,
) -> Response {
    let inner = stub.inner.lock().expect("stub lock");
    match &inner.pod {
        Some(pod) => (StatusCode::OK, Json(pod.clone())).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn get_log(
    State(stub): State<Stub>,
    Path((_namespace, _name)): Path<(String, String)>,
) -> Response {
    let inner = stub.inner.lock().expect("stub lock");
    (StatusCode::OK, inner.log.clone()).into_response()
}

async fn delete_pod(
    State(stub): State<Stub>,
    Path((_namespace, name)): Path<(String, String)>,
) -> Response {
    stub.inner.lock().expect("stub lock").deletes.push(name);
    StatusCode::OK.into_response()
}

async fn sandbox_for(stub: &Stub) -> Sandbox {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let address = listener.local_addr().expect("stub address");
    let app = stub_app(stub.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Sandbox::new(config(&format!("http://{address}"))).expect("sandbox client")
}

fn config(api_url: &str) -> SandboxConfig {
    SandboxConfig {
        namespace: "coding-runs".to_owned(),
        runner_image: IMAGE.to_owned(),
        profiles: vec![
            coding::domain::Profile::Rust,
            coding::domain::Profile::Python,
        ],
        multi_source: true,
        runtime_class: "coding-kata".to_owned(),
        api_url: api_url.to_owned(),
        token: "test-token".to_owned(),
        ca_pem: None,
        pull_secret: Some("registry-pull-secret".to_owned()),
        cache: None,
    }
}

/// A pool that never connects: the per-tick cancel check fails closed, is logged, and the
/// executor carries on. These tests are about the Pod lifecycle, not about Postgres.
fn detached_pool() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("lazy pool")
}

/// [`execute_sandbox`] with the pool these tests do not exercise.
async fn run_job(
    sandbox: &Sandbox,
    claim: JobClaim,
) -> Result<serde_json::Value, coding::domain::JobError> {
    execute_sandbox(sandbox, &detached_pool(), None, None, claim).await
}

fn claim(id: &str, kind: JobKind, timeout_seconds: i64) -> JobClaim {
    JobClaim {
        id: id.to_owned(),
        tenant_id: "tenant-a".to_owned(),
        kind,
        workspace_id: None,
        input: json!({ "repo": "example" }),
        timeout_seconds,
        attempts: 1,
        lease_owner: LeaseOwner::new("test-worker/unit"),
    }
}

fn env<'a>(pairs: &'a [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |key| {
        pairs
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Configuration is fail-closed
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn config_is_fail_closed() {
    // No namespace: never fall back to "default".
    let no_namespace = SandboxConfig::from_lookup(env(&[(RUNNER_IMAGE_ENV, IMAGE)]), |_| None);
    assert!(matches!(
        no_namespace,
        Err(SandboxError::Missing("CODING_RUNS_NAMESPACE"))
    ));

    // A tag is not a digest.
    let tag_only = SandboxConfig::from_lookup(
        env(&[
            (NAMESPACE_ENV, "coding-runs"),
            (RUNNER_IMAGE_ENV, "example.com/coding/runner:latest"),
            (API_URL_ENV, "http://127.0.0.1:1"),
            (TOKEN_ENV, "token"),
        ]),
        |_| None,
    );
    assert!(matches!(
        tag_only,
        Err(SandboxError::Invalid {
            name: "CODING_RUNNER_IMAGE",
            ..
        })
    ));

    // An https API with no readable CA is refused; the client never trusts public roots.
    let no_ca = SandboxConfig::from_lookup(
        env(&[
            (NAMESPACE_ENV, "coding-runs"),
            (RUNNER_IMAGE_ENV, IMAGE),
            (API_URL_ENV, "https://10.43.0.1:443"),
            (TOKEN_ENV, "token"),
        ]),
        |_| None,
    );
    assert!(matches!(no_ca, Err(SandboxError::Credential(_))));

    // The happy path supplies the documented defaults.
    let config = SandboxConfig::from_lookup(
        env(&[
            (NAMESPACE_ENV, "coding-runs"),
            (RUNNER_IMAGE_ENV, IMAGE),
            (API_URL_ENV, "http://127.0.0.1:1"),
            (TOKEN_ENV, "token"),
        ]),
        |_| None,
    )
    .expect("valid configuration");
    assert_eq!(config.runtime_class, "coding-kata");
    assert!(config.ca_pem.is_none());
}

// ─────────────────────────────────────────────────────────────────────────────
// The manifest carries the Restricted-PSS fields for real
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn manifest_is_restricted_and_carries_the_spec() {
    let sandbox = Sandbox::new(config("http://127.0.0.1:1")).expect("client");
    let manifest = sandbox
        .pod_manifest(
            "job-1",
            "tenant-a",
            JobKind::Test,
            &json!({ "kind": "test" }),
        )
        .expect("manifest");

    assert_eq!(manifest["metadata"]["namespace"], "coding-runs");
    assert_eq!(manifest["metadata"]["labels"]["coding.io/job"], "job-1");
    let created_at = manifest["metadata"]["annotations"][CREATED_AT_ANNOTATION]
        .as_str()
        .expect("created-at annotation");
    assert!(
        created_at.parse::<u64>().is_ok(),
        "created-at is unix seconds"
    );
    // A test fixture carries no job deadline, so there is nothing to mint a reap time from; the
    // reaper's age fallback covers it.
    assert!(
        manifest["metadata"]["annotations"][REAP_AT_ANNOTATION].is_null(),
        "no reap-at without a deadline"
    );

    let spec = &manifest["spec"];
    assert_eq!(spec["runtimeClassName"], "coding-kata");
    assert_eq!(spec["restartPolicy"], "Never");
    // Cancel deletes the Pod, and the runner needs this window to write its final result.
    assert_eq!(spec["terminationGracePeriodSeconds"], 10);
    assert_eq!(spec["automountServiceAccountToken"], false);
    assert_eq!(spec["enableServiceLinks"], false);
    // The runner image is private and the Pod uses the default ServiceAccount: without a pull
    // secret it never leaves ImagePullBackOff.
    assert_eq!(
        spec["imagePullSecrets"],
        json!([{ "name": "registry-pull-secret" }])
    );

    let pod_security = &spec["securityContext"];
    assert_eq!(pod_security["runAsNonRoot"], true);
    assert_eq!(pod_security["runAsUser"], 65532);
    assert_eq!(pod_security["runAsGroup"], 65532);
    assert_eq!(pod_security["seccompProfile"]["type"], "RuntimeDefault");

    let container = &spec["containers"][0];
    assert_eq!(container["image"], IMAGE);
    let job_spec: Value =
        serde_json::from_str(container["env"][0]["value"].as_str().expect("spec env"))
            .expect("spec json");
    assert_eq!(container["env"][0]["name"], "CODING_JOB_SPEC");
    assert_eq!(job_spec, json!({ "kind": "test" }));

    let security = &container["securityContext"];
    assert_eq!(security["runAsNonRoot"], true);
    assert_eq!(security["runAsUser"], 65532);
    assert_eq!(security["runAsGroup"], 65532);
    assert_eq!(security["allowPrivilegeEscalation"], false);
    assert_eq!(security["readOnlyRootFilesystem"], true);
    assert_eq!(security["capabilities"]["drop"], json!(["ALL"]));
    assert_eq!(security["seccompProfile"]["type"], "RuntimeDefault");

    // A read-only root filesystem needs a bounded writable scratch: 8 GiB, because a cargo
    // target dir does not fit in 1 GiB.
    assert_eq!(container["volumeMounts"][0]["mountPath"], "/tmp");
    assert_eq!(spec["volumes"][0]["emptyDir"]["sizeLimit"], "8Gi");

    // Per-kind resources: build/test are the larger tier, analyze/scan the smaller one.
    //
    // The CPU *limit* is what cargo actually sees: `available_parallelism()` reports the cgroup
    // quota, so 2 ran at most ~2 rustc jobs on any node. It is raised without raising the
    // request, because `requests.cpu` is quota-capped at 8 in `coding-runs` (8 concurrent jobs)
    // while `limits.cpu` is not capped at all. The LimitRange ceiling there is 6 CPU.
    //
    // Memory is 8Gi because one compile alone can need it: a single `clippy` on the heaviest
    // workspace peaked at 3.9 GiB RSS (2026-11-19), which cannot fit a 4 GiB Pod once cargo,
    // sccache and the OS are counted. The namespace quota's `limits.memory` (64Gi) is the real
    // concurrency ceiling at this size.
    assert_eq!(container["resources"]["requests"]["cpu"], "1");
    assert_eq!(container["resources"]["requests"]["memory"], "2Gi");
    assert_eq!(container["resources"]["limits"]["cpu"], "4");
    assert_eq!(container["resources"]["limits"]["memory"], "8Gi");

    let analyze = sandbox
        .pod_manifest("job-2", "tenant-a", JobKind::Analyze, &json!({}))
        .expect("analyze manifest");
    let analyze = &analyze["spec"]["containers"][0]["resources"];
    assert_eq!(analyze["requests"]["cpu"], "500m");
    assert_eq!(analyze["requests"]["memory"], "1Gi");
    assert_eq!(analyze["limits"]["cpu"], "1");
    assert_eq!(analyze["limits"]["memory"], "2Gi");
}

#[test]
fn cache_namespaces_separate_trusted_git_from_uploaded_source() {
    let mut cfg = config("http://127.0.0.1:1");
    cfg.cache = Some(coding::sandbox::CacheConfig {
        bucket: "ci-sccache".to_owned(),
        endpoint: "http://127.0.0.1:9000".to_owned(),
        region: coding::sandbox::DEFAULT_SCCACHE_REGION.to_owned(),
        use_ssl: false,
        key_prefix: "coding/".to_owned(),
        secret_git_rw: "coding-sccache-git-rw".to_owned(),
        secret_uploads_rw: "coding-sccache-uploads-rw".to_owned(),
        write_repos: vec!["https://github.com/nonagenticai/coding".to_owned()],
    });
    let sandbox = Sandbox::new(cfg).expect("client");

    let env_of = |spec: Value| -> Vec<Value> {
        sandbox
            .pod_manifest("job-1", "tenant-a", JobKind::Check, &spec)
            .expect("manifest")["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env array")
            .clone()
    };
    let find = |env: &[Value], name: &str| -> Value {
        env.iter()
            .find(|entry| entry["name"] == name)
            .unwrap_or_else(|| panic!("{name} is in the pod env"))
            .clone()
    };
    let creds = |env: &[Value]| -> String {
        find(env, "AWS_ACCESS_KEY_ID")["valueFrom"]["secretKeyRef"]["name"]
            .as_str()
            .expect("secret name")
            .to_owned()
    };

    // The commit gate uploads the working tree: it gets the uploads namespace, whose credential
    // cannot write the git subtree. This is the namespace that makes a warm pre-commit possible.
    let upload = env_of(json!({
        "deadline_seconds": 60,
        "input": { "source": { "kind": "upload", "id": "abc" } }
    }));
    assert_eq!(creds(&upload), "coding-sccache-uploads-rw");
    let reference = find(&upload, "AWS_ACCESS_KEY_ID");
    assert_eq!(reference["valueFrom"]["secretKeyRef"]["key"], "access_key");
    assert_eq!(reference["valueFrom"]["secretKeyRef"]["optional"], true);
    assert_eq!(
        find(&upload, "SCCACHE_S3_KEY_PREFIX")["value"],
        "coding/uploads/"
    );
    assert_eq!(find(&upload, "SCCACHE_BUCKET")["value"], "ci-sccache");
    // The credential is a reference; no key ever rides in the manifest.
    assert!(
        reference["value"].is_null(),
        "the value is never in the spec"
    );

    // An allowlisted git repository uses the git namespace — the `.git` spelling must not miss
    // the allowlist, and the two namespaces must never share a prefix.
    let git = env_of(json!({
        "deadline_seconds": 60,
        "input": { "source": { "kind": "git", "url": "https://github.com/nonagenticai/coding.git", "ref": "main" } }
    }));
    assert_eq!(creds(&git), "coding-sccache-git-rw");
    assert_eq!(find(&git, "SCCACHE_S3_KEY_PREFIX")["value"], "coding/git/");

    // One untrusted source is enough to take the whole job out of the git namespace.
    let mixed = env_of(json!({
        "deadline_seconds": 60,
        "input": {
            "sources": [
                { "name": "target", "profile": "rust",
                  "source": { "kind": "git", "url": "https://github.com/nonagenticai/coding", "ref": "main" } },
                { "name": "other", "profile": "rust",
                  "source": { "kind": "upload", "id": "abc" } }
            ]
        }
    }));
    assert_eq!(creds(&mixed), "coding-sccache-uploads-rw");

    // A git repository outside the allowlist — a fork, say — is not trusted either.
    let foreign = env_of(json!({
        "deadline_seconds": 60,
        "input": { "source": { "kind": "git", "url": "https://github.com/someone/else", "ref": "main" } }
    }));
    assert_eq!(creds(&foreign), "coding-sccache-uploads-rw");

    // Cache disabled: the Pod carries no wrapper at all, so a plain build is all it can do.
    let plain = Sandbox::new(config("http://127.0.0.1:1")).expect("client");
    let env = plain
        .pod_manifest("job-2", "tenant-a", JobKind::Check, &json!({}))
        .expect("manifest")["spec"]["containers"][0]["env"]
        .as_array()
        .expect("env array")
        .clone();
    assert!(
        env.iter().all(|entry| entry["name"] != "RUSTC_WRAPPER"),
        "no cache means no wrapper"
    );
}

#[test]
fn cache_config_defaults_and_normalises_the_write_allowlist() {
    let build = |pairs: &[(&str, &str)]| {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        SandboxConfig::from_lookup(
            |key| {
                pairs
                    .iter()
                    .find(|(name, _)| name == key)
                    .map(|(_, value)| value.clone())
            },
            |_| None,
        )
    };
    let base = [
        ("CODING_RUNS_NAMESPACE", "coding-runs"),
        ("CODING_RUNNER_IMAGE", IMAGE),
        ("SANDBOX_API_URL", "http://127.0.0.1:1"),
        ("SANDBOX_TOKEN", "test-token"),
        ("CODING_SCCACHE_BUCKET", "ci-sccache"),
        ("CODING_SCCACHE_ENDPOINT", "http://127.0.0.1:9000"),
    ];
    let mut pairs = base.to_vec();
    pairs.push((
        "CODING_SCCACHE_WRITE_REPOS",
        "https://github.com/o/a.git, https://github.com/o/b/",
    ));
    pairs.push(("CODING_SCCACHE_KEY_PREFIX", "coding"));
    let cache = build(&pairs).expect("config").cache.expect("cache");
    assert_eq!(cache.endpoint, "http://127.0.0.1:9000");
    assert_eq!(cache.region, coding::sandbox::DEFAULT_SCCACHE_REGION);
    assert!(!cache.use_ssl, "a plaintext endpoint is not TLS");
    assert_eq!(cache.key_prefix, "coding/", "the prefix always ends in /");
    assert_eq!(cache.secret_git_rw, "coding-sccache-git-rw");
    assert_eq!(cache.secret_uploads_rw, "coding-sccache-uploads-rw");
    assert_eq!(
        cache.write_repos,
        vec!["https://github.com/o/a", "https://github.com/o/b"]
    );

    // An unset bucket is the off switch — and then no endpoint is needed either.
    let none = build(&base[..4]).expect("config");
    assert!(none.cache.is_none());

    // A bucket without an endpoint is a deploy-time error, not a Pod that silently cannot reach
    // its cache (this repository is public; no private address is baked in).
    assert!(
        build(&base[..5]).is_err(),
        "an endpoint is required with a bucket"
    );

    // A typo in the boolean is a startup failure, not a silently misleading client.
    let mut broken = base.to_vec();
    broken.push(("CODING_SCCACHE_USE_SSL", "maybe"));
    assert!(build(&broken).is_err());
}

#[test]
fn manifest_mints_a_reap_at_from_the_job_deadline() {
    let sandbox = Sandbox::new(config("http://127.0.0.1:1")).expect("client");
    let manifest = sandbox
        .pod_manifest(
            "job-1",
            "tenant-a",
            JobKind::Test,
            &json!({ "kind": "test", "deadline_seconds": 1200 }),
        )
        .expect("manifest");
    let annotations = &manifest["metadata"]["annotations"];
    let created: u64 = annotations[CREATED_AT_ANNOTATION]
        .as_str()
        .expect("created-at")
        .parse()
        .expect("unix seconds");
    let reap: u64 = annotations[REAP_AT_ANNOTATION]
        .as_str()
        .expect("reap-at")
        .parse()
        .expect("unix seconds");
    assert_eq!(
        reap,
        created + 1200 + REAP_SLACK_SECONDS,
        "a job's Pod lives for its own deadline plus the pull/exit slack"
    );
}

#[test]
fn oversized_spec_is_refused() {
    let sandbox = Sandbox::new(config("http://127.0.0.1:1")).expect("client");
    let oversized = json!({ "payload": "a".repeat(64 * 1024) });
    let error = sandbox
        .pod_manifest("job-1", "tenant-a", JobKind::Test, &oversized)
        .expect_err("a spec over 64 KiB must be refused");
    assert!(matches!(error, SandboxError::SpecTooLarge { .. }));
}

// ─────────────────────────────────────────────────────────────────────────────
// The reaper honours each Pod's own deadline
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
// reason: `deleted` is the reap count and `deletes` is the stub's request log; both are earned.
#[allow(clippy::similar_names)]
async fn reap_uses_the_pods_own_deadline_not_the_fixed_age() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let pod = |name: &str, job: &str, created: u64, reap_at: Option<u64>| {
        let mut annotations = json!({ (CREATED_AT_ANNOTATION): created.to_string() });
        if let Some(reap_at) = reap_at {
            annotations[REAP_AT_ANNOTATION] = json!(reap_at.to_string());
        }
        json!({
            "metadata": {
                "name": name,
                "labels": { "coding.io/job": job },
                "annotations": annotations,
            }
        })
    };
    let stub = stub(StubInner {
        pods: vec![
            // Older than the 900 s fallback, but the job's own deadline is still ahead: a
            // healthy long job. This is the regression the fixed age used to kill.
            pod("long-job", "j1", now - 1000, Some(now + 600)),
            // Its own deadline has passed.
            pod("expired", "j2", now, Some(now - 1)),
            // No reap-at (a Pod from an older build): the age fallback decides.
            pod("legacy", "j3", now - 1000, None),
            // No reap-at and young: keep.
            pod("young", "j4", now, None),
        ],
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    let deleted = sandbox.reap(Duration::from_mins(15)).await.expect("reap");
    assert_eq!(
        deleted, 2,
        "only the expired and the un-annotated aged Pod go"
    );
    let deletes = stub.inner.lock().expect("stub lock").deletes.clone();
    assert!(deletes.contains(&"expired".to_owned()));
    assert!(deletes.contains(&"legacy".to_owned()));
    assert!(
        !deletes.contains(&"long-job".to_owned()),
        "a long job must never be reaped at the fixed age"
    );
    assert!(!deletes.contains(&"young".to_owned()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Reattach is only ever to our own Pod
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn conflict_reattaches_only_to_our_own_pod() {
    let own = stub(StubInner {
        create_status: StatusCode::CONFLICT.as_u16(),
        pod: Some(json!({
            "metadata": {
                "name": "coding-run-job-1",
                "labels": { "coding.io/job": "job-1" },
            }
        })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&own).await;
    let name = sandbox
        .create_run("job-1", "tenant-a", JobKind::Test, &json!({}))
        .await
        .expect("a 409 with our label is our own retry");
    assert_eq!(name, "coding-run-job-1");

    let foreign = stub(StubInner {
        create_status: StatusCode::CONFLICT.as_u16(),
        pod: Some(json!({
            "metadata": {
                "name": "coding-run-job-2",
                "labels": { "coding.io/job": "someone-else" },
            }
        })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&foreign).await;
    let error = sandbox
        .create_run("job-2", "tenant-a", JobKind::Test, &json!({}))
        .await
        .expect_err("a 409 for another job's pod must be refused");
    assert!(matches!(error, SandboxError::ForeignPod { .. }));
}

#[tokio::test]
async fn a_success_naming_another_pod_is_refused() {
    let stub = stub(StubInner {
        create_status: StatusCode::OK.as_u16(),
        create_body: Some(json!({ "metadata": { "name": "someone-elses-pod" } })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;
    let error = sandbox
        .create_run("job-1", "tenant-a", JobKind::Test, &json!({}))
        .await
        .expect_err("the API returned a pod we did not ask for");
    assert!(matches!(error, SandboxError::ForeignPodName { .. }));
}

// ─────────────────────────────────────────────────────────────────────────────
// The executor: terminal states, bounded logs, and always a delete
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn timeout_deletes_the_pod() {
    let stub = stub(StubInner {
        pod: Some(json!({ "status": { "phase": "Pending" } })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    let error = run_job(&sandbox, claim("job-1", JobKind::Test, 1))
        .await
        .expect_err("a pending pod past its deadline is a timeout");
    assert_eq!(error.code, "timeout");
    assert!(error.retryable, "a timeout may succeed on a retry");
    assert_eq!(
        stub.inner.lock().expect("stub lock").deletes,
        vec!["coding-run-job-1".to_owned()],
        "the timed-out pod is deleted before returning"
    );
}

#[tokio::test]
async fn oversized_log_is_truncated_below_the_cap() {
    let stub = stub(StubInner {
        log: "x".repeat(LOG_BYTE_CAP * 2),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;
    let log = sandbox.logs("coding-run-job-1", 1000).await.expect("log");
    assert!(
        log.len() <= LOG_BYTE_CAP,
        "the log read returned {} bytes, over the {LOG_BYTE_CAP}-byte cap",
        log.len()
    );
    assert!(
        log.contains(LOG_TRUNCATION_MARKER),
        "the truncation is marked"
    );
}

#[tokio::test]
async fn succeeded_returns_the_last_coding_result() {
    let stub = stub(StubInner {
        pod: Some(json!({ "status": { "phase": "Succeeded" } })),
        log: concat!(
            "{\"coding_result\":{\"tests\":1}}\n",
            "{\"coding_result\":{\"tests\":3,\"passed\":3}}\n",
        )
        .to_owned(),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    let result = run_job(&sandbox, claim("job-1", JobKind::Test, 30))
        .await
        .expect("a succeeded pod is a job success");
    assert_eq!(result, json!({ "tests": 3, "passed": 3 }));
    assert_eq!(
        stub.inner.lock().expect("stub lock").deletes,
        vec!["coding-run-job-1".to_owned()],
        "the pod is deleted before returning"
    );
}

#[tokio::test]
async fn state_reports_the_success_exit_code() {
    let terminated = stub(StubInner {
        pod: Some(json!({
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "state": { "terminated": { "exitCode": 0 } }
                }]
            }
        })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&terminated).await;
    assert_eq!(
        sandbox.state("coding-run-job-1").await.expect("state"),
        PodState::Succeeded { exit_code: Some(0) },
        "the success path must not throw the container exit code away"
    );

    // A Pod that flipped to Succeeded without publishing containerStatuses parses as `None`,
    // never as a fabricated 0.
    let unpublished = stub(StubInner {
        pod: Some(json!({ "status": { "phase": "Succeeded" } })),
        ..Default::default()
    });
    let sandbox = sandbox_for(&unpublished).await;
    assert_eq!(
        sandbox.state("coding-run-job-1").await.expect("state"),
        PodState::Succeeded { exit_code: None }
    );
}

#[tokio::test]
async fn succeeded_without_a_result_line_is_a_runner_failure_not_a_pass() {
    // A Pod that exits 0 without the runner's verdict is not evidence about the code. The old
    // fallback invented `{"exit":"succeeded"}`, which a gate read as green.
    let stub = stub(StubInner {
        pod: Some(json!({
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "state": { "terminated": { "exitCode": 0 } }
                }]
            }
        })),
        log: "build ok\n".to_owned(),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    let error = run_job(&sandbox, claim("job-1", JobKind::Test, 30))
        .await
        .expect_err("no verdict line is not a pass");
    assert_eq!(error.code, "runner_failed");
    assert!(!error.retryable);
    assert_eq!(error.details, Some(json!("build ok\n")));
}

#[tokio::test]
async fn failed_reports_the_reason_and_the_log_tail() {
    let stub = stub(StubInner {
        pod: Some(json!({
            "status": {
                "phase": "Failed",
                "containerStatuses": [{
                    "state": { "terminated": { "reason": "Error", "exitCode": 101 } }
                }]
            }
        })),
        log: "boom\n".to_owned(),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    let error = run_job(&sandbox, claim("job-1", JobKind::Build, 30))
        .await
        .expect_err("a failed pod is a failed job");
    assert_eq!(error.code, "runner_failed");
    assert!(!error.retryable, "a runner failure is not retryable");
    assert_eq!(error.message, "Error (exit 101)");
    assert_eq!(error.details, Some(Value::String("boom\n".to_owned())));
    assert_eq!(
        stub.inner.lock().expect("stub lock").deletes,
        vec!["coding-run-job-1".to_owned()],
        "the pod is deleted before returning"
    );
}

/// Every upload source of a multi-source job is prepared with its own fetch URL, capability
/// token and hash before the Pod exists. Without a test here the loop could silently skip the
/// upload branch and every job would fail later, after a 202 — fail-open in the wrong direction.
#[tokio::test]
// reason: one linear arrange/act/assert narrative for the multi-source upload path.
#[allow(clippy::too_many_lines)]
async fn every_named_upload_source_is_prepared_with_its_own_capability() {
    async fn store(pool: &sqlx::PgPool, tenant: &str, marker: &str) -> String {
        let blob = coding::source_archive::pack(&[
            coding::source_archive::PackFile {
                path: "Cargo.toml".to_owned(),
                data: b"[package]\nname = \"sample\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"
                    .to_vec(),
                executable: false,
            },
            coding::source_archive::PackFile {
                path: "src/lib.rs".to_owned(),
                data: marker.as_bytes().to_vec(),
                executable: false,
            },
        ])
        .expect("pack");
        coding::store::create_source(
            pool,
            tenant,
            &coding::source_archive::sha256_hex(&blob),
            i64::try_from(blob.len()).expect("the sample archive fits in i64"),
            2,
            &blob,
        )
        .await
        .expect("store")
    }

    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a real Postgres 18: this test never skips");
    let pool = coding::db::connect(&url)
        .await
        .expect("connect TEST_DATABASE_URL");
    coding::db::migrate(&pool).await.expect("migrate");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let tenant = format!("sandbox-sources-{}-{nanos}", std::process::id());

    let first = store(&pool, &tenant, "pub fn one() {}").await;
    let second = store(&pool, &tenant, "pub fn two() {}").await;

    let stub = stub(StubInner {
        pod: Some(json!({ "status": { "phase": "Succeeded" } })),
        log:
            "{\"coding_result\":{\"status\":\"succeeded\",\"summary\":\"2 of 2 checks passed\"}}\n"
                .to_owned(),
        ..Default::default()
    });
    let sandbox = sandbox_for(&stub).await;

    // The mint casts the job id to uuid (token_job_id) and is scoped to the claim's tenant, so
    // both must match what the store holds.
    let mut claim = claim("11111111-1111-4111-8111-111111111111", JobKind::Test, 60);
    claim.tenant_id = tenant.clone();
    claim.input = json!({"sources": [
        {"name": "one", "profile": "rust", "source": {"kind": "upload", "id": first}},
        {"name": "two", "profile": "rust", "source": {"kind": "upload", "id": second}}
    ]});
    // An upload source needs the API base the Pod fetches from; `run_job` passes none.
    let result = execute_sandbox(
        &sandbox,
        &pool,
        Some("http://coding.example.test:8687"),
        None,
        claim,
    )
    .await
    .expect("the stub pod succeeds");
    assert_eq!(result["status"], "succeeded");

    let created = stub.inner.lock().expect("stub lock").created.clone();
    assert_eq!(created.len(), 1, "exactly one Pod was created");
    let spec_text = created[0]["spec"]["containers"][0]["env"][0]["value"]
        .as_str()
        .expect("the spec rides in CODING_JOB_SPEC");
    let spec: Value = serde_json::from_str(spec_text).expect("spec json");
    let sources = spec["input"]["sources"].as_array().expect("sources");
    assert_eq!(sources.len(), 2);
    for (index, id) in [&first, &second].iter().enumerate() {
        let source = &sources[index]["source"];
        let url = source["url"].as_str().expect("a fetch url");
        assert!(
            url.ends_with(&format!("/api/sources/{id}/blob")),
            "source {index} url {url}"
        );
        assert!(
            source["sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64),
            "source {index} carries the recorded hash"
        );
        assert!(
            source["token"]
                .as_str()
                .is_some_and(|token| !token.is_empty()),
            "source {index} carries its capability"
        );
    }
    assert_ne!(
        sources[0]["source"]["token"], sources[1]["source"]["token"],
        "each upload gets its own capability, not one shared by the job"
    );

    sqlx::query("DELETE FROM sources WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .expect("cleanup");
    pool.close().await;
}
