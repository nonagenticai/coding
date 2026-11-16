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
        runtime_class: "coding-kata".to_owned(),
        api_url: api_url.to_owned(),
        token: "test-token".to_owned(),
        ca_pem: None,
        pull_secret: Some("registry-pull-secret".to_owned()),
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
    assert_eq!(container["resources"]["requests"]["cpu"], "1");
    assert_eq!(container["resources"]["requests"]["memory"], "2Gi");
    assert_eq!(container["resources"]["limits"]["cpu"], "2");
    assert_eq!(container["resources"]["limits"]["memory"], "4Gi");

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

    let deleted = sandbox.reap(Duration::from_secs(900)).await.expect("reap");
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
async fn succeeded_without_a_result_line_reports_the_exit_code_and_log_tail() {
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

    let result = run_job(&sandbox, claim("job-1", JobKind::Test, 30))
        .await
        .expect("a succeeded pod is a job success");
    assert_eq!(
        result,
        json!({ "exit": "succeeded", "exit_code": 0, "log_tail": "build ok\n" })
    );
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
