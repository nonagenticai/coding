//! The kata sandbox: one plain Pod per job, driven directly against the Kubernetes API.
//!
//! **Client choice.** The surface is five REST calls (`create`, `state`, `logs`, `delete`,
//! `reap`), the credential is already a mounted service-account file, and the workspace's
//! `reqwest` carries it. `kube` + `k8s-openapi` would add a second API client to a lock file
//! several lanes share; conveyor made the same call (`apps/server/src/sandbox.rs`), and this is
//! a deliberate port of it — with the sharp edges removed:
//!
//! - a **Pod**, not a `batch/v1` Job, created directly by the worker. No ConfigMap: the job
//!   spec rides in a bounded `CODING_JOB_SPEC` env var, so a run that dies leaves one Pod that
//!   the reaper can still find and delete.
//! - the image is **digest-pinned and configured once**, never carried by a request payload.
//! - logs are read once, at the end, with `tailLines` **and** a hard byte cap; an oversized log
//!   is truncated, never buffered whole.
//! - a `409 AlreadyExists` is a reattach **only** when the existing Pod carries our
//!   `coding.io/job` label value. Reusing someone else's Pod is refused.
//! - Restricted-PSS fields are actually set, not merely claimed.
//!
//! **Fail-closed.** [`SandboxConfig::from_lookup`] refuses to build without a namespace, without
//! a digest-pinned image, and — for an `https://` API — without a readable CA. It never falls
//! back to public roots, and it never defaults the namespace to `default`.

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use crate::domain::JobKind;

/// `CODING_RUNS_NAMESPACE`: the namespace runner Pods are created in. Required.
pub const NAMESPACE_ENV: &str = "CODING_RUNS_NAMESPACE";
/// `CODING_RUNNER_IMAGE`: the runner image, `image@sha256:<digest>`. Required.
pub const RUNNER_IMAGE_ENV: &str = "CODING_RUNNER_IMAGE";
/// `CODING_KATA_RUNTIME_CLASS`: the RuntimeClass that puts the Pod on the kata runtime.
pub const RUNTIME_CLASS_ENV: &str = "CODING_KATA_RUNTIME_CLASS";
/// `CODING_RUNS_PULL_SECRET`: the imagePullSecret in the runs namespace. The runner image is
/// private, and a Pod created with the default ServiceAccount has no pull credential — without
/// this the run dies in ImagePullBackOff. Empty omits the field (public image).
pub const PULL_SECRET_ENV: &str = "CODING_RUNS_PULL_SECRET";
/// The estate's standard pull-secret name, present in every service namespace.
pub const DEFAULT_PULL_SECRET: &str = "registry-pull-secret";
/// `SANDBOX_API_URL`: overrides the in-cluster API address (tests use a local stub).
pub const API_URL_ENV: &str = "SANDBOX_API_URL";
/// `SANDBOX_TOKEN`: overrides the mounted service-account token.
pub const TOKEN_ENV: &str = "SANDBOX_TOKEN";
/// `SANDBOX_CA_PATH`: overrides the mounted cluster CA.
pub const CA_PATH_ENV: &str = "SANDBOX_CA_PATH";

/// The default RuntimeClass that selects the kata runtime on the runner node.
pub const DEFAULT_RUNTIME_CLASS: &str = "coding-kata";
/// Unix seconds when the Pod was created: the clock for the fallback age policy.
pub const CREATED_AT_ANNOTATION: &str = "coding.io/created-at";
/// When the reaper may delete this Pod. Minted from the job's own deadline in
/// [`Sandbox::pod_manifest`]: a job's Pod lives as long as the job, never a fixed 15 minutes.
pub const REAP_AT_ANNOTATION: &str = "coding.io/reap-at";
/// Image-pull and final-log slack added to a job's deadline before the reaper may delete it.
pub const REAP_SLACK_SECONDS: u64 = 300;
/// The in-cluster service-account directory.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
/// The runner spec is passed as an env var; Kubernetes' object cap makes a bigger request
/// unworkable, so it is refused before create.
pub const MAX_SPEC_BYTES: usize = 64 * 1024;
/// The hard cap on a log read. A larger log is truncated with a marker.
pub const LOG_BYTE_CAP: usize = 256 * 1024;
/// Appended to a log that hit [`LOG_BYTE_CAP`].
pub const LOG_TRUNCATION_MARKER: &str = "\n...[log truncated at 262144 bytes]\n";
/// The scratch `emptyDir` cap. A read-only root filesystem needs somewhere writable.
/// 8 GiB, not 1 GiB: a cargo target dir does not fit in 1 GiB.
pub const SCRATCH_SIZE_LIMIT: &str = "8Gi";
/// The non-root uid/gid every runner process runs as.
pub const RUN_AS_ID: u64 = 65532;
/// The per-request HTTP timeout.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const STATE_SUCCEEDED: &str = "Succeeded";
const STATE_FAILED: &str = "Failed";
const STATE_RUNNING: &str = "Running";

/// Why a sandbox call did not succeed.
#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    /// A required environment variable is unset.
    #[error("{0} is not set")]
    Missing(&'static str),
    /// A defined value cannot be used.
    #[error("{name} is invalid: {reason}")]
    Invalid { name: &'static str, reason: String },
    /// The service-account credential or CA could not be read.
    #[error("sandbox credential: {0}")]
    Credential(String),
    /// The API request did not reach the server.
    #[error("kubernetes API: {0}")]
    Transport(String),
    /// The API answered a non-success status.
    #[error("kubernetes API returned {status}: {body}")]
    Api { status: u16, body: String },
    /// The named Pod does not exist.
    #[error("pod {0:?} does not exist")]
    NotFound(String),
    /// The manifest is unusable (bad name, unreadable spec).
    #[error("manifest: {0}")]
    Manifest(String),
    /// The job spec is over the env-size cap.
    #[error("job spec is {size} bytes, over the {MAX_SPEC_BYTES}-byte cap")]
    SpecTooLarge { size: usize },
    /// An existing Pod with the deterministic name belongs to another job.
    #[error("pod {name:?} carries coding.io/job={found:?}, not {expected:?}")]
    ForeignPod {
        name: String,
        expected: String,
        found: Option<String>,
    },
    /// The create response named a Pod other than the one we asked for.
    #[error("API returned pod {returned:?} for job {expected:?}")]
    ForeignPodName { expected: String, returned: String },
}

/// The sandbox client's configuration. Built from env in a pod, from a lookup in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxConfig {
    /// The namespace runner Pods are created in.
    pub namespace: String,
    /// The digest-pinned runner image. One image for every kind at M1; per-kind overrides can
    /// be added without changing the wire contract because a payload can never name an image.
    pub runner_image: String,
    /// The RuntimeClass that selects the kata runtime.
    pub runtime_class: String,
    /// The API server base URL, e.g. `https://10.43.0.1:443`.
    pub api_url: String,
    /// The service-account bearer token.
    pub token: String,
    /// The cluster CA in PEM. Required for `https://`; absent only for a plaintext test stub.
    pub ca_pem: Option<Vec<u8>>,
    /// The imagePullSecret placed on every runner Pod (`None` only for a public image).
    pub pull_secret: Option<String>,
}

impl SandboxConfig {
    /// Read the sandbox configuration from the process environment.
    pub fn from_env() -> Result<Self, SandboxError> {
        Self::from_lookup(
            |key| std::env::var(key).ok(),
            |path| std::fs::read(path).ok(),
        )
    }

    /// Build from a lookup pair (env, file read) so every fail-closed path is testable.
    ///
    /// `CODING_RUNS_NAMESPACE` and `CODING_RUNNER_IMAGE` are required; the image must be
    /// digest-pinned. The API address, token and CA default to the mounted service account and
    /// are overridable for tests. A CA that cannot be read fails **closed** for an `https://`
    /// endpoint: the client is never allowed to fall back to public roots.
    pub fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
        read_file: impl Fn(&str) -> Option<Vec<u8>>,
    ) -> Result<Self, SandboxError> {
        let get = |key: &str| {
            get(key)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };

        let namespace = get(NAMESPACE_ENV).ok_or(SandboxError::Missing(NAMESPACE_ENV))?;
        let runner_image = get(RUNNER_IMAGE_ENV).ok_or(SandboxError::Missing(RUNNER_IMAGE_ENV))?;
        if !runner_image.contains("@sha256:") {
            return Err(SandboxError::Invalid {
                name: RUNNER_IMAGE_ENV,
                reason: "must be digest-pinned as image@sha256:<digest>".to_owned(),
            });
        }

        let runtime_class =
            get(RUNTIME_CLASS_ENV).unwrap_or_else(|| DEFAULT_RUNTIME_CLASS.to_owned());

        let api_url = match get(API_URL_ENV) {
            Some(url) => url,
            None => {
                let host = get("KUBERNETES_SERVICE_HOST").ok_or(SandboxError::Missing(
                    "KUBERNETES_SERVICE_HOST (or SANDBOX_API_URL)",
                ))?;
                let port = get("KUBERNETES_SERVICE_PORT").unwrap_or_else(|| "443".to_owned());
                format!("https://{host}:{port}")
            }
        };
        if !(api_url.starts_with("https://") || api_url.starts_with("http://")) {
            return Err(SandboxError::Invalid {
                name: API_URL_ENV,
                reason: format!("{api_url:?} is not an http(s) URL"),
            });
        }

        let token = match get(TOKEN_ENV) {
            Some(token) => token,
            None => read_file(&format!("{SERVICE_ACCOUNT_DIR}/token"))
                .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_owned())
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    SandboxError::Credential(format!(
                        "{SERVICE_ACCOUNT_DIR}/token is missing or empty"
                    ))
                })?,
        };

        let ca_pem = match get(CA_PATH_ENV) {
            Some(path) => {
                let pem = read_file(&path)
                    .filter(|bytes| !bytes.is_empty())
                    .ok_or_else(|| {
                        SandboxError::Credential(format!("{path} is missing or empty"))
                    })?;
                Some(pem)
            }
            None => match read_file(&format!("{SERVICE_ACCOUNT_DIR}/ca.crt")) {
                Some(pem) if !pem.is_empty() => Some(pem),
                _ if api_url.starts_with("https://") => {
                    return Err(SandboxError::Credential(format!(
                        "{SERVICE_ACCOUNT_DIR}/ca.crt is missing or empty; refusing to fall \
                         back to public roots"
                    )));
                }
                // A plaintext endpoint has no TLS trust to configure.
                _ => None,
            },
        };

        // An explicit empty value disables the field (public image); unset keeps the estate
        // default, because a Pod with no pull credential can never start the private runner.
        let pull_secret = match get(PULL_SECRET_ENV) {
            Some(value) if value == "none" => None,
            Some(value) => Some(value),
            None => Some(DEFAULT_PULL_SECRET.to_owned()),
        };

        Ok(Self {
            namespace,
            runner_image,
            runtime_class,
            api_url,
            token,
            ca_pem,
            pull_secret,
        })
    }
}

/// `imagePullSecrets` as the API expects it: an empty list when no secret is configured.
fn pull_secrets_json(pull_secret: Option<&str>) -> Value {
    match pull_secret {
        Some(name) => json!([{ "name": name }]),
        None => json!([]),
    }
}

/// The observed state of a runner Pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PodState {
    /// Accepted, not yet running (scheduling, image pull).
    Pending,
    /// At least one container is running.
    Running,
    /// Every container exited 0. `exit_code` is the terminated container's code when the API
    /// reported one; a Pod that is `Succeeded` but never published `containerStatuses` carries
    /// `None`, never a fabricated `0`.
    Succeeded { exit_code: Option<i32> },
    /// The Pod failed; `reason` comes from the Pod or its terminated container.
    Failed { reason: String },
    /// The Pod no longer exists (reaped, deleted, or never created).
    Gone,
}

/// A configured Kubernetes API client for the kata sandbox.
#[derive(Clone, Debug)]
pub struct Sandbox {
    config: SandboxConfig,
    client: Client,
}

impl Sandbox {
    /// Build a client from an explicit configuration.
    pub fn new(config: SandboxConfig) -> Result<Self, SandboxError> {
        let mut builder = Client::builder().timeout(REQUEST_TIMEOUT);
        if let Some(pem) = config.ca_pem.as_deref() {
            let certificate = reqwest::Certificate::from_pem(pem)
                .map_err(|err| SandboxError::Credential(format!("CA PEM: {err}")))?;
            // Trust ONLY the cluster CA. This is the opposite of merging it into a public
            // bundle: an API server is never authenticated against the public web PKI.
            builder = builder.tls_certs_only([certificate]);
        }
        let client = builder
            .build()
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        Ok(Self { config, client })
    }

    /// Build from the process environment. The fail-closed path is what makes the worker
    /// refuse to start without a namespace or a digest-pinned image.
    pub fn from_env() -> Result<Self, SandboxError> {
        Self::new(SandboxConfig::from_env()?)
    }

    /// The namespace runner Pods are created in.
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }

    /// The deterministic Pod name for a job. A repeated create therefore names the same object
    /// and the API answers `409`, which [`Sandbox::create_run`] treats as a reattach.
    pub fn pod_name(job_id: &str) -> String {
        let mut slug = String::with_capacity(job_id.len());
        for character in job_id.chars() {
            if character.is_ascii_alphanumeric() {
                slug.push(character.to_ascii_lowercase());
            } else {
                slug.push('-');
            }
        }
        let slug = slug.trim_matches('-');
        if slug.is_empty() {
            return format!("coding-run-{:08x}", fnv1a(job_id.as_bytes()));
        }
        let name = format!("coding-run-{slug}");
        if name.len() <= 63 {
            return name;
        }
        let hash = fnv1a(name.as_bytes());
        let head: String = name.chars().take(54).collect();
        format!("{}-{hash:08x}", head.trim_end_matches('-'))
    }

    /// The Pod manifest for one job. Public so tests can assert the Restricted-PSS fields
    /// without a cluster.
    pub fn pod_manifest(
        &self,
        job_id: &str,
        tenant: &str,
        kind: JobKind,
        spec_json: &Value,
    ) -> Result<Value, SandboxError> {
        if !is_label_value(job_id) {
            return Err(SandboxError::Manifest(format!(
                "job id {job_id:?} is not a valid Kubernetes label value"
            )));
        }
        let spec_text = serde_json::to_string(spec_json)
            .map_err(|err| SandboxError::Manifest(format!("job spec: {err}")))?;
        if spec_text.len() > MAX_SPEC_BYTES {
            return Err(SandboxError::SpecTooLarge {
                size: spec_text.len(),
            });
        }
        let name = Self::pod_name(job_id);
        let created_at = unix_now();
        // The reaper must never delete a healthy long job. `deadline_seconds` is the runner's own
        // self-timeout (the worker sets `timeout-5`); the slack covers image pull and the final
        // log read. A spec without one (a test fixture) gets no annotation and falls back to the
        // worker's age policy.
        let reap_at = spec_json["deadline_seconds"].as_u64().map(|deadline| {
            created_at
                .saturating_add(deadline)
                .saturating_add(REAP_SLACK_SECONDS)
        });
        let mut annotations = json!({
            (CREATED_AT_ANNOTATION): created_at.to_string(),
            "coding.io/tenant": tenant,
            "coding.io/kind": kind.as_str(),
        });
        if let Some(reap_at) = reap_at {
            annotations[REAP_AT_ANNOTATION] = json!(reap_at.to_string());
        }

        Ok(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": self.config.namespace,
                "labels": { "coding.io/job": job_id },
                "annotations": annotations,
            },
            "spec": {
                "runtimeClassName": self.config.runtime_class,
                "restartPolicy": "Never",
                // Cancel deletes the Pod; the runner needs this window to write its result.
                "terminationGracePeriodSeconds": 10,
                "automountServiceAccountToken": false,
                "enableServiceLinks": false,
                "imagePullSecrets": pull_secrets_json(self.config.pull_secret.as_deref()),
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": RUN_AS_ID,
                    "runAsGroup": RUN_AS_ID,
                    "fsGroup": RUN_AS_ID,
                    "seccompProfile": { "type": "RuntimeDefault" },
                },
                "containers": [{
                    "name": "runner",
                    "image": self.config.runner_image,
                    "env": [{ "name": "CODING_JOB_SPEC", "value": spec_text }],
                    "resources": resources_for(kind),
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": RUN_AS_ID,
                        "runAsGroup": RUN_AS_ID,
                        "allowPrivilegeEscalation": false,
                        "readOnlyRootFilesystem": true,
                        "capabilities": { "drop": ["ALL"] },
                        "seccompProfile": { "type": "RuntimeDefault" },
                    },
                    "volumeMounts": [{ "name": "scratch", "mountPath": "/tmp" }],
                }],
                "volumes": [{
                    "name": "scratch",
                    "emptyDir": { "sizeLimit": SCRATCH_SIZE_LIMIT },
                }],
            },
        }))
    }

    /// Create the runner Pod for a job, returning its deterministic name.
    ///
    /// A `409 AlreadyExists` is this worker's own retry **only** when the existing Pod carries
    /// our `coding.io/job` label; anything else is refused. A success response naming a Pod
    /// other than the one requested is refused too — that would be someone else's run.
    pub async fn create_run(
        &self,
        job_id: &str,
        tenant: &str,
        kind: JobKind,
        spec_json: &Value,
    ) -> Result<String, SandboxError> {
        let manifest = self.pod_manifest(job_id, tenant, kind, spec_json)?;
        let name = Self::pod_name(job_id);
        let path = format!("/api/v1/namespaces/{}/pods", self.config.namespace);
        let response = self
            .client
            .post(self.url(&path))
            .bearer_auth(&self.config.token)
            .json(&manifest)
            .send()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let status = response.status();

        if status == StatusCode::CONFLICT {
            return self.reattach(job_id, &name).await;
        }
        if !status.is_success() {
            return Err(SandboxError::Api {
                status: status.as_u16(),
                body: capped(response.text().await.unwrap_or_default()),
            });
        }

        if let Ok(body) = response.json::<Value>().await
            && let Some(returned) = body["metadata"]["name"].as_str()
            && returned != name
        {
            return Err(SandboxError::ForeignPodName {
                expected: job_id.to_owned(),
                returned: returned.to_owned(),
            });
        }
        Ok(name)
    }

    /// Read one Pod's state.
    pub async fn state(&self, name: &str) -> Result<PodState, SandboxError> {
        let pod = match self.get_pod(name).await {
            Ok(pod) => pod,
            Err(SandboxError::NotFound(_)) => return Ok(PodState::Gone),
            Err(error) => return Err(error),
        };
        let status = &pod["status"];
        Ok(match status["phase"].as_str() {
            Some(STATE_SUCCEEDED) => PodState::Succeeded {
                exit_code: success_exit_code(status),
            },
            Some(STATE_FAILED) => PodState::Failed {
                reason: failure_reason(status),
            },
            Some(STATE_RUNNING) => PodState::Running,
            _ => PodState::Pending,
        })
    }

    /// Read a Pod's log, bounded by [`LOG_BYTE_CAP`]. The body is streamed and the read stops
    /// at the cap, so an oversized log is never buffered whole.
    pub async fn logs(&self, name: &str, tail_lines: u32) -> Result<String, SandboxError> {
        let path = format!(
            "/api/v1/namespaces/{}/pods/{name}/log?tailLines={tail_lines}",
            self.config.namespace
        );
        let mut response = self
            .client
            .get(self.url(&path))
            .bearer_auth(&self.config.token)
            .send()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(SandboxError::NotFound(name.to_owned()));
        }
        if !status.is_success() {
            return Err(SandboxError::Api {
                status: status.as_u16(),
                body: capped(response.text().await.unwrap_or_default()),
            });
        }

        let keep = LOG_BYTE_CAP.saturating_sub(LOG_TRUNCATION_MARKER.len());
        // Keep the END of the stream: a runner writes its one result line LAST, and a log read
        // that keeps the head silently drops the verdict (measured: any job whose cargo output
        // passes 256 KiB would answer `succeeded` with no result at all).
        let mut body: std::collections::VecDeque<u8> = std::collections::VecDeque::new();
        let mut truncated = false;
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    body.extend(chunk.iter().copied());
                    while body.len() > keep {
                        body.pop_front();
                        truncated = true;
                    }
                }
                Ok(None) => break,
                Err(err) => return Err(SandboxError::Transport(err.to_string())),
            }
        }
        let body: Vec<u8> = body.into();
        Ok(bounded_log(&body, truncated))
    }

    /// Delete a Pod. Best effort: a `404` means the reaper or the TTL already got it.
    pub async fn delete(&self, name: &str) -> Result<(), SandboxError> {
        let path = format!("/api/v1/namespaces/{}/pods/{name}", self.config.namespace);
        let response = self
            .client
            .delete(self.url(&path))
            .bearer_auth(&self.config.token)
            .send()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let status = response.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(SandboxError::Api {
            status: status.as_u16(),
            body: capped(response.text().await.unwrap_or_default()),
        })
    }

    /// Delete runner Pods whose own deadline has passed (`coding.io/reap-at`). A Pod without
    /// that annotation — one created by a version before per-job deadlines — falls back to the
    /// `older_than` age policy. Returns how many Pods were deleted.
    pub async fn reap(&self, older_than: Duration) -> Result<usize, SandboxError> {
        // An existence selector on the label; `%2F` is the encoded `/` in `coding.io/job`.
        let path = format!(
            "/api/v1/namespaces/{}/pods?labelSelector=coding.io%2Fjob",
            self.config.namespace
        );
        let response = self
            .client
            .get(self.url(&path))
            .bearer_auth(&self.config.token)
            .send()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(SandboxError::Api {
                status: status.as_u16(),
                body: capped(response.text().await.unwrap_or_default()),
            });
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let items = body["items"].as_array().cloned().unwrap_or_default();
        let now = unix_now();
        let mut deleted = 0;
        for pod in items {
            let Some(name) = pod["metadata"]["name"]
                .as_str()
                .filter(|name| !name.is_empty())
            else {
                continue;
            };
            let annotations = &pod["metadata"]["annotations"];
            let created = annotations[CREATED_AT_ANNOTATION]
                .as_str()
                .and_then(|value| value.parse::<u64>().ok());
            // A Pod created by this version carries its own deadline, so a long job is never
            // killed by the age policy. Only an older build's Pod falls back to `older_than`; a
            // labelled Pod with neither annotation is still ours to clean up.
            let reap_at = annotations[REAP_AT_ANNOTATION]
                .as_str()
                .and_then(|value| value.parse::<u64>().ok());
            let expired = match reap_at {
                Some(reap_at) => now >= reap_at,
                None => created
                    .is_none_or(|created| now.saturating_sub(created) >= older_than.as_secs()),
            };
            if !expired {
                continue;
            }
            match self.delete(name).await {
                Ok(()) => deleted += 1,
                Err(error) => {
                    tracing::warn!(pod = %name, error = %error, "sandbox: reaper delete failed")
                }
            }
        }
        Ok(deleted)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.config.api_url.trim_end_matches('/'), path)
    }

    async fn reattach(&self, job_id: &str, name: &str) -> Result<String, SandboxError> {
        let pod = self.get_pod(name).await?;
        let found = pod["metadata"]["labels"]["coding.io/job"]
            .as_str()
            .map(str::to_owned);
        if found.as_deref() != Some(job_id) {
            return Err(SandboxError::ForeignPod {
                name: name.to_owned(),
                expected: job_id.to_owned(),
                found,
            });
        }
        Ok(name.to_owned())
    }

    async fn get_pod(&self, name: &str) -> Result<Value, SandboxError> {
        let path = format!("/api/v1/namespaces/{}/pods/{name}", self.config.namespace);
        let response = self
            .client
            .get(self.url(&path))
            .bearer_auth(&self.config.token)
            .send()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(SandboxError::NotFound(name.to_owned()));
        }
        if !status.is_success() {
            return Err(SandboxError::Api {
                status: status.as_u16(),
                body: capped(response.text().await.unwrap_or_default()),
            });
        }
        response
            .json::<Value>()
            .await
            .map_err(|err| SandboxError::Transport(err.to_string()))
    }
}

/// The per-kind resource table. Test and build legs compile things and get the larger tier;
/// analysis and scan (and every kind that does not run in the sandbox yet) get the smaller one.
pub fn resources_for(kind: JobKind) -> Value {
    let (request_cpu, request_memory, limit_cpu, limit_memory) = match kind {
        JobKind::Test | JobKind::Build | JobKind::Lint | JobKind::Check => ("1", "2Gi", "2", "4Gi"),
        _ => ("500m", "1Gi", "1", "2Gi"),
    };
    json!({
        "requests": { "cpu": request_cpu, "memory": request_memory },
        "limits": { "cpu": limit_cpu, "memory": limit_memory },
    })
}

/// The first container's terminated exit code, when the API published one. A Pod that just
/// flipped to `Succeeded` may not carry `containerStatuses` yet, so this is an `Option`.
fn success_exit_code(status: &Value) -> Option<i32> {
    let exit_code = status["containerStatuses"][0]["state"]["terminated"]["exitCode"].as_i64()?;
    i32::try_from(exit_code).ok()
}

/// The reason a failed Pod should report: the Pod's own reason first, then the first
/// terminated container's reason and exit code.
fn failure_reason(status: &Value) -> String {
    if let Some(reason) = status["reason"]
        .as_str()
        .filter(|reason| !reason.is_empty())
    {
        return reason.to_owned();
    }
    if let Some(containers) = status["containerStatuses"].as_array() {
        for container in containers {
            let terminated = &container["state"]["terminated"];
            let reason = terminated["reason"]
                .as_str()
                .filter(|reason| !reason.is_empty());
            let exit_code = terminated["exitCode"].as_i64();
            match (reason, exit_code) {
                (Some(reason), Some(code)) => return format!("{reason} (exit {code})"),
                (Some(reason), None) => return reason.to_owned(),
                (None, Some(code)) => return format!("exit {code}"),
                (None, None) => {}
            }
        }
    }
    "failed".to_owned()
}

/// Decode the streamed body and guarantee the result never exceeds [`LOG_BYTE_CAP`]. Lossy
/// UTF-8 decoding can expand invalid bytes, so the cap is enforced again after decoding — and the
/// **tail** is what survives, because the runner's verdict is the last line it writes.
fn bounded_log(body: &[u8], truncated: bool) -> String {
    let text = String::from_utf8_lossy(body).into_owned();
    if !truncated && text.len() <= LOG_BYTE_CAP {
        return text;
    }
    let keep = LOG_BYTE_CAP.saturating_sub(LOG_TRUNCATION_MARKER.len());
    let mut start = text.len().saturating_sub(keep);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    let mut out = String::with_capacity(LOG_TRUNCATION_MARKER.len() + text.len() - start);
    out.push_str(LOG_TRUNCATION_MARKER);
    out.push_str(&text[start..]);
    out
}

/// A Kubernetes label value: non-empty, at most 63 characters of `[A-Za-z0-9._-]`.
fn is_label_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

/// A stable 64-bit hash for name truncation. Not a security boundary — only a collision
/// suffix so two long job ids cannot silently share a Pod name.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn capped(body: String) -> String {
    const CAP: usize = 500;
    if body.len() <= CAP {
        return body;
    }
    let mut end = CAP;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &body[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_log_is_returned_unchanged() {
        let text = "line one\n{\"coding_result\":{\"status\":\"succeeded\"}}\n";
        assert_eq!(bounded_log(text.as_bytes(), false), text);
    }

    /// The runner writes its verdict LAST. A log read that keeps the head drops it, and the job
    /// then answers "succeeded" with no result at all — which a client cannot tell from a pass.
    #[test]
    fn an_oversized_log_keeps_the_tail_where_the_verdict_is() {
        let mut text = String::new();
        while text.len() < LOG_BYTE_CAP * 2 {
            text.push_str("cargo chatter that a human will never read\n");
        }
        text.push_str("{\"coding_result\":{\"status\":\"failed\"}}\n");
        let bounded = bounded_log(text.as_bytes(), false);
        assert!(bounded.len() <= LOG_BYTE_CAP, "cap {}", bounded.len());
        assert!(bounded.starts_with(LOG_TRUNCATION_MARKER));
        assert!(
            bounded.ends_with("{\"coding_result\":{\"status\":\"failed\"}}\n"),
            "the verdict line survives: …{}",
            &bounded[bounded.len().saturating_sub(120)..]
        );
    }

    #[test]
    fn the_truncation_marker_stays_within_the_cap() {
        let bounded = bounded_log(&vec![b'x'; LOG_BYTE_CAP * 2], true);
        assert!(bounded.len() <= LOG_BYTE_CAP);
    }
}
