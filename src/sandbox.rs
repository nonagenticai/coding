//! The kata sandbox: one plain Pod per job, driven directly against the Kubernetes API.
//!
//! **Client choice.** The surface is five REST calls (`create`, `state`, `logs`, `delete`,
//! `reap`), the credential is already a mounted service-account file, and the workspace's
//! `reqwest` carries it. `kube` + `k8s-openapi` would add a second API client to a lock file
//! several lanes share; conveyor made the same call (`apps/server/src/sandbox.rs`), and this is
//! a deliberate port of it — with the sharp edges removed:
//!
//! - a **Pod**, not a `batch/v1` Job, created directly by the worker. No `ConfigMap`: the job
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

use crate::domain::{JobKind, Profile};

/// `CODING_RUNS_NAMESPACE`: the namespace runner Pods are created in. Required.
pub const NAMESPACE_ENV: &str = "CODING_RUNS_NAMESPACE";
/// `CODING_RUNNER_IMAGE`: the runner image, `image@sha256:<digest>`. Required.
pub const RUNNER_IMAGE_ENV: &str = "CODING_RUNNER_IMAGE";
/// `CODING_RUNNER_MULTI_SOURCE`: whether the pinned runner image executes a job's `sources` list.
/// Absent or anything but an affirmative value is `false` — a typo refuses the job before a Pod
/// rather than sending a spec the image cannot parse.
pub const MULTI_SOURCE_ENV: &str = "CODING_RUNNER_MULTI_SOURCE";
/// `CODING_RUNNER_PROFILES`: the toolchain profiles the runner image carries, comma-separated.
/// Unset means `rust`, the image's historical content. A job naming a profile that is not listed
/// is refused before a Pod is created, so a deployment that has not been told about a profile
/// fails with a message instead of an `invalid job spec` from an older runner.
pub const PROFILES_ENV: &str = "CODING_RUNNER_PROFILES";
/// `CODING_KATA_RUNTIME_CLASS`: the `RuntimeClass` that puts the Pod on the kata runtime.
pub const RUNTIME_CLASS_ENV: &str = "CODING_KATA_RUNTIME_CLASS";
/// `CODING_RUNS_PULL_SECRET`: the imagePullSecret in the runs namespace. The runner image is
/// private, and a Pod created with the default `ServiceAccount` has no pull credential — without
/// this the run dies in `ImagePullBackOff`. Empty omits the field (public image).
pub const PULL_SECRET_ENV: &str = "CODING_RUNS_PULL_SECRET";
/// The estate's standard pull-secret name, present in every service namespace.
pub const DEFAULT_PULL_SECRET: &str = "registry-pull-secret";

/// `CODING_SCCACHE_BUCKET` (optional): the `MinIO` bucket for the compilation cache. Unset or empty
/// disables the cache entirely, and a job then runs exactly as it did before caching existed.
pub const SCCACHE_BUCKET_ENV: &str = "CODING_SCCACHE_BUCKET";
/// `CODING_SCCACHE_ENDPOINT`: the S3 endpoint sccache talks to. **Required whenever the bucket is
/// set** — this is a public repository, so no private infrastructure address is baked in here; the
/// deployment supplies it.
pub const SCCACHE_ENDPOINT_ENV: &str = "CODING_SCCACHE_ENDPOINT";
/// `CODING_SCCACHE_REGION`: `MinIO` ignores a region; the AWS SDK does not accept a request without
/// one.
pub const SCCACHE_REGION_ENV: &str = "CODING_SCCACHE_REGION";
/// `CODING_SCCACHE_USE_SSL`: `true` when the endpoint is https.
pub const SCCACHE_USE_SSL_ENV: &str = "CODING_SCCACHE_USE_SSL";
/// `CODING_SCCACHE_KEY_PREFIX`: the base object prefix. Trusted git builds live under `<base>git/`
/// and everything else under `<base>uploads/` — two caches that never read each other's objects.
/// The CI runners share the same bucket and write at the root
/// (`.github/workflows/rust-library.yml`).
pub const SCCACHE_KEY_PREFIX_ENV: &str = "CODING_SCCACHE_KEY_PREFIX";
/// `CODING_SCCACHE_SECRET_GIT_RW`: the write credential scoped to the git namespace.
pub const SCCACHE_SECRET_GIT_RW_ENV: &str = "CODING_SCCACHE_SECRET_GIT_RW";
/// `CODING_SCCACHE_SECRET_UPLOADS_RW`: the write credential scoped to the uploads namespace.
pub const SCCACHE_SECRET_UPLOADS_RW_ENV: &str = "CODING_SCCACHE_SECRET_UPLOADS_RW";
/// `CODING_SCCACHE_WRITE_REPOS`: comma-separated repository URLs whose jobs use the **git**
/// namespace, with the write credential. Empty is the safe default: every job uses `uploads/`.
pub const SCCACHE_WRITE_REPOS_ENV: &str = "CODING_SCCACHE_WRITE_REPOS";

/// `MinIO` ignores the region, but the S3 client requires a syntactically valid one.
pub const DEFAULT_SCCACHE_REGION: &str = "us-east-1";
pub const DEFAULT_SCCACHE_SECRET_GIT_RW: &str = "coding-sccache-git-rw";
pub const DEFAULT_SCCACHE_SECRET_UPLOADS_RW: &str = "coding-sccache-uploads-rw";
pub const DEFAULT_SCCACHE_KEY_PREFIX: &str = "coding/";
/// `SANDBOX_API_URL`: overrides the in-cluster API address (tests use a local stub).
pub const API_URL_ENV: &str = "SANDBOX_API_URL";
/// `SANDBOX_TOKEN`: overrides the mounted service-account token.
pub const TOKEN_ENV: &str = "SANDBOX_TOKEN";
/// `SANDBOX_CA_PATH`: overrides the mounted cluster CA.
pub const CA_PATH_ENV: &str = "SANDBOX_CA_PATH";

/// The default `RuntimeClass` that selects the kata runtime on the runner node.
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
    /// The job asks for a profile this worker's runner image was not configured to execute.
    #[error(
        "profile {requested:?} is not enabled on this worker (CODING_RUNNER_PROFILES={enabled:?})"
    )]
    ProfileNotEnabled { requested: String, enabled: String },
    /// The job carries a `sources` list and the pinned runner image predates support for it.
    #[error(
        "this worker's runner image does not execute multi-source jobs (set {MULTI_SOURCE_ENV}=1 \
         when the image does)"
    )]
    SourcesNotEnabled,
}

/// The compilation-cache settings carried into every runner Pod.
///
/// Two namespaces, two write identities: a trusted git build uses `<prefix>git/` with a credential
/// the `MinIO` policy confines to that subtree, and everything else — uploaded working trees and
/// inline files — uses `<prefix>uploads/` with its own confined credential. Neither builder can
/// write the other's subtree, and neither reads the other's objects.
///
/// The credential travels as a `secretKeyRef`, never as a value in the spec. A missing Secret is
/// not an error: the references are `optional`, so the Pod starts and compiles without a cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub use_ssl: bool,
    /// Base prefix, always ending in `/`.
    pub key_prefix: String,
    pub secret_git_rw: String,
    pub secret_uploads_rw: String,
    /// Normalised repository URLs whose jobs use the git namespace and its write credential.
    pub write_repos: Vec<String>,
}

impl CacheConfig {
    /// The namespace and credential for one job: `(prefix, Secret name)`.
    fn namespace(&self, trusted_git: bool) -> (String, &str) {
        if trusted_git {
            (format!("{}git/", self.key_prefix), &self.secret_git_rw)
        } else {
            (
                format!("{}uploads/", self.key_prefix),
                &self.secret_uploads_rw,
            )
        }
    }
}

/// The sandbox client's configuration. Built from env in a pod, from a lookup in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxConfig {
    /// The namespace runner Pods are created in.
    pub namespace: String,
    /// The digest-pinned runner image. One image for every kind at M1; per-kind overrides can
    /// be added without changing the wire contract because a payload can never name an image.
    pub runner_image: String,
    /// The toolchain profiles that image can execute. `rust` unless `CODING_RUNNER_PROFILES`
    /// says otherwise; a job asking for anything else is refused before a Pod exists.
    pub profiles: Vec<Profile>,
    /// Whether that image executes a job's `sources` list. `false` unless
    /// `CODING_RUNNER_MULTI_SOURCE` says otherwise, so a job created before the image was
    /// rolled is refused with a legible message instead of `invalid job spec` from the Pod.
    pub multi_source: bool,
    /// The `RuntimeClass` that selects the kata runtime.
    pub runtime_class: String,
    /// The API server base URL, e.g. `https://10.43.0.1:443`.
    pub api_url: String,
    /// The service-account bearer token.
    pub token: String,
    /// The cluster CA in PEM. Required for `https://`; absent only for a plaintext test stub.
    pub ca_pem: Option<Vec<u8>>,
    /// The imagePullSecret placed on every runner Pod (`None` only for a public image).
    pub pull_secret: Option<String>,
    /// The compilation cache, when `CODING_SCCACHE_BUCKET` names one.
    pub cache: Option<CacheConfig>,
}

impl SandboxConfig {
    /// Read the sandbox configuration from the process environment.
    ///
    /// # Errors
    ///
    /// `SandboxError::Missing` for an unset required variable, `SandboxError::Invalid` for a
    /// value that cannot be used, and `SandboxError::Credential` when the service-account token
    /// or CA PEM cannot be read.
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Missing` for an unset required variable, `SandboxError::Invalid` for a
    /// value that cannot be used, and `SandboxError::Credential` when the service-account token
    /// or CA PEM cannot be read.
    // reason: one linear list of fail-closed checks; splitting it would hide a required env.
    #[allow(clippy::too_many_lines)]
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
        let profiles = profiles_from(get(PROFILES_ENV).as_deref())?;
        let multi_source = multi_source_from(get(MULTI_SOURCE_ENV).as_deref());

        let api_url = if let Some(url) = get(API_URL_ENV) {
            url
        } else {
            let host = get("KUBERNETES_SERVICE_HOST").ok_or(SandboxError::Missing(
                "KUBERNETES_SERVICE_HOST (or SANDBOX_API_URL)",
            ))?;
            let port = get("KUBERNETES_SERVICE_PORT").unwrap_or_else(|| "443".to_owned());
            format!("https://{host}:{port}")
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

        // An unset bucket is the off switch: the Pod then carries no cache environment at all.
        // With a bucket set, the endpoint is REQUIRED — this repository is public, so it must not
        // carry a private address as a default, and a missing endpoint is a deploy-time error
        // (CrashLoop), never a Pod that silently cannot reach its cache.
        let cache = match get(SCCACHE_BUCKET_ENV) {
            Some(bucket) => {
                let endpoint =
                    get(SCCACHE_ENDPOINT_ENV).ok_or(SandboxError::Missing(SCCACHE_ENDPOINT_ENV))?;
                if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                    return Err(SandboxError::Invalid {
                        name: SCCACHE_ENDPOINT_ENV,
                        reason: format!("{endpoint:?} is not an http(s) URL"),
                    });
                }
                let use_ssl = match get(SCCACHE_USE_SSL_ENV).as_deref() {
                    None => endpoint.starts_with("https://"),
                    Some("true" | "1" | "yes") => true,
                    Some("false" | "0" | "no") => false,
                    Some(other) => {
                        return Err(SandboxError::Invalid {
                            name: SCCACHE_USE_SSL_ENV,
                            reason: format!("{other:?} is not a boolean"),
                        });
                    }
                };
                let prefix = get(SCCACHE_KEY_PREFIX_ENV)
                    .unwrap_or_else(|| DEFAULT_SCCACHE_KEY_PREFIX.to_owned());
                let prefix = if prefix.ends_with('/') {
                    prefix
                } else {
                    format!("{prefix}/")
                };
                Some(CacheConfig {
                    bucket,
                    endpoint,
                    region: get(SCCACHE_REGION_ENV)
                        .unwrap_or_else(|| DEFAULT_SCCACHE_REGION.to_owned()),
                    use_ssl,
                    key_prefix: prefix,
                    secret_git_rw: get(SCCACHE_SECRET_GIT_RW_ENV)
                        .unwrap_or_else(|| DEFAULT_SCCACHE_SECRET_GIT_RW.to_owned()),
                    secret_uploads_rw: get(SCCACHE_SECRET_UPLOADS_RW_ENV)
                        .unwrap_or_else(|| DEFAULT_SCCACHE_SECRET_UPLOADS_RW.to_owned()),
                    write_repos: repos_from(get(SCCACHE_WRITE_REPOS_ENV).as_deref()),
                })
            }
            None => None,
        };

        Ok(Self {
            namespace,
            runner_image,
            profiles,
            multi_source,
            runtime_class,
            api_url,
            token,
            ca_pem,
            pull_secret,
            cache,
        })
    }
}

/// Parse `CODING_RUNNER_PROFILES`. Unset or empty is `rust`, the image's historical content; an
/// unknown name fails closed at startup, so a typo in the deployment is a `CrashLoop` rather than
/// a worker that silently refuses a profile it was meant to run.
fn profiles_from(value: Option<&str>) -> Result<Vec<Profile>, SandboxError> {
    let Some(value) = value else {
        return Ok(vec![Profile::Rust]);
    };
    let mut profiles = Vec::new();
    for name in value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let profile = Profile::parse(name).ok_or_else(|| SandboxError::Invalid {
            name: PROFILES_ENV,
            reason: format!("unknown profile {name:?} (known: rust, python)"),
        })?;
        if !profiles.contains(&profile) {
            profiles.push(profile);
        }
    }
    if profiles.is_empty() {
        profiles.push(Profile::Rust);
    }
    Ok(profiles)
}

/// Parse `CODING_RUNNER_MULTI_SOURCE`. Only an affirmative value enables the capability; a typo
/// leaves it off, which refuses the job instead of sending a spec the image cannot parse.
fn multi_source_from(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        Some("1" | "true" | "yes" | "TRUE" | "YES")
    )
}

/// Parse `CODING_SCCACHE_WRITE_REPOS`: a comma-separated allowlist, normalised so the job's own
/// URL spelling (`https://github.com/o/r.git` vs `https://github.com/o/r/`) cannot miss.
fn repos_from(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(normalise_repo)
        .collect()
}

/// The form both sides of the allowlist comparison use: lower-case, no trailing slash, no `.git`.
fn normalise_repo(url: &str) -> String {
    let url = url.trim().trim_end_matches('/').to_lowercase();
    url.strip_suffix(".git").unwrap_or(&url).to_owned()
}

/// Every `Source` object a job spec carries, in either wire form.
fn source_objects(spec_json: &Value) -> Vec<&Value> {
    let input = &spec_json["input"];
    let mut sources = Vec::new();
    if let Some(single) = input.get("source").filter(|value| !value.is_null()) {
        sources.push(single);
    }
    if let Some(list) = input.get("sources").and_then(Value::as_array) {
        for named in list {
            if let Some(source) = named.get("source") {
                sources.push(source);
            }
        }
    }
    sources
}

/// The profile a job spec asks for. Absent means `rust`, exactly as the API's default does.
fn profile_of_spec(spec_json: &Value) -> Result<Profile, SandboxError> {
    match spec_json.pointer("/input/profile").and_then(Value::as_str) {
        None => Ok(Profile::Rust),
        Some(name) => Profile::parse(name).ok_or_else(|| SandboxError::Invalid {
            name: PROFILES_ENV,
            reason: format!("job asks for unknown profile {name:?}"),
        }),
    }
}

/// Every profile a job spec asks for: one per named source, or the job-level profile for a
/// single-source job. A named source without a profile is a request error, and an unknown one is
/// refused exactly as a job-level unknown profile is.
fn profiles_of_spec(spec_json: &Value) -> Result<Vec<Profile>, SandboxError> {
    let Some(sources) = spec_json
        .pointer("/input/sources")
        .and_then(Value::as_array)
        .filter(|sources| !sources.is_empty())
    else {
        return Ok(vec![profile_of_spec(spec_json)?]);
    };
    let mut profiles = Vec::with_capacity(sources.len());
    for (index, source) in sources.iter().enumerate() {
        let Some(name) = source.get("profile").and_then(Value::as_str) else {
            return Err(SandboxError::Invalid {
                name: PROFILES_ENV,
                reason: format!("input.sources[{index}] names no profile"),
            });
        };
        let profile = Profile::parse(name).ok_or_else(|| SandboxError::Invalid {
            name: PROFILES_ENV,
            reason: format!("input.sources[{index}] asks for unknown profile {name:?}"),
        })?;
        if !profiles.contains(&profile) {
            profiles.push(profile);
        }
    }
    Ok(profiles)
}

/// Whether the spec carries the multi-source form.
fn has_sources(spec_json: &Value) -> bool {
    spec_json
        .pointer("/input/sources")
        .and_then(Value::as_array)
        .is_some_and(|sources| !sources.is_empty())
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Credential` when the CA PEM cannot be parsed, and
    /// `SandboxError::Transport` when the HTTP client cannot be built.
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Missing`, `Invalid` or `Credential` from reading the environment, and
    /// `SandboxError::Credential` or `Transport` from building the client.
    pub fn from_env() -> Result<Self, SandboxError> {
        Self::new(SandboxConfig::from_env()?)
    }

    /// The namespace runner Pods are created in.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }

    /// The deterministic Pod name for a job. A repeated create therefore names the same object
    /// and the API answers `409`, which [`Sandbox::create_run`] treats as a reattach.
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Manifest` when the job id is not a valid label value or the spec cannot
    /// be serialized, and `SandboxError::SpecTooLarge` when the encoded spec exceeds the
    /// env-size cap.
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

        // The spec must stay env[0]: tests and readers of the manifest have always found it there,
        // and the cache variables are appended after it rather than around it.
        let mut env = vec![json!({ "name": "CODING_JOB_SPEC", "value": spec_text })];
        env.extend(self.cache_env(spec_json));

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
                    "env": env,
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

    /// The cache environment for one job, or nothing when the cache is disabled.
    ///
    /// Every name is the one `.github/workflows/rust-library.yml` already uses for the CI cache,
    /// so one sccache dialect serves both. The job's namespace — `git/` or `uploads/` — decides
    /// both the object prefix and which write credential is referenced, and the reference is
    /// `optional: true`: a missing Secret leaves the variable unset and the build runs uncached,
    /// which is the only failure mode a gate may have.
    fn cache_env(&self, spec_json: &Value) -> Vec<Value> {
        let Some(cache) = self.config.cache.as_ref() else {
            return Vec::new();
        };
        let (prefix, secret) = cache.namespace(self.cache_trusted_git(spec_json));
        let reference = |env_name: &str, secret_key: &str| {
            json!({
                "name": env_name,
                "valueFrom": {
                    "secretKeyRef": { "name": secret, "key": secret_key, "optional": true }
                }
            })
        };
        let use_ssl = if cache.use_ssl { "true" } else { "false" };
        vec![
            json!({ "name": "RUSTC_WRAPPER", "value": "sccache" }),
            json!({ "name": "CARGO_INCREMENTAL", "value": "0" }),
            json!({ "name": "SCCACHE_BUCKET", "value": &cache.bucket }),
            json!({ "name": "SCCACHE_ENDPOINT", "value": &cache.endpoint }),
            json!({ "name": "SCCACHE_REGION", "value": &cache.region }),
            json!({ "name": "SCCACHE_S3_USE_SSL", "value": use_ssl }),
            json!({ "name": "SCCACHE_S3_KEY_PREFIX", "value": prefix }),
            json!({ "name": "SCCACHE_IDLE_TIMEOUT", "value": "0" }),
            json!({ "name": "SCCACHE_LOG", "value": "warn" }),
            json!({ "name": "AWS_EC2_METADATA_DISABLED", "value": "true" }),
            reference("AWS_ACCESS_KEY_ID", "access_key"),
            reference("AWS_SECRET_ACCESS_KEY", "secret_key"),
        ]
    }

    /// Whether this job belongs in the **trusted git** namespace.
    ///
    /// A Pod holding a write key can PUT arbitrary objects regardless of how sccache derives its
    /// own keys, so the git namespace is reserved for jobs whose every source is a git repository
    /// on the allowlist — a repository code cannot enter without push access. Anything else
    /// (uploaded working trees, inline files, a fork's URL) uses the uploads namespace, whose
    /// credential cannot write the git subtree at all.
    fn cache_trusted_git(&self, spec_json: &Value) -> bool {
        let Some(cache) = self.config.cache.as_ref() else {
            return false;
        };
        if cache.write_repos.is_empty() {
            return false;
        }
        let sources = source_objects(spec_json);
        if sources.is_empty() {
            return false;
        }
        sources.iter().all(|source| {
            source.get("kind").and_then(Value::as_str) == Some("git")
                && source
                    .get("url")
                    .and_then(Value::as_str)
                    .is_some_and(|url| cache.write_repos.contains(&normalise_repo(url)))
        })
    }

    /// Create the runner Pod for a job, returning its deterministic name.
    ///
    /// A `409 AlreadyExists` is this worker's own retry **only** when the existing Pod carries
    /// our `coding.io/job` label; anything else is refused. A success response naming a Pod
    /// other than the one requested is refused too — that would be someone else's run.
    ///
    /// # Errors
    ///
    /// `SandboxError::SourcesNotEnabled` or `SandboxError::ProfileNotEnabled` when the spec
    /// asks for a capability this worker was not configured with, the manifest and spec-size
    /// errors [`Self::pod_manifest`] raises, `SandboxError::NotFound`,
    /// `SandboxError::ForeignPod` or `SandboxError::ForeignPodName` when a `409` names a Pod
    /// that is not this job's, and `SandboxError::Transport` or `SandboxError::Api` when the
    /// create request fails.
    pub async fn create_run(
        &self,
        job_id: &str,
        tenant: &str,
        kind: JobKind,
        spec_json: &Value,
    ) -> Result<String, SandboxError> {
        // Refuse a spec shape or profile this worker was not configured with BEFORE a Pod is
        // created: the runner image that starts would not understand the spec, and that would
        // surface as an `invalid job spec` failure for a job that is perfectly well formed.
        if has_sources(spec_json) && !self.config.multi_source {
            return Err(SandboxError::SourcesNotEnabled);
        }
        for profile in profiles_of_spec(spec_json)? {
            if !self.config.profiles.contains(&profile) {
                return Err(SandboxError::ProfileNotEnabled {
                    requested: profile.as_str().to_owned(),
                    enabled: self
                        .config
                        .profiles
                        .iter()
                        .map(|profile| profile.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                });
            }
        }
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Transport` when the request does not reach the API, and
    /// `SandboxError::Api` when it answers a non-success status other than `404`.
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
    ///
    /// # Errors
    ///
    /// `SandboxError::NotFound` when the Pod is gone, `SandboxError::Transport` when the
    /// request or the streamed body fails, and `SandboxError::Api` for any other non-success
    /// status.
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Transport` when the request fails, and `SandboxError::Api` for a
    /// non-success status other than `404`.
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
    ///
    /// # Errors
    ///
    /// `SandboxError::Transport` when the list request or its body fails, and
    /// `SandboxError::Api` when the list answers a non-success status. A failed delete of one
    /// Pod is logged and not returned.
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
                    tracing::warn!(pod = %name, error = %error, "sandbox: reaper delete failed");
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
///
/// ⚠️ The CPU **limit** is what bounds cargo: inside a cgroup, `available_parallelism()` reports
/// the quota, so a 2-core limit runs at most ~2 rustc jobs however large the node is. Measured on
/// `coding` itself (2026-11-19): clippy peaked at 1426m of a 2000m limit and took ~149 s, i.e. the
/// limit, not the node, was the ceiling. The limit is raised without touching the request on
/// purpose — the `coding-runs` `ResourceQuota` caps `requests.cpu` at 8, so raising the *request*
/// would cut concurrency from 8 jobs to 4, while `limits.cpu` is not capped by the quota at all.
/// The `LimitRange` ceiling in `coding-runs` is 6 CPU / 12Gi; a value above that is admitted by the
/// API and then refused at Pod creation, so keep this table inside it.
///
/// ⚠️ CPU and memory are coupled through cargo: a larger CPU *limit* raises the job count cargo
/// derives from the cgroup, so raising CPU without memory buys SIGKILLs on heavy workspaces
/// (measured 2026-11-19 on `agent`: four rustc jobs over a 4 GiB limit). The runner bounds that
/// coupling itself with `CARGO_BUILD_JOBS` — see `runner::exec::run_child`. Raise both together or
/// neither.
///
/// ⚠️ 8 GiB, not 4, because a single compile need not fit 4: measured 2026-11-19, one `clippy`
/// process on `agent-api` peaked at **3.9 GiB RSS**, leaving nothing of a 4 GiB Pod for cargo,
/// sccache and the OS — the kernel killed it with `SIGKILL` even with two compile jobs. The sum of
/// limits is what the `coding-runs` `ResourceQuota` caps (`limits.memory: 40Gi`, raised to 64Gi
/// alongside this), so at 8 GiB up to five jobs run concurrently; that is the concurrency the
/// quota buys, not a scheduling limit.
#[must_use]
pub fn resources_for(kind: JobKind) -> Value {
    let (request_cpu, request_memory, limit_cpu, limit_memory) = match kind {
        JobKind::Test | JobKind::Build | JobKind::Lint | JobKind::Check => ("1", "2Gi", "4", "8Gi"),
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
        .map_or(0, |duration| duration.as_secs())
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

    /// `coding-runs` carries a `LimitRange` of at most 6 CPU / 12Gi per container. A value above it
    /// is accepted by the API and then refused when the Pod is created — a job that can never run,
    /// and the failure surfaces as `sandbox_error` rather than as anything about the code. Pin the
    /// table against that ceiling here so a future bump fails in review and not in production.
    #[test]
    fn every_kind_fits_the_coding_runs_limit_range() {
        fn millicores(raw: &str) -> i64 {
            match raw.strip_suffix('m') {
                Some(value) => value.parse().expect("millicores"),
                None => raw.parse::<i64>().expect("cores") * 1000,
            }
        }
        fn mebibytes(raw: &str) -> i64 {
            match raw.strip_suffix("Gi") {
                Some(value) => value.parse::<i64>().expect("GiB") * 1024,
                None => raw.parse::<i64>().expect("MiB"),
            }
        }

        for kind in [JobKind::Test, JobKind::Build, JobKind::Lint, JobKind::Check] {
            let resources = resources_for(kind);
            let request_cpu =
                millicores(resources["requests"]["cpu"].as_str().expect("request cpu"));
            let limit_cpu = millicores(resources["limits"]["cpu"].as_str().expect("limit cpu"));
            let request_memory = mebibytes(
                resources["requests"]["memory"]
                    .as_str()
                    .expect("request memory"),
            );
            let limit_memory = mebibytes(
                resources["limits"]["memory"]
                    .as_str()
                    .expect("limit memory"),
            );

            assert!(
                request_cpu <= limit_cpu,
                "{kind:?}: a request above the limit"
            );
            assert!(
                request_memory <= limit_memory,
                "{kind:?}: a memory request above the limit"
            );
            assert!(
                limit_cpu <= 6_000,
                "{kind:?}: {limit_cpu}m is over the 6 CPU LimitRange ceiling"
            );
            assert!(
                limit_memory <= 12 * 1024,
                "{kind:?}: {limit_memory}Mi is over the 12Gi LimitRange ceiling"
            );
        }
    }

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

    fn config_with(pairs: &[(&str, &str)]) -> SandboxConfig {
        SandboxConfig::from_lookup(
            |key| {
                pairs
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| (*value).to_owned())
            },
            |_| None,
        )
        .expect("a plaintext test configuration")
    }

    fn base_pairs() -> Vec<(&'static str, &'static str)> {
        vec![
            (NAMESPACE_ENV, "coding-runs"),
            (RUNNER_IMAGE_ENV, "example@sha256:abc"),
            (API_URL_ENV, "http://127.0.0.1:1"),
            (TOKEN_ENV, "test-token"),
        ]
    }

    #[test]
    fn multi_source_is_off_unless_the_value_is_affirmative() {
        assert!(multi_source_from(Some("1")));
        assert!(multi_source_from(Some("true")));
        assert!(!multi_source_from(Some("0")));
        assert!(!multi_source_from(Some("maybe")));
        assert!(!multi_source_from(None));
    }

    #[test]
    fn every_named_source_profile_is_collected() {
        let spec = serde_json::json!({"input": {"sources": [
            {"name": "target", "profile": "rust", "source": {"kind": "git", "url": "https://x/y"}},
            {"name": "source-1", "profile": "python", "source": {"kind": "git", "url": "https://x/z"}}
        ]}});
        assert_eq!(
            profiles_of_spec(&spec).expect("known profiles"),
            vec![Profile::Rust, Profile::Python]
        );
        // A named source without a profile is a request error, not a default.
        let spec = serde_json::json!({"input": {"sources": [
            {"name": "target", "source": {"kind": "git", "url": "https://x/y"}}
        ]}});
        assert!(profiles_of_spec(&spec).is_err());
        // A single-source job still answers with its job-level profile.
        let spec = serde_json::json!({"input": {"profile": "python"}});
        assert_eq!(
            profiles_of_spec(&spec).expect("known"),
            vec![Profile::Python]
        );
    }

    #[tokio::test]
    async fn a_sources_job_is_refused_before_any_pod_when_the_image_predates_it() {
        let sandbox = Sandbox::new(config_with(&base_pairs())).expect("client");
        let spec = serde_json::json!({"input": {"sources": [
            {"name": "target", "profile": "rust", "source": {"kind": "git", "url": "https://x/y"}}
        ]}});
        match sandbox
            .create_run("job-1", "tenant", JobKind::Check, &spec)
            .await
        {
            Err(SandboxError::SourcesNotEnabled) => {}
            other => panic!("expected SourcesNotEnabled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_named_source_profile_the_image_lacks_is_refused_before_any_pod() {
        let mut pairs = base_pairs();
        pairs.push((MULTI_SOURCE_ENV, "1"));
        pairs.push((PROFILES_ENV, "rust"));
        let sandbox = Sandbox::new(config_with(&pairs)).expect("client");
        let spec = serde_json::json!({"input": {"sources": [
            {"name": "target", "profile": "rust", "source": {"kind": "git", "url": "https://x/y"}},
            {"name": "source-1", "profile": "python", "source": {"kind": "git", "url": "https://x/z"}}
        ]}});
        match sandbox
            .create_run("job-1", "tenant", JobKind::Check, &spec)
            .await
        {
            Err(SandboxError::ProfileNotEnabled { requested, .. }) => {
                assert_eq!(requested, "python");
            }
            other => panic!("expected ProfileNotEnabled, got {other:?}"),
        }
    }
}
