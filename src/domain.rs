//! Domain types shared by the HTTP API, the store and the worker.
//!
//! Timestamps are RFC 3339 UTC strings produced by Postgres (`to_char`), so the API needs no
//! date crate and `--locked` stays valid.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// What a job does. Closed set: the worker maps each kind to an allowlisted runner image and
/// command plan, so a request can never name an image or an arbitrary binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    /// Run the repository's test suite and return normalized results.
    Test,
    /// Run the repository's linters/formatters and return their parsed output.
    Lint,
    /// Compile/build the repository.
    Build,
    /// The pre-commit gate: format, clippy and the test suite, in one Pod and one clone.
    Check,
    /// Static analysis families (structure, complexity, dependencies, quality, hunts).
    Analyze,
    /// Dependency/CVE and secret scanning.
    Scan,
    /// A GitHub action: push, create a PR, mark ready, comment.
    GithubOp,
    /// Bulk dependency upgrade (replaces coding-repo-jobs).
    DependencyUpgrade,
    /// `PyPI` package -> crate mapping for the rewrite capability.
    CrateMap,
    /// Rewrite parity: `OpenAPI` contract diff.
    ParityOpenapi,
    /// Rewrite parity: database schema diff.
    ParitySchema,
    /// Rewrite parity: recorded request/response behaviour diff.
    ParityBehaviour,
    /// Rewrite parity: latency/throughput comparison.
    ParityPerf,
}

impl JobKind {
    /// Stable wire/DB spelling. Kept explicit so the DB and the API cannot drift.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::Lint => "lint",
            Self::Build => "build",
            Self::Check => "check",
            Self::Analyze => "analyze",
            Self::Scan => "scan",
            Self::GithubOp => "github_op",
            Self::DependencyUpgrade => "dependency_upgrade",
            Self::CrateMap => "crate_map",
            Self::ParityOpenapi => "parity_openapi",
            Self::ParitySchema => "parity_schema",
            Self::ParityBehaviour => "parity_behaviour",
            Self::ParityPerf => "parity_perf",
        }
    }

    /// Parse the DB/wire spelling. `None` means a row written by a newer build, not a request
    /// error, so the caller renders it as `503` rather than blaming the client.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "test" => Self::Test,
            "lint" => Self::Lint,
            "build" => Self::Build,
            "check" => Self::Check,
            "analyze" => Self::Analyze,
            "scan" => Self::Scan,
            "github_op" => Self::GithubOp,
            "dependency_upgrade" => Self::DependencyUpgrade,
            "crate_map" => Self::CrateMap,
            "parity_openapi" => Self::ParityOpenapi,
            "parity_schema" => Self::ParitySchema,
            "parity_behaviour" => Self::ParityBehaviour,
            "parity_perf" => Self::ParityPerf,
            _ => return None,
        })
    }
}

/// The toolchain family a job's command plan runs under.
///
/// Closed set, and deliberately not a [`JobKind`]: the estate ruling is that a language is a
/// property of the code under test, not a new kind of job. `Rust` is the default, and the wire
/// shape is kept unchanged for it (see [`JobInput::profile`]) so that a runner image built before
/// profiles existed parses a Rust job exactly as it always did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// Cargo: fmt, clippy, build, test.
    #[default]
    Rust,
    /// uv: lock/sync, ruff, ty, pytest.
    Python,
}

impl Profile {
    /// Stable wire/DB spelling, for the same reason [`JobKind::as_str`] exists.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "rust" => Self::Rust,
            "python" => Self::Python,
            _ => return None,
        })
    }

    /// Whether this is the default. Serde uses it to omit the field entirely for Rust jobs.
    #[must_use]
    pub fn is_rust(&self) -> bool {
        matches!(self, Self::Rust)
    }

    /// The file whose presence proves the tree is this profile's, checked by the runner against
    /// the materialised source. A mismatch is reported as `unsupported`, never as a failing check.
    #[must_use]
    pub fn marker_file(self) -> &'static str {
        match self {
            Self::Rust => "Cargo.toml",
            Self::Python => "pyproject.toml",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// Maximum total size of an inline source, in bytes of file text. The whole job spec rides in one
/// Pod env var, so the files must leave room for the envelope.
const MAX_INLINE_BYTES: usize = 49_152;

/// How many named sources one job may carry. The unified run the rewrite gate needs is one target
/// plus a handful of pinned sources; the cap bounds the Pod's 8 GiB scratch and its deadline.
pub const MAX_SOURCES: usize = 8;

/// A source name is a path component under `/tmp/sources`: lowercase, a leading letter, no
/// separators. Validating it here keeps a request from naming a path outside that directory.
pub const MAX_SOURCE_NAME_BYTES: usize = 32;

/// Whether a source name is one the runner can materialise as `/tmp/sources/<name>`.
#[must_use]
pub fn valid_source_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_SOURCE_NAME_BYTES {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

/// How large the serialised input may be. The Pod spec rides in one 64 KiB env var, so the input
/// must leave room for the worker's envelope (job ids, the deadline) *and* the fields the worker
/// adds afterwards (an upload's url/token/sha256, an installation token). Counted on the JSON,
/// not the file text: escaping can make a small tree large.
const MAX_INPUT_JSON_BYTES: usize = 52 * 1024;

/// The typed, closed input a job carries. `source` says where the code comes from; a kind that
/// needs code must supply it. `sources` is the multi-source form — every entry names its own
/// toolchain profile — and the two are mutually exclusive. Unknown fields are a parse error, so a
/// caller cannot smuggle a field this contract does not name into the stored jsonb.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobInput {
    /// The toolchain family the plan runs under.
    ///
    /// Omitted from the wire (and from the stored jsonb) for `rust`, the default: a runner image
    /// built before profiles existed denies unknown fields, so sending the key for every job
    /// would break a Rust job during a mixed rollout for no gain.
    ///
    /// With `sources`, this must be left at the default: each named source carries its own
    /// profile, and two opinions about one tree are a request error rather than a precedence rule.
    #[serde(default, skip_serializing_if = "Profile::is_rust")]
    pub profile: Profile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Source>,
    /// Several named sources checked in one Pod, each under its own profile.
    ///
    /// Omitted when empty, for the same mixed-rollout reason as `profile`: a runner image built
    /// before this field existed denies unknown fields, so a single-source job must not carry it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<NamedSource>,
}

/// One source of a multi-source job: the name it is materialised under and the profile its plan
/// runs under. The pair is explicit because a unified run checks a Rust target and its pinned
/// Python sources in one Pod.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NamedSource {
    /// The directory name: `/tmp/sources/<name>`.
    pub name: String,
    /// The toolchain family this source's plan runs under.
    pub profile: Profile,
    /// Where this source comes from.
    pub source: Source,
}

/// Where the code under test comes from. Tagged by `kind`, so a typo is a parse error rather than
/// a source the runner silently cannot materialise.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    /// Files carried inline with the request; written to the work dir before the plan runs.
    Inline { files: Vec<InlineFile> },
    /// A bounded clone of a git repository at one ref.
    ///
    /// `token` is set by the worker only, and only for a github.com URL: a short-lived GitHub App
    /// installation token that lets the clone reach a private repository. A request that sets it
    /// is refused, so a caller cannot make the Pod present a credential of its choosing.
    Git {
        url: String,
        #[serde(rename = "ref", default = "default_ref")]
        git_ref: String,
        #[serde(default = "default_depth")]
        depth: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    /// An archive the client uploaded to `POST /api/sources`.
    ///
    /// A request names only `id`. `url`, `token` and `sha256` are filled in by the worker when it
    /// hands the spec to the Pod: the fetch address and the capability token are the server's to
    /// decide, so a caller cannot point a runner at a URL of its choosing.
    Upload {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },
}

/// One file of an inline source. `path` is relative and checked by [`JobInput::validate`].
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InlineFile {
    pub path: String,
    pub text: String,
}

fn default_depth() -> u32 {
    1
}

/// Why [`JobInput::validate`] rejected a request. The route renders this as the service's one
/// error envelope; `code` is always `"invalid_job_input"`, so a client branches on one value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputError {
    pub code: &'static str,
    pub message: String,
}

impl InputError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_job_input",
            message: message.into(),
        }
    }
}

/// Kinds that cannot run without code. One place inside this module, so the requirement and the
/// error message cannot drift apart.
fn needs_source(kind: JobKind) -> bool {
    matches!(
        kind,
        JobKind::Test | JobKind::Lint | JobKind::Build | JobKind::Check
    )
}

impl JobInput {
    /// Enforce the input contract for `kind`: a source is required for the kinds that run code,
    /// and every source that is present must be materialisable.
    ///
    /// # Errors
    ///
    /// `InputError` naming the first violated rule: a source missing for a code-running kind,
    /// or a source field that is malformed, out of range or not allowed for its variant.
    pub fn validate(&self, kind: JobKind) -> Result<(), InputError> {
        self.validate_shape()?;
        if self.source.is_none() && self.sources.is_empty() && needs_source(kind) {
            return Err(InputError::new(format!(
                "kind {} requires input.source or input.sources",
                kind.as_str()
            )));
        }
        Ok(())
    }

    /// Every upload id this input references, in whichever form it uses.
    ///
    /// The route checks each one belongs to the caller before a row exists: without that, a
    /// foreign or stale id is accepted here and only refused by the worker at claim time, after
    /// the caller has been told 202.
    #[must_use]
    pub fn upload_source_ids(&self) -> Vec<&str> {
        let mut ids = Vec::new();
        if let Some(Source::Upload { id, .. }) = &self.source {
            ids.push(id.as_str());
        }
        for named in &self.sources {
            if let Source::Upload { id, .. } = &named.source {
                ids.push(id.as_str());
            }
        }
        ids
    }

    /// The rules that hold for every kind, including the ones that run no code.
    fn validate_shape(&self) -> Result<(), InputError> {
        if self.source.is_some() && !self.sources.is_empty() {
            return Err(InputError::new(
                "input.source and input.sources are mutually exclusive; send exactly one form",
            ));
        }
        if let Some(source) = &self.source {
            validate_source(source)?;
        }
        if !self.sources.is_empty() {
            if !self.profile.is_rust() {
                return Err(InputError::new(
                    "input.profile must be omitted when input.sources is used: each named source \
                     carries its own profile",
                ));
            }
            if self.sources.len() > MAX_SOURCES {
                return Err(InputError::new(format!(
                    "input.sources carries {} sources, over the {MAX_SOURCES}-source cap",
                    self.sources.len()
                )));
            }
            let mut names = std::collections::HashSet::with_capacity(self.sources.len());
            let mut inline_total = 0usize;
            for named in &self.sources {
                if !valid_source_name(&named.name) {
                    return Err(InputError::new(format!(
                        "source name {:?} must match ^[a-z][a-z0-9-]{{0,{}}}",
                        named.name,
                        MAX_SOURCE_NAME_BYTES - 1
                    )));
                }
                if !names.insert(named.name.as_str()) {
                    return Err(InputError::new(format!(
                        "input.sources repeats the name {:?}",
                        named.name
                    )));
                }
                validate_source(&named.source)?;
                inline_total = inline_total.saturating_add(inline_bytes(&named.source));
            }
            // The per-source cap bounds one tree; this bounds the job, so several inline sources
            // cannot together outgrow the Pod env the whole spec rides in.
            if inline_total > MAX_INLINE_BYTES {
                return Err(InputError::new(format!(
                    "inline sources total {inline_total} bytes, over the {MAX_INLINE_BYTES}-byte cap"
                )));
            }
        }
        // The spec is serialised into a Pod env var; refuse here what the sandbox would reject
        // later, before a row exists and a caller waits on it.
        let bytes = match serde_json::to_string(self) {
            Ok(json) => json.len(),
            Err(err) => {
                return Err(InputError::new(format!(
                    "input could not be serialised: {err}"
                )));
            }
        };
        if bytes > MAX_INPUT_JSON_BYTES {
            return Err(InputError::new(format!(
                "input serialises to {bytes} bytes, over the {MAX_INPUT_JSON_BYTES}-byte cap \
                 (escaping counts against it)"
            )));
        }
        Ok(())
    }
}

/// The file text an inline source carries; zero for every source that is a reference.
fn inline_bytes(source: &Source) -> usize {
    match source {
        Source::Inline { files } => files.iter().map(|file| file.text.len()).sum(),
        Source::Git { .. } | Source::Upload { .. } => 0,
    }
}

/// The source rules. The runner enforces the same rules again on the spec it is handed, so keep
/// the two in step: whatever this function admits must be something the runner can materialise.
fn validate_source(source: &Source) -> Result<(), InputError> {
    match source {
        Source::Inline { files } => {
            if files.is_empty() {
                return Err(InputError::new(
                    "inline source must contain at least one file",
                ));
            }
            let mut total = 0usize;
            let mut seen = std::collections::HashSet::with_capacity(files.len());
            for file in files {
                total = total.saturating_add(file.text.len());
                validate_path(&file.path)?;
                if !seen.insert(file.path.as_str()) {
                    return Err(InputError::new(format!(
                        "inline source repeats path {:?}",
                        file.path
                    )));
                }
            }
            if total > MAX_INLINE_BYTES {
                return Err(InputError::new(format!(
                    "inline source is {total} bytes, over the {MAX_INLINE_BYTES}-byte cap"
                )));
            }
            Ok(())
        }
        Source::Git {
            url,
            git_ref,
            depth,
            token,
        } => {
            if token.is_some() {
                return Err(InputError::new(
                    "a git source's token is minted by the server, never supplied by a request",
                ));
            }
            if url.is_empty() || url.len() > 2048 {
                return Err(InputError::new("git url must be 1..=2048 characters"));
            }
            if url.starts_with('-') {
                return Err(InputError::new("git url must not start with '-'"));
            }
            if !(url.starts_with("https://") || url.starts_with("http://")) {
                return Err(InputError::new(
                    "git url must start with https:// or http://",
                ));
            }
            if url
                .chars()
                .any(|c| c.is_ascii_whitespace() || c.is_ascii_control())
            {
                return Err(InputError::new(
                    "git url must not contain whitespace or control characters",
                ));
            }
            if git_ref.is_empty() || git_ref.len() > 200 {
                return Err(InputError::new("git ref must be 1..=200 characters"));
            }
            if git_ref.starts_with('-') || git_ref.starts_with('/') {
                return Err(InputError::new("git ref must not start with '-' or '/'"));
            }
            if git_ref.contains("..") {
                return Err(InputError::new("git ref must not contain '..'"));
            }
            if !git_ref
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'))
            {
                return Err(InputError::new(
                    "git ref has characters outside [A-Za-z0-9._/-]",
                ));
            }
            if !(1..=1000).contains(depth) {
                return Err(InputError::new("git depth must be between 1 and 1000"));
            }
            Ok(())
        }
        Source::Upload {
            id,
            url,
            token,
            sha256,
        } => {
            validate_uuid(id, "source id")?;
            if url.is_some() || token.is_some() || sha256.is_some() {
                return Err(InputError::new(
                    "an upload source is created by the server: a request may set only its id",
                ));
            }
            Ok(())
        }
    }
}

/// A canonical UUID, as Postgres renders one. The id is cast in SQL, so a malformed one would
/// otherwise surface as a database error rather than a 400 that names the field.
///
/// # Errors
///
/// `InputError` when `value` is not 36 characters or not a canonical hyphenated UUID.
pub fn validate_uuid(value: &str, field: &str) -> Result<(), InputError> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return Err(InputError::new(format!(
            "{field} must be a 36-character UUID"
        )));
    }
    for (index, byte) in bytes.iter().enumerate() {
        let ok = match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        };
        if !ok {
            return Err(InputError::new(format!("{field} is not a UUID")));
        }
    }
    Ok(())
}

/// A relative path inside the source tree, with no way out of it.
fn validate_path(path: &str) -> Result<(), InputError> {
    if path.is_empty() || path.len() > 512 {
        return Err(InputError::new("file path must be 1..=512 characters"));
    }
    if path.contains('\0') {
        return Err(InputError::new(format!("file path {path:?} contains NUL")));
    }
    if path.starts_with('/') || path.contains('\\') || path.ends_with('/') {
        return Err(InputError::new(format!(
            "file path {path:?} is not a relative file path"
        )));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(InputError::new(format!(
                "file path {path:?} escapes the source tree"
            )));
        }
    }
    Ok(())
}

/// The one error shape. `retryable` tells a caller whether re-submitting can help.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Timings {
    pub queued_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

/// A job as returned by every read endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Job {
    pub id: String,
    pub kind: JobKind,
    pub status: JobStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JobError>,
    pub timings: Timings,
}

/// Where the work happens. Either reference an existing workspace (`workspace_id`) or ask for a
/// one-shot one by naming the repository.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkspaceSpec {
    pub repo_url: String,
    #[serde(default = "default_ref")]
    pub git_ref: String,
    #[serde(default)]
    pub toolchain: Option<String>,
}

fn default_ref() -> String {
    "main".to_owned()
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateJobRequest {
    pub kind: JobKind,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub workspace: Option<WorkspaceSpec>,
    #[serde(default)]
    pub input: JobInput,
    /// Caller-supplied dedupe key. Unique per tenant: a replay returns the original job.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u32,
}

fn default_timeout() -> u32 {
    900
}

/// The `POST /api/jobs/status` body. A POST because conveyor's action engine cannot template a
/// path and a GET carries no body.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct JobStatusRequest {
    pub id: String,
}

/// First HTTP-level failure code: the request never reached the store.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    pub retryable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_wire_names_are_stable() {
        assert_eq!(JobKind::GithubOp.as_str(), "github_op");
        assert_eq!(JobKind::ParityOpenapi.as_str(), "parity_openapi");
        assert_eq!(
            serde_json::to_string(&JobKind::DependencyUpgrade).unwrap(),
            "\"dependency_upgrade\""
        );
    }

    #[test]
    fn terminal_states_are_the_three_outcomes() {
        assert!(JobStatus::Succeeded.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
        assert!(!JobStatus::Queued.is_terminal());
    }

    fn inline(files: &[(&str, &str)]) -> JobInput {
        JobInput {
            profile: Profile::Rust,
            sources: Vec::new(),
            source: Some(Source::Inline {
                files: files
                    .iter()
                    .map(|(path, text)| InlineFile {
                        path: (*path).to_owned(),
                        text: (*text).to_owned(),
                    })
                    .collect(),
            }),
        }
    }

    fn git(url: &str, git_ref: &str) -> JobInput {
        JobInput {
            profile: Profile::Rust,
            sources: Vec::new(),
            source: Some(Source::Git {
                url: url.to_owned(),
                git_ref: git_ref.to_owned(),
                depth: 1,
                token: None,
            }),
        }
    }

    fn reject(input: &JobInput, kind: JobKind) -> InputError {
        input
            .validate(kind)
            .expect_err(&format!("{input:?} must be rejected for {kind:?}"))
    }

    #[test]
    fn valid_inline_source_is_accepted() {
        for kind in [JobKind::Test, JobKind::Lint, JobKind::Build, JobKind::Check] {
            assert!(
                inline(&[("src/main.rs", "fn main() {}")])
                    .validate(kind)
                    .is_ok(),
                "{kind:?}"
            );
        }
        assert!(
            inline(&[("Cargo.toml", ""), ("src/lib.rs", "pub fn f() {}")])
                .validate(JobKind::Test)
                .is_ok()
        );
    }

    #[test]
    fn test_lint_build_require_a_source() {
        for kind in [JobKind::Test, JobKind::Lint, JobKind::Build, JobKind::Check] {
            let error = reject(&JobInput::default(), kind);
            assert_eq!(error.code, "invalid_job_input");
            assert_eq!(
                error.message,
                format!(
                    "kind {} requires input.source or input.sources",
                    kind.as_str()
                )
            );
        }
        // Every other kind runs without code, so a source is optional there.
        for kind in [
            JobKind::Analyze,
            JobKind::Scan,
            JobKind::GithubOp,
            JobKind::DependencyUpgrade,
            JobKind::CrateMap,
            JobKind::ParityOpenapi,
            JobKind::ParitySchema,
            JobKind::ParityBehaviour,
            JobKind::ParityPerf,
        ] {
            assert!(JobInput::default().validate(kind).is_ok(), "{kind:?}");
        }
    }

    #[test]
    fn empty_inline_source_is_rejected() {
        let error = reject(&inline(&[]), JobKind::Test);
        assert_eq!(error.code, "invalid_job_input");
        assert!(
            error.message.contains("at least one file"),
            "{}",
            error.message
        );
    }

    #[test]
    fn inline_cap_is_the_sum_of_the_file_text() {
        let cap = MAX_INLINE_BYTES;
        assert!(
            inline(&[("a.txt", "x".repeat(cap).as_str())])
                .validate(JobKind::Test)
                .is_ok(),
            "exactly the cap is allowed"
        );
        let error = reject(
            &inline(&[("a.txt", "x".repeat(cap + 1).as_str())]),
            JobKind::Test,
        );
        assert_eq!(error.code, "invalid_job_input");
        assert_eq!(
            error.message,
            format!(
                "inline source is {} bytes, over the {cap}-byte cap",
                cap + 1
            )
        );

        // The cap is the sum across files, not a per-file allowance.
        let half = "x".repeat(cap / 2 + 1);
        let error = reject(
            &inline(&[("a.txt", half.as_str()), ("b.txt", half.as_str())]),
            JobKind::Test,
        );
        assert_eq!(error.code, "invalid_job_input");
        assert_eq!(
            error.message,
            format!(
                "inline source is {} bytes, over the {cap}-byte cap",
                2 * (cap / 2 + 1)
            )
        );
    }

    #[test]
    fn inline_paths_cannot_escape_the_source_tree() {
        for bad in [
            "../etc/passwd",
            "a/../b",
            "/etc/passwd",
            "a//b",
            "a/",
            "a\\b",
            ".",
            "..",
            "",
        ] {
            let error = reject(&inline(&[(bad, "x")]), JobKind::Test);
            assert_eq!(error.code, "invalid_job_input", "{bad:?}");
        }
        assert!(
            inline(&[("src/deep/mod.rs", "x"), ("Cargo.toml", "x")])
                .validate(JobKind::Test)
                .is_ok()
        );
    }

    #[test]
    fn duplicate_inline_paths_are_rejected() {
        let error = reject(
            &inline(&[("Cargo.toml", "a"), ("Cargo.toml", "b")]),
            JobKind::Test,
        );
        assert_eq!(error.code, "invalid_job_input");
        assert!(error.message.contains("repeats path"), "{}", error.message);
    }

    #[test]
    fn valid_git_source_is_accepted() {
        assert!(
            git("https://github.com/nonagenticai/coding.git", "main")
                .validate(JobKind::Build)
                .is_ok()
        );
        assert!(
            git("http://git.example.com/o/r", "feature/x-1.2")
                .validate(JobKind::Test)
                .is_ok()
        );
    }

    #[test]
    fn git_url_cannot_be_argv() {
        for bad in [
            "-x",
            "--upload-pack=x",
            "git://host/repo",
            "https://host/repo with space",
            "",
        ] {
            let error = reject(&git(bad, "main"), JobKind::Test);
            assert_eq!(error.code, "invalid_job_input", "url {bad:?}");
        }
    }

    #[test]
    fn git_ref_cannot_be_argv_or_escape() {
        for bad in [
            "--",
            "--upload-pack=x",
            "/main",
            "a..b",
            "main~1",
            "main^",
            "main space",
        ] {
            let error = reject(&git("https://host/repo", bad), JobKind::Test);
            assert_eq!(error.code, "invalid_job_input", "ref {bad:?}");
        }
        let error = reject(&git("https://host/repo", &"r".repeat(201)), JobKind::Test);
        assert_eq!(error.code, "invalid_job_input");
    }

    #[test]
    fn git_depth_is_bounded() {
        for depth in [0u32, 1001] {
            let input = JobInput {
                profile: Profile::Rust,
                sources: Vec::new(),
                source: Some(Source::Git {
                    url: "https://host/repo".to_owned(),
                    git_ref: "main".to_owned(),
                    depth,
                    token: None,
                }),
            };
            let error = reject(&input, JobKind::Test);
            assert_eq!(error.code, "invalid_job_input");
            assert!(error.message.contains("depth"), "{}", error.message);
        }
    }

    fn upload(id: &str) -> JobInput {
        JobInput {
            profile: Profile::Rust,
            sources: Vec::new(),
            source: Some(Source::Upload {
                id: id.to_owned(),
                url: None,
                token: None,
                sha256: None,
            }),
        }
    }

    #[test]
    fn an_upload_source_names_only_its_id() {
        assert!(
            upload("6f1a1f6a-1d3a-4f2c-9a1e-2f4a1b7c8d90")
                .validate(JobKind::Check)
                .is_ok()
        );
        // The fetch address, the token and the hash are the server's to decide.
        let mut with_url = upload("6f1a1f6a-1d3a-4f2c-9a1e-2f4a1b7c8d90");
        if let Some(Source::Upload { url, .. }) = with_url.source.as_mut() {
            *url = Some("http://elsewhere/blob".to_owned());
        }
        let error = reject(&with_url, JobKind::Check);
        assert!(error.message.contains("created by the server"), "{error:?}");
        // And the id must be one Postgres can cast.
        for bad in [
            "",
            "nope",
            "6f1a1f6a1d3a4f2c9a1e2f4a1b7c8d90",
            "zz1a1f6a-1d3a-4f2c-9a1e-2f4a1b7c8d90",
        ] {
            let error = reject(&upload(bad), JobKind::Check);
            assert!(error.message.contains("source id"), "{bad:?}: {error:?}");
        }
    }

    #[test]
    fn a_client_cannot_supply_a_git_token() {
        let input = JobInput {
            profile: Profile::Rust,
            sources: Vec::new(),
            source: Some(Source::Git {
                url: "https://github.com/o/r".to_owned(),
                git_ref: "main".to_owned(),
                depth: 1,
                token: Some("ghs_supplied".to_owned()),
            }),
        };
        let error = reject(&input, JobKind::Test);
        assert!(error.message.contains("minted by the server"), "{error:?}");
        // And it never serialises into a stored request when absent.
        let value = serde_json::to_value(git("https://host/repo", "main")).unwrap();
        assert!(value["source"].get("token").is_none(), "{value}");
    }

    #[test]
    fn job_input_rejects_unknown_fields() {
        let raw = r#"{"source":{"kind":"inline","files":[{"path":"a.txt","text":""}]},"bogus":1}"#;
        assert!(serde_json::from_str::<JobInput>(raw).is_err());
    }

    #[test]
    fn job_input_serde_defaults_source_and_git_fields() {
        assert!(JobInput::default().source.is_none());
        assert!(
            serde_json::from_str::<JobInput>("{}")
                .expect("an empty object is the default input")
                .source
                .is_none()
        );
        let input: JobInput =
            serde_json::from_str(r#"{"source":{"kind":"git","url":"https://host/repo"}}"#)
                .expect("git source with defaults");
        match input.source {
            Some(Source::Git { git_ref, depth, .. }) => {
                assert_eq!(git_ref, "main");
                assert_eq!(depth, 1);
            }
            other => panic!("expected a git source, got {other:?}"),
        }
    }

    #[test]
    fn source_wire_shape_is_tagged_and_stable() {
        let value = serde_json::to_value(git("https://host/repo", "main")).unwrap();
        assert_eq!(value["source"]["kind"], "git");
        assert_eq!(value["source"]["url"], "https://host/repo");
        assert_eq!(value["source"]["ref"], "main");
        assert_eq!(value["source"]["depth"], 1);

        let value = serde_json::to_value(inline(&[("Cargo.toml", "x")])).unwrap();
        assert_eq!(value["source"]["kind"], "inline");
        assert_eq!(value["source"]["files"][0]["path"], "Cargo.toml");
        assert_eq!(value["source"]["files"][0]["text"], "x");
    }

    fn named(name: &str, profile: Profile, git_ref: &str) -> NamedSource {
        NamedSource {
            name: name.to_owned(),
            profile,
            source: Source::Git {
                url: format!("https://github.com/o/{name}"),
                git_ref: git_ref.to_owned(),
                depth: 1,
                token: None,
            },
        }
    }

    fn sources(list: Vec<NamedSource>) -> JobInput {
        JobInput {
            profile: Profile::Rust,
            source: None,
            sources: list,
        }
    }

    #[test]
    fn a_multi_source_job_is_two_named_sources() {
        let input = sources(vec![
            named("target", Profile::Rust, "main"),
            named("source-1", Profile::Python, "abc123"),
        ]);
        assert!(input.validate(JobKind::Check).is_ok());
        // The wire keeps the named form, and a source names its own profile.
        let value = serde_json::to_value(&input).unwrap();
        assert_eq!(value["sources"][0]["name"], "target");
        assert_eq!(value["sources"][1]["profile"], "python");
        assert!(value.get("source").is_none());
        // A single-source job must not gain a `sources` key: an old runner denies unknown fields.
        let value = serde_json::to_value(git("https://host/repo", "main")).unwrap();
        assert!(value.get("sources").is_none());
    }

    #[test]
    fn the_two_source_forms_are_mutually_exclusive() {
        for kind in [JobKind::Test, JobKind::Lint, JobKind::Build, JobKind::Check] {
            let mut input = sources(vec![named("target", Profile::Rust, "main")]);
            input.source = Some(Source::Git {
                url: "https://host/repo".to_owned(),
                git_ref: "main".to_owned(),
                depth: 1,
                token: None,
            });
            let error = reject(&input, kind);
            assert!(
                error.message.contains("mutually exclusive"),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn a_named_source_must_be_a_safe_path_component() {
        for bad in [
            "",
            "Target",
            "1target",
            "tar get",
            "target/one",
            "target.one",
            "../target",
            "target_1",
            &"a".repeat(MAX_SOURCE_NAME_BYTES + 1),
        ] {
            let error = reject(
                &sources(vec![named(bad, Profile::Rust, "main")]),
                JobKind::Check,
            );
            assert!(error.message.contains("source name"), "{bad:?}");
        }
        for good in ["target", "source-1", "a", "py-source-2024"] {
            assert!(
                sources(vec![named(good, Profile::Rust, "main")])
                    .validate(JobKind::Check)
                    .is_ok(),
                "{good:?}"
            );
        }
    }

    #[test]
    fn names_are_unique_and_the_count_is_bounded() {
        let error = reject(
            &sources(vec![
                named("target", Profile::Rust, "main"),
                named("target", Profile::Python, "main"),
            ]),
            JobKind::Check,
        );
        assert!(error.message.contains("repeats the name"), "{error:?}");

        let over: Vec<NamedSource> = (0..=MAX_SOURCES)
            .map(|index| named(&format!("source-{index}"), Profile::Rust, "main"))
            .collect();
        let error = reject(&sources(over), JobKind::Check);
        assert!(error.message.contains("source cap"), "{error:?}");
    }

    #[test]
    fn the_job_level_profile_must_be_omitted_with_sources() {
        let mut input = sources(vec![named("target", Profile::Rust, "main")]);
        input.profile = Profile::Python;
        let error = reject(&input, JobKind::Check);
        assert!(error.message.contains("input.profile"), "{error:?}");
    }

    #[test]
    fn inline_bytes_are_summed_across_sources() {
        let big = "x".repeat(MAX_INLINE_BYTES / 2 + 1);
        let input = sources(vec![
            NamedSource {
                name: "one".to_owned(),
                profile: Profile::Rust,
                source: Source::Inline {
                    files: vec![InlineFile {
                        path: "Cargo.toml".to_owned(),
                        text: big.clone(),
                    }],
                },
            },
            NamedSource {
                name: "two".to_owned(),
                profile: Profile::Rust,
                source: Source::Inline {
                    files: vec![InlineFile {
                        path: "Cargo.toml".to_owned(),
                        text: big,
                    }],
                },
            },
        ]);
        let error = reject(&input, JobKind::Check);
        assert!(error.message.contains("inline sources total"), "{error:?}");
    }

    #[test]
    fn an_escaping_source_cannot_outgrow_the_pod_spec() {
        // Control characters are one byte of text and six bytes of JSON; the cap is on the JSON.
        let input = sources(vec![NamedSource {
            name: "one".to_owned(),
            profile: Profile::Rust,
            source: Source::Inline {
                files: vec![InlineFile {
                    path: "src/lib.rs".to_owned(),
                    text: "\u{1}".repeat(MAX_INLINE_BYTES / 2),
                }],
            },
        }]);
        let error = reject(&input, JobKind::Check);
        assert!(error.message.contains("serialises to"), "{}", error.message);
    }

    #[test]
    fn a_named_git_source_cannot_supply_its_own_token() {
        let mut source = named("target", Profile::Rust, "main");
        if let Source::Git { token, .. } = &mut source.source {
            *token = Some("ghs_supplied".to_owned());
        }
        let error = reject(&sources(vec![source]), JobKind::Check);
        assert!(error.message.contains("minted by the server"), "{error:?}");
    }
}
