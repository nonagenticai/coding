//! The job spec the worker passes to a runner Pod, and the source rules this process enforces.
//!
//! The API validates the same shape before a row is written. The runner validates again on
//! purpose: the spec arrives through a Pod env var, and a process that trusts its input because
//! "the API checked it" has no defence of its own.

use crate::domain::{InlineFile, JobInput, JobKind, Source};

/// The env var carrying the spec. Set by [`crate::sandbox`]; read here.
pub const SPEC_ENV: &str = "CODING_JOB_SPEC";
/// Inline source cap. The Pod spec cap is 64 KiB and the whole spec rides in one env var, so the
/// files must leave room for the envelope.
pub const MAX_INLINE_BYTES: usize = 48 * 1024;
/// The largest deadline this process will honour, in seconds (24 h).
pub const MAX_DEADLINE_SECONDS: u64 = 86_400;
/// Default deadline when the spec omits one.
pub const DEFAULT_DEADLINE_SECONDS: u64 = 900;

/// Why a spec could not be used. Every variant produces a result line, never a silent exit.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SpecError {
    /// `CODING_JOB_SPEC` is unset or not unicode.
    #[error("{SPEC_ENV} is not set")]
    Missing,
    /// The env var is not JSON, or does not match the spec shape.
    #[error("{SPEC_ENV} is not a valid job spec: {0}")]
    Json(String),
    /// The spec is JSON but its content is unusable.
    #[error("invalid job spec: {0}")]
    Invalid(String),
}

/// One job as the worker hands it to a Pod.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerSpec {
    /// The job row id, echoed in the result.
    pub job_id: String,
    /// The owning tenant, echoed in the result.
    #[serde(default)]
    pub tenant_id: String,
    /// What to do. Closed enum; see [`crate::runner::plan`].
    pub kind: JobKind,
    /// The attempt number (1-based), echoed in the result.
    #[serde(default)]
    pub attempt: i32,
    /// The workspace the job was created against, if any. Accepted (the worker sends it) and
    /// reserved for workspace-mode execution; a job Pod does not use it.
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// The runner stops the child at this many seconds and reports `timeout`.
    #[serde(default = "default_deadline")]
    pub deadline_seconds: u64,
    /// Where the code comes from.
    #[serde(default)]
    pub input: JobInput,
}

fn default_deadline() -> u64 {
    DEFAULT_DEADLINE_SECONDS
}

impl RunnerSpec {
    /// Read and validate the spec from the process environment.
    ///
    /// # Errors
    ///
    /// `SpecError::Missing` when `CODING_JOB_SPEC` is unset or not unicode, plus every error
    /// [`RunnerSpec::from_json`] raises.
    pub fn from_env() -> Result<Self, SpecError> {
        let raw = std::env::var(SPEC_ENV).map_err(|_| SpecError::Missing)?;
        Self::from_json(&raw)
    }

    /// Parse and validate a spec. Split out so the rules are testable without a process env.
    ///
    /// # Errors
    ///
    /// `SpecError::Json` when `raw` is not a spec-shaped JSON document, and
    /// `SpecError::Invalid` when a validation rule fails.
    pub fn from_json(raw: &str) -> Result<Self, SpecError> {
        let spec: Self =
            serde_json::from_str(raw).map_err(|err| SpecError::Json(err.to_string()))?;
        spec.validate()?;
        Ok(spec)
    }

    /// Every rule that makes a spec usable.
    ///
    /// # Errors
    ///
    /// `SpecError::Invalid` naming the first broken rule: an empty job id, a deadline out of
    /// range, mutually exclusive source forms, or a source that fails its own rules.
    pub fn validate(&self) -> Result<(), SpecError> {
        if self.job_id.trim().is_empty() {
            return Err(SpecError::Invalid("job_id is empty".to_owned()));
        }
        if self.deadline_seconds == 0 || self.deadline_seconds > MAX_DEADLINE_SECONDS {
            return Err(SpecError::Invalid(format!(
                "deadline_seconds must be between 1 and {MAX_DEADLINE_SECONDS}"
            )));
        }
        if self.input.source.is_some() && !self.input.sources.is_empty() {
            return Err(SpecError::Invalid(
                "input.source and input.sources are mutually exclusive".to_owned(),
            ));
        }
        if let Some(source) = &self.input.source {
            return validate_source(source).map_err(SpecError::Invalid);
        }
        if !self.input.sources.is_empty() {
            if !self.input.profile.is_rust() {
                return Err(SpecError::Invalid(
                    "input.profile must be omitted when input.sources is used".to_owned(),
                ));
            }
            if self.input.sources.len() > crate::domain::MAX_SOURCES {
                return Err(SpecError::Invalid(format!(
                    "input.sources carries {} sources, over the {}-source cap",
                    self.input.sources.len(),
                    crate::domain::MAX_SOURCES
                )));
            }
            let mut names: Vec<&str> = Vec::with_capacity(self.input.sources.len());
            let mut inline_total = 0usize;
            for named in &self.input.sources {
                if !crate::domain::valid_source_name(&named.name) {
                    return Err(SpecError::Invalid(format!(
                        "source name {:?} is not a lowercase path component",
                        named.name
                    )));
                }
                if names.contains(&named.name.as_str()) {
                    return Err(SpecError::Invalid(format!(
                        "input.sources repeats the name {:?}",
                        named.name
                    )));
                }
                names.push(named.name.as_str());
                validate_source(&named.source).map_err(SpecError::Invalid)?;
                if let Source::Inline { files } = &named.source {
                    inline_total = inline_total
                        .saturating_add(files.iter().map(|file| file.text.len()).sum::<usize>());
                }
            }
            if inline_total > MAX_INLINE_BYTES {
                return Err(SpecError::Invalid(format!(
                    "inline sources total {inline_total} bytes, over the {MAX_INLINE_BYTES}-byte cap"
                )));
            }
            return Ok(());
        }
        if self.needs_source() {
            return Err(SpecError::Invalid(format!(
                "kind {:?} requires input.source or input.sources",
                self.kind.as_str()
            )));
        }
        Ok(())
    }

    /// Kinds that cannot run without code.
    #[must_use]
    pub fn needs_source(&self) -> bool {
        matches!(
            self.kind,
            JobKind::Test | JobKind::Lint | JobKind::Build | JobKind::Check
        )
    }
}

/// The source rules, enforced identically here and in the API.
// reason: one linear rule list; splitting it would scatter the shared error messages.
#[allow(clippy::too_many_lines)]
/// Validate one source.
///
/// # Errors
///
/// A `String` naming the first broken rule: no files, a path that leaves the tree, a repeated
/// path, or a size over the inline cap.
pub fn validate_source(source: &Source) -> Result<(), String> {
    match source {
        Source::Inline { files } => {
            if files.is_empty() {
                return Err("inline source has no files".to_owned());
            }
            let mut total = 0usize;
            let mut seen: Vec<&str> = Vec::with_capacity(files.len());
            for file in files {
                total = total.saturating_add(file.text.len());
                validate_path(&file.path)?;
                if seen.contains(&file.path.as_str()) {
                    return Err(format!("inline source repeats path {:?}", file.path));
                }
                seen.push(file.path.as_str());
            }
            if total > MAX_INLINE_BYTES {
                return Err(format!(
                    "inline source is {total} bytes, over the {MAX_INLINE_BYTES}-byte cap"
                ));
            }
            Ok(())
        }
        Source::Git {
            url,
            git_ref,
            depth,
            token,
        } => {
            if url.is_empty() || url.len() > 2048 {
                return Err("git url must be 1..=2048 characters".to_owned());
            }
            if url.starts_with('-') {
                return Err("git url must not start with '-'".to_owned());
            }
            if !(url.starts_with("https://") || url.starts_with("http://")) {
                return Err("git url must be http(s) in this version".to_owned());
            }
            if url
                .chars()
                .any(|c| c.is_ascii_whitespace() || c.is_control())
            {
                return Err("git url must not contain whitespace or control characters".to_owned());
            }
            if git_ref.is_empty() || git_ref.len() > 200 {
                return Err("git ref must be 1..=200 characters".to_owned());
            }
            if git_ref.starts_with('-') || git_ref.starts_with('/') {
                return Err("git ref must not start with '-' or '/'".to_owned());
            }
            if git_ref.contains("..") {
                return Err("git ref must not contain '..'".to_owned());
            }
            if !git_ref
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'))
            {
                return Err("git ref has characters outside [A-Za-z0-9._/-]".to_owned());
            }
            if !(1..=1000).contains(depth) {
                return Err("git depth must be between 1 and 1000".to_owned());
            }
            if let Some(token) = token {
                // A GitHub App installation token. Measured 2026-11-14 against the real API: it
                // is **383 characters** and contains **dots** (`ghs_` followed by a JWT) — not the
                // short base62 string this check first assumed, which rejected every real token
                // with `git token must be 16..=512 characters`. Dots are harmless here: the value
                // is an env var, never an argv or a shell word. The bound only stops a value that
                // could not be a token at all.
                if !(16..=1024).contains(&token.len())
                    || !token
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                {
                    return Err(
                        "git token must be 16..=1024 characters of [A-Za-z0-9._-]".to_owned()
                    );
                }
            }
            Ok(())
        }
        Source::Upload {
            id,
            url,
            token,
            sha256,
        } => {
            validate_uuid(id)?;
            let url = url.as_deref().ok_or("upload source has no url")?;
            if url.len() < 8 || url.len() > 2048 {
                return Err("upload url must be 8..=2048 characters".to_owned());
            }
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err("upload url must be http(s) in this version".to_owned());
            }
            if url
                .chars()
                .any(|c| c.is_ascii_whitespace() || c.is_control())
            {
                return Err(
                    "upload url must not contain whitespace or control characters".to_owned(),
                );
            }
            // A capability token: hex only, so it cannot carry a header or a URL semantics.
            let token = token.as_deref().ok_or("upload source has no token")?;
            if !(16..=256).contains(&token.len()) || !token.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err("upload token must be 16..=256 hex characters".to_owned());
            }
            let sha256 = sha256.as_deref().ok_or("upload source has no sha256")?;
            if sha256.len() != 64 || !sha256.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err("upload sha256 must be 64 hex characters".to_owned());
            }
            Ok(())
        }
    }
}

/// A canonical UUID. The runner never casts one in SQL, but the id is echoed into errors and used
/// as an opaque key, so the shape is checked for the same reason the API checks it.
fn validate_uuid(value: &str) -> Result<(), String> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return Err("source id must be a 36-character UUID".to_owned());
    }
    for (index, byte) in bytes.iter().enumerate() {
        let ok = match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        };
        if !ok {
            return Err("source id is not a UUID".to_owned());
        }
    }
    Ok(())
}

/// A relative path inside the source tree, with no way out of it.
fn validate_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.len() > 512 {
        return Err("file path must be 1..=512 characters".to_owned());
    }
    if path.contains('\0') {
        return Err(format!("file path {path:?} contains NUL"));
    }
    if path.starts_with('/') || path.contains('\\') || path.ends_with('/') {
        return Err(format!("file path {path:?} is not a relative file path"));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(format!("file path {path:?} escapes the source tree"));
        }
    }
    Ok(())
}

/// Number of files in a source, for the result's `source` summary.
#[must_use]
pub fn file_count(source: &Source) -> usize {
    match source {
        Source::Inline { files } => files.len(),
        // The archive's real count is known only after it is fetched; `exec` fills it in.
        Source::Git { .. } | Source::Upload { .. } => 0,
    }
}

/// Unused here but part of the shared shape: keeps `InlineFile` imported in one place.
#[must_use]
pub fn first_inline_file(source: &Source) -> Option<&InlineFile> {
    match source {
        Source::Inline { files } => files.first(),
        Source::Git { .. } | Source::Upload { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inline(path: &str, text: &str) -> Source {
        Source::Inline {
            files: vec![InlineFile {
                path: path.to_owned(),
                text: text.to_owned(),
            }],
        }
    }

    fn spec_with(source: Option<Source>) -> RunnerSpec {
        RunnerSpec {
            job_id: "job-1".to_owned(),
            tenant_id: "t".to_owned(),
            kind: JobKind::Test,
            attempt: 1,
            workspace_id: None,
            deadline_seconds: 60,
            input: JobInput {
                source,
                ..Default::default()
            },
        }
    }

    #[test]
    fn missing_source_is_invalid_for_test_lint_build() {
        for kind in [JobKind::Test, JobKind::Lint, JobKind::Build] {
            let mut spec = spec_with(None);
            spec.kind = kind;
            assert!(spec.validate().is_err(), "{kind:?} must need a source");
        }
        let mut spec = spec_with(None);
        spec.kind = JobKind::Scan;
        assert!(spec.validate().is_ok(), "scan may run without a source");
    }

    #[test]
    fn inline_paths_cannot_escape() {
        for bad in ["../x", "/etc/passwd", "a/../b", "a//b", "a/", "a\\b"] {
            assert!(validate_path(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(validate_path("src/main.rs").is_ok());
        assert!(validate_path("Cargo.toml").is_ok());
    }

    #[test]
    fn inline_cap_is_enforced() {
        let big = "x".repeat(MAX_INLINE_BYTES + 1);
        assert!(validate_source(&inline("a.txt", &big)).is_err());
    }

    #[test]
    fn git_rules_reject_argument_injection() {
        let cases = [
            Source::Git {
                url: "-u".to_owned(),
                git_ref: "main".to_owned(),
                depth: 1,
                token: None,
            },
            Source::Git {
                url: "https://h/r".to_owned(),
                git_ref: "--upload-pack=x".to_owned(),
                depth: 1,
                token: None,
            },
            Source::Git {
                url: "https://h/r".to_owned(),
                git_ref: "a..b".to_owned(),
                depth: 1,
                token: None,
            },
            Source::Git {
                url: "https://h/r".to_owned(),
                git_ref: "main".to_owned(),
                depth: 0,
                token: None,
            },
        ];
        for source in cases {
            assert!(
                validate_source(&source).is_err(),
                "{source:?} must be rejected"
            );
        }
        assert!(
            validate_source(&Source::Git {
                url: "https://github.com/o/r.git".to_owned(),
                git_ref: "deadbeef".to_owned(),
                depth: 1,
                token: None,
            })
            .is_ok()
        );
    }

    #[test]
    fn a_git_token_is_bounded_and_header_safe() {
        let base = |token: Option<&str>| Source::Git {
            url: "https://github.com/o/r".to_owned(),
            git_ref: "main".to_owned(),
            depth: 1,
            token: token.map(str::to_owned),
        };
        assert!(validate_source(&base(Some("ghs_0123456789abcdef"))).is_ok());
        // The shape GitHub actually issues (measured 2026-11-14): long, with dots.
        let real = format!("ghs_{}.{}", "a".repeat(120), "b".repeat(255));
        assert!(real.len() > 300);
        assert!(
            validate_source(&base(Some(&real))).is_ok(),
            "a real installation token must be accepted: {} chars",
            real.len()
        );
        for bad in ["short", "has space", "has\nnewline", &"x".repeat(1025)] {
            assert!(
                validate_source(&base(Some(bad))).is_err(),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn spec_round_trips_the_worker_shape() {
        let raw = r#"{"job_id":"j","tenant_id":"t","kind":"test","attempt":1,
                      "deadline_seconds":60,"input":{"source":{"kind":"inline",
                      "files":[{"path":"Cargo.toml","text":"x"}]}}}"#;
        let spec = RunnerSpec::from_json(raw).expect("valid spec");
        assert_eq!(spec.kind, JobKind::Test);
        assert_eq!(file_count(spec.input.source.as_ref().unwrap()), 1);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let raw = r#"{"job_id":"j","kind":"test","bogus":1,"input":{"source":null}}"#;
        assert!(matches!(
            RunnerSpec::from_json(raw),
            Err(SpecError::Json(_))
        ));
    }
}
