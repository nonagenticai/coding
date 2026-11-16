//! The result contract: exactly one NDJSON line, last on stdout.
//!
//! The worker reads the Pod log once, at the end, and looks for the last line carrying
//! `coding_result`. Everything before that line is ordinary job output and is never parsed.
//!
//! **Exit-code rule.** The runner exits 0 whenever it wrote a result, even when the job's checks
//! failed: a failing test is data about the code, not a broken runner. A non-zero exit from this
//! process therefore means the runner itself crashed, and the worker reports `runner_failed`
//! instead of inventing a verdict.

use std::io::Write;

use serde::Serialize;

/// The job's overall verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Every check passed.
    Succeeded,
    /// At least one check ran and failed.
    Failed,
    /// This runner has no plan for the kind (a later milestone owns it).
    Unsupported,
    /// The job deadline was reached; the child was killed.
    Timeout,
    /// SIGTERM arrived (the Pod is being deleted, usually a cancel); the child was killed.
    Cancelled,
    /// The spec could not be used at all.
    InvalidSpec,
}

/// One command's outcome.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// Human name of the step, e.g. `cargo test`.
    pub name: String,
    /// Whether it passed.
    pub status: CheckStatus,
    /// The child's exit code when it exited normally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Wall time for this step.
    pub duration_ms: u64,
    /// The tail of the check's own output, last [`MAX_CHECK_OUTPUT_BYTES`] bytes, on a failure
    /// only. Without it a verdict is a check name and an exit code: a developer cannot act on
    /// "clippy failed, 101".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tail: Option<String>,
}

/// How much of one check's output travels back in the result. Three checks × this stays an order
/// of magnitude under the worker's 256 KiB log read, so the result line is never truncated.
pub const MAX_CHECK_OUTPUT_BYTES: usize = 8 * 1024;

/// A check's own outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    /// Exit status 0.
    Passed,
    /// Non-zero exit status.
    Failed,
}

/// Where the code came from, in the result.
#[derive(Debug, Clone, Serialize)]
pub struct SourceInfo {
    /// `inline` or `git`.
    pub kind: String,
    /// Number of files written for an inline source; 0 for git.
    pub files: usize,
    /// The checked-out commit for a git source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

/// Whether the Pod log was truncated when the worker read it.
#[derive(Debug, Clone, Serialize)]
pub struct LogInfo {
    /// Always `false` from the runner; the worker sets it when its capped read truncated.
    pub truncated: bool,
}

/// The value inside the final `{"coding_result": …}` line.
#[derive(Debug, Clone, Serialize)]
pub struct RunnerResult {
    /// The verdict.
    pub status: Status,
    /// One line a human can read without parsing.
    pub summary: String,
    /// Every step that ran, in order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Check>,
    /// Where the code came from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInfo>,
    /// Why the spec or the source setup failed, when that is the outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Log-read metadata.
    pub log: LogInfo,
}

impl RunnerResult {
    /// A result with no checks yet.
    pub fn new(status: Status, summary: impl Into<String>) -> Self {
        Self {
            status,
            summary: summary.into(),
            checks: Vec::new(),
            source: None,
            error: None,
            log: LogInfo { truncated: false },
        }
    }

    /// Attach the source summary.
    pub fn with_source(mut self, source: Option<SourceInfo>) -> Self {
        self.source = source;
        self
    }

    /// Attach an error explanation.
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }
}

/// Write the result as the last stdout line.
///
/// Returns `true` when the line was written. A `false` return is the only case where the process
/// should exit non-zero: no result line means the worker has nothing to read.
pub fn emit(result: &RunnerResult) -> bool {
    // A child that exited without a trailing newline must not fuse with our JSON line.
    println!();
    match serde_json::to_string(&serde_json::json!({ "coding_result": result })) {
        Ok(line) => {
            println!("{line}");
            std::io::stdout().flush().is_ok()
        }
        Err(err) => {
            eprintln!("coding-runner: could not serialise the result: {err}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_are_stable() {
        assert_eq!(
            serde_json::to_string(&Status::InvalidSpec).unwrap(),
            "\"invalid_spec\""
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Failed).unwrap(),
            "\"failed\""
        );
    }

    #[test]
    fn a_result_carries_checks_and_an_optional_source() {
        let mut result = RunnerResult::new(Status::Failed, "1 of 1 checks failed");
        result.checks.push(Check {
            name: "cargo test".to_owned(),
            status: CheckStatus::Failed,
            exit_code: Some(101),
            duration_ms: 812,
            output_tail: Some("assertion failed".to_owned()),
        });
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["checks"][0]["exit_code"], 101);
        assert_eq!(value["checks"][0]["output_tail"], "assertion failed");
        assert!(value.get("source").is_none());
        assert!(value.get("error").is_none());
    }
}
