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
    /// The source this check ran against, for a multi-source job. Absent for a single-source job,
    /// whose result has exactly the shape it had before multi-source existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
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

/// How much of one check's output travels back in the result.
pub const MAX_CHECK_OUTPUT_BYTES: usize = 8 * 1024;

/// The largest result line this process will write. The worker reads the Pod log with a 256 KiB
/// cap, and a result line cut in half is a line the worker cannot read — which must never be
/// mistaken for a pass. A multi-source job can produce several dozen checks, so the line is
/// bounded here, at the producer, instead of trusting the consumer's cap.
pub const RESULT_LINE_BUDGET: usize = 192 * 1024;

/// What replaces a tail dropped to keep the line inside [`RESULT_LINE_BUDGET`].
const TAIL_DROPPED: &str = "...[output tail dropped: the job result must stay inside the runner's \
                             result-line budget]";

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
    /// The source's name, for a multi-source job. Absent for a single-source job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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

/// What the compilation cache did for this job.
///
/// Informational only: the verdict comes from the checks, and a job whose cache was unreachable
/// renders exactly like one whose cache missed every time. Reporting it is what makes the warm/cold
/// measurement possible without scraping logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub errors: u64,
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
    /// Where the code came from, for a single-source job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceInfo>,
    /// Every source of a multi-source job, in the order the job declared them. A single-source
    /// job reports `source` instead, so its result keeps the shape callers already parse.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceInfo>,
    /// Why the spec or the source setup failed, when that is the outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Compilation-cache counters, when a cache was configured and it answered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheStats>,
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
            sources: Vec::new(),
            error: None,
            cache: None,
            log: LogInfo { truncated: false },
        }
    }

    /// Attach the source summary of a single-source job.
    pub fn with_source(mut self, source: Option<SourceInfo>) -> Self {
        self.source = source;
        self
    }

    /// Attach the sources of a multi-source job.
    pub fn with_sources(mut self, sources: Vec<SourceInfo>) -> Self {
        self.sources = sources;
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
    match bounded_line(result) {
        Some(line) => {
            println!("{line}");
            std::io::stdout().flush().is_ok()
        }
        None => false,
    }
}

/// Serialise the result inside [`RESULT_LINE_BUDGET`].
///
/// A multi-source job runs a plan per source, so a job that fails everywhere can carry dozens of
/// 8 KiB tails. The worker reads at most 256 KiB of log and keeps the END; a line larger than
/// that is cut at its head and can no longer be parsed, which the worker must never read as a
/// pass. So the producer bounds its own line: output tails are replaced by a marker, newest
/// first, until the line fits. Checks, names and the verdict always survive.
fn bounded_line(result: &RunnerResult) -> Option<String> {
    let mut result = result.clone();
    let mut line = serialise(&result)?;
    for index in (0..result.checks.len()).rev() {
        if line.len() <= RESULT_LINE_BUDGET {
            break;
        }
        if result.checks[index].output_tail.is_some() {
            result.checks[index].output_tail = Some(TAIL_DROPPED.to_owned());
            line = serialise(&result)?;
        }
    }
    Some(line)
}

fn serialise(result: &RunnerResult) -> Option<String> {
    match serde_json::to_string(&serde_json::json!({ "coding_result": result })) {
        Ok(line) => Some(line),
        Err(err) => {
            eprintln!("coding-runner: could not serialise the result: {err}");
            None
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
            source: None,
            name: "cargo test".to_owned(),
            status: CheckStatus::Failed,
            exit_code: Some(101),
            duration_ms: 812,
            output_tail: Some("assertion failed".to_owned()),
        });
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["checks"][0]["exit_code"], 101);
        assert_eq!(value["checks"][0]["output_tail"], "assertion failed");
        // A single-source job's result must not grow the fields multi-source introduced.
        assert!(value.get("source").is_none());
        assert!(value.get("sources").is_none());
        assert!(value["checks"][0].get("source").is_none());
        assert!(value.get("error").is_none());
    }

    #[test]
    fn a_multi_source_result_names_every_check_and_source() {
        let mut result =
            RunnerResult::new(Status::Failed, "1 of 2 checks failed").with_sources(vec![
                SourceInfo {
                    name: Some("target".to_owned()),
                    kind: "git".to_owned(),
                    files: 0,
                    commit: Some("abc123".to_owned()),
                },
                SourceInfo {
                    name: Some("source-1".to_owned()),
                    kind: "git".to_owned(),
                    files: 0,
                    commit: None,
                },
            ]);
        result.checks.push(Check {
            source: Some("target".to_owned()),
            name: "cargo fmt".to_owned(),
            status: CheckStatus::Passed,
            exit_code: Some(0),
            duration_ms: 4,
            output_tail: None,
        });
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["sources"][0]["name"], "target");
        assert_eq!(value["checks"][0]["source"], "target");
        assert!(value.get("source").is_none());
    }

    #[test]
    fn the_result_line_stays_inside_its_budget() {
        // 40 failing checks with full tails is what a large multi-source job looks like; the
        // unbounded line would be ~320 KiB, over the worker's 256 KiB log read.
        let mut result = RunnerResult::new(Status::Failed, "40 of 40 checks failed");
        for index in 0..40 {
            result.checks.push(Check {
                source: Some(format!("source-{index}")),
                name: "cargo test".to_owned(),
                status: CheckStatus::Failed,
                exit_code: Some(101),
                duration_ms: 1,
                output_tail: Some("x".repeat(MAX_CHECK_OUTPUT_BYTES)),
            });
        }
        let line = bounded_line(&result).expect("serialises");
        assert!(
            line.len() <= RESULT_LINE_BUDGET,
            "line is {} bytes, over {RESULT_LINE_BUDGET}",
            line.len()
        );
        // The bounded line is still one parseable document with all its checks.
        let value: serde_json::Value = serde_json::from_str(&line).expect("parses");
        assert_eq!(
            value["coding_result"]["checks"].as_array().unwrap().len(),
            40
        );
    }
}
