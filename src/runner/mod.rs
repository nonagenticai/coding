//! The in-pod job executor: what `coding-runner` runs inside a kata Pod.
//!
//! One job arrives as JSON in `CODING_JOB_SPEC`, the runner materialises its source into
//! `/tmp/src`, runs a **closed** command plan for the job kind, and writes exactly one NDJSON
//! `{"coding_result": …}` line as the last line of stdout. The worker reads that line; there is
//! no other channel back.
//!
//! Properties this module deliberately has:
//!
//! * **No docker.** A runner is a process in a Pod; it never starts a container.
//! * **No payload-chosen argv.** [`plan`] maps a closed [`JobKind`] to fixed command plans. A
//!   request cannot name a binary, an argument or an image.
//! * **No shell.** Every child is spawned with an argv; nothing is interpolated into `sh -c`.
//! * **Self-bounded.** The job deadline and SIGTERM (Pod deletion on cancel) both stop the child
//!   and still produce a result, so a cancelled job is data rather than a missing log.

pub mod exec;
pub mod postgres;
pub mod redis;
pub mod result;
pub mod spec;

use crate::domain::JobKind;

/// One command in a job's plan. `program` and `args` are static: the plan is compiled in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Planned {
    /// The check name reported in the result.
    pub name: &'static str,
    /// The executable, resolved on `PATH`.
    pub program: &'static str,
    /// Fixed arguments.
    pub args: &'static [&'static str],
    /// Whether this step needs the job's throwaway database.
    ///
    /// Per **step**, not per kind: the cluster is started immediately before the first step that
    /// sets this, so a formatting or lint step never holds one open. Measured 2026-11-15 — a
    /// cluster started before `fmt`/`clippy` was gone by the time the tests ran, and the failed
    /// check that followed read as "your code is bad".
    pub needs_database: bool,
    /// Whether this step needs the job's throwaway Redis.
    ///
    /// The same per-step rule as [`Planned::needs_database`]: the server starts immediately before
    /// the step that sets this, so a formatting or lint step never holds one open.
    pub needs_redis: bool,
}

/// Whether a kind's plan needs a database at all.
///
/// The estate's test harnesses fail closed without `TEST_DATABASE_URL`, so a kind that runs a
/// test suite gets a throwaway cluster; a kind that only formats, lints or compiles does not pay
/// for one. [`Planned::needs_database`] decides *which step* starts it.
pub fn needs_database(kind: JobKind) -> bool {
    matches!(kind, JobKind::Test | JobKind::Check)
}

/// Whether a kind's plan needs a Redis at all.
///
/// The estate's harnesses read `TEST_REDIS_URL` / `REDIS_URL` and many fail closed without one, so
/// a kind that runs a test suite gets a throwaway server. [`Planned::needs_redis`] decides *which
/// step* starts it.
pub fn needs_redis(kind: JobKind) -> bool {
    matches!(kind, JobKind::Test | JobKind::Check)
}

/// The command plan for a kind. `None` means the kind is not executable by this runner (yet);
/// the runner reports `unsupported` rather than guessing.
pub fn plan(kind: JobKind) -> Option<&'static [Planned]> {
    Some(match kind {
        JobKind::Test => &[Planned {
            name: "cargo test",
            program: "cargo",
            args: &["test", "--locked"],
            needs_database: true,
            needs_redis: true,
        }],
        JobKind::Lint => &[
            Planned {
                name: "cargo fmt",
                program: "cargo",
                args: &["fmt", "--check"],
                needs_database: false,
                needs_redis: false,
            },
            Planned {
                name: "cargo clippy",
                program: "cargo",
                args: &["clippy", "--all-targets", "--", "-D", "warnings"],
                needs_database: false,
                needs_redis: false,
            },
        ],
        JobKind::Build => &[Planned {
            name: "cargo build",
            program: "cargo",
            args: &["build", "--locked"],
            needs_database: false,
            needs_redis: false,
        }],
        // The pre-commit gate: the three checks that make a push green, in the order that fails
        // fastest. `exec` aggregates, so a formatting problem never hides a failing test.
        JobKind::Check => &[
            Planned {
                name: "cargo fmt",
                program: "cargo",
                args: &["fmt", "--check"],
                needs_database: false,
                needs_redis: false,
            },
            Planned {
                name: "cargo clippy",
                program: "cargo",
                args: &[
                    "clippy",
                    "--all-targets",
                    "--locked",
                    "--",
                    "-D",
                    "warnings",
                ],
                needs_database: false,
                needs_redis: false,
            },
            Planned {
                name: "cargo test",
                program: "cargo",
                args: &["test", "--locked"],
                needs_database: true,
                needs_redis: true,
            },
        ],
        // Every other kind belongs to a later milestone (analysis, scans, GitHub, parity).
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plans_that_run_tests_get_a_database() {
        for kind in [JobKind::Test, JobKind::Check] {
            assert!(needs_database(kind), "{kind:?} runs a test suite");
        }
        for kind in [JobKind::Lint, JobKind::Build, JobKind::Scan] {
            assert!(!needs_database(kind), "{kind:?} must not pay for a cluster");
        }
    }

    #[test]
    fn only_plans_that_run_tests_get_a_redis() {
        for kind in [JobKind::Test, JobKind::Check] {
            assert!(needs_redis(kind), "{kind:?} runs a test suite");
        }
        for kind in [JobKind::Lint, JobKind::Build, JobKind::Scan] {
            assert!(!needs_redis(kind), "{kind:?} must not pay for a server");
        }
    }

    #[test]
    fn only_the_test_step_asks_for_a_database() {
        // The cluster is started lazily, so a step that does not need one must never start it:
        // in the measured failure of 2026-11-15 the cluster died during fmt/clippy's idle
        // window, and the failed `cargo test` that followed read as a bad repository.
        for kind in [JobKind::Test, JobKind::Check] {
            let plan = plan(kind).expect("runnable kind");
            for step in plan {
                assert_eq!(
                    step.needs_database,
                    step.name == "cargo test",
                    "{kind:?}: {} has the wrong database flag",
                    step.name
                );
                assert_eq!(
                    step.needs_redis,
                    step.name == "cargo test",
                    "{kind:?}: {} has the wrong Redis flag",
                    step.name
                );
            }
        }
        for kind in [JobKind::Lint, JobKind::Build] {
            for step in plan(kind).expect("runnable kind") {
                assert!(
                    !step.needs_database && !step.needs_redis,
                    "{kind:?}: {} must not start a cluster or a server",
                    step.name
                );
            }
        }
    }

    #[test]
    fn only_runnable_kinds_have_a_plan() {
        assert!(plan(JobKind::Test).is_some());
        assert!(plan(JobKind::Lint).is_some());
        assert!(plan(JobKind::Build).is_some());
        assert!(plan(JobKind::Check).is_some());
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
            assert!(plan(kind).is_none(), "{kind:?} must be unsupported");
        }
    }

    #[test]
    fn plans_never_contain_a_shell() {
        for kind in [JobKind::Test, JobKind::Lint, JobKind::Build, JobKind::Check] {
            for planned in plan(kind).expect("runnable kind") {
                assert_ne!(planned.program, "sh");
                assert_ne!(planned.program, "bash");
                assert!(!planned.args.contains(&"-c"));
            }
        }
    }
}
