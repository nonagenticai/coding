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

use crate::domain::{JobKind, Profile};

/// Where the runner image keeps the `CPython` it preinstalled for the Python profile. The image
/// installs it read-only; [`crate::runner::exec`] points uv at it for every Python step.
pub const PYTHON_INSTALL_DIR: &str = "/opt/uv/python";

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
#[must_use]
pub fn needs_database(kind: JobKind) -> bool {
    matches!(kind, JobKind::Test | JobKind::Check)
}

/// Whether a kind's plan needs a Redis at all.
///
/// The estate's harnesses read `TEST_REDIS_URL` / `REDIS_URL` and many fail closed without one, so
/// a kind that runs a test suite gets a throwaway server. [`Planned::needs_redis`] decides *which
/// step* starts it.
#[must_use]
pub fn needs_redis(kind: JobKind) -> bool {
    matches!(kind, JobKind::Test | JobKind::Check)
}

/// The Rust plans: one compiled-in table per runnable kind. The pre-commit gate lists the same
/// three checks as `lint` plus `test`, in the order that fails fastest, so a formatting problem
/// never hides a failing test.
/// The Rust test plan mirrors what CI runs, and the choice of runner is not cosmetic.
///
/// `cargo test` executes a workspace's test binaries ONE AT A TIME. On a workspace with hundreds
/// of integration-test files that is not a slower measurement of the same thing, it is a
/// different order of magnitude: measured on `conveyor` (266 integration-test files, sccache
/// fully warm at 997–1006 hits and 0–9 misses, so nothing was being compiled) plain `cargo test`
/// took 894 s and twice exceeded the 1800 s budget, while the same suite under `nextest` takes
/// about 170 s in CI. A timeout exits 3 and the hook FAILS OPEN, so the real cost was 30 minutes
/// of a blocked push buying no verdict at all.
///
/// `cargo test --doc` stays: nextest does not run doctests, and CI runs both for the same reason.
/// ⚠️ Order matters, and not for reporting: the throwaway `PostgreSQL`/Redis cluster is started
/// lazily immediately before the step that needs it and dies if it sits idle, so the doctest run —
/// which needs neither — goes FIRST and the suite that needs them is LAST. Doctests are also the
/// faster failure, which is the ordering rule the other plans already follow.
const RUST_TEST: &[Planned] = &[
    Planned {
        name: "cargo test --doc",
        program: "cargo",
        args: &["test", "--doc", "--locked"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "cargo nextest",
        program: "cargo-nextest",
        args: &[
            "nextest",
            "run",
            "--no-tests",
            "fail",
            "--color",
            "never",
            "--locked",
        ],
        needs_database: true,
        needs_redis: true,
    },
];

const RUST_LINT: &[Planned] = &[
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
];

const RUST_BUILD: &[Planned] = &[Planned {
    name: "cargo build",
    program: "cargo",
    args: &["build", "--locked"],
    needs_database: false,
    needs_redis: false,
}];

const RUST_CHECK: &[Planned] = &[
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
];

/// The Python plans mirror the estate's Python archetype command for command (`uv lock --check`,
/// `uv sync --all-extras`, `ruff check`, `ruff format --check`, `ty check`, `pytest`), so the
/// gate's verdict is the one CI would give. `uv sync --locked` instead of the archetype's plain
/// `uv sync`: the gate must never rewrite the lock file it is judging.
const PYTHON_LINT: &[Planned] = &[
    Planned {
        name: "uv lock",
        program: "uv",
        args: &["lock", "--check"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "uv sync",
        program: "uv",
        args: &["sync", "--all-extras", "--locked"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ruff check",
        program: "uv",
        args: &["run", "ruff", "check", "."],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ruff format",
        program: "uv",
        // The `--config` value is ONE argv element, not shell text: this plan never goes
        // through a shell. It is the archetype's spelling, kept identical on purpose.
        args: &[
            "run",
            "ruff",
            "format",
            "--check",
            "--config",
            "extend-exclude=['*.md']",
            ".",
        ],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ty check",
        program: "uv",
        // `src` — the archetype's default scope, mirroring CI. Measured 2026-11-18 on a real
        // estate repository: a whole-project `ty check` reported 18 diagnostics that all live in
        // `tests/` and that CI (which checks `src`) never sees. A gate stricter than CI blocks a
        // push of code CI would accept, which is the one failure a gate must not have.
        args: &["run", "ty", "check", "src"],
        needs_database: false,
        needs_redis: false,
    },
];

const PYTHON_TEST: &[Planned] = &[
    Planned {
        name: "uv sync",
        program: "uv",
        args: &["sync", "--all-extras", "--locked"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "pytest",
        program: "uv",
        args: &["run", "pytest", "-v", "--tb=short"],
        needs_database: true,
        needs_redis: true,
    },
];

const PYTHON_CHECK: &[Planned] = &[
    Planned {
        name: "uv lock",
        program: "uv",
        args: &["lock", "--check"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "uv sync",
        program: "uv",
        args: &["sync", "--all-extras", "--locked"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ruff check",
        program: "uv",
        args: &["run", "ruff", "check", "."],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ruff format",
        program: "uv",
        args: &[
            "run",
            "ruff",
            "format",
            "--check",
            "--config",
            "extend-exclude=['*.md']",
            ".",
        ],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "ty check",
        program: "uv",
        args: &["run", "ty", "check", "src"],
        needs_database: false,
        needs_redis: false,
    },
    Planned {
        name: "pytest",
        program: "uv",
        args: &["run", "pytest", "-v", "--tb=short"],
        needs_database: true,
        needs_redis: true,
    },
];

/// The command plan for a `(profile, kind)` pair. `None` means this runner cannot execute it —
/// an unimplemented kind, or a kind the profile has no gate for — and the runner reports
/// `unsupported` rather than guessing.
#[must_use]
pub fn plan(profile: Profile, kind: JobKind) -> Option<&'static [Planned]> {
    Some(match (profile, kind) {
        (Profile::Rust, JobKind::Test) => RUST_TEST,
        (Profile::Rust, JobKind::Lint) => RUST_LINT,
        (Profile::Rust, JobKind::Build) => RUST_BUILD,
        (Profile::Rust, JobKind::Check) => RUST_CHECK,
        (Profile::Python, JobKind::Test) => PYTHON_TEST,
        (Profile::Python, JobKind::Lint) => PYTHON_LINT,
        (Profile::Python, JobKind::Check) => PYTHON_CHECK,
        // Python has no `build` gate in the estate's archetype. Inventing one would produce a
        // verdict CI never gives, so this runner says `unsupported` instead. Every other kind
        // belongs to a later milestone (analysis, scans, GitHub, parity).
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
    fn only_the_last_step_of_a_test_plan_asks_for_a_database() {
        // The cluster is started lazily, so a step that does not need one must never start it:
        // in the measured failure of 2026-11-15 the cluster died during fmt/clippy's idle
        // window, and the failed `cargo test` that followed read as a bad repository.
        for profile in [Profile::Rust, Profile::Python] {
            for kind in [JobKind::Test, JobKind::Check] {
                let plan = plan(profile, kind).expect("runnable kind");
                let last = plan.last().expect("a plan has at least one step");
                for step in plan {
                    let expected = std::ptr::eq(step, last);
                    assert_eq!(
                        step.needs_database, expected,
                        "{profile:?}/{kind:?}: {} has the wrong database flag",
                        step.name
                    );
                    assert_eq!(
                        step.needs_redis, expected,
                        "{profile:?}/{kind:?}: {} has the wrong Redis flag",
                        step.name
                    );
                }
            }
        }
        for (profile, kind) in [
            (Profile::Rust, JobKind::Lint),
            (Profile::Rust, JobKind::Build),
            (Profile::Python, JobKind::Lint),
        ] {
            for step in plan(profile, kind).expect("runnable kind") {
                assert!(
                    !step.needs_database && !step.needs_redis,
                    "{profile:?}/{kind:?}: {} must not start a cluster or a server",
                    step.name
                );
            }
        }
    }

    #[test]
    fn only_runnable_kinds_have_a_plan() {
        for profile in [Profile::Rust, Profile::Python] {
            for kind in [JobKind::Test, JobKind::Lint, JobKind::Check] {
                assert!(plan(profile, kind).is_some(), "{profile:?}/{kind:?}");
            }
        }
        assert!(plan(Profile::Rust, JobKind::Build).is_some());
        assert!(
            plan(Profile::Python, JobKind::Build).is_none(),
            "python has no build gate in the archetype"
        );
        for profile in [Profile::Rust, Profile::Python] {
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
                assert!(
                    plan(profile, kind).is_none(),
                    "{profile:?}/{kind:?} must be unsupported"
                );
            }
        }
    }

    #[test]
    fn plans_never_contain_a_shell() {
        for profile in [Profile::Rust, Profile::Python] {
            for kind in [JobKind::Test, JobKind::Lint, JobKind::Build, JobKind::Check] {
                let Some(planned) = plan(profile, kind) else {
                    continue;
                };
                for step in planned {
                    assert_ne!(step.program, "sh");
                    assert_ne!(step.program, "bash");
                    assert!(!step.args.contains(&"-c"));
                }
            }
        }
    }

    #[test]
    fn python_plans_are_the_archetype_commands() {
        // Drift guard: the gate's verdict is only worth something if these are the commands the
        // estate's Python CI runs (python-service.yml). Change both together or neither.
        let lint: Vec<(&str, &[&str])> = plan(Profile::Python, JobKind::Lint)
            .expect("lint")
            .iter()
            .map(|step| (step.program, step.args))
            .collect();
        assert_eq!(
            lint,
            vec![
                ("uv", &["lock", "--check"][..]),
                ("uv", &["sync", "--all-extras", "--locked"][..]),
                ("uv", &["run", "ruff", "check", "."][..]),
                (
                    "uv",
                    &[
                        "run",
                        "ruff",
                        "format",
                        "--check",
                        "--config",
                        "extend-exclude=['*.md']",
                        ".",
                    ][..],
                ),
                ("uv", &["run", "ty", "check", "src"][..]),
            ]
        );
        let test = plan(Profile::Python, JobKind::Test).expect("test");
        assert_eq!(test.len(), 2, "sync, then pytest");
        assert_eq!(test[1].name, "pytest");
        assert_eq!(test[1].args, ["run", "pytest", "-v", "--tb=short"]);
    }
}
