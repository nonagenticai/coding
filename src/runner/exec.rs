//! Materialising a job's source and running its plan, bounded by a deadline and SIGTERM.
//!
//! Nothing here uses a shell: every child is an argv from [`crate::runner::plan`] or a fixed git
//! command, so a request cannot inject arguments. The source path is validated before it is
//! joined, so a file cannot be written outside the work tree.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::time::Instant as TokioInstant;

use crate::domain::{Profile, Source};
use crate::runner::plan;
use crate::runner::postgres;
use crate::runner::redis;
use crate::runner::result::{
    CacheStats, Check, CheckStatus, MAX_CHECK_OUTPUT_BYTES, RunnerResult, SourceInfo, Status,
};
use crate::runner::spec::RunnerSpec;

/// The work base. Inside the Pod this is the writable `emptyDir` mounted at `/tmp`; every source
/// and every scratch file lives under it.
pub const WORK_DIR: &str = "/tmp";
/// Where a single-source job's tree is written. Unchanged, so a job that names one source runs
/// exactly as it did before multi-source existed.
pub const SOURCE_DIR: &str = "/tmp/src";
/// The most stderr text carried into an error message.
const MAX_ERROR_TEXT: usize = 2048;
/// How long the output pumps are given to finish after the child is gone. A grandchild can hold
/// the pipe open; waiting on it forever would cost the job its result line.
const PUMP_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
/// How many times the first fetch of an uploaded source is attempted. A kata Pod is `Running`
/// before its guest network is, so the first connect can be refused outright.
const DOWNLOAD_ATTEMPTS: u32 = 6;
/// The first wait between fetch attempts; each later wait doubles.
const DOWNLOAD_BACKOFF: std::time::Duration = std::time::Duration::from_millis(500);

/// SIGTERM, as a value the caller creates once. The worker's cancel deletes the Pod, which sends
/// SIGTERM before SIGKILL; the runner uses that window to write `cancelled`.
pub struct Cancellation {
    signal: Signal,
}

impl Cancellation {
    /// Subscribe to SIGTERM. A failure here is fatal for the process, not for a job.
    ///
    /// # Errors
    ///
    /// The `std::io::Error` from registering the SIGTERM handler.
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            signal: signal(SignalKind::terminate())?,
        })
    }
}

/// Why a child stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// It exited on its own.
    Exited(Option<i32>),
    /// The job deadline was reached; the runner killed it.
    TimedOut,
    /// SIGTERM arrived; the runner killed it.
    Cancelled,
}

/// What one child produced. `stdout`/`stderr` are only filled in capture mode; `tail` is the
/// bounded last bytes of a streamed child, which is what a failed check reports.
struct Outcome {
    stop: Stop,
    stdout: String,
    stderr: String,
    tail: String,
}

#[derive(Clone, Copy)]
enum Mode {
    /// Child writes straight to the Pod log.
    Stream,
    /// Child output is captured (setup steps, where the message belongs in `error`).
    Capture,
}

/// Run a job. Always returns a result; a crash is the only way to get no result line.
pub async fn run(spec: &RunnerSpec, cancel: &mut Cancellation) -> RunnerResult {
    run_in(spec, cancel, Path::new(WORK_DIR)).await
}

/// One source to check, with the name it reports under (multi-source only) and its own profile.
struct Unit<'a> {
    name: Option<&'a str>,
    profile: Profile,
    source: Option<&'a Source>,
    dir: PathBuf,
}

/// The units of a job: one unnamed source at `<work>/src`, or every named source at
/// `<work>/sources/<name>`. Names and count were validated by the API and again by [`RunnerSpec`];
/// this only maps them onto the filesystem.
fn units<'a>(spec: &'a RunnerSpec, work_dir: &Path) -> Vec<Unit<'a>> {
    if spec.input.sources.is_empty() {
        return vec![Unit {
            name: None,
            profile: spec.input.profile,
            source: spec.input.source.as_ref(),
            dir: work_dir.join("src"),
        }];
    }
    spec.input
        .sources
        .iter()
        .map(|named| Unit {
            name: Some(named.name.as_str()),
            profile: named.profile,
            source: Some(&named.source),
            dir: work_dir.join("sources").join(&named.name),
        })
        .collect()
}

/// A step's name, plus the source it ran against when the job has several.
fn step_label(unit: &Unit<'_>, step: &str) -> String {
    match unit.name {
        Some(name) => format!("{step} on source {name:?}"),
        None => step.to_owned(),
    }
}

/// Put the (possibly partial) source summaries on the result, in the shape the caller used: a
/// single-source job reports `source`, a multi-source job `sources`. Called before a failure
/// return too, so the result of a job that died while materialising source 2 still names source 1.
fn attach_sources(result: &mut RunnerResult, multi: bool, infos: &mut [Option<SourceInfo>]) {
    if multi {
        result.sources = infos.iter_mut().filter_map(Option::take).collect();
    } else {
        result.source = infos.first_mut().and_then(Option::take);
    }
}

/// Wall-clock milliseconds since `started`, as the result's `duration_ms` type. A step short of
/// 584 million years fits in `u64`, so the conversion cannot fail.
fn elapsed_millis(started: TokioInstant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).expect("elapsed ms fit in u64")
}

/// [`run`] against an explicit work base. Tests use this; the Pod uses [`WORK_DIR`].
// reason: the measured step sequence (clone -> services -> run -> classify) reads as one flow.
#[allow(clippy::too_many_lines)]
pub async fn run_in(spec: &RunnerSpec, cancel: &mut Cancellation, work_dir: &Path) -> RunnerResult {
    let units = units(spec, work_dir);
    // Every unit must have a plan before anything is cloned or started: a job that would check
    // two sources and cannot check one of them is answered before it burns a clone or a database.
    for unit in &units {
        if plan(unit.profile, spec.kind).is_none() {
            return RunnerResult::new(
                Status::Unsupported,
                match unit.name {
                    Some(name) => format!(
                        "kind {} is not implemented for profile {} by this runner (source {name:?})",
                        spec.kind.as_str(),
                        unit.profile.as_str()
                    ),
                    None => format!(
                        "kind {} is not implemented for profile {} by this runner",
                        spec.kind.as_str(),
                        unit.profile.as_str()
                    ),
                },
            );
        }
    }

    let deadline = TokioInstant::now() + std::time::Duration::from_secs(spec.deadline_seconds);
    let multi = !spec.input.sources.is_empty();
    let mut infos: Vec<Option<SourceInfo>> = units
        .iter()
        .map(|unit| {
            unit.source.map(|source| SourceInfo {
                name: unit.name.map(str::to_owned),
                kind: match source {
                    Source::Inline { .. } => "inline".to_owned(),
                    Source::Git { .. } => "git".to_owned(),
                    Source::Upload { .. } => "upload".to_owned(),
                },
                files: crate::runner::spec::file_count(source),
                commit: None,
            })
        })
        .collect();

    println!(
        "== coding-runner {} job={} kind={} deadline={}s{} ==",
        env!("CARGO_PKG_VERSION"),
        spec.job_id,
        spec.kind.as_str(),
        spec.deadline_seconds,
        if multi {
            format!(" sources={}", units.len())
        } else {
            String::new()
        },
    );

    let mut result = RunnerResult::new(Status::Succeeded, String::new());
    for (index, unit) in units.iter().enumerate() {
        let Some(source) = unit.source else {
            continue;
        };
        match materialise(
            source,
            &unit.dir,
            &target_dir_for(&unit.dir),
            deadline,
            cancel,
        )
        .await
        {
            Ok(materialised) => {
                if let Some(info) = infos[index].as_mut() {
                    info.commit = materialised.commit;
                    if let Some(files) = materialised.files {
                        info.files = files;
                    }
                }
            }
            Err(failure) => {
                attach_sources(&mut result, multi, &mut infos);
                return failure.into_result(result, unit.name);
            }
        }
    }
    attach_sources(&mut result, multi, &mut infos);

    // Each source's profile is the caller's claim about that tree. A tree without the profile's
    // marker is a mismatch, not a failing check: running cargo in a Python tree (or uv in a Rust
    // one) fails for a reason that says nothing about the repository. The API checked the same
    // shape, and this process checks it again because the spec arrives through a Pod env var.
    for unit in &units {
        let marker = unit.profile.marker_file();
        if unit.dir.join(marker).is_file() {
            continue;
        }
        result.status = Status::Unsupported;
        result.summary = match unit.name {
            Some(name) => format!(
                "profile {} expects {marker} at the root of source {name:?}; it is not there",
                unit.profile.as_str()
            ),
            None => format!(
                "profile {} expects {marker} at the root of the source; it is not there",
                unit.profile.as_str()
            ),
        };
        return result;
    }

    // A plan that runs a test suite gets a throwaway database, exactly as CI and rbuild give one:
    // the estate's harnesses fail CLOSED without `TEST_DATABASE_URL`, so a repository whose tests
    // touch a database would otherwise report a failing run for a reason that is not its code.
    //
    // It starts LAZILY, immediately before the first step that needs it. Measured 2026-11-15: a
    // cluster opened before `fmt`/`clippy` was gone by the time `cargo test` ran, the failed check
    // that followed read as "your code is bad", and the gate blocked a push of good code. See
    // [`Database`] for the lazy start and the classification that make that unreadable as a
    // repository's fault.
    let exported = postgres::exported_url();
    if let Some(url) = exported.as_deref() {
        // The environment already names a database: use it, never start a second one. This is what
        // makes the runner composable — a repository's own tests that call the runner inherit this
        // URL instead of colliding with the job's cluster on port 5432 and its data directory.
        println!("== postgres: using the database this process was given ({url})");
    }
    let mut database = Database::new(work_dir, exported);
    if crate::runner::needs_database(spec.kind) && !database.available() {
        // No cluster possible on this host (a developer's Mac, or the control-plane image): run
        // the plan as it ran before this module existed, and say so rather than fail.
        println!("== postgres: not installed on this host; running without TEST_DATABASE_URL");
    }

    // The same contract for Redis: the estate's harnesses read `TEST_REDIS_URL` / `REDIS_URL` and
    // many fail closed without one. An exported URL wins; otherwise the server starts lazily.
    let exported_redis = redis::exported_url();
    if let Some(url) = exported_redis.as_deref() {
        println!("== redis: using the server this process was given ({url})");
    }
    let exported_private_redis = redis::exported_private_url();
    if let Some(url) = exported_private_redis.as_deref() {
        println!("== redis: using the private server this process was given ({url})");
    }
    let mut redis = Redis::new(work_dir, exported_redis, exported_private_redis);
    if crate::runner::needs_redis(spec.kind) && !redis.available() {
        println!("== redis: not installed on this host; running without TEST_REDIS_URL/REDIS_URL");
    }

    let mut database_url: Option<String> = None;
    // The same database in the spelling the estate's Python suites read (SQLAlchemy + psycopg).
    let mut database_alias: Option<String> = None;
    let mut redis_url: Option<String> = None;
    // The port the started Redis actually listens on, for the Python suite's REDIS_HOST/REDIS_PORT.
    let mut redis_port: Option<String> = None;
    let mut private_redis_url: Option<String> = None;
    // One plan per source, in the order the job declared them. The throwaway Postgres and Redis
    // are the job's, not a source's: they start lazily at the first step anywhere that needs one
    // and every later step of every later source reuses them.
    for unit in &units {
        // Checked for every unit before a Pod ran anything; a `None` here cannot happen, and is
        // skipped rather than unwrapped because a panic costs the job its result line.
        let Some(planned) = plan(unit.profile, spec.kind) else {
            continue;
        };
        // A per-source target dir beside its tree: deterministic, and never the shell's own
        // CARGO_TARGET_DIR (which in CI points at the outer build). Two Rust sources must not
        // share one, or their builds would race in one directory.
        let target_dir = target_dir_for(&unit.dir);
        let python = unit.profile == Profile::Python;
        for step in planned {
            // Every message about this step names its source when the job has several: one Pod
            // can run four plans, and "pytest timed out" does not say which tree hung.
            let label = step_label(unit, step.name);
            // A step is attempted twice **only** when the database died under it. The first attempt's
            // death is printed with its log tail, so the retry hides nothing; a second death is the
            // job's error. The measured failure (a cluster that vanished while the test step was still
            // compiling) is exactly the case this recovers from: the re-run finds the build done and
            // a fresh cluster listening.
            let mut attempt: u32 = 0;
            let mut duration_ms = 0u64;
            'attempt: loop {
                attempt += 1;
                if step.needs_database && database.available() {
                    match database.ensure().await {
                        Ok(url) => {
                            // Printed on purpose: the URL carries no secret (trust auth, a throwaway
                            // cluster), and when a test reports "Connection refused" the first
                            // question is always *which* database it was told to use.
                            println!(
                                "== postgres: `{}` gets TEST_DATABASE_URL={url} accepting={}",
                                label,
                                crate::runner::postgres::accepting_connections().await
                            );
                            database_alias = Some(sqlalchemy_dsn(&url));
                            database_url = Some(url);
                        }
                        Err(error) => {
                            result.status = Status::Failed;
                            result.summary =
                                format!("the throwaway Postgres is not usable for {label}");
                            result.error = Some(error);
                            return result;
                        }
                    }
                }
                if step.needs_redis && redis.available() {
                    match redis.ensure().await {
                        Ok(url) => {
                            // Probe the port the URL actually names: the CI exports a dind Redis on a
                            // random host port, and dialling 6379 would report a healthy server as
                            // unreachable.
                            let port = crate::runner::redis::port_of(&url)
                                .unwrap_or(crate::runner::redis::PORT);
                            println!(
                                "== redis: `{}` gets TEST_REDIS_URL={url} accepting={}",
                                label,
                                crate::runner::redis::accepting_connections(port).await
                            );
                            redis_port = Some(port.to_string());
                            redis_url = Some(url);
                        }
                        Err(error) => {
                            result.status = Status::Failed;
                            result.summary =
                                format!("the throwaway Redis is not usable for {label}");
                            result.error = Some(error);
                            return result;
                        }
                    }
                    let can_private = redis.can_start_private();
                    match redis.ensure_private().await {
                        Ok(url) => {
                            println!("== redis: `{label}` gets TEST_REDIS_PRIVATE_URL={url}");
                            private_redis_url = Some(url);
                        }
                        // The private server exists for one test shape (a FLUSHALL that must not touch
                        // its siblings). A host without `redis-server` — the control-plane image, a
                        // developer's Mac, the CI runner — runs the step without it; a test that needs
                        // it reports its own failure. Only a host that CAN provide one and fails is a
                        // setup error.
                        Err(error) if can_private => {
                            result.status = Status::Failed;
                            result.summary =
                                format!("the private throwaway Redis is not usable for {label}");
                            result.error = Some(error);
                            return result;
                        }
                        Err(error) => println!(
                            "== redis: no private server on this host ({error}); running without \
                         TEST_REDIS_PRIVATE_URL"
                        ),
                    }
                }
                // Only a step that needs a service is told about it: a build script must not probe an
                // address whose server has not started.
                let env: Vec<(&str, &str)> = {
                    let mut env = Vec::new();
                    if python {
                        // The runner image's Python is read-only and preinstalled; every writable
                        // path a job needs is under the /tmp scratch the sandbox mounts, and
                        // downloads are off so uv cannot fetch a CPython into that scratch per job.
                        env.push(("HOME", "/tmp"));
                        env.push(("UV_CACHE_DIR", "/tmp/uv-cache"));
                        env.push(("UV_PYTHON_INSTALL_DIR", crate::runner::PYTHON_INSTALL_DIR));
                        env.push(("UV_PYTHON_DOWNLOADS", "never"));
                    }
                    if let (true, Some(url)) = (step.needs_database, database_url.as_deref()) {
                        env.push(("TEST_DATABASE_URL", url));
                        if python {
                            // The same server in the estate's Python spelling: SQLAlchemy names the
                            // driver, and the driver is psycopg v3 (`postgresql+psycopg://`).
                            if let Some(alias) = database_alias.as_deref() {
                                env.push(("DATABASE_URL", alias));
                            }
                        }
                    }
                    if let (true, Some(url)) = (step.needs_redis, redis_url.as_deref()) {
                        env.push(("TEST_REDIS_URL", url));
                        env.push(("REDIS_URL", url));
                        if python {
                            // The estate's Python services read the host/port pair, not a URL.
                            env.push(("REDIS_HOST", "127.0.0.1"));
                            env.push(("REDIS_PORT", redis_port.as_deref().unwrap_or("6379")));
                        }
                    }
                    if let (true, Some(url)) = (step.needs_redis, private_redis_url.as_deref()) {
                        env.push(("TEST_REDIS_PRIVATE_URL", url));
                    }
                    env
                };
                let started = TokioInstant::now();
                let outcome = run_child(
                    Mode::Stream,
                    step.program,
                    step.args,
                    &ChildCtx {
                        dir: &unit.dir,
                        target_dir: &target_dir,
                        env: env.as_slice(),
                    },
                    deadline,
                    cancel,
                )
                .await;
                duration_ms += elapsed_millis(started);
                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        result.status = Status::Failed;
                        result.error = Some(format!("{label} could not start: {err}"));
                        result.summary = format!("{label} could not start");
                        return result;
                    }
                };
                match outcome.stop {
                    Stop::Exited(code) => {
                        let failed = code != Some(0);
                        // A step that needed a database and failed while nothing was listening is an
                        // infrastructure failure, not a verdict about the repository. The probe
                        // decides, never the child's exit code: a test can fail for its own reasons
                        // while the server is healthy, and that failure must still be reported as one.
                        //
                        // `is_ours()` is part of the test, for two reasons. On a host with no Postgres
                        // binaries this runner deliberately runs the plan without a database, and a
                        // test that fails there is the repository's business. On a database the
                        // environment provided, the probe below (which dials 127.0.0.1:5432) would be
                        // asking about a different server than the one the step used — so only a
                        // cluster this process started can be judged dead.
                        if failed
                            && step.needs_database
                            && database.is_ours()
                            && !crate::runner::postgres::accepting_connections().await
                        {
                            let death = database.died(&label);
                            println!("== postgres: {death}");
                            if attempt >= SERVICE_ATTEMPTS {
                                result.status = Status::Failed;
                                result.summary =
                                    format!("the throwaway Postgres died during {label}");
                                result.error = Some(death);
                                return result;
                            }
                            match database.restart().await {
                                Ok(()) => {
                                    println!(
                                        "== postgres: restarted; re-running `{}` (attempt {} of \
                                     {SERVICE_ATTEMPTS})",
                                        label,
                                        attempt + 1
                                    );
                                    continue 'attempt;
                                }
                                Err(error) => {
                                    result.status = Status::Failed;
                                    result.summary =
                                        format!("the throwaway Postgres died during {label}");
                                    result.error = Some(format!("{death}\n{error}"));
                                    return result;
                                }
                            }
                        }
                        // The same classification for Redis: a step that failed while the server this
                        // process started is not listening is infrastructure, not a verdict about the
                        // repository. An exported URL is never judged or restarted — it is not ours.
                        let redis_dead = redis.is_ours()
                            && !crate::runner::redis::accepting_connections(
                                crate::runner::redis::PORT,
                            )
                            .await;
                        let private_redis_dead = redis.private_is_ours()
                            && !crate::runner::redis::accepting_connections(
                                crate::runner::redis::PRIVATE_PORT,
                            )
                            .await;
                        if failed && step.needs_redis && (redis_dead || private_redis_dead) {
                            let death = redis.died(&label);
                            println!("== redis: {death}");
                            if attempt >= SERVICE_ATTEMPTS {
                                result.status = Status::Failed;
                                result.summary = format!("the throwaway Redis died during {label}");
                                result.error = Some(death);
                                return result;
                            }
                            match redis.restart().await {
                                Ok(()) => {
                                    println!(
                                        "== redis: restarted; re-running `{}` (attempt {} of \
                                     {SERVICE_ATTEMPTS})",
                                        label,
                                        attempt + 1
                                    );
                                    continue 'attempt;
                                }
                                Err(error) => {
                                    result.status = Status::Failed;
                                    result.summary =
                                        format!("the throwaway Redis died during {label}");
                                    result.error = Some(format!("{death}\n{error}"));
                                    return result;
                                }
                            }
                        }
                        let status = if failed {
                            CheckStatus::Failed
                        } else {
                            CheckStatus::Passed
                        };
                        result.checks.push(Check {
                            source: unit.name.map(str::to_owned),
                            name: step.name.to_owned(),
                            status,
                            exit_code: code,
                            duration_ms,
                            output_tail: if failed {
                                Some(outcome.tail.clone()).filter(|tail| !tail.is_empty())
                            } else {
                                None
                            },
                        });
                        break 'attempt;
                    }
                    Stop::TimedOut => {
                        result.status = Status::Timeout;
                        result.summary = format!(
                            "the job deadline of {}s was exceeded during {label}",
                            spec.deadline_seconds
                        );
                        return result;
                    }
                    Stop::Cancelled => {
                        result.status = Status::Cancelled;
                        result.summary = match unit.name {
                            Some(_) => format!("the job was cancelled while running (at {label})"),
                            None => "the job was cancelled while running".to_owned(),
                        };
                        return result;
                    }
                }
            }
        }
    }

    // Bounded evidence for the next reader: how the cluster lived and how it ended. The URL is
    // deterministic and carries no secret, so nothing here needs redacting.
    if let Some(diagnostics) = database.diagnostics() {
        println!("{diagnostics}");
    }
    if let Some(diagnostics) = redis.diagnostics() {
        println!("{diagnostics}");
    }

    // Informational only: the verdict was decided by the checks above, and a cache that is
    // unreachable, unconfigured or absent leaves this `None` rather than changing the result.
    if let Some(stats) = sccache_stats() {
        println!(
            "sccache hits={} misses={} errors={}",
            stats.hits, stats.misses, stats.errors
        );
        result.cache = Some(stats);
    }

    finish(result)
}

/// Derive the verdict and the human summary from the checks collected so far.
fn finish(mut result: RunnerResult) -> RunnerResult {
    let total = result.checks.len();
    let failing: Vec<&Check> = result
        .checks
        .iter()
        .filter(|check| check.status == CheckStatus::Failed)
        .collect();
    if failing.is_empty() {
        result.status = Status::Succeeded;
        result.summary = format!("{total} of {total} checks passed");
    } else {
        result.status = Status::Failed;
        // Name the failing sources: with several plans in one Pod, "3 of 9 checks failed" does
        // not tell a caller which tree to look at.
        let names: Vec<&str> = failing
            .iter()
            .filter_map(|check| check.source.as_deref())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        result.summary = if names.is_empty() {
            format!("{} of {total} checks failed", failing.len())
        } else {
            format!(
                "{} of {total} checks failed ({})",
                failing.len(),
                names.join(", ")
            )
        };
    }
    result
}

/// How many times a step is run when one of its services dies under it. Two, not a loop: the first
/// death is printed with its log tail, and a second one is the job's error rather than another
/// retry.
const SERVICE_ATTEMPTS: u32 = 2;

/// The job's throwaway database: started lazily, immediately before the step that needs it, and
/// never allowed to read as a verdict about the repository.
///
/// Four properties, each earned by a measured failure on 2026-11-15:
///
/// * **Inherited** — a `TEST_DATABASE_URL` already in the environment wins and nothing is
///   started. Without this, a repository whose tests call the runner (`coding`'s `tests/runner.rs`
///   does) starts a second Postgres on the same port *and the same data directory*, and its drop
///   stops the job's cluster — measured: `received immediate shutdown request` 163 s into a run.
/// * **Lazy** — nothing is started until a step actually needs a database, so `fmt` and `clippy`
///   never hold one open. The measured failure had the cluster idle for four minutes before the
///   test step, and gone by the time it ran.
/// * **Classified** — a step that fails while nothing is listening becomes a job error (the
///   client's exit 3, which a hook does not block on), never a failed check (exit 1, which does).
/// * **Retried once** — a cluster that died mid-step is replaced and the step is re-run once. The
///   death is printed with its postmaster log tail first, so the retry hides no cause; if the new
///   cluster dies too, the job fails as infrastructure.
struct Database {
    /// The directory the cluster lives in: `/tmp` in the Pod, beside the source tree.
    scratch: PathBuf,
    /// A database the environment already provides (`TEST_DATABASE_URL`). When set, this process
    /// uses it and never starts a cluster of its own — see [`postgres::exported_url`].
    exported: Option<String>,
    handle: Option<postgres::ThrowawayPostgres>,
}

impl Database {
    fn new(scratch: &Path, exported: Option<String>) -> Self {
        Self {
            scratch: scratch.to_path_buf(),
            exported,
            handle: None,
        }
    }

    /// Whether a step that needs a database can be given one: either the environment supplies it
    /// or this host can start a cluster.
    fn available(&self) -> bool {
        self.exported.is_some() || postgres::available()
    }

    /// Whether the database is one this process started — the only kind whose death this runner
    /// can judge (and the only kind it may stop or restart).
    fn is_ours(&self) -> bool {
        self.handle.is_some()
    }

    /// The DSN, starting the cluster the first time it is asked for.
    ///
    /// A database the environment already provides wins, always: starting a second cluster would
    /// collide with it on port 5432 and, worse, on its data directory. Every plan in this runner
    /// has at most one step that needs a database, so this starts once; the handle is kept so a
    /// plan with a second such step reuses the cluster instead of starting another on the port.
    async fn ensure(&mut self) -> Result<String, String> {
        if let Some(url) = self.exported.as_ref() {
            return Ok(url.clone());
        }
        if let Some(handle) = self.handle.as_ref() {
            return Ok(handle.url());
        }
        match postgres::start(&self.scratch).await {
            Ok(handle) => {
                let url = handle.url();
                self.handle = Some(handle);
                Ok(url)
            }
            Err(error) => Err(format!("the throwaway Postgres could not start: {error}")),
        }
    }

    /// Replace a cluster that died mid-step. The caller has already captured its log; dropping the
    /// handle stops whatever is left of it, and `start` discards the old data directory.
    ///
    /// Nothing is restarted when the database came from the environment: it is not this process's
    /// to replace, and the step is re-run against it as it is.
    async fn restart(&mut self) -> Result<(), String> {
        if self.exported.is_some() {
            return Ok(());
        }
        self.handle = None;
        match postgres::start(&self.scratch).await {
            Ok(handle) => {
                self.handle = Some(handle);
                Ok(())
            }
            Err(error) => Err(format!("restarting the throwaway Postgres failed: {error}")),
        }
    }

    /// The error for a step that failed while the database was gone. The step's own output is not
    /// a verdict about the repository, so it is not reported as one.
    fn died(&self, step: &str) -> String {
        let tail = self
            .handle
            .as_ref()
            .map_or_default(postgres::ThrowawayPostgres::log_tail);
        format!(
            "the throwaway Postgres died during `{step}`; the check did not run against a \
             database. {}\n--- postgres log (tail) ---\n{}",
            postgres::cgroup_memory(),
            tail.trim()
        )
    }

    /// A bounded block for the Pod log: how the cluster lived and how it ended.
    fn diagnostics(&self) -> Option<String> {
        let handle = self.handle.as_ref()?;
        Some(format!(
            "== postgres: {} pid={:?} rss_peak_kb={:?}\n== postgres log (tail):\n{}",
            postgres::cgroup_memory(),
            handle.pid(),
            handle.rss_peak_kb(),
            handle.log_tail().trim()
        ))
    }
}

/// The job's throwaway Redis: started lazily, immediately before the step that needs it, and never
/// allowed to read as a verdict about the repository.
///
/// The properties mirror [`Database`]: inherited (a `TEST_REDIS_URL`/`REDIS_URL` already in the
/// environment wins), lazy, classified (a failed step while ours is not listening is a job error,
/// the client's exit 3), and restarted once with the death printed first.
struct Redis {
    scratch: PathBuf,
    exported: Option<String>,
    exported_private: Option<String>,
    /// The shared server (`TEST_REDIS_URL` / `REDIS_URL`).
    handle: Option<redis::ThrowawayRedis>,
    /// The private server (`TEST_REDIS_PRIVATE_URL`), for a test that wipes Redis.
    private: Option<redis::ThrowawayRedis>,
}

impl Redis {
    fn new(scratch: &Path, exported: Option<String>, exported_private: Option<String>) -> Self {
        Self {
            scratch: scratch.to_path_buf(),
            exported,
            exported_private,
            handle: None,
            private: None,
        }
    }

    /// Whether a step that needs Redis can be given one: either the environment supplies it or
    /// this host can start a server.
    fn available(&self) -> bool {
        self.exported.is_some() || redis::available()
    }

    /// Whether the shared server is one this process started — the only kind whose death this
    /// runner can judge (and the only kind it may stop or restart).
    fn is_ours(&self) -> bool {
        self.handle.is_some()
    }

    /// Whether the private server is ours.
    fn private_is_ours(&self) -> bool {
        self.private.is_some()
    }

    /// Whether this host can provide a private server at all: one was exported, or the binary is
    /// installed. The kata runner image always has it; the control-plane image and the CI host do
    /// not. A host that *can* start one and fails is a setup error; a host that cannot is not.
    fn can_start_private(&self) -> bool {
        self.exported_private.is_some() || redis::available()
    }

    /// The shared URL, starting the server the first time it is asked for.
    async fn ensure(&mut self) -> Result<String, String> {
        if let Some(url) = self.exported.as_ref() {
            return Ok(url.clone());
        }
        if let Some(handle) = self.handle.as_ref() {
            return Ok(handle.url());
        }
        match redis::start(&self.scratch, redis::PORT).await {
            Ok(handle) => {
                let url = handle.url();
                self.handle = Some(handle);
                Ok(url)
            }
            Err(error) => Err(format!("the throwaway Redis could not start: {error}")),
        }
    }

    /// The private URL, starting the second server when the shared one is ours.
    async fn ensure_private(&mut self) -> Result<String, String> {
        if let Some(url) = self.exported_private.as_ref() {
            return Ok(url.clone());
        }
        if let Some(handle) = self.private.as_ref() {
            return Ok(handle.url());
        }
        // A suite that runs several runner jobs in **one** process (this repository's own tests)
        // shares the Pod's private port. Use the server already listening instead of failing to
        // bind a second one; a server this process did not start is never stopped or judged.
        if redis::accepting_connections(redis::PRIVATE_PORT).await {
            return Ok(redis::PRIVATE_DSN.to_owned());
        }
        match redis::start(&self.scratch, redis::PRIVATE_PORT).await {
            Ok(handle) => {
                let url = handle.url();
                self.private = Some(handle);
                Ok(url)
            }
            Err(error) => Err(format!(
                "the private throwaway Redis could not start: {error}"
            )),
        }
    }

    /// Replace the servers that died mid-step. Nothing is restarted when the URL came from the
    /// environment: it is not this process's to replace.
    async fn restart(&mut self) -> Result<(), String> {
        self.handle = None;
        self.private = None;
        if self.exported.is_none() {
            match redis::start(&self.scratch, redis::PORT).await {
                Ok(handle) => self.handle = Some(handle),
                Err(error) => {
                    return Err(format!("restarting the throwaway Redis failed: {error}"));
                }
            }
        }
        if self.exported_private.is_none() {
            match redis::start(&self.scratch, redis::PRIVATE_PORT).await {
                Ok(handle) => self.private = Some(handle),
                Err(error) => {
                    return Err(format!(
                        "restarting the private throwaway Redis failed: {error}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The error for a step that failed while a server was gone.
    fn died(&self, step: &str) -> String {
        let main = self
            .handle
            .as_ref()
            .map_or_default(redis::ThrowawayRedis::log_tail);
        let private = self
            .private
            .as_ref()
            .map_or_default(redis::ThrowawayRedis::log_tail);
        format!(
            "the throwaway Redis died during `{step}`; the check did not run against a server. \
             {}\n--- redis log (tail) ---\n{}\n--- private redis log (tail) ---\n{}",
            postgres::cgroup_memory(),
            main.trim(),
            private.trim()
        )
    }

    /// A bounded block for the Pod log: how the servers lived and how they ended.
    fn diagnostics(&self) -> Option<String> {
        let handle = self.handle.as_ref()?;
        let private = self.private.as_ref().map_or_default(|private| {
            format!(
                "== private redis: pid={:?} rss_peak_kb={:?}\n== private redis log (tail):\n{}",
                private.pid(),
                private.rss_peak_kb(),
                private.log_tail().trim()
            )
        });
        Some(format!(
            "== redis: pid={:?} rss_peak_kb={:?}\n== redis log (tail):\n{}\n{private}",
            handle.pid(),
            handle.rss_peak_kb(),
            handle.log_tail().trim(),
        ))
    }
}

/// A setup step that stopped the job before any check ran.
struct Failure {
    stop: Stop,
    message: String,
}

impl Failure {
    /// The result for a setup failure. `source_name` names which source could not be
    /// materialised; a multi-source job must not report an anonymous setup failure.
    fn into_result(self, mut result: RunnerResult, source_name: Option<&str>) -> RunnerResult {
        result.status = match self.stop {
            Stop::TimedOut => Status::Timeout,
            Stop::Cancelled => Status::Cancelled,
            Stop::Exited(_) => Status::Failed,
        };
        let message = match source_name {
            Some(name) => format!("source {name:?}: {}", self.message),
            None => self.message,
        };
        result.summary.clone_from(&message);
        result.error = Some(message);
        result
    }
}

/// What materialising a source produced, beyond the files on disk.
#[derive(Debug, Default)]
struct Materialised {
    /// The checked-out commit for a git source.
    commit: Option<String>,
    /// The number of files in an uploaded archive (known only after it is unpacked).
    files: Option<usize>,
}

/// Put the source at `dir`.
async fn materialise(
    source: &Source,
    dir: &Path,
    target_dir: &Path,
    deadline: TokioInstant,
    cancel: &mut Cancellation,
) -> Result<Materialised, Failure> {
    reset_dir(dir)?;
    match source {
        Source::Inline { files } => {
            for file in files {
                let path = dir.join(&file.path);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|err| Failure {
                        stop: Stop::Exited(None),
                        message: format!("could not create {:?}: {err}", parent.display()),
                    })?;
                }
                std::fs::write(&path, file.text.as_bytes()).map_err(|err| Failure {
                    stop: Stop::Exited(None),
                    message: format!("could not write {:?}: {err}", path.display()),
                })?;
            }
            Ok(Materialised::default())
        }
        Source::Git {
            url,
            git_ref,
            depth,
            token,
        } => {
            let depth_arg = format!("--depth={depth}");
            // The installation token travels as a git config value, never in the URL: a URL is
            // what a proxy, a log or a panic message records.
            let auth = token
                .as_deref()
                .map(crate::github_app::git_authorization_header);
            let git_env: Vec<(&str, &str)> = match auth.as_deref() {
                Some(header) => vec![
                    ("GIT_CONFIG_COUNT", "1"),
                    ("GIT_CONFIG_KEY_0", "http.extraHeader"),
                    ("GIT_CONFIG_VALUE_0", header),
                ],
                None => Vec::new(),
            };
            let ctx = ChildCtx {
                dir,
                target_dir,
                env: git_env.as_slice(),
            };
            run_git(&["init", "-q"], &ctx, deadline, cancel).await?;
            run_git(&["remote", "add", "origin", url], &ctx, deadline, cancel).await?;
            run_git(
                &["fetch", &depth_arg, "origin", git_ref],
                &ctx,
                deadline,
                cancel,
            )
            .await?;
            run_git(
                &["checkout", "--detach", "FETCH_HEAD"],
                &ctx,
                deadline,
                cancel,
            )
            .await?;
            let head = run_git(&["rev-parse", "HEAD"], &ctx, deadline, cancel).await?;
            Ok(Materialised {
                commit: Some(head.trim().to_owned()),
                files: None,
            })
        }
        Source::Upload {
            id,
            url,
            token,
            sha256,
        } => {
            let url = url.as_deref().ok_or_else(|| Failure {
                stop: Stop::Exited(None),
                message: "upload source has no url".to_owned(),
            })?;
            let blob =
                download(url, token.as_deref().unwrap_or_default(), deadline, cancel).await?;
            // Verify before unpacking: the hash is what the API recorded for these exact bytes.
            let got = crate::source_archive::sha256_hex(&blob);
            if let Some(expected) = sha256.as_deref()
                && got != expected
            {
                return Err(Failure {
                    stop: Stop::Exited(None),
                    message: format!(
                        "the downloaded source hashes to {got}, not the recorded {expected}"
                    ),
                });
            }
            let manifest = crate::source_archive::unpack(&blob, dir).map_err(|err| Failure {
                stop: Stop::Exited(None),
                message: format!("uploaded source {id} could not be unpacked: {err}"),
            })?;
            Ok(Materialised {
                commit: None,
                files: Some(manifest.files),
            })
        }
    }
}

/// Fetch an uploaded archive from the API with the capability token the worker minted.
///
/// The byte cap is enforced here as well as on the server: a runner that trusts a `Content-Length`
/// has no defence of its own.
///
/// A transient failure is retried, because the fetch is the first thing this process does and a
/// kata Pod is `Running` before its guest network is. Measured 2026-11-14: an immediate connect
/// was refused while the same connect six seconds later reached the API, so a single attempt
/// failed **every** job at startup.
async fn download(
    url: &str,
    token: &str,
    deadline: TokioInstant,
    cancel: &mut Cancellation,
) -> Result<Vec<u8>, Failure> {
    let client = reqwest::Client::builder().build().map_err(|err| Failure {
        stop: Stop::Exited(None),
        message: format!("could not build an http client: {err}"),
    })?;

    let mut attempt = 1;
    loop {
        match fetch_once(&client, url, token, deadline, cancel).await {
            Ok(blob) => return Ok(blob),
            Err(error) if error.retryable && attempt < DOWNLOAD_ATTEMPTS => {
                let wait = DOWNLOAD_BACKOFF * 2u32.pow(attempt - 1);
                if TokioInstant::now() + wait >= deadline {
                    return Err(error.failure);
                }
                println!(
                    "== source fetch: {} — attempt {attempt}/{DOWNLOAD_ATTEMPTS}, retrying in {}ms",
                    error.failure.message,
                    wait.as_millis()
                );
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(Failure { stop: Stop::TimedOut, message: "the deadline was reached while waiting to retry the source fetch".to_owned() });
                    }
                    _ = cancel.signal.recv() => {
                        return Err(Failure { stop: Stop::Cancelled, message: "the job was cancelled while waiting to retry the source fetch".to_owned() });
                    }
                }
                attempt += 1;
            }
            Err(error) => return Err(error.failure),
        }
    }
}

/// A failed fetch, and whether trying again could help.
struct FetchError {
    failure: Failure,
    retryable: bool,
}

/// One attempt: connect, read the body to the cap, return it.
async fn fetch_once(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    deadline: TokioInstant,
    cancel: &mut Cancellation,
) -> Result<Vec<u8>, FetchError> {
    let setup = |message: String| FetchError {
        failure: Failure {
            stop: Stop::Exited(None),
            message,
        },
        retryable: true,
    };

    let response = tokio::select! {
        response = client.get(url).header("x-source-token", token).send() => response,
        () = tokio::time::sleep_until(deadline) => {
            return Err(FetchError { failure: Failure { stop: Stop::TimedOut, message: "the deadline was reached while fetching the uploaded source".to_owned() }, retryable: false });
        }
        _ = cancel.signal.recv() => {
            return Err(FetchError { failure: Failure { stop: Stop::Cancelled, message: "the job was cancelled while fetching the uploaded source".to_owned() }, retryable: false });
        }
    };
    let response =
        response.map_err(|err| setup(format!("could not fetch the uploaded source: {err}")))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let message = format!("the source download answered {status}: {}", tail(&body));
        return Err(FetchError {
            failure: Failure {
                stop: Stop::Exited(None),
                message,
            },
            // A client error is the server saying no; a 5xx or 429 is worth one more try.
            retryable: status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS,
        });
    }

    let cap = crate::source_archive::MAX_COMPRESSED_BYTES;
    let mut response = response;
    let mut blob = Vec::new();
    loop {
        let next = tokio::select! {
            chunk = response.chunk() => {
                chunk.map_err(|err| setup(format!("the source download failed mid-body: {err}")))?
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(FetchError { failure: Failure { stop: Stop::TimedOut, message: "the deadline was reached while reading the uploaded source".to_owned() }, retryable: false });
            }
            _ = cancel.signal.recv() => {
                return Err(FetchError { failure: Failure { stop: Stop::Cancelled, message: "the job was cancelled while reading the uploaded source".to_owned() }, retryable: false });
            }
        };
        let Some(chunk) = next else { break };
        if blob.len() + chunk.len() > cap {
            return Err(FetchError {
                failure: Failure {
                    stop: Stop::Exited(None),
                    message: format!("the uploaded source is over the {cap}-byte cap"),
                },
                retryable: false,
            });
        }
        blob.extend_from_slice(&chunk);
    }
    if blob.is_empty() {
        return Err(FetchError {
            failure: Failure {
                stop: Stop::Exited(None),
                message: "the source download returned no bytes".to_owned(),
            },
            retryable: true,
        });
    }
    Ok(blob)
}

/// Everything a child needs beyond its argv. Grouped so the signatures stay readable and clippy's
/// argument-count lint has nothing to say.
struct ChildCtx<'a> {
    dir: &'a Path,
    target_dir: &'a Path,
    /// Extra environment for this child only (the GitHub App header, currently).
    env: &'a [(&'a str, &'a str)],
}

/// One fixed git command. A non-zero exit is a setup failure carrying the stderr tail.
async fn run_git(
    args: &[&str],
    ctx: &ChildCtx<'_>,
    deadline: TokioInstant,
    cancel: &mut Cancellation,
) -> Result<String, Failure> {
    let outcome = run_child(Mode::Capture, "git", args, ctx, deadline, cancel)
        .await
        .map_err(|err| Failure {
            stop: Stop::Exited(None),
            message: format!("git {} could not start: {err}", args.join(" ")),
        })?;
    match outcome.stop {
        Stop::TimedOut | Stop::Cancelled => Err(Failure {
            stop: outcome.stop,
            message: "the job stopped during the git checkout".to_owned(),
        }),
        Stop::Exited(code) if code != Some(0) => Err(Failure {
            stop: Stop::Exited(code),
            message: format!(
                "git {} failed (exit {code:?}): {}",
                args.join(" "),
                tail(&outcome.stderr)
            ),
        }),
        Stop::Exited(_) => Ok(outcome.stdout),
    }
}

/// Empty the work directory so a retry never inherits files from an earlier attempt.
fn reset_dir(dir: &Path) -> Result<(), Failure> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(Failure {
                stop: Stop::Exited(None),
                message: format!("could not clear {:?}: {err}", dir.display()),
            });
        }
    }
    std::fs::create_dir_all(dir).map_err(|err| Failure {
        stop: Stop::Exited(None),
        message: format!("could not create {:?}: {err}", dir.display()),
    })
}

/// The last [`MAX_ERROR_TEXT`] bytes of a child's stderr, lossy-decoded.
fn tail(text: &str) -> String {
    if text.len() <= MAX_ERROR_TEXT {
        return text.trim().to_owned();
    }
    let mut keep = MAX_ERROR_TEXT;
    while keep > 0 && !text.is_char_boundary(text.len() - keep) {
        keep -= 1;
    }
    format!("…{}", &text[text.len() - keep..])
}

/// The cargo target dir for a run: a sibling of the source dir, so two runs never share one and
/// the shell's own `CARGO_TARGET_DIR` (the outer build, in CI) is never reused.
fn target_dir_for(source_dir: &Path) -> PathBuf {
    PathBuf::from(format!("{}.target", source_dir.display()))
}

/// The SQLAlchemy/psycopg v3 spelling of a PostgreSQL URL, derived from the same URL the step is
/// told to use. `postgres://` and `postgresql://` are the same DSN; `SQLAlchemy` needs the driver
/// named, and the estate's driver is psycopg v3 (`postgresql+psycopg://`).
fn sqlalchemy_dsn(url: &str) -> String {
    if let Some(rest) = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
    {
        format!("postgresql+psycopg://{rest}")
    } else {
        url.to_owned()
    }
}

/// Whether this image carries `sccache`. Answered once per process: a binary cannot appear
/// midway through a job, and every child would otherwise pay for the check.
fn sccache_available() -> bool {
    static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        std::process::Command::new("sccache")
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    })
}

/// What sccache reports for this job, when it is present and its output parses.
///
/// Never an error: the cache cannot change a verdict, so every failure here is `None`.
fn sccache_stats() -> Option<CacheStats> {
    if !sccache_available() {
        return None;
    }
    let output = std::process::Command::new("sccache")
        .args(["--show-stats", "--stats-format=json"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_sccache_stats(&String::from_utf8_lossy(&output.stdout))
}

/// One counter from sccache's stats document.
///
/// sccache 0.19 serialises `cache_hits`, `cache_misses` and `cache_errors` as `PerLanguageCount`
/// (`{"counts": {"Rust": 12}}`); older releases used a plain number. Both are accepted, and a shape
/// that is neither is `None` rather than a zero that reads like a real measurement. A missing
/// `cache_errors` means no errors were reported, so it defaults to zero.
fn counter(stats: &serde_json::Value, key: &str) -> Option<u64> {
    match stats.get(key)? {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::Object(_) => Some(
            stats.get(key)?["counts"]
                .as_object()?
                .values()
                .filter_map(serde_json::Value::as_u64)
                .sum(),
        ),
        _ => None,
    }
}

fn parse_sccache_stats(raw: &str) -> Option<CacheStats> {
    let doc: serde_json::Value = serde_json::from_str(raw.trim()).ok()?;
    let stats = doc.get("stats").unwrap_or(&doc);
    Some(CacheStats {
        hits: counter(stats, "cache_hits")?,
        misses: counter(stats, "cache_misses")?,
        errors: counter(stats, "cache_errors").unwrap_or(0),
    })
}

/// Run one child under the job deadline and SIGTERM.
async fn run_child(
    mode: Mode,
    program: &str,
    args: &[&str],
    ctx: &ChildCtx<'_>,
    deadline: TokioInstant,
    cancel: &mut Cancellation,
) -> std::io::Result<Outcome> {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(ctx.dir)
        .stdin(Stdio::null())
        .env("CARGO_TERM_COLOR", "never")
        .env("CARGO_TARGET_DIR", ctx.target_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        // A job Pod is 4 CPUs over a 4 GiB limit (`sandbox::resources_for`). Cargo derives its
        // job count from the cgroup's CPU quota, so a 4-core limit runs four rustc processes at
        // once — and a heavy workspace bursts past the memory limit: measured 2026-11-19 on
        // `agent`, agent-api's lib and lib-test together were SIGKILLed by the kernel
        // (`sccache: Compile terminated by signal 9`) on every job while every other crate came
        // from sccache (886 hits, 0 misses). Two compile jobs is the parallelism that fitted the
        // same 4 GiB before the CPU limit was raised.
        .env("CARGO_BUILD_JOBS", "2")
        // CI's rust job sets this; without it every dev-profile artifact carries DWARF, which is
        // what makes a target directory multi-gigabyte. The gate exists to reproduce CI's
        // verdict, so it builds the way CI builds.
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .kill_on_drop(true);
    // A cache that cannot work must never fail a check. The Pod environment carries
    // RUSTC_WRAPPER because the worker cannot know what the image contains; without the binary
    // every rustc invocation would fail on the missing wrapper, so the runner drops it here.
    if !sccache_available() {
        command.env_remove("RUSTC_WRAPPER");
    }
    for (key, value) in ctx.env {
        command.env(key, value);
    }
    match mode {
        Mode::Stream => {
            // Tee: the child's output still goes to the Pod log, and a bounded copy travels back
            // in the result so a failing check carries its own diagnostics.
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
            let mut child = command.spawn()?;
            let buffer = OutputBuffer::new(MAX_CHECK_OUTPUT_BYTES);
            let out = child
                .stdout
                .take()
                .map(|stdout| tokio::spawn(pump(stdout, tokio::io::stdout(), buffer.clone())));
            let err = child
                .stderr
                .take()
                .map(|stderr| tokio::spawn(pump(stderr, tokio::io::stderr(), buffer.clone())));
            let stop = tokio::select! {
                status = child.wait() => Stop::Exited(status?.code()),
                () = tokio::time::sleep_until(deadline) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    Stop::TimedOut
                }
                _ = cancel.signal.recv() => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    Stop::Cancelled
                }
            };
            // The pipes close when the child is gone, so both pumps end on their own — unless a
            // grandchild inherited the pipe and outlived the child (a test that leaks a process).
            // Then the pump would never see EOF and this await would hang the whole job: bound it
            // and let the orphan go rather than lose the result line.
            for mut task in [out, err].into_iter().flatten() {
                if tokio::time::timeout(PUMP_GRACE, &mut task).await.is_err() {
                    task.abort();
                }
            }
            Ok(Outcome {
                stop,
                stdout: String::new(),
                stderr: String::new(),
                tail: buffer.snapshot(),
            })
        }
        Mode::Capture => {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
            let output = tokio::select! {
                output = command.output() => output?,
                () = tokio::time::sleep_until(deadline) => {
                    return Ok(Outcome {
                        stop: Stop::TimedOut,
                        stdout: String::new(),
                        stderr: String::new(),
                        tail: String::new(),
                    });
                }
                _ = cancel.signal.recv() => {
                    return Ok(Outcome {
                        stop: Stop::Cancelled,
                        stdout: String::new(),
                        stderr: String::new(),
                        tail: String::new(),
                    });
                }
            };
            Ok(Outcome {
                stop: Stop::Exited(output.status.code()),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                tail: String::new(),
            })
        }
    }
}

/// A bounded tail of a streamed child's output: the last `cap` bytes, across both streams.
#[derive(Clone)]
struct OutputBuffer {
    inner: Arc<Mutex<VecDeque<u8>>>,
    cap: usize,
}

impl OutputBuffer {
    fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(cap))),
            cap,
        }
    }

    fn push(&self, chunk: &[u8]) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.extend(chunk.iter().copied());
        while inner.len() > self.cap {
            // Dropping the oldest bytes may split a UTF-8 sequence at the front; the tail is
            // decoded lossily, so a replacement character there is expected and harmless.
            let excess = inner.len() - self.cap;
            inner.drain(..excess);
        }
    }

    fn snapshot(&self) -> String {
        let Ok(inner) = self.inner.lock() else {
            return String::new();
        };
        let bytes: Vec<u8> = inner.iter().copied().collect();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

/// Copy one child stream to `sink` and to the shared tail buffer until the pipe closes.
async fn pump<R, W>(mut reader: R, mut sink: W, buffer: OutputBuffer)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut chunk = vec![0u8; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let _ = sink.write_all(&chunk[..read]).await;
                let _ = sink.flush().await;
                buffer.push(&chunk[..read]);
            }
        }
    }
}

/// The default work directory, so callers can print it.
#[must_use]
pub fn default_source_dir() -> PathBuf {
    PathBuf::from(SOURCE_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_sccache_stats_shapes_and_refuses_a_guess() {
        // The shape the runner actually receives: sccache 0.19 serialises ALL THREE counters as
        // per-language counts. The original fixture below used numbers for misses and errors,
        // which is why a parser that read them as zero passed the test and lied in production.
        let counts = r#"{"stats":{"compile_requests":9,"cache_hits":{"counts":{"Rust":5,"C/C++":2}},"cache_misses":{"counts":{"Rust":2}},"cache_errors":{"counts":{"Rust":1}}}}"#;
        assert_eq!(
            parse_sccache_stats(counts),
            Some(CacheStats {
                hits: 7,
                misses: 2,
                errors: 1
            })
        );
        // Older releases reported a single number, and may omit cache_errors entirely.
        let number = r#"{"stats":{"cache_hits":4,"cache_misses":1}}"#;
        assert_eq!(
            parse_sccache_stats(number),
            Some(CacheStats {
                hits: 4,
                misses: 1,
                errors: 0
            })
        );
        // Anything else is absent — never a guessed zero that reads like "the cache did nothing".
        assert_eq!(parse_sccache_stats("not json"), None);
        assert_eq!(parse_sccache_stats(r#"{"stats":{"unexpected":1}}"#), None);
        // Present but unreadable is refused too: a zero there would be a lie, not a measurement.
        assert_eq!(
            parse_sccache_stats(r#"{"stats":{"cache_hits":{},"cache_misses":{"counts":{}}}}"#),
            None
        );
    }

    #[test]
    fn a_step_label_names_its_source_only_when_the_job_has_several() {
        let multi = Unit {
            name: Some("source-1"),
            profile: Profile::Rust,
            source: None,
            dir: PathBuf::from("/tmp/sources/source-1"),
        };
        assert_eq!(
            step_label(&multi, "cargo test"),
            "cargo test on source \"source-1\""
        );
        let single = Unit {
            name: None,
            profile: Profile::Rust,
            source: None,
            dir: PathBuf::from("/tmp/src"),
        };
        assert_eq!(step_label(&single, "cargo test"), "cargo test");
    }

    #[test]
    fn tail_bounds_the_error_text() {
        let big = "x".repeat(MAX_ERROR_TEXT * 2);
        assert!(tail(&big).len() <= MAX_ERROR_TEXT + 4);
        assert_eq!(tail("  short  "), "short");
    }

    #[test]
    fn the_python_database_alias_names_the_driver() {
        // Both PostgreSQL spellings are the same DSN; SQLAlchemy must still be told the driver,
        // and a test suite that reads DATABASE_URL fails on "no driver" otherwise.
        assert_eq!(
            sqlalchemy_dsn("postgres://postgres:pw@127.0.0.1:5432/postgres"),
            "postgresql+psycopg://postgres:pw@127.0.0.1:5432/postgres"
        );
        assert_eq!(
            sqlalchemy_dsn("postgresql://a@b:1/c"),
            "postgresql+psycopg://a@b:1/c"
        );
        // Anything already carrying a driver-qualified scheme is left alone.
        assert_eq!(
            sqlalchemy_dsn("postgresql+psycopg://a@b:1/c"),
            "postgresql+psycopg://a@b:1/c"
        );
    }

    #[tokio::test]
    async fn an_exported_database_is_used_and_no_cluster_is_started() {
        // The live failure this guards: this repository's own `tests/runner.rs` calls `run_in`,
        // whose scratch is `/tmp` — the job's own data directory. A nested runner that starts a
        // second cluster there stops the job's cluster on drop, and the failed test that follows
        // reads as the repository's fault.
        let scratch =
            std::env::temp_dir().join(format!("coding-db-exported-{}", std::process::id()));
        let url = "postgres://harness@127.0.0.1:5432/harness".to_owned();
        let mut database = Database::new(&scratch, Some(url.clone()));

        assert!(
            database.available(),
            "an exported URL is a database, whether or not this host has the binaries"
        );
        assert_eq!(database.ensure().await.unwrap(), url);
        assert!(
            database.handle.is_none(),
            "no cluster may be started beside an exported database"
        );
        assert!(
            database.restart().await.is_ok(),
            "a no-op, not a second cluster"
        );
        assert!(database.handle.is_none(), "still nothing of ours to stop");
        assert!(
            database.diagnostics().is_none(),
            "there is no cluster of ours to diagnose"
        );
        assert!(
            database
                .died("cargo test")
                .starts_with("the throwaway Postgres died during `cargo test`")
        );
    }
}
