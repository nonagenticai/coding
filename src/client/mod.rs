//! The client: `coding check`, a thin HTTP caller for a box with no toolchain, no docker and no
//! cluster access beyond a reachable URL.
//!
//! It packs one snapshot of the repository (the working tree by default; the index for a
//! pre-commit gate; HEAD for a pre-push gate), uploads it, asks for one `check` job and polls to a
//! verdict. The exit code is the contract a hook or a pipeline branches on:
//!
//! | code | meaning |
//! |---|---|
//! | 0 | every check passed |
//! | 1 | a check failed — the code is the problem |
//! | 2 | usage, configuration, or a kind this runner cannot execute |
//! | 3 | infrastructure: unreachable app, 5xx, timeout, cancelled, a dead runner |
//! | 130 | interrupted |
//!
//! A failed *check* is never 3 and an infrastructure failure is never 1: a hook can fail open on
//! 3 and still block on 1.

pub mod hook;
pub mod pack;

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};

use crate::domain::{JobKind, Profile};

/// The `--profile` the caller asked for. `auto` is resolved from the packed tree; the explicit
/// values exist for the one layout auto cannot decide (a repository that carries both toolchains).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ProfileArg {
    /// Detect from the tree: Cargo.toml + Cargo.lock is rust; pyproject.toml + uv.lock is python.
    Auto,
    /// Require Cargo.toml + Cargo.lock.
    Rust,
    /// Require pyproject.toml + uv.lock.
    Python,
}

/// The tree, check names and verdict are all in order.
pub const EXIT_OK: u8 = 0;
/// A check failed.
pub const EXIT_FAILED: u8 = 1;
/// The caller asked for something this tool or this runner cannot do.
pub const EXIT_USAGE: u8 = 2;
/// The app, the network or the runner failed — not the code.
pub const EXIT_INFRA: u8 = 3;
/// Ctrl-C.
pub const EXIT_INTERRUPTED: u8 = 130;

/// How often the client asks for the job's state.
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Per-request timeout. The upload gets its own, longer one.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// One `coding check` invocation.
#[derive(Debug)]
pub struct CheckArgs {
    /// `--url` or `CODING_URL`. Required.
    pub url: Option<String>,
    /// `--tenant` or `CODING_TENANT`, default `local`.
    pub tenant: Option<String>,
    /// The licence bearer token. `--token` or `CODING_TOKEN`, then `git config coding.token`.
    /// Never printed and never passed on a command line by the hooks.
    pub token: Option<String>,
    pub kind: JobKind,
    /// `--profile`; `auto` (the default) detects the toolchain from the packed tree.
    pub profile: ProfileArg,
    /// Which snapshot to pack: the working tree (default), the index, or HEAD.
    pub snapshot: pack::Snapshot,
    pub timeout_seconds: u32,
    /// Print the job JSON instead of the human verdict.
    pub json: bool,
    /// Pack and print the manifest; upload nothing.
    pub dry_run: bool,
    /// The directory to pack; the current directory by default.
    pub dir: Option<PathBuf>,
}

/// A failure that is not a check result.
#[derive(Debug)]
pub enum ClientError {
    Usage(String),
    Infra(String),
}

impl ClientError {
    fn exit(&self) -> u8 {
        match self {
            Self::Usage(_) => EXIT_USAGE,
            Self::Infra(_) => EXIT_INFRA,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) | Self::Infra(message) => f.write_str(message),
        }
    }
}

/// Run `coding check` and return the process exit code.
pub async fn check(args: CheckArgs) -> u8 {
    match run_check(args).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("coding check: {error}");
            error.exit()
        }
    }
}

// reason: one linear upload -> poll -> verdict flow reads better kept together.
#[allow(clippy::too_many_lines)]
async fn run_check(args: CheckArgs) -> Result<u8, ClientError> {
    // The URL is resolved only after the dry-run exit below: what a dry run shows is the tree it
    // would send, and that is true with no app reachable at all.
    let tenant = args
        .tenant
        .clone()
        .or_else(|| std::env::var("CODING_TENANT").ok())
        .map(|tenant| tenant.trim().to_owned())
        .filter(|tenant| !tenant.is_empty())
        .unwrap_or_else(|| "local".to_owned());
    let dir = match args.dir {
        Some(dir) => dir,
        None => std::env::current_dir().map_err(|err| {
            ClientError::Usage(format!("cannot read the working directory: {err}"))
        })?,
    };
    // The hook body never passes a token on the command line (it would be visible in `ps`);
    // the CLI reads `coding.token` from the same git config the hook already uses.
    let token = resolve_token(args.token.clone(), git_config(&dir, "coding.token"));

    let tree = pack::collect(&dir, args.snapshot).map_err(|err| match err {
        pack::PackError::Usage(message) => ClientError::Usage(message),
        pack::PackError::Io(message) => ClientError::Infra(message),
    })?;
    for path in &tree.skipped_secrets {
        eprintln!("warning: not sending {path}: it looks like a credential");
    }
    for path in &tree.skipped_irregular {
        eprintln!("warning: not sending {path}: not a regular file");
    }
    let profile = detect_profile(&tree, args.profile)?;
    println!("profile: {}", profile.as_str());
    println!("snapshot: {}", args.snapshot.as_str());
    let blob = pack::pack(&tree).map_err(|err| ClientError::Infra(err.to_string()))?;
    let sha256 = crate::source_archive::sha256_hex(&blob);
    let total = tree.bytes();
    println!(
        "packed {} files, {total} bytes ({} compressed), sha256 {sha256}",
        tree.files.len(),
        blob.len()
    );
    match (&tree.head, tree.dirty) {
        (Some(head), dirty) => println!(
            "source: {} at {} ({})",
            tree.root.display(),
            &head[..head.len().min(12)],
            if dirty { "dirty" } else { "clean" }
        ),
        (None, _) => println!("source: {} (no commits yet)", tree.root.display()),
    }
    if args.dry_run {
        for file in &tree.files {
            println!("  {} ({} bytes)", file.path, file.data.len());
        }
        println!("dry run: nothing uploaded");
        return Ok(EXIT_OK);
    }
    let base = resolve_url(args.url.as_deref(), std::env::var("CODING_URL").ok())?;

    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|err| ClientError::Infra(format!("cannot build an http client: {err}")))?;

    // 1. Upload the archive. The first request leaves a laptop moments after an `ssh -L` tunnel
    //    starts, and a tunnel is not ready the instant it prints — one attempt would report
    //    "cannot reach the app" for a tunnel that is fine a second later.
    let upload = send_with_retry(|| {
        authorize(
            client
                .post(format!("{base}/api/sources"))
                .header("x-tenant-id", &tenant)
                .header(reqwest::header::CONTENT_TYPE, "application/gzip")
                .timeout(UPLOAD_TIMEOUT)
                .body(blob.clone()),
            token.as_deref(),
        )
    })
    .await?;
    let upload = read_json(upload).await?;
    let source_id = upload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::Infra(format!("the upload response has no id: {upload}")))?
        .to_owned();
    println!("uploaded source {source_id}");

    // 2. Ask for one check job.
    let mut input = json!({"source": {"kind": "upload", "id": source_id}});
    if profile != Profile::Rust {
        // Omitted for rust on purpose: rust is the server's default, and a runner image built
        // before profiles existed denies unknown fields, so a Rust job must keep the exact body
        // it sent before profiles existed.
        input["profile"] = json!(profile.as_str());
    }
    let request = json!({
        "kind": args.kind.as_str(),
        "input": input,
        "timeout_seconds": args.timeout_seconds,
    });
    let created = send_with_retry(|| {
        authorize(
            client
                .post(format!("{base}/api/jobs"))
                .header("x-tenant-id", &tenant)
                .json(&request),
            token.as_deref(),
        )
    })
    .await?;
    let created = read_json(created).await?;
    let job_id = created
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::Infra(format!("the job response has no id: {created}")))?
        .to_owned();
    println!("job {job_id} {}", created_status(&created));

    // 3. Poll to a verdict, or cancel on Ctrl-C.
    let poll = poll_to_verdict(
        &client,
        &base,
        &tenant,
        &job_id,
        args.timeout_seconds,
        token.as_deref(),
    );
    tokio::pin!(poll);
    let job = tokio::select! {
        result = &mut poll => result?,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\ninterrupted: asking the app to cancel job {job_id}");
            let _ = authorize(
                client
                    .post(format!("{base}/api/jobs/{job_id}/cancel"))
                    .header("x-tenant-id", &tenant),
                token.as_deref(),
            )
            .send()
            .await;
            return Ok(EXIT_INTERRUPTED);
        }
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&job).unwrap_or_else(|_| job.to_string())
        );
        return Ok(verdict_code(&job));
    }
    print_verdict(&job);
    Ok(verdict_code(&job))
}

/// The URL to talk to, from the flag, the environment, or neither.
fn resolve_url(flag: Option<&str>, env: Option<String>) -> Result<String, ClientError> {
    let raw = flag
        .map(str::to_owned)
        .or(env)
        .map(|url| url.trim().to_owned())
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            ClientError::Usage(
                "--url (or CODING_URL) is required, e.g. http://127.0.0.1:8687 through \
                 `ssh -N -L 8687:localhost:8687 user@host`"
                    .to_owned(),
            )
        })?;
    if !(raw.starts_with("http://") || raw.starts_with("https://")) {
        return Err(ClientError::Usage(
            "--url must start with http:// or https://".to_owned(),
        ));
    }
    Ok(raw.trim_end_matches('/').to_owned())
}

/// The licence bearer, if one was supplied. `--token`/`CODING_TOKEN` wins over `git config
/// coding.token`; a blank value is treated as absent so an empty config cannot send `Bearer `.
fn resolve_token(flag: Option<String>, git_config_value: Option<String>) -> Option<String> {
    [flag, git_config_value]
        .into_iter()
        .flatten()
        .map(|token| token.trim().to_owned())
        .find(|token| !token.is_empty())
}

/// Read one git config value from `dir`. Any failure is `None`: the token is optional, and a
/// missing git or a missing key must not turn an unauthenticated call into an error.
fn git_config(dir: &std::path::Path, key: &str) -> Option<String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["config", "--get", key])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

/// Add the licence bearer when one is configured. In-cluster callers send no token and keep the
/// exact requests they sent before this existed.
fn authorize(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.header(reqwest::header::AUTHORIZATION, format!("Bearer {token}")),
        None => request,
    }
}

/// How many attempts a request that has not reach the app yet gets.
const TRANSPORT_ATTEMPTS: u32 = 3;
/// The first wait between those attempts; each later wait doubles.
const TRANSPORT_BACKOFF: Duration = Duration::from_millis(500);

/// Send a request, retrying a **transport** failure a couple of times. A refusal or an error
/// status is the app answering, and is never retried here.
async fn send_with_retry(
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ClientError> {
    let mut attempt = 1;
    loop {
        match build().send().await {
            Ok(response) => return Ok(response),
            Err(error) if attempt < TRANSPORT_ATTEMPTS => {
                let wait = TRANSPORT_BACKOFF * 2u32.pow(attempt - 1);
                eprintln!(
                    "coding check: {error} — attempt {attempt}/{TRANSPORT_ATTEMPTS}, retrying in {}ms",
                    wait.as_millis()
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(error) => {
                return Err(ClientError::Infra(format!("cannot reach the app: {error}")));
            }
        }
    }
}

/// Read a response as JSON, mapping a refusal to the right exit class.
async fn read_json(response: reqwest::Response) -> Result<Value, ClientError> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if status.is_success() {
        if value.is_null() {
            return Err(ClientError::Infra(
                "the app answered with an empty body".to_owned(),
            ));
        }
        return Ok(value);
    }
    let message = value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map_or_else(|| body.trim().chars().take(400).collect(), str::to_owned);
    let message = format!("the app refused the request ({status}): {message}");
    // 4xx is something the caller can fix (the tree, the tenant); 429 and 5xx are not.
    if status.is_client_error() && status != reqwest::StatusCode::TOO_MANY_REQUESTS {
        Err(ClientError::Usage(message))
    } else {
        Err(ClientError::Infra(message))
    }
}

fn created_status(job: &Value) -> String {
    job.get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned()
}

/// Poll the job until it is terminal, narrating each transition. Returns the final job.
async fn poll_to_verdict(
    client: &reqwest::Client,
    base: &str,
    tenant: &str,
    job_id: &str,
    timeout_seconds: u32,
    token: Option<&str>,
) -> Result<Value, ClientError> {
    let started = std::time::Instant::now();
    let budget = Duration::from_secs(u64::from(timeout_seconds) + 120);
    let mut last = String::new();
    let mut last_transport_error = String::new();
    loop {
        // A failed *read* is transient by nature — the tunnel can blip — so it is reported once
        // and the loop keeps going until its budget runs out. A refusal from the app still ends
        // the poll at once through read_json, because that is the app answering.
        match authorize(
            client
                .get(format!("{base}/api/jobs/{job_id}"))
                .header("x-tenant-id", tenant),
            token,
        )
        .send()
        .await
        {
            Ok(response) => {
                let job = read_json(response).await?;
                let status = created_status(&job);
                if status != last {
                    println!("  [{:>5.1}s] {status}", started.elapsed().as_secs_f32());
                    last.clone_from(&status);
                }
                if matches!(status.as_str(), "succeeded" | "failed" | "cancelled") {
                    return Ok(job);
                }
            }
            Err(error) => {
                let message = error.to_string();
                if message != last_transport_error {
                    eprintln!(
                        "  [{:>5.1}s] could not read the job ({message}); still polling",
                        started.elapsed().as_secs_f32()
                    );
                    last_transport_error = message;
                }
            }
        }
        if started.elapsed() > budget {
            return Err(ClientError::Infra(format!(
                "job {job_id} was still {last} after {}s",
                started.elapsed().as_secs()
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The exit code a finished job maps to.
fn verdict_code(job: &Value) -> u8 {
    let result = job.get("result");
    let result_status = result
        .and_then(|result| result.get("status"))
        .and_then(Value::as_str);
    match result_status {
        Some("succeeded") => EXIT_OK,
        Some("failed") => {
            // A *check* failed only if a check actually ran. A runner that failed to set the job
            // up (a bad archive, an expired token, an unreachable API) reports `failed` with an
            // error and no checks — infrastructure, which a hook must not block on.
            let ran_a_check = result
                .and_then(|result| result.get("checks"))
                .and_then(Value::as_array)
                .is_some_and(|checks| !checks.is_empty());
            let carried_an_error = result
                .and_then(|result| result.get("error"))
                .is_some_and(|error| !error.is_null());
            if ran_a_check && !carried_an_error {
                EXIT_FAILED
            } else {
                EXIT_INFRA
            }
        }
        // The runner has no plan for the kind, or the spec was unusable: nothing ran.
        Some("unsupported" | "invalid_spec") => EXIT_USAGE,
        Some("timeout" | "cancelled") => EXIT_INFRA,
        // A job that ended without a result ended in the job's own error path.
        _ => match job
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
        {
            Some("invalid_job_spec" | "sandbox_not_configured") => EXIT_USAGE,
            _ => EXIT_INFRA,
        },
    }
}

/// The human verdict: one line per check, then the tail of each failure.
fn print_verdict(job: &Value) {
    let result = job.get("result");
    let Some(result) = result else {
        let error = job.get("error").cloned().unwrap_or(Value::Null);
        println!(
            "verdict: {} — {}",
            job.get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("no result and no message")
        );
        return;
    };

    if let Some(checks) = result.get("checks").and_then(Value::as_array) {
        println!();
        for check in checks {
            let name = check.get("name").and_then(Value::as_str).unwrap_or("?");
            let status = check
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let exit = check
                .get("exit_code")
                .and_then(Value::as_i64)
                .map_or_default(|code| format!(" (exit {code})"));
            let duration = check
                .get("duration_ms")
                .and_then(Value::as_u64)
                .map_or_default(|ms| {
                    format!(
                        "  {:.1}s",
                        std::time::Duration::from_millis(ms).as_secs_f64()
                    )
                });
            let marker = if status == "passed" { "PASS" } else { "FAIL" };
            println!("  {marker}  {name}{exit}{duration}");
        }
    }
    for check in result
        .get("checks")
        .and_then(Value::as_array)
        .map_or_default(Vec::as_slice)
    {
        if check.get("status").and_then(Value::as_str) != Some("failed") {
            continue;
        }
        if let Some(tail) = check.get("output_tail").and_then(Value::as_str) {
            println!(
                "\n--- {} ---\n{}",
                check.get("name").and_then(Value::as_str).unwrap_or("?"),
                tail.trim_end()
            );
        }
    }
    println!(
        "\nverdict: {}",
        result
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("no summary")
    );
    if let Some(error) = result.get("error").and_then(Value::as_str) {
        println!("error: {error}");
    }
}

/// Resolve the profile from the packed tree.
///
/// The profile decides the whole command plan, so a wrong answer spends a Pod and reports a
/// failure that says nothing about the code. Every ambiguous or unsupported layout is therefore a
/// usage error here, before anything is uploaded.
fn detect_profile(tree: &pack::Tree, requested: ProfileArg) -> Result<Profile, ClientError> {
    let has_rust = tree.has("Cargo.toml") && tree.has("Cargo.lock");
    let has_python = tree.has("pyproject.toml") && tree.has("uv.lock");
    match requested {
        ProfileArg::Rust if has_rust => Ok(Profile::Rust),
        ProfileArg::Python if has_python => Ok(Profile::Python),
        ProfileArg::Rust | ProfileArg::Python => {
            let (flag, files) = if requested == ProfileArg::Rust {
                ("rust", "Cargo.toml and Cargo.lock")
            } else {
                ("python", "pyproject.toml and uv.lock")
            };
            Err(ClientError::Usage(format!(
                "--profile {flag} needs {files} at the root of {}",
                tree.root.display()
            )))
        }
        ProfileArg::Auto if has_rust && has_python => Err(ClientError::Usage(format!(
            "{} carries both Cargo.toml/Cargo.lock and pyproject.toml/uv.lock; choose with \
             --profile rust|python",
            tree.root.display()
        ))),
        ProfileArg::Auto if has_python => Ok(Profile::Python),
        ProfileArg::Auto if has_rust => Ok(Profile::Rust),
        ProfileArg::Auto => Err(ClientError::Usage(format!(
            "{} has neither Cargo.toml + Cargo.lock (rust) nor pyproject.toml + uv.lock (python); \
             the gate runs one of those toolchains",
            tree.root.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(paths: &[&str]) -> pack::Tree {
        pack::Tree {
            root: PathBuf::from("/tmp/tree"),
            files: paths
                .iter()
                .map(|path| crate::source_archive::PackFile {
                    path: (*path).to_owned(),
                    data: Vec::new(),
                    executable: false,
                })
                .collect(),
            head: None,
            dirty: false,
            skipped_secrets: Vec::new(),
            skipped_irregular: Vec::new(),
        }
    }

    #[test]
    fn auto_detects_each_layout_and_refuses_the_undecidable_ones() {
        assert_eq!(
            detect_profile(&tree(&["Cargo.toml", "Cargo.lock"]), ProfileArg::Auto).unwrap(),
            Profile::Rust
        );
        assert_eq!(
            detect_profile(&tree(&["pyproject.toml", "uv.lock"]), ProfileArg::Auto).unwrap(),
            Profile::Python
        );
        // Both toolchains: the client cannot know which one the caller means.
        assert!(matches!(
            detect_profile(
                &tree(&["Cargo.toml", "Cargo.lock", "pyproject.toml", "uv.lock"]),
                ProfileArg::Auto
            ),
            Err(ClientError::Usage(_))
        ));
        // Neither, and each half of a pair, are all usage errors: a plan without its lock file
        // would fail as a check, which reads like the repository's fault.
        for paths in [
            &["README.md"][..],
            &["Cargo.toml"][..],
            &["pyproject.toml"][..],
            &["Cargo.toml", "pyproject.toml"][..],
        ] {
            assert!(
                matches!(
                    detect_profile(&tree(paths), ProfileArg::Auto),
                    Err(ClientError::Usage(_))
                ),
                "{paths:?} must be a usage error"
            );
        }
    }

    #[test]
    fn an_explicit_profile_still_needs_its_own_files() {
        assert_eq!(
            detect_profile(&tree(&["Cargo.toml", "Cargo.lock"]), ProfileArg::Rust).unwrap(),
            Profile::Rust
        );
        assert_eq!(
            detect_profile(&tree(&["pyproject.toml", "uv.lock"]), ProfileArg::Python).unwrap(),
            Profile::Python
        );
        assert!(matches!(
            detect_profile(&tree(&["Cargo.toml", "Cargo.lock"]), ProfileArg::Python),
            Err(ClientError::Usage(_))
        ));
        assert!(matches!(
            detect_profile(&tree(&["pyproject.toml", "uv.lock"]), ProfileArg::Rust),
            Err(ClientError::Usage(_))
        ));
    }

    fn job(result: Option<&Value>, error: Option<&Value>) -> Value {
        json!({
            "id": "j",
            "kind": "check",
            "status": if result.is_some() { "succeeded" } else { "failed" },
            "result": result,
            "error": error,
        })
    }

    #[test]
    fn the_exit_code_separates_a_failed_check_from_a_failed_run() {
        let passed =
            json!({"status": "succeeded", "checks": [{"name": "cargo fmt", "status": "passed"}]});
        let failed =
            json!({"status": "failed", "checks": [{"name": "cargo test", "status": "failed"}]});
        // A runner that died setting the job up: `failed`, an error, and no check ever ran.
        let setup_failure =
            json!({"status": "failed", "error": "the downloaded source hashes to …"});
        assert_eq!(verdict_code(&job(Some(&passed), None)), EXIT_OK);
        assert_eq!(verdict_code(&job(Some(&failed), None)), EXIT_FAILED);
        assert_eq!(
            verdict_code(&job(Some(&setup_failure), None)),
            EXIT_INFRA,
            "an infrastructure failure must not block a hook as if the code were bad"
        );
        assert_eq!(
            verdict_code(&job(Some(&json!({"status": "unsupported"})), None)),
            EXIT_USAGE
        );
        assert_eq!(
            verdict_code(&job(Some(&json!({"status": "invalid_spec"})), None)),
            EXIT_USAGE
        );
        assert_eq!(
            verdict_code(&job(Some(&json!({"status": "cancelled"})), None)),
            EXIT_INFRA
        );
        assert_eq!(
            verdict_code(&job(
                None,
                Some(&json!({"code": "runner_failed", "retryable": false}))
            )),
            EXIT_INFRA
        );
        assert_eq!(
            verdict_code(&job(
                None,
                Some(&json!({"code": "sandbox_not_configured", "retryable": false}))
            )),
            EXIT_USAGE
        );
    }

    #[test]
    fn the_url_falls_back_to_the_environment_and_trails_no_slash() {
        assert_eq!(
            resolve_url(None, Some("http://127.0.0.1:8687/".to_owned())).unwrap(),
            "http://127.0.0.1:8687"
        );
        assert!(matches!(
            resolve_url(None, None).unwrap_err(),
            ClientError::Usage(_)
        ));
        assert!(matches!(
            resolve_url(Some("ftp://x"), None).unwrap_err(),
            ClientError::Usage(_)
        ));
    }

    #[test]
    fn the_token_prefers_the_flag_then_git_config_and_ignores_blanks() {
        assert_eq!(
            resolve_token(Some(" flag ".to_owned()), Some("git".to_owned())).as_deref(),
            Some("flag")
        );
        assert_eq!(
            resolve_token(None, Some("git".to_owned())).as_deref(),
            Some("git")
        );
        assert_eq!(
            resolve_token(Some("   ".to_owned()), Some("git".to_owned())).as_deref(),
            Some("git"),
            "a blank flag must not mask a configured token"
        );
        assert_eq!(resolve_token(None, Some("  ".to_owned())), None);
        assert_eq!(resolve_token(None, None), None);
    }

    #[test]
    fn the_authorization_header_is_only_added_when_a_token_exists() {
        let client = reqwest::Client::new();
        let with = authorize(client.get("http://127.0.0.1:1/health"), Some("t"))
            .build()
            .unwrap();
        assert_eq!(
            with.headers().get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer t"
        );
        let without = authorize(client.get("http://127.0.0.1:1/health"), None)
            .build()
            .unwrap();
        assert!(
            without
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .is_none()
        );
    }
}
