//! A throwaway Postgres for one job, started inside the Pod.
//!
//! The estate's test suites fail **closed** without `TEST_DATABASE_URL` — `coding`'s own
//! `tests/db.rs` panics with "no `TEST_DATABASE_URL/DATABASE_URL` and `docker` cannot run". Inside
//! a kata Pod there is no docker (deliberately) and no database, so any repository whose tests
//! touch a database reported a failing `cargo test` for a reason that had nothing to do with its
//! code. This module removes that excuse the same way `rbuild` does on the build host and the CI
//! archetype does in dind: a throwaway cluster, trust auth, `fsync=off`, thrown away with the Pod.
//!
//! The binaries come from the estate's own `library/postgres:18` image (copied into the runner
//! image at build time), so the version matches CI and no package repository is needed at runtime.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Command;
use tokio::time::Instant;

/// The port the cluster listens on. Inside the Pod's own network namespace 5432 cannot collide
/// with anything, and the URL matches what CI and rbuild hand to the same test harnesses.
pub const PORT: u16 = 5432;
/// How long `initdb` + `pg_ctl start` may take before the job fails as a setup error.
const STARTUP_BUDGET: Duration = Duration::from_secs(60);
/// Where the image puts the Postgres 18 binaries.
const BIN_DIR: &str = "/usr/lib/postgresql/18/bin";

/// Whether this host can start a cluster at all.
///
/// The shipped runner image always can — its build asserts `initdb` exists — but `cargo test` on
/// the control-plane image and on a developer's Mac cannot, and those tests exercise the plans
/// themselves. A host without the binaries runs the plan without a database, exactly as it did
/// before this module existed, and says so.
#[must_use]
pub fn available() -> bool {
    std::path::Path::new(BIN_DIR).join("initdb").is_file()
}

/// A database the environment already provides, which this process must **use instead of
/// starting a second one**.
///
/// `TEST_DATABASE_URL` is the estate's harness convention (CI exports it, `rbuild` exports it, and
/// this runner exports it to the step that runs tests). Honouring it is what makes the runner
/// composable: a repository whose own tests call the runner — `coding`'s `tests/runner.rs` does —
/// would otherwise start a *second* Postgres on port 5432 with the **same data directory**
/// (`/tmp/pgdata`), and its drop would stop the job's cluster. Measured live 2026-11-15: the job's
/// postmaster log read `received immediate shutdown request` 163 s into the run, and the failed
/// test that followed was the repository's own.
///
/// `DATABASE_URL` is deliberately **not** consulted: in a deployment it names the service's own
/// database, and a repository's tests must never be pointed at that.
#[must_use]
pub fn exported_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// A running cluster, stopped when this value is dropped.
///
/// ⚠️ **A handle created by [`start`] owns the cluster its `data_dir` names, and its `Drop` stops
/// it.** A hand-made value is a view, not an owner ([`ThrowawayPostgres::owned`]) — but never
/// build one that names the job's directory (`/tmp/pgdata`) unless you mean to observe it:
/// the first version of a unit test here did exactly that and used to stop the job's own cluster
/// two minutes into every check job (measured 2026-11-15), which read as a broken repository.
pub struct ThrowawayPostgres {
    data_dir: PathBuf,
    log_path: PathBuf,
    /// Whether **this** handle started the cluster. Only an owned handle stops it on drop; a
    /// hand-made value is a view of a directory, not an owner of the server in it.
    owned: bool,
}

impl ThrowawayPostgres {
    /// The DSN a test harness should use. A constant so that reading it never requires owning a
    /// cluster — see the warning on the struct.
    pub const DSN: &'static str = "postgres://postgres:postgres@127.0.0.1:5432/postgres";

    /// The DSN a test harness should use.
    #[must_use]
    pub fn url(&self) -> String {
        Self::DSN.to_owned()
    }

    /// Stop the cluster, keeping the log so a caller can report why it died.
    pub fn stop(&self) {
        let _ = std::process::Command::new(format!("{BIN_DIR}/pg_ctl"))
            .arg("-D")
            .arg(&self.data_dir)
            .args(["-m", "immediate", "stop"])
            .output();
    }

    /// The last lines of the cluster log, for an error message.
    #[must_use]
    pub fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        tail(&text, 2048).to_owned()
    }

    /// The pid the postmaster recorded, when it recorded one. `postmaster.pid` carries it on the
    /// first line. Diagnostic only: whether a test can connect is decided by the socket, never by
    /// this number.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        let text = std::fs::read_to_string(self.data_dir.join("postmaster.pid")).ok()?;
        text.lines().next()?.trim().parse().ok()
    }

    /// The postmaster's peak resident size in kB, from `/proc`. `None` where `/proc` is absent or
    /// the process is gone — the absence of a counter is not a zero.
    #[must_use]
    pub fn rss_peak_kb(&self) -> Option<u64> {
        let pid = self.pid()?;
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        status.lines().find_map(|line| {
            let value = line.strip_prefix("VmHWM:")?.trim();
            value.trim_end_matches(" kB").trim().parse().ok()
        })
    }
}

/// The cgroup's memory counters as one line.
///
/// This is what tells a killed postmaster apart from one that exited on its own: a non-zero
/// `oom_kill` means the kernel took it. Read from the Pod's cgroup v2 root; a host without those
/// files answers `unknown`, which is a hole in the evidence and is printed as one.
#[must_use]
pub fn cgroup_memory() -> String {
    let oom_kill = read_counter("/sys/fs/cgroup/memory.events", "oom_kill ")
        .unwrap_or_else(|| "unknown".to_owned());
    let peak = std::fs::read_to_string("/sys/fs/cgroup/memory.peak")
        .ok()
        .map_or_else(|| "unknown".to_owned(), |text| text.trim().to_owned());
    format!("cgroup_oom_kill={oom_kill} cgroup_peak={peak}")
}

/// The last `keep` bytes of `text`, moved forward to a character boundary.
///
/// A byte slice at `len - keep` panics when a multi-byte character straddles the cut, and the
/// postmaster log carries server messages verbatim (`FATAL: database "café" …`). This log is read
/// on the happy path — the end-of-job diagnostics — so a panic here would take the runner down
/// *before* it writes its one result line, which the worker would then read as a successful job.
fn tail(text: &str, keep: usize) -> &str {
    let mut start = text.len().saturating_sub(keep);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

/// A `key value` counter from a file, when both the file and the key exist.
fn read_counter(path: &str, prefix: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines().find_map(|line| {
        line.strip_prefix(prefix)
            .map(|value| value.trim().to_owned())
    })
}

impl Drop for ThrowawayPostgres {
    fn drop(&mut self) {
        // Only the handle that started the cluster stops it. A hand-made value is a *view* of a
        // directory, and stopping what it points at is exactly how a unit test killed the job's
        // own cluster (measured 2026-11-15).
        if self.owned {
            self.stop();
        }
    }
}

/// Start a cluster under `scratch` and wait until it accepts connections.
///
/// # Errors
///
/// A `String` naming the failure when the data directory cannot be cleared or created,
/// `initdb` fails or exceeds its 60s limit, `pg_ctl` refuses to start the cluster, or the
/// socket never accepts a connection.
pub async fn start(scratch: &Path) -> Result<ThrowawayPostgres, String> {
    let data_dir = scratch.join("pgdata");
    let log_path = scratch.join("postgres.log");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir)
            .map_err(|err| format!("cannot clear {:?}: {err}", data_dir.display()))?;
    }

    let initdb = Command::new(format!("{BIN_DIR}/initdb"))
        .args(["-D"])
        .arg(&data_dir)
        .args(["-A", "trust", "-U", "postgres", "--no-locale", "-E", "UTF8"])
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(STARTUP_BUDGET, initdb)
        .await
        .map_err(|_| "initdb did not finish within 60s".to_owned())?
        .map_err(|err| format!("initdb could not start: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail: String = stderr
            .chars()
            .rev()
            .take(800)
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        return Err(format!("initdb failed ({}): {tail}", output.status));
    }

    // The flags mirror rbuild and CI: no fsync (a crash discards the cluster anyway), no locale,
    // a connection count the estate's harnesses need, and no parallel-query shared memory — a
    // kata guest's /dev/shm is small and dynamic shared memory is the first thing to fail there.
    //
    // `unix_socket_directories` MUST point inside the data dir: this image's Postgres defaults to
    // Debian's `/var/run/postgresql`, and the Pod's root filesystem is read-only, so the server
    // dies with `could not create lock file … Read-only file system` after initdb succeeds.
    let settings = format!(
        "-p {PORT} -c listen_addresses=127.0.0.1 -c unix_socket_directories={} -c fsync=off \
         -c synchronous_commit=off -c max_connections=300 -c max_parallel_workers_per_gather=0",
        data_dir.display()
    );
    let start = Command::new(format!("{BIN_DIR}/pg_ctl"))
        .arg("-D")
        .arg(&data_dir)
        .args(["-o", &settings, "-l"])
        .arg(&log_path)
        .args(["start", "-w", "-t", "60"])
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|err| format!("pg_ctl could not start: {err}"))?;
    if !start.status.success() {
        let text = std::fs::read_to_string(&log_path).unwrap_or_default();
        return Err(format!(
            "pg_ctl start failed ({}): {}",
            start.status,
            tail(&text, 800).trim()
        ));
    }

    let postgres = ThrowawayPostgres {
        data_dir,
        log_path,
        owned: true,
    };
    if let Err(error) = wait_for_socket().await {
        return Err(format!("{error}\n{}", postgres.log_tail()));
    }
    Ok(postgres)
}

/// Whether the cluster is accepting connections right now. Used just before a plan runs, so a
/// "Connection refused" from a test can be told apart from a server that stopped.
pub async fn accepting_connections() -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", PORT))
        .await
        .is_ok()
}

/// Wait until the port accepts a TCP connection — `pg_ctl -w` returns before the socket answers
/// in some kata guests, and a harness that starts a fraction early reports a connection error.
async fn wait_for_socket() -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match tokio::net::TcpStream::connect(("127.0.0.1", PORT)).await {
            Ok(_) => return Ok(()),
            Err(err) if Instant::now() >= deadline => {
                return Err(format!(
                    "the throwaway Postgres never accepted a connection on port {PORT}: {err}"
                ));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_url_matches_what_ci_hands_to_the_same_harnesses() {
        // The constant, not a `ThrowawayPostgres`: owning a handle gives it a `Drop` that stops
        // whatever cluster its `data_dir` names, and this test used to name the job's own
        // `/tmp/pgdata`. That killed the cluster two minutes into every check job (2026-11-15).
        assert_eq!(
            ThrowawayPostgres::DSN,
            "postgres://postgres:postgres@127.0.0.1:5432/postgres"
        );
    }

    #[test]
    fn the_pid_comes_from_the_postmaster_file_and_nowhere_else() {
        // `postmaster.pid` is a documented four-line format; line 1 is the pid. The diagnostic
        // block prints it, so parsing it wrong would mislead the one reader who needs the truth.
        let dir = std::env::temp_dir().join(format!("coding-pg-pid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let postgres = ThrowawayPostgres {
            data_dir: dir.clone(),
            log_path: dir.join("postgres.log"),
            owned: false,
        };
        assert_eq!(postgres.pid(), None, "no file means no guess");
        std::fs::write(
            dir.join("postmaster.pid"),
            // A pid far above `/proc/sys/kernel/pid_max`, so the RSS probe below cannot read a
            // stranger's status file when the build host happens to own that pid.
            "999999999\n/tmp/pgdata\n1700000000\n5432\n/tmp/pgdata\n",
        )
        .unwrap();
        assert_eq!(postgres.pid(), Some(999_999_999));
        assert_eq!(postgres.rss_peak_kb(), None, "no such process on this host");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_log_tail_never_slices_through_a_character() {
        // A cut in the middle of a multi-byte character panics, and this tail is read after every
        // job that started a database: the panic would land before the result line is written,
        // and the worker would read the missing line as a successful job.
        let text = "xéé"; // five bytes: 'x' then two two-byte characters
        assert_eq!(tail(text, 5), "xéé", "a short text is returned whole");
        assert_eq!(tail(text, 4), "éé", "a cut on a boundary is exact");
        // `len - 3 = 2`, the second byte of the first é: step forward, never panic.
        assert_eq!(
            tail(text, 3),
            "é",
            "a cut inside a character moves to the next boundary"
        );
        assert_eq!(tail(text, 0), "", "a zero keep is empty, not a panic");
    }

    #[tokio::test]
    async fn a_start_without_the_binaries_is_a_clear_error_not_a_panic() {
        // The test binary runs on the build host, which has no /usr/lib/postgresql/18 unless the
        // image does; either way this must come back as an Err with a message, never a panic.
        let scratch = std::env::temp_dir().join(format!("coding-pg-{}", std::process::id()));
        std::fs::create_dir_all(&scratch).unwrap();
        match start(&scratch).await {
            Ok(postgres) => {
                // If the host happens to have PG 18, the cluster really started: prove it answers.
                assert!(std::net::TcpStream::connect(("127.0.0.1", PORT)).is_ok());
                postgres.stop();
            }
            Err(message) => assert!(
                message.contains("initdb") || message.contains("pg_ctl"),
                "{message}"
            ),
        }
        let _ = std::fs::remove_dir_all(&scratch);
    }
}
