//! A throwaway Redis for one job, started inside the Pod.
//!
//! The estate's harnesses fail **closed** without `TEST_REDIS_URL` / `REDIS_URL` — conveyor's bus
//! tests panic with "no TEST_REDIS_URL/REDIS_URL and `docker` cannot run", and many services read
//! the same pair. Inside a kata Pod there is no docker (deliberately) and no Redis, so any
//! repository whose tests need one reported a failing check for a reason that had nothing to do
//! with its code. This module removes that excuse the same way [`postgres`](super::postgres) does:
//! a server with no persistence, thrown away with the Pod.
//!
//! The binary is installed in the runner image (`redis-server` from the image's own distribution),
//! so no package repository is needed at runtime.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::time::Instant;

/// The port the shared test Redis listens on. Inside the Pod's own network namespace 6379 cannot
/// collide with anything, and the URL matches what CI and `rbuild` hand to the same harnesses.
pub const PORT: u16 = 6379;
/// A **second** server, for a test that must wipe Redis without touching its siblings.
///
/// `FLUSHALL` is server-wide, not database-scoped: conveyor's `bus_delivery` flush test therefore
/// refuses a shared URL and starts a private server of its own. In a kata Pod it cannot start a
/// container, so the runner provides this one and exports `TEST_REDIS_PRIVATE_URL`.
pub const PRIVATE_PORT: u16 = 6380;
/// The URL the estate's harnesses expect. A constant so reading it never requires owning a server.
pub const DSN: &str = "redis://127.0.0.1:6379/";
/// The URL of the private server (see [`PRIVATE_PORT`]).
pub const PRIVATE_DSN: &str = "redis://127.0.0.1:6380/";
/// How long `redis-server` may take to accept a connection before the job fails as a setup error.
const STARTUP_BUDGET: Duration = Duration::from_secs(30);

/// Whether this host can start a Redis at all.
///
/// The shipped runner image always can — its build asserts `redis-server --version` — but the
/// control-plane image and a developer's Mac may not, and those run the plan without one (the
/// `exec` module says so instead of failing).
pub fn available() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| dir.join("redis-server").is_file())
}

/// A Redis the environment already provides, which this process must **use instead of starting a
/// second one**.
///
/// `TEST_REDIS_URL` is the estate's harness convention; `REDIS_URL` is the CI spelling. A
/// repository whose own tests call the runner would otherwise start a second server on port 6379
/// and its drop would stop the job's.
pub fn exported_url() -> Option<String> {
    ["TEST_REDIS_URL", "REDIS_URL"].iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

/// A private Redis the environment already provides (`TEST_REDIS_PRIVATE_URL`). The runner exports
/// it from its second server; a caller-provided one wins and nothing is started beside it.
pub fn exported_private_url() -> Option<String> {
    std::env::var("TEST_REDIS_PRIVATE_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// The TCP port a `redis://host:port[/db]` URL names.
///
/// The CI hands the runner a dind Redis on a random host port (`redis://127.0.0.1:32768`), so a
/// liveness probe that always dials [`PORT`] asks about a different server than the step used.
pub fn port_of(url: &str) -> Option<u16> {
    let rest = url.strip_prefix("redis://").unwrap_or(url);
    let authority = rest.split('/').next()?;
    authority.rsplit_once(':')?.1.parse().ok()
}

/// A running Redis, stopped when this value is dropped.
pub struct ThrowawayRedis {
    port: u16,
    log_path: PathBuf,
    child: Child,
    /// Whether **this** handle started the server. Only an owned handle stops it on drop.
    owned: bool,
}

impl ThrowawayRedis {
    /// The URL a test harness should use.
    pub fn url(&self) -> String {
        if self.port == PRIVATE_PORT {
            PRIVATE_DSN.to_owned()
        } else {
            DSN.to_owned()
        }
    }

    /// The child's pid, for diagnostics only.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Stop the server, keeping the log so a caller can report why it died.
    pub fn stop(&mut self) {
        let _ = self.child.start_kill();
    }

    /// The last bytes of the server log, for an error message.
    pub fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        tail(&text, 2048).to_owned()
    }

    /// The server's peak resident size in kB, from `/proc`. `None` where `/proc` is absent or the
    /// process is gone.
    pub fn rss_peak_kb(&self) -> Option<u64> {
        let pid = self.pid()?;
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        status.lines().find_map(|line| {
            let value = line.strip_prefix("VmHWM:")?.trim();
            value.trim_end_matches(" kB").trim().parse().ok()
        })
    }
}

impl Drop for ThrowawayRedis {
    fn drop(&mut self) {
        // Only the handle that started the server stops it. A hand-made value is a view.
        if self.owned {
            self.stop();
        }
    }
}

/// Start a Redis on `port` under `scratch` and wait until it accepts connections.
pub async fn start(scratch: &Path, port: u16) -> Result<ThrowawayRedis, String> {
    let dir = scratch.join(format!("redis-{port}"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .map_err(|err| format!("cannot clear {:?}: {err}", dir.display()))?;
    }
    std::fs::create_dir_all(&dir)
        .map_err(|err| format!("cannot create {:?}: {err}", dir.display()))?;
    let log_path = scratch.join(format!("redis-{port}.log"));

    // No persistence: a crash discards the data anyway, and a dump file would only take the
    // writable scratch. `--daemonize no` keeps the child, so `kill_on_drop`/`start_kill` owns it.
    let child = Command::new("redis-server")
        .arg("--port")
        .arg(port.to_string())
        .args(["--bind", "127.0.0.1"])
        .args(["--save", ""])
        .args(["--appendonly", "no"])
        .args(["--daemonize", "no"])
        .arg("--dir")
        .arg(&dir)
        .arg("--logfile")
        .arg(&log_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| format!("redis-server could not start: {err}"))?;

    let redis = ThrowawayRedis {
        port,
        log_path,
        child,
        owned: true,
    };
    if let Err(error) = wait_for_socket(port).await {
        return Err(format!(
            "redis-server did not accept a connection: {error}\n{}",
            redis.log_tail()
        ));
    }
    Ok(redis)
}

/// Whether the server on `port` is accepting connections right now. Used just before a plan runs,
/// so a "Connection refused" from a test can be told apart from a server that stopped.
pub async fn accepting_connections(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_ok()
}

/// Wait until the port accepts a TCP connection.
async fn wait_for_socket(port: u16) -> Result<(), String> {
    let deadline = Instant::now() + STARTUP_BUDGET;
    loop {
        match tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            Ok(_) => return Ok(()),
            Err(err) if Instant::now() >= deadline => {
                return Err(format!(
                    "the throwaway Redis never accepted a connection on port {port}: {err}"
                ));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// The last `keep` bytes of `text`, moved forward to a character boundary.
///
/// A byte slice at `len - keep` panics when a multi-byte character straddles the cut; this tail is
/// read on the happy path, before the runner writes its one result line.
fn tail(text: &str, keep: usize) -> &str {
    let mut start = text.len().saturating_sub(keep);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_urls_match_what_ci_hands_to_the_same_harnesses() {
        // The constants, never a handle: a handle's Drop stops whatever server its dir names.
        assert_eq!(DSN, "redis://127.0.0.1:6379/");
        assert_eq!(PRIVATE_DSN, "redis://127.0.0.1:6380/");
    }

    #[test]
    fn the_url_port_is_read_from_the_url() {
        assert_eq!(port_of(DSN), Some(6379));
        assert_eq!(port_of(PRIVATE_DSN), Some(6380));
        assert_eq!(port_of("redis://127.0.0.1:32768"), Some(32768));
        assert_eq!(port_of("redis://127.0.0.1:32768/2"), Some(32768));
        assert_eq!(port_of("redis://localhost"), None, "no port, no guess");
    }

    #[test]
    fn a_tail_never_slices_through_a_character() {
        let text = "xéé";
        assert_eq!(tail(text, 5), "xéé");
        assert_eq!(tail(text, 4), "éé");
        assert_eq!(tail(text, 3), "é");
        assert_eq!(tail(text, 0), "");
    }

    #[tokio::test]
    async fn a_start_that_cannot_bind_is_a_clear_error_not_a_panic() {
        let scratch = std::env::temp_dir().join(format!("coding-redis-{}", std::process::id()));
        std::fs::create_dir_all(&scratch).unwrap();
        match start(&scratch, PORT).await {
            Ok(mut redis) => {
                assert!(std::net::TcpStream::connect(("127.0.0.1", PORT)).is_ok());
                redis.stop();
            }
            Err(message) => assert!(
                message.contains("redis-server"),
                "the error names the binary: {message}"
            ),
        }
        let _ = std::fs::remove_dir_all(&scratch);
    }
}
