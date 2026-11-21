//! What the snapshot path costs — timings and peak RSS, on a corpus the size of the estate's
//! biggest tree (942 files, ~16 MiB compressed; see `source_archive::MAX_COMPRESSED_BYTES`).
//!
//! Ignored on purpose: wall-clock numbers on a shared box are evidence, never a gate. Run it in
//! release, which is what a developer's binary is:
//!
//! ```text
//! cargo test --release --test perf -- --ignored --nocapture
//! ```
//!
//! Every file is generated from a seeded PRNG, so two runs (and two machines) pack the same bytes.
//! Each phase runs in a **fresh child process**: the parent samples the child's resident set while
//! it works. That matters — measured in one process, a phase reuses heap freed by the setup and the
//! peak a phase actually reaches is invisible.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use coding::client::pack::{self, Snapshot};
use coding::source_archive;

/// The estate's biggest Rust tree: `conveyor`, 942 tracked files (`source_archive.rs:25-30`).
const FILES: usize = 942;
/// ~55 MiB of source-shaped text in total, which is what 15.76 MiB of gzip comes from.
const FILE_BYTES: usize = 60 * 1024;
/// Enough repetitions that a 20 % difference is not noise, cheap enough to keep the run short.
const ROUNDS: usize = 3;

/// Set in a child: which phase to run. Absent in the parent.
const PHASE_ENV: &str = "CODING_PERF_PHASE";
/// Set in a child: the scratch repository the parent built.
const REPO_ENV: &str = "CODING_PERF_REPO";

/// Resident set of `pid` in MiB, from `ps` — no libc, no unsafe.
fn rss_mib(pid: u32) -> Option<u64> {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kib: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kib / 1024)
}

/// A deterministic, source-shaped file: varied enough that gzip cannot cheat, repetitive
/// enough that it compresses like real code rather than like a random blob.
fn contents(seed: u64, bytes: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut out = Vec::with_capacity(bytes);
    let mut line = 0_u64;
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let body = format!(
            "pub fn item_{line:05}() -> u64 {{ let value_{:04} = {}u64; value_{:04} + {line} }}\n",
            state % 10_000,
            state % 1_000_000,
            state % 10_000,
        );
        out.extend_from_slice(body.as_bytes());
        line += 1;
    }
    out.truncate(bytes);
    out
}

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
}

/// A throwaway repository holding [`FILES`] files of [`FILE_BYTES`] bytes, committed.
fn scratch_repo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("coding-perf-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).expect("scratch dir");
    for index in 0..FILES {
        std::fs::write(
            dir.join("src").join(format!("file_{index:04}.rs")),
            contents(u64::try_from(index).expect("fits"), FILE_BYTES),
        )
        .expect("write");
    }
    git(&dir, &["init", "-q", "-b", "main"]);
    git(&dir, &["add", "-A"]);
    git(
        &dir,
        &[
            "-c",
            "user.email=perf@example.com",
            "-c",
            "user.name=perf",
            "commit",
            "-qm",
            "corpus",
        ],
    );
    dir
}

/// The archive the corpus packs to. Used untimed by the phases that measure unpacking.
fn repack(repo: &Path) -> Vec<u8> {
    let tree = pack::collect(repo, Snapshot::Head).expect("collect");
    pack::pack(&tree).expect("pack")
}

/// Runs one phase in this process, prints `millis=… bytes=…`, and exits. Returns only when this is
/// the parent (no [`PHASE_ENV`]).
fn run_phase_child() {
    let Ok(phase) = std::env::var(PHASE_ENV) else {
        return;
    };
    let repo = PathBuf::from(std::env::var(REPO_ENV).expect("the parent sets the repo"));
    let unpack_dir =
        std::env::temp_dir().join(format!("coding-perf-unpack-{}", std::process::id()));

    let (millis, bytes) = match phase.as_str() {
        "worktree" => {
            let started = Instant::now();
            let tree = pack::collect(&repo, Snapshot::Worktree).expect("collect worktree");
            let blob = pack::pack(&tree).expect("pack");
            (started.elapsed().as_millis(), blob.len())
        }
        "head" => {
            let started = Instant::now();
            let tree = pack::collect(&repo, Snapshot::Head).expect("collect head");
            let blob = pack::pack(&tree).expect("pack");
            (started.elapsed().as_millis(), blob.len())
        }
        "validate" => {
            let blob = repack(&repo);
            let started = Instant::now();
            let manifest = source_archive::validate(&blob).expect("validate");
            assert_eq!(manifest.files, FILES);
            assert_eq!(
                usize::try_from(manifest.bytes).expect("fits"),
                FILES * FILE_BYTES
            );
            (started.elapsed().as_millis(), blob.len())
        }
        "unpack" => {
            let blob = repack(&repo);
            let _ = std::fs::remove_dir_all(&unpack_dir);
            let started = Instant::now();
            source_archive::unpack(&blob, &unpack_dir).expect("unpack");
            let millis = started.elapsed().as_millis();
            let _ = std::fs::remove_dir_all(&unpack_dir);
            (millis, blob.len())
        }
        other => panic!("unknown phase {other}"),
    };
    println!("millis={millis} bytes={bytes}");
    std::process::exit(0);
}

/// One phase, measured in its own process: wall clock from the child, resident peak from the parent.
#[derive(Debug, Clone, Copy)]
struct Cost {
    millis: u128,
    peak_mib: u64,
    bytes: usize,
}

fn run_child(phase: &str, repo: &Path) -> Cost {
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "the_snapshot_path_costs",
            "--ignored",
            "--nocapture",
        ])
        .env(PHASE_ENV, phase)
        .env(REPO_ENV, repo)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the phase");
    let pid = child.id();
    let mut peak_mib = 0;
    loop {
        peak_mib = peak_mib.max(rss_mib(pid).unwrap_or(0));
        match child.try_wait().expect("wait for the phase") {
            Some(_) => break,
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let out = child.wait_with_output().expect("phase output");
    assert!(out.status.success(), "phase {phase} failed");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let field = |key: &str| -> u128 {
        text.split_whitespace()
            .find_map(|word| word.strip_prefix(key)?.parse().ok())
            .unwrap_or_else(|| panic!("phase {phase} printed no {key}: {text:?}"))
    };
    Cost {
        millis: field("millis="),
        peak_mib,
        bytes: usize::try_from(field("bytes=")).expect("fits"),
    }
}

fn median(costs: &[Cost]) -> Cost {
    let mut sorted: Vec<u128> = costs.iter().map(|cost| cost.millis).collect();
    sorted.sort_unstable();
    Cost {
        millis: sorted[sorted.len() / 2],
        peak_mib: costs.iter().map(|cost| cost.peak_mib).max().unwrap_or(0),
        bytes: costs.first().map_or(0, |cost| cost.bytes),
    }
}

#[test]
#[ignore = "measurement, not a gate — run in release with --ignored --nocapture"]
fn the_snapshot_path_costs() {
    run_phase_child();

    let repo = scratch_repo("snapshot");
    let phases = ["worktree", "head", "validate", "unpack"];
    let mut measured: Vec<(&str, Cost)> = Vec::with_capacity(phases.len());
    for phase in phases {
        let costs: Vec<Cost> = (0..ROUNDS).map(|_| run_child(phase, &repo)).collect();
        assert!(
            costs.iter().all(|cost| cost.bytes == costs[0].bytes),
            "{phase}: the same tree must pack to the same bytes"
        );
        measured.push((phase, median(&costs)));
    }
    let blob_len = measured[0].1.bytes;
    let uncompressed_mib = FILES * FILE_BYTES / (1024 * 1024);
    println!("\nsnapshot path: {FILES} files, ~{uncompressed_mib} MiB uncompressed");
    for (phase, cost) in &measured {
        println!(
            "  {:<10} {:>7} ms {:>14} MiB peak RSS",
            phase, cost.millis, cost.peak_mib
        );
    }
    println!(
        "  {:<10} {:>7} B  ({} % of the {}-byte cap)",
        "archive",
        blob_len,
        blob_len * 100 / source_archive::MAX_COMPRESSED_BYTES,
        source_archive::MAX_COMPRESSED_BYTES
    );
    assert!(
        blob_len < source_archive::MAX_COMPRESSED_BYTES,
        "the corpus must fit inside the compressed cap"
    );

    let _ = std::fs::remove_dir_all(&repo);
}
