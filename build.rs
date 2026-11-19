//! Stamp the build with the commit it was built from.
//!
//! `coding` is installed on a developer machine as a copy of one build (`~/.local/bin/coding`),
//! and nothing ties that copy to `origin/main`. Measured 2026-11-19: a client built from before
//! #17 (which added the transient-5xx retry) failed a push open on a 500 that the current client
//! rides out — and `coding --version` printed `0.1.0` for both, so the staleness was invisible.
//!
//! `CODING_PKG_VERSION` alone cannot answer "which build is this?", so the commit goes into the
//! binary. `unknown` is a first-class answer: a build from a source archive has no `.git`, and
//! inventing a SHA there would be worse than admitting it.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // A checkout moves between commits without touching this file.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| !output.stdout.is_empty());

    println!(
        "cargo:rustc-env=CODING_BUILD_STAMP={sha}{}",
        if dirty { "-dirty" } else { "" }
    );
}
