//! `coding hook install` / `coding hook uninstall`: a git hook that runs the gate.
//!
//! Two rules shape the generated hook:
//!
//! * **It fails open.** Only exit code 1 — a check that failed — blocks the commit or push. An
//!   unreachable app, a missing binary or an infrastructure error lets the developer through with
//!   a warning. A gate that blocks work when the cluster is down gets removed within a day.
//! * **Its configuration is git config, not the file.** The hook body reads `coding.url` and
//!   `coding.tenant`, so the committed-ish file carries no endpoint and no credential, and one
//!   `git config` change re-points every repository.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::domain::JobKind;

/// The first line of every hook this tool writes. Uninstall refuses to remove a hook without it.
pub const MARKER: &str = "# coding-check-hook v1";

/// Which git hook to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookKind {
    PrePush,
    PreCommit,
}

impl HookKind {
    pub fn file_name(self) -> &'static str {
        match self {
            Self::PrePush => "pre-push",
            Self::PreCommit => "pre-commit",
        }
    }
}

/// Install one hook in the repository containing `dir`.
///
/// `kind` is the job kind the hook runs: `check` for pre-push (fmt + clippy + test) and `lint`
/// for pre-commit (fmt + clippy), because a commit-time gate that waits for a cold test build is
/// a gate people disable.
pub fn install(dir: &Path, hook: HookKind, kind: JobKind, force: bool) -> Result<PathBuf, String> {
    let path = hook_path(dir, hook)?;
    if path.exists() && !force {
        let existing = std::fs::read_to_string(&path).unwrap_or_default();
        if !existing.contains(MARKER) {
            return Err(format!(
                "{} already exists and was not installed by coding; pass --force to replace it",
                path.display()
            ));
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| format!("cannot create {}: {err}", parent.display()))?;
    }
    std::fs::write(&path, body(hook, kind))
        .map_err(|err| format!("cannot write {}: {err}", path.display()))?;
    set_executable(&path)?;
    Ok(path)
}

/// Remove a hook this tool installed. A foreign hook is never touched.
pub fn uninstall(dir: &Path, hook: HookKind) -> Result<PathBuf, String> {
    let path = hook_path(dir, hook)?;
    if !path.exists() {
        return Ok(path);
    }
    let existing = std::fs::read_to_string(&path)
        .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    if !existing.contains(MARKER) {
        return Err(format!(
            "{} was not installed by coding; refusing to remove it",
            path.display()
        ));
    }
    std::fs::remove_file(&path)
        .map_err(|err| format!("cannot remove {}: {err}", path.display()))?;
    Ok(path)
}

/// The hook file's path, honouring `core.hooksPath`.
fn hook_path(dir: &Path, hook: HookKind) -> Result<PathBuf, String> {
    let root = git(dir, &["rev-parse", "--show-toplevel"])
        .map_err(|err| format!("{} is not a git repository: {err}", dir.display()))?;
    let root = PathBuf::from(root);
    let relative = git(&root, &["rev-parse", "--git-path", "hooks"])
        .map_err(|err| format!("cannot locate the hooks directory: {err}"))?;
    let hooks = PathBuf::from(relative);
    let hooks = if hooks.is_absolute() {
        hooks
    } else {
        root.join(hooks)
    };
    Ok(hooks.join(hook.file_name()))
}

fn git(dir: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|err| format!("git could not start: {err}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The script, as written.
fn body(hook: HookKind, kind: JobKind) -> String {
    // A pre-push hook is handed the ref list on stdin; draining it first is the standard shape
    // and keeps git from ever waiting on a pipe nobody reads.
    let drain = if hook == HookKind::PrePush {
        "cat >/dev/null 2>&1 || true\n"
    } else {
        ""
    };
    format!(
        r#"#!/bin/sh
{MARKER} — installed by `coding hook install`, removed by `coding hook uninstall`.
# Fails OPEN: only a failed check blocks this {what}; an unreachable app does not.
{drain}if ! command -v coding >/dev/null 2>&1; then
    echo "coding: not on PATH; skipping the quality gate" >&2
    exit 0
fi
url=$(git config --get coding.url 2>/dev/null || true)
tenant=$(git config --get coding.tenant 2>/dev/null || true)
set -- check --kind {kind}
if [ -n "$url" ]; then set -- "$@" --url "$url"; fi
if [ -n "$tenant" ]; then set -- "$@" --tenant "$tenant"; fi
coding "$@"
code=$?
if [ "$code" -eq 1 ]; then
    exit 1
fi
if [ "$code" -ne 0 ]; then
    echo "coding: the gate could not run (exit $code); letting the {what} through" >&2
fi
exit 0
"#,
        what = if hook == HookKind::PrePush {
            "push"
        } else {
            "commit"
        },
        kind = kind.as_str(),
        drain = drain,
    )
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .map_err(|err| format!("cannot make {} executable: {err}", path.display()))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_repo(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "coding-hook-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let status = Command::new("git")
            .arg("-C")
            .arg(&dir)
            .args(["init", "-q"])
            .status()
            .expect("git init");
        assert!(status.success());
        dir
    }

    #[test]
    fn install_then_uninstall_leaves_nothing_behind() {
        let dir = scratch_repo("roundtrip");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains(MARKER));
        assert!(written.contains("--kind check"), "{written}");
        assert!(
            written.contains("coding.url"),
            "config comes from git config"
        );
        assert!(
            !written.contains("http://"),
            "the hook must not carry an endpoint: {written}"
        );

        uninstall(&dir, HookKind::PrePush).expect("uninstall");
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_pre_commit_variant_uses_the_fast_kind() {
        let dir = scratch_repo("precommit");
        let path = install(&dir, HookKind::PreCommit, JobKind::Lint, false).expect("install");
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("--kind lint"), "{written}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_foreign_hook_is_never_clobbered_or_removed() {
        let dir = scratch_repo("foreign");
        let path = hook_path(&dir, HookKind::PrePush).expect("path");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "#!/bin/sh\necho mine\n").unwrap();

        let error = install(&dir, HookKind::PrePush, JobKind::Check, false).expect_err("refuse");
        assert!(error.contains("--force"), "{error}");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "#!/bin/sh\necho mine\n"
        );

        let error = uninstall(&dir, HookKind::PrePush).expect_err("refuse");
        assert!(error.contains("not installed by coding"), "{error}");
        assert!(path.exists());

        // --force replaces only when the caller says so.
        install(&dir, HookKind::PrePush, JobKind::Check, true).expect("force");
        assert!(std::fs::read_to_string(&path).unwrap().contains(MARKER));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
