//! `coding hook install` / `coding hook uninstall`: a git hook that runs the gate.
//!
//! Two rules shape the generated hook:
//!
//! * **It fails open.** Only exit code 1 — a check that failed — blocks the commit or push. An
//!   unreachable app, a missing binary or an infrastructure error lets the developer through with
//!   a warning. A gate that blocks work when the cluster is down gets removed within a day.
//! * **Its configuration is git config, not the file.** The hook body reads `coding.url`,
//!   `coding.tenant` and (via the client) `coding.token`, so the file carries no endpoint and no
//!   credential, and one `git config` change re-points every repository.
//!
//! The snapshots are the point of the two hooks: a pre-commit checks the **index** (what the
//! commit would record), a pre-push checks **HEAD** (what the push would send), and a push runs
//! the build and test gates as two separate requests so a build failure is not reported as a test
//! failure.

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
    #[must_use]
    pub fn file_name(self) -> &'static str {
        match self {
            Self::PrePush => "pre-push",
            Self::PreCommit => "pre-commit",
        }
    }
}

/// Install one hook in the repository containing `dir`.
///
/// A pre-commit hook runs `kind` (default `lint`) against the **index**. A pre-push hook ignores
/// `kind` and runs `build` then `test` against **HEAD**, as two requests.
///
/// # Errors
///
/// A `String` naming the failure when `dir` is not inside a git repository, when a hook this
/// tool did not install already exists and `force` is false, or when the hook file cannot be
/// created, written or made executable.
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
///
/// # Errors
///
/// A `String` naming the failure when `dir` is not inside a git repository, when the existing
/// hook was not installed by this tool, or when it cannot be read or removed.
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
    match hook {
        HookKind::PreCommit => format!(
            r#"#!/bin/sh
{MARKER} — installed by `coding hook install`, removed by `coding hook uninstall`.
# Fails OPEN: only a failed check blocks this commit; an unreachable app does not.
# Checks the INDEX — exactly what this commit would record.
if ! command -v coding >/dev/null 2>&1; then
    echo "coding: not on PATH; skipping the quality gate" >&2
    exit 0
fi
url=$(git config --get coding.url 2>/dev/null || true)
tenant=$(git config --get coding.tenant 2>/dev/null || true)
set -- check --kind {kind} --snapshot index
if [ -n "$url" ]; then set -- "$@" --url "$url"; fi
if [ -n "$tenant" ]; then set -- "$@" --tenant "$tenant"; fi
coding "$@"
code=$?
if [ "$code" -eq 1 ]; then
    exit 1
fi
if [ "$code" -ne 0 ]; then
    echo "coding: the gate could not run (exit $code); letting the commit through" >&2
fi
exit 0
"#,
            kind = kind.as_str(),
        ),
        HookKind::PrePush => r#"#!/bin/sh
{MARKER} — installed by `coding hook install`, removed by `coding hook uninstall`.
# Fails OPEN: only a failed check blocks this push; an unreachable app does not.
# Checks HEAD — exactly what this push sends — and runs build and test as TWO gates.
# A build failure blocks and reports the test gate as not run.
cat >/dev/null 2>&1 || true
if ! command -v coding >/dev/null 2>&1; then
    echo "coding: not on PATH; skipping the quality gate" >&2
    exit 0
fi
url=$(git config --get coding.url 2>/dev/null || true)
tenant=$(git config --get coding.tenant 2>/dev/null || true)
run_gate() {
    kind=$1
    set -- check --kind "$kind" --snapshot head
    if [ -n "$url" ]; then set -- "$@" --url "$url"; fi
    if [ -n "$tenant" ]; then set -- "$@" --tenant "$tenant"; fi
    coding "$@"
    code=$?
    if [ "$code" -eq 1 ]; then
        return 1
    fi
    if [ "$code" -ne 0 ]; then
        echo "coding: the $kind gate could not run (exit $code); letting the push through" >&2
    fi
    return 0
}
if ! run_gate build; then
    echo "coding: the build gate failed; the test gate did not run" >&2
    exit 1
fi
if ! run_gate test; then
    exit 1
fi
exit 0
"#
        .replace("{MARKER}", MARKER),
    }
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

    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .expect("git");
        assert!(status.success(), "git {args:?}");
    }

    #[test]
    fn install_then_uninstall_leaves_nothing_behind() {
        let dir = scratch_repo("roundtrip");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains(MARKER));
        assert!(written.contains("run_gate build"), "{written}");
        assert!(written.contains("run_gate test"), "{written}");
        assert!(written.contains("--snapshot head"), "{written}");
        assert!(
            written.contains("coding.url"),
            "config comes from git config"
        );
        assert!(
            !written.contains("http://"),
            "the hook must not carry an endpoint: {written}"
        );
        assert!(
            !written.contains("coding.token"),
            "the hook must not carry a credential: {written}"
        );

        uninstall(&dir, HookKind::PrePush).expect("uninstall");
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_pre_commit_variant_uses_the_fast_kind_on_the_index() {
        let dir = scratch_repo("precommit");
        let path = install(&dir, HookKind::PreCommit, JobKind::Lint, false).expect("install");
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("--kind lint"), "{written}");
        assert!(written.contains("--snapshot index"), "{written}");
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

    #[cfg(unix)]
    fn stub_coding(name: &str) -> (PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "coding-stub-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("log");
        let stub = dir.join("coding");
        std::fs::write(
            &stub,
            r#"#!/bin/sh
echo "$*" >> "$STUB_LOG"
case "$*" in
  *"--kind build"*) exit "${STUB_BUILD_EXIT:-0}" ;;
  *"--kind test"*) exit "${STUB_TEST_EXIT:-0}" ;;
  *) exit "${STUB_EXIT:-0}" ;;
esac
"#,
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        (dir, log)
    }

    #[cfg(unix)]
    fn run_hook(
        hook: &Path,
        repo: &Path,
        stub_dir: &Path,
        log: &Path,
        envs: &[(&str, &str)],
        stdin: &str,
    ) -> std::process::Output {
        use std::io::Write as _;
        use std::process::Stdio;
        let mut command = Command::new("sh");
        command.arg(hook).current_dir(repo);
        command
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", stub_dir.display()),
            )
            .env("STUB_LOG", log);
        for (key, value) in envs {
            command.env(key, value);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("hook");
        let mut pipe = child.stdin.take().expect("stdin");
        pipe.write_all(stdin.as_bytes()).expect("write stdin");
        drop(pipe);
        child.wait_with_output().expect("hook output")
    }

    #[cfg(unix)]
    fn logged(log: &Path) -> Vec<String> {
        std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[cfg(unix)]
    #[test]
    fn pre_push_runs_build_then_test_on_head() {
        let dir = scratch_repo("push-order");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        git(
            &dir,
            &["config", "coding.url", "https://example.test/coding"],
        );
        git(&dir, &["config", "coding.tenant", "t1"]);
        git(&dir, &["config", "coding.token", "SEKRIT"]);
        let (stub_dir, log) = stub_coding("push-order");
        let output = run_hook(
            &path,
            &dir,
            &stub_dir,
            &log,
            &[("STUB_BUILD_EXIT", "0"), ("STUB_TEST_EXIT", "0")],
            "refs/heads/main 1234 refs/heads/main 0000\n",
        );
        assert!(output.status.success(), "{output:?}");
        let lines = logged(&log);
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines[0].starts_with("check --kind build --snapshot head"),
            "{lines:?}"
        );
        assert!(
            lines[1].starts_with("check --kind test --snapshot head"),
            "{lines:?}"
        );
        for line in &lines {
            assert!(line.contains("--url https://example.test/coding"), "{line}");
            assert!(line.contains("--tenant t1"), "{line}");
            assert!(
                !line.contains("SEKRIT"),
                "the token must never be on argv: {line}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&stub_dir);
    }

    #[cfg(unix)]
    #[test]
    fn a_build_failure_blocks_and_reports_the_test_as_not_run() {
        let dir = scratch_repo("build-fail");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        let (stub_dir, log) = stub_coding("build-fail");
        let output = run_hook(
            &path,
            &dir,
            &stub_dir,
            &log,
            &[("STUB_BUILD_EXIT", "1"), ("STUB_TEST_EXIT", "0")],
            "",
        );
        assert_eq!(output.status.code(), Some(1));
        let lines = logged(&log);
        assert_eq!(lines.len(), 1, "the test gate must not run: {lines:?}");
        assert!(lines[0].starts_with("check --kind build"), "{lines:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("test gate did not run"), "{stderr}");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&stub_dir);
    }

    #[cfg(unix)]
    #[test]
    fn a_test_failure_blocks_after_both_gates_ran() {
        let dir = scratch_repo("test-fail");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        let (stub_dir, log) = stub_coding("test-fail");
        let output = run_hook(
            &path,
            &dir,
            &stub_dir,
            &log,
            &[("STUB_BUILD_EXIT", "0"), ("STUB_TEST_EXIT", "1")],
            "",
        );
        assert_eq!(output.status.code(), Some(1));
        let lines = logged(&log);
        assert_eq!(lines.len(), 2, "{lines:?}");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&stub_dir);
    }

    #[cfg(unix)]
    #[test]
    fn infrastructure_exit_codes_warn_and_allow() {
        let dir = scratch_repo("infra");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        for code in ["3", "124", "130"] {
            let (stub_dir, log) = stub_coding(&format!("infra-{code}"));
            let output = run_hook(
                &path,
                &dir,
                &stub_dir,
                &log,
                &[("STUB_BUILD_EXIT", "0"), ("STUB_TEST_EXIT", code)],
                "",
            );
            assert!(
                output.status.success(),
                "exit {code} must not block: {output:?}"
            );
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains(&format!("could not run (exit {code})")),
                "{stderr}"
            );
            let _ = std::fs::remove_dir_all(&stub_dir);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn a_missing_binary_warns_and_allows() {
        use std::io::Write as _;
        use std::process::Stdio;
        let dir = scratch_repo("missing");
        let path = install(&dir, HookKind::PrePush, JobKind::Check, false).expect("install");
        let mut child = Command::new("sh")
            .arg(&path)
            .current_dir(&dir)
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("hook");
        let mut pipe = child.stdin.take().expect("stdin");
        pipe.write_all(b"refs\n").expect("stdin");
        drop(pipe);
        let output = child.wait_with_output().expect("hook output");
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("not on PATH"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn pre_commit_maps_exit_1_to_block_and_3_to_warn() {
        let dir = scratch_repo("commit-map");
        let path = install(&dir, HookKind::PreCommit, JobKind::Lint, false).expect("install");
        let (stub_dir, log) = stub_coding("commit-map");
        let blocked = run_hook(&path, &dir, &stub_dir, &log, &[("STUB_EXIT", "1")], "");
        assert_eq!(blocked.status.code(), Some(1));
        let lines = logged(&log);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("check --kind lint --snapshot index"),
            "{lines:?}"
        );
        let allowed = run_hook(&path, &dir, &stub_dir, &log, &[("STUB_EXIT", "3")], "");
        assert!(allowed.status.success());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&stub_dir);
    }

    #[cfg(unix)]
    #[test]
    fn core_hooks_path_is_honoured() {
        let dir = scratch_repo("hookspath");
        git(&dir, &["config", "core.hooksPath", ".githooks"]);
        let path = install(&dir, HookKind::PreCommit, JobKind::Lint, false).expect("install");
        // git reports the canonical root (/private/var on macOS) while the temp dir is /var, so
        // compare the meaningful suffix instead of the whole path.
        assert!(path.ends_with(".githooks/pre-commit"), "{}", path.display());
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
