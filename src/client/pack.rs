//! Turning a working tree into the archive a job runs against.
//!
//! The tree is what `git` says you are about to commit or push: every tracked file, plus every
//! untracked file that is not ignored — read from the working tree, not from the index. Ignored
//! paths (`target/`, `.env`) never travel, and a fixed set of secret-shaped names is skipped even
//! when a repository forgot to ignore it: the app must never receive a credential by accident.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::source_archive::{self, MAX_FILE_BYTES, PackFile};

/// What one collection produced.
#[derive(Debug)]
pub struct Tree {
    /// The repository root the files came from.
    pub root: PathBuf,
    pub files: Vec<PackFile>,
    /// `HEAD`, when the repository has a commit.
    pub head: Option<String>,
    /// Whether the working tree differs from `HEAD`.
    pub dirty: bool,
    /// Secret-shaped paths that were left out, so the caller can say which and why.
    pub skipped_secrets: Vec<String>,
    /// Symlinks and other non-regular entries that were left out.
    pub skipped_irregular: Vec<String>,
}

impl Tree {
    /// The unpressed size of the tree.
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|file| file.data.len() as u64).sum()
    }

    /// Whether a file with this path is in the tree.
    pub fn has(&self, path: &str) -> bool {
        self.files.iter().any(|file| file.path == path)
    }
}

/// Why a tree could not be collected.
#[derive(Debug, thiserror::Error)]
pub enum PackError {
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Io(String),
}

/// Collect the working tree under `dir` (which must be inside a git repository).
pub fn collect(dir: &Path) -> Result<Tree, PackError> {
    let root = git_output(dir, &["rev-parse", "--show-toplevel"])
        .map(PathBuf::from)
        .map_err(|err| {
            PackError::Usage(format!("{} is not a git repository: {err}", dir.display()))
        })?;
    let head = git_output(&root, &["rev-parse", "HEAD"]).ok();
    let dirty = !git_output(&root, &["status", "--porcelain"])
        .map(|status| status.is_empty())
        .unwrap_or(false);

    let listing = git_bytes(&root, &["ls-files", "-z", "-c", "-o", "--exclude-standard"])
        .map_err(|err| PackError::Io(format!("git ls-files failed: {err}")))?;
    let mut paths: Vec<String> = listing
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .collect();
    paths.sort();
    paths.dedup();

    let mut files = Vec::with_capacity(paths.len());
    let mut skipped_secrets = Vec::new();
    let mut skipped_irregular = Vec::new();
    for path in paths {
        if is_secret_shaped(&path) {
            skipped_secrets.push(path);
            continue;
        }
        let full = root.join(&path);
        let metadata = match std::fs::symlink_metadata(&full) {
            Ok(metadata) => metadata,
            // A path listed by git but gone from the working tree (deleted, not staged) is not
            // something a check needs; skip it rather than fail the whole run.
            Err(_) => continue,
        };
        if !metadata.file_type().is_file() {
            skipped_irregular.push(path);
            continue;
        }
        if metadata.len() > MAX_FILE_BYTES {
            return Err(PackError::Usage(format!(
                "{path} is {} bytes, over the {MAX_FILE_BYTES}-byte per-file cap",
                metadata.len()
            )));
        }
        let data = std::fs::read(&full)
            .map_err(|err| PackError::Io(format!("cannot read {path}: {err}")))?;
        let executable = is_executable(&metadata);
        files.push(PackFile {
            path,
            data,
            executable,
        });
    }
    if files.is_empty() {
        return Err(PackError::Usage(
            "the working tree has no files to check".to_owned(),
        ));
    }

    Ok(Tree {
        root,
        files,
        head,
        dirty,
        skipped_secrets,
        skipped_irregular,
    })
}

/// Whether a path names a credential by shape. Deliberately narrow and name-based: the job is to
/// stop the obvious accident, not to classify every possible secret.
fn is_secret_shaped(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name == ".env"
        || name.starts_with(".env.")
        || name.ends_with(".pem")
        || name.ends_with(".key")
        || name.ends_with(".p12")
        || name.ends_with(".pfx")
        || name.starts_with("id_rsa")
        || name.starts_with("id_ed25519")
        || name.starts_with("id_ecdsa")
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    false
}

/// A git command's stdout as text, trimmed.
fn git_output(dir: &Path, args: &[&str]) -> Result<String, String> {
    let bytes = git_bytes(dir, args)?;
    Ok(String::from_utf8_lossy(&bytes).trim().to_owned())
}

/// A git command's raw stdout.
fn git_bytes(dir: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|err| format!("git could not start: {err}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(output.stdout)
}

/// Pack a collected tree into the archive the API accepts.
pub fn pack(tree: &Tree) -> Result<Vec<u8>, PackError> {
    source_archive::pack(&tree.files).map_err(|err| PackError::Io(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_shaped_names_are_recognised() {
        for path in [
            ".env",
            ".env.local",
            "certs/server.pem",
            "private.key",
            "id_rsa",
            "id_ed25519.pub",
        ] {
            assert!(is_secret_shaped(path), "{path}");
        }
        for path in ["src/main.rs", "README.md", "envoy.yaml", "keys.md"] {
            assert!(!is_secret_shaped(path), "{path}");
        }
    }

    /// A throwaway repository: `collect` is only meaningful against real git.
    fn scratch_repo(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "coding-pack-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        std::fs::write(dir.join("Cargo.lock"), "").unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(dir.join(".gitignore"), "target/\n.env\n").unwrap();
        std::fs::write(dir.join(".env"), "SECRET=1\n").unwrap();
        // Tracked on purpose: an ignored secret never reaches us, so only a committed one proves
        // the name-based skip.
        std::fs::create_dir_all(dir.join("certs")).unwrap();
        std::fs::write(dir.join("certs/server.pem"), "PRIVATE\n").unwrap();
        git(&dir, &["init", "-q"]);
        git(&dir, &["add", "-A"]);
        git(
            &dir,
            &[
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=test",
                "commit",
                "-qm",
                "init",
            ],
        );
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

    #[cfg(unix)]
    #[test]
    fn collects_tracked_untracked_and_never_a_secret() {
        let dir = scratch_repo("collect");
        std::fs::write(dir.join("src/new.rs"), "pub fn n() {}\n").unwrap();
        std::fs::create_dir_all(dir.join("target/debug")).unwrap();
        std::fs::write(dir.join("target/debug/junk"), "junk").unwrap();
        std::os::unix::fs::symlink("src/main.rs", dir.join("link.rs")).unwrap();

        let tree = collect(&dir).expect("collect");
        let paths: Vec<&str> = tree.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"src/new.rs"), "untracked: {paths:?}");
        assert!(paths.contains(&"src/main.rs"), "tracked: {paths:?}");
        assert!(!paths.iter().any(|p| p.starts_with("target/")), "ignored");
        assert!(!paths.contains(&".env"), "ignored secret: {paths:?}");
        assert!(
            !paths.contains(&"certs/server.pem"),
            "tracked secret-shaped file: {paths:?}"
        );
        assert!(
            tree.skipped_secrets
                .contains(&"certs/server.pem".to_owned()),
            "the caller must be told what was left out: {:?}",
            tree.skipped_secrets
        );
        assert!(tree.skipped_irregular.contains(&"link.rs".to_owned()));
        assert_eq!(tree.head.as_deref().map(str::len), Some(40));
        assert!(tree.dirty, "a new untracked file makes the tree dirty");
        let blob = pack(&tree).expect("pack");
        assert!(source_archive::validate(&blob).is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_that_is_not_a_repository_is_a_usage_error() {
        let dir = std::env::temp_dir().join(format!("coding-norepo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let error = collect(&dir).expect_err("not a repository");
        assert!(matches!(error, PackError::Usage(_)), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
