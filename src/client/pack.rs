//! Turning a snapshot of the repository into the archive a job runs against.
//!
//! Three snapshots, because the gate must check what is actually being committed or pushed:
//!
//! * `worktree` — tracked files as they are on disk, plus untracked-not-ignored files (the
//!   historical behaviour; useful for an ad-hoc check).
//! * `index` — exactly what `git commit` would record (the pre-commit gate).
//! * `head` — exactly what is already committed, i.e. what a push sends (the pre-push gate).
//!
//! Ignored paths (`target/`, `.env`) never travel, and a fixed set of secret-shaped names is
//! skipped even when a repository forgot to ignore them: the app must never receive a credential
//! by accident.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::source_archive::{self, MAX_FILE_BYTES, PackFile};

/// Which snapshot of the repository a job checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Snapshot {
    /// The working tree: tracked plus untracked-not-ignored files as they are on disk.
    Worktree,
    /// The index: exactly what `git commit` would record.
    Index,
    /// HEAD: exactly what is already committed (what a push sends).
    Head,
}

impl Snapshot {
    /// The wire/CLI spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worktree => "worktree",
            Self::Index => "index",
            Self::Head => "head",
        }
    }
}

/// What one collection produced.
#[derive(Debug)]
pub struct Tree {
    /// The repository root the files came from.
    pub root: PathBuf,
    pub files: Vec<PackFile>,
    /// `HEAD`, when the repository has a commit.
    pub head: Option<String>,
    /// Whether the snapshot differs from `HEAD` (always false for the `head` snapshot).
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

/// Collect `snapshot` of the repository under `dir` (which must be inside a git repository).
pub fn collect(dir: &Path, snapshot: Snapshot) -> Result<Tree, PackError> {
    let root = git_output(dir, &["rev-parse", "--show-toplevel"])
        .map(PathBuf::from)
        .map_err(|err| {
            PackError::Usage(format!("{} is not a git repository: {err}", dir.display()))
        })?;
    let head = git_output(&root, &["rev-parse", "HEAD"]).ok();
    match snapshot {
        Snapshot::Worktree => collect_worktree(&root, head),
        Snapshot::Index => collect_index(&root, head),
        Snapshot::Head => collect_head(&root, head),
    }
}

/// The historical snapshot: tracked files read from disk plus untracked-not-ignored files.
fn collect_worktree(root: &Path, head: Option<String>) -> Result<Tree, PackError> {
    let dirty = !git_output(root, &["status", "--porcelain"])
        .map(|status| status.is_empty())
        .unwrap_or(false);

    let listing = git_bytes(root, &["ls-files", "-z", "-c", "-o", "--exclude-standard"])
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
        root: root.to_path_buf(),
        files,
        head,
        dirty,
        skipped_secrets,
        skipped_irregular,
    })
}

/// Exactly what `git commit` would record: stage-0 index entries, read from the object store (so a
/// partially staged file is checked as staged, not as it happens to look on disk).
fn collect_index(root: &Path, head: Option<String>) -> Result<Tree, PackError> {
    let staging = git_bytes(root, &["ls-files", "-z", "--cached", "--stage"])
        .map_err(|err| PackError::Io(format!("git ls-files --stage failed: {err}")))?;
    let entries = parse_index_entries(&staging)?;
    if entries.is_empty() {
        return Err(PackError::Usage(
            "the index has no files to check (nothing is staged)".to_owned(),
        ));
    }
    // `git diff --cached --quiet` exits 1 when the index differs from HEAD; anything else
    // (including an error such as an unborn HEAD) is treated as dirty, which only affects the
    // human-readable source line.
    let dirty = git_exit(root, &["diff", "--cached", "--quiet"]) != Some(0);
    build_from_entries(root, head, dirty, entries, "the index")
}

/// Exactly what is committed: the HEAD tree, read from the object store.
fn collect_head(root: &Path, head: Option<String>) -> Result<Tree, PackError> {
    let Some(head) = head else {
        return Err(PackError::Usage(
            "the repository has no commits to check (HEAD is unset)".to_owned(),
        ));
    };
    let listing = git_bytes(root, &["ls-tree", "-r", "-z", "--full-tree", &head])
        .map_err(|err| PackError::Io(format!("git ls-tree failed: {err}")))?;
    let entries = parse_tree_entries(&listing);
    if entries.is_empty() {
        return Err(PackError::Usage("HEAD has no files to check".to_owned()));
    }
    build_from_entries(root, Some(head), false, entries, "HEAD")
}

/// Build the tree from `<mode, blob sha, path>` entries, reading the blobs in one batch.
fn build_from_entries(
    root: &Path,
    head: Option<String>,
    dirty: bool,
    entries: Vec<(String, String, String)>,
    what: &str,
) -> Result<Tree, PackError> {
    let mut skipped_secrets = Vec::new();
    let mut skipped_irregular = Vec::new();
    let mut wanted: Vec<(String, bool, String)> = Vec::new();
    for (mode, sha, path) in entries {
        // 100644/100755 are regular files; 120000 is a symlink and 160000 a submodule. Neither
        // can be checked as source, and the archive must not turn a symlink into a regular file
        // silently.
        if mode != "100644" && mode != "100755" {
            skipped_irregular.push(path);
            continue;
        }
        if is_secret_shaped(&path) {
            skipped_secrets.push(path);
            continue;
        }
        wanted.push((path, mode == "100755", sha));
    }
    if wanted.is_empty() {
        return Err(PackError::Usage(format!("{what} has no files to check")));
    }
    let shas: Vec<String> = wanted.iter().map(|(_, _, sha)| sha.clone()).collect();
    let blobs = read_blobs(root, &shas)?;
    let mut files = Vec::with_capacity(wanted.len());
    for (path, executable, sha) in wanted {
        let Some(data) = blobs.get(&sha) else {
            return Err(PackError::Io(format!(
                "git could not read the blob for {path}"
            )));
        };
        if data.len() as u64 > MAX_FILE_BYTES {
            return Err(PackError::Usage(format!(
                "{path} is {} bytes, over the {MAX_FILE_BYTES}-byte per-file cap",
                data.len()
            )));
        }
        files.push(PackFile {
            path,
            data: data.clone(),
            executable,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Tree {
        root: root.to_path_buf(),
        files,
        head,
        dirty,
        skipped_secrets,
        skipped_irregular,
    })
}

/// Read every blob in one `git cat-file --batch` process. Spawning git once per file would make a
/// large repository's gate minutes slower than the checks themselves.
fn read_blobs(root: &Path, shas: &[String]) -> Result<HashMap<String, Vec<u8>>, PackError> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|err| PackError::Io(format!("git cat-file could not start: {err}")))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| PackError::Io("git cat-file has no stdin".to_owned()))?;
        for sha in shas {
            writeln!(stdin, "{sha}")
                .map_err(|err| PackError::Io(format!("git cat-file write failed: {err}")))?;
        }
        // The handle drops here, so cat-file sees EOF after the queued commands instead of
        // waiting for more input while `wait()` blocks below.
    }
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| PackError::Io("git cat-file has no stdout".to_owned()))?;
    let mut reader = BufReader::new(stdout);
    let mut blobs = HashMap::with_capacity(shas.len());
    for _ in 0..shas.len() {
        let mut header = String::new();
        reader
            .read_line(&mut header)
            .map_err(|err| PackError::Io(format!("git cat-file read failed: {err}")))?;
        let mut fields = header.split_whitespace();
        let sha = fields.next().unwrap_or_default().to_owned();
        let kind = fields.next().unwrap_or_default();
        let size = fields.next().and_then(|size| size.parse::<usize>().ok());
        let Some(size) = size.filter(|_| kind == "blob") else {
            return Err(PackError::Io(format!(
                "git cat-file did not return a blob for {sha} ({})",
                header.trim()
            )));
        };
        let mut data = vec![0u8; size];
        reader
            .read_exact(&mut data)
            .map_err(|err| PackError::Io(format!("git cat-file read failed: {err}")))?;
        let mut newline = [0u8; 1];
        reader
            .read_exact(&mut newline)
            .map_err(|err| PackError::Io(format!("git cat-file read failed: {err}")))?;
        blobs.insert(sha, data);
    }
    let status = child
        .wait()
        .map_err(|err| PackError::Io(format!("git cat-file wait failed: {err}")))?;
    if !status.success() {
        return Err(PackError::Io("git cat-file exited non-zero".to_owned()));
    }
    Ok(blobs)
}

/// `<mode> <sha> <stage>\t<path>` records from `git ls-files --stage -z`.
fn parse_index_entries(bytes: &[u8]) -> Result<Vec<(String, String, String)>, PackError> {
    let mut entries = Vec::new();
    for record in bytes
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let text = String::from_utf8_lossy(record);
        let Some((meta, path)) = text.split_once('\t') else {
            continue;
        };
        let mut fields = meta.split_whitespace();
        let mode = fields.next().unwrap_or_default();
        let sha = fields.next().unwrap_or_default();
        let stage = fields.next().unwrap_or_default();
        if stage != "0" {
            return Err(PackError::Usage(
                "the index has unresolved merge conflicts".to_owned(),
            ));
        }
        entries.push((mode.to_owned(), sha.to_owned(), path.to_owned()));
    }
    Ok(entries)
}

/// `<mode> <type> <sha>\t<path>` records from `git ls-tree -r -z`.
fn parse_tree_entries(bytes: &[u8]) -> Vec<(String, String, String)> {
    let mut entries = Vec::new();
    for record in bytes
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let text = String::from_utf8_lossy(record);
        let Some((meta, path)) = text.split_once('\t') else {
            continue;
        };
        let mut fields = meta.split_whitespace();
        let mode = fields.next().unwrap_or_default();
        let kind = fields.next().unwrap_or_default();
        let sha = fields.next().unwrap_or_default();
        // A non-blob (a submodule's commit) has no contents to read; the mode branch in
        // `build_from_entries` records it as skipped-irregular.
        let sha = if kind == "blob" { sha } else { "" };
        entries.push((mode.to_owned(), sha.to_owned(), path.to_owned()));
    }
    entries
}

/// The exit code of a git command, or `None` if it could not run.
fn git_exit(dir: &Path, args: &[&str]) -> Option<i32> {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .status()
        .ok()
        .and_then(|status| status.code())
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

        let tree = collect(&dir, Snapshot::Worktree).expect("collect");
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

    #[cfg(unix)]
    #[test]
    fn the_snapshots_are_what_git_would_commit_and_push() {
        let dir = scratch_repo("snapshots");
        let main = dir.join("src/main.rs");
        // Stage one version, then change the working tree to another: the index snapshot must
        // check what a commit would record, not what is on disk.
        std::fs::write(&main, "fn main() { /* index */ }\n").unwrap();
        git(&dir, &["add", "src/main.rs"]);
        std::fs::write(&main, "fn main() { /* worktree */ }\n").unwrap();
        std::fs::write(dir.join("src/new.rs"), "pub fn new() {}\n").unwrap();

        let collected = |snapshot| {
            let tree = collect(&dir, snapshot).expect("collect");
            let data = tree
                .files
                .iter()
                .find(|file| file.path == "src/main.rs")
                .expect("src/main.rs")
                .data
                .clone();
            (String::from_utf8(data).unwrap(), tree)
        };
        let (worktree, worktree_tree) = collected(Snapshot::Worktree);
        let (index, index_tree) = collected(Snapshot::Index);
        let (head, head_tree) = collected(Snapshot::Head);
        assert_eq!(worktree, "fn main() { /* worktree */ }\n");
        assert_eq!(index, "fn main() { /* index */ }\n");
        assert_eq!(head, "fn main() {}\n");
        assert!(worktree_tree.has("src/new.rs"), "untracked in the worktree");
        assert!(!index_tree.has("src/new.rs"), "untracked is not staged");
        assert!(!head_tree.has("src/new.rs"), "untracked is not committed");
        assert!(index_tree.dirty, "the index differs from HEAD");
        assert!(!head_tree.dirty, "HEAD is clean by definition");
        assert!(
            index_tree
                .skipped_secrets
                .contains(&"certs/server.pem".to_owned())
        );
        assert!(
            head_tree
                .skipped_secrets
                .contains(&"certs/server.pem".to_owned())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_that_is_not_a_repository_is_a_usage_error() {
        let dir = std::env::temp_dir().join(format!("coding-norepo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let error = collect(&dir, Snapshot::Worktree).expect_err("not a repository");
        assert!(matches!(error, PackError::Usage(_)), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
