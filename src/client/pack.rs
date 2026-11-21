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

use std::collections::{HashMap, HashSet};
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
    #[must_use]
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
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|file| file.data.len() as u64).sum()
    }

    /// Whether a file with this path is in the tree.
    #[must_use]
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
///
/// # Errors
///
/// `PackError::Usage` when `dir` is not inside a git repository or the chosen snapshot has no
/// files, and `PackError::Io` when a git or filesystem read fails.
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
    let dirty = !git_output(root, &["status", "--porcelain"]).is_ok_and(|status| status.is_empty());

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
        // A path listed by git but gone from the working tree (deleted, not staged) is not
        // something a check needs; skip it rather than fail the whole run.
        let Ok(metadata) = std::fs::symlink_metadata(&full) else {
            continue;
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
    // Two files with identical content are one git blob. Counting first keeps the move below
    // correct for them: a blob one file uses is moved out (no second copy), a shared one is
    // copied for every file that needs it.
    let mut seen: HashSet<String> = HashSet::with_capacity(wanted.len());
    let mut shared: HashSet<String> = HashSet::new();
    for (_, _, sha) in &wanted {
        if !seen.insert(sha.clone()) {
            shared.insert(sha.clone());
        }
    }
    let mut blobs = read_blobs(root, shas)?;
    let mut files = Vec::with_capacity(wanted.len());
    for (path, executable, sha) in wanted {
        // Move each blob out of the map rather than cloning it: the map holds the only other
        // copy, so cloning made a tree resident twice over — measured on a 942-file tree, 137 MiB
        // of peak RSS against 79 MiB when the blob is moved.
        let data = if shared.contains(&sha) {
            blobs.get(&sha).cloned()
        } else {
            blobs.remove(&sha)
        };
        let Some(data) = data else {
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
            data,
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
///
/// The requests are written on their own thread **while** the responses are read. Writing every
/// sha first deadlocks any tree past ~1,500 files on Linux: `git cat-file` fills its 64 KiB stdout
/// pipe with a blob and stops reading, the client fills its 64 KiB stdin pipe with shas and stops
/// writing, and neither moves. Measured 2026-11-19 on the fleet gate: agent's 1,698-file HEAD sat
/// for 43 minutes until the job timeout, leaving an orphaned `git`. macOS pipes grow with the
/// write, which is why every local run passed.
fn read_blobs(root: &Path, shas: Vec<String>) -> Result<HashMap<String, Vec<u8>>, PackError> {
    let count = shas.len();
    let mut child = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|err| PackError::Io(format!("git cat-file could not start: {err}")))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| PackError::Io("git cat-file has no stdin".to_owned()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| PackError::Io("git cat-file has no stdout".to_owned()))?;
    let writer = std::thread::spawn(move || -> Result<(), String> {
        for sha in &shas {
            writeln!(stdin, "{sha}").map_err(|err| format!("git cat-file write failed: {err}"))?;
        }
        // The handle drops here, so cat-file sees EOF after the queued commands instead of
        // waiting for more input while the read side finishes.
        Ok(())
    });
    let read = read_batch(BufReader::new(stdout), count);
    if read.is_err() {
        // A failed read leaves git writing into a full pipe; the kill releases the writer thread.
        let _ = child.kill();
    }
    let wrote = writer
        .join()
        .map_err(|_| PackError::Io("the git cat-file writer panicked".to_owned()))?;
    let blobs = match (read, wrote) {
        (_, Err(error)) => return Err(PackError::Io(error)),
        (Err(error), Ok(())) => return Err(error),
        (Ok(blobs), Ok(())) => blobs,
    };
    let status = child
        .wait()
        .map_err(|err| PackError::Io(format!("git cat-file wait failed: {err}")))?;
    if !status.success() {
        return Err(PackError::Io("git cat-file exited non-zero".to_owned()));
    }
    Ok(blobs)
}

/// Read `count` batch responses: for each, a `<sha> <kind> <size>` header, the blob, and a newline.
fn read_batch(
    mut reader: BufReader<std::process::ChildStdout>,
    count: usize,
) -> Result<HashMap<String, Vec<u8>>, PackError> {
    let mut blobs = HashMap::with_capacity(count);
    for _ in 0..count {
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
    let lower = name.to_ascii_lowercase();
    // A dotfile whose whole name is a secret-shaped suffix (`.pem`) has no
    // `Path::extension`; the string form must stay, or the file would travel.
    if matches!(lower.as_str(), ".pem" | ".key" | ".p12" | ".pfx") {
        return true;
    }
    let extension = Path::new(name).extension();
    let has_extension =
        |wanted: &str| extension.is_some_and(|ext| ext.eq_ignore_ascii_case(wanted));
    lower == ".env"
        || lower.starts_with(".env.")
        || has_extension("pem")
        || has_extension("key")
        || has_extension("p12")
        || has_extension("pfx")
        || lower.starts_with("id_rsa")
        || lower.starts_with("id_ed25519")
        || lower.starts_with("id_ecdsa")
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
///
/// # Errors
///
/// `PackError::Io` when a path fails archive validation, the archive exceeds the compressed
/// size cap, or the gzip/tar encoding fails.
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
            ".pem",
            "private.PEM",
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
    fn a_tree_larger_than_a_pipe_buffer_packs_without_deadlocking() {
        // 2,000 extra blobs: more sha bytes than a Linux pipe holds (64 KiB). The old writer
        // pushed every sha before reading any response, so `git cat-file` filled its stdout pipe
        // with a blob while the client filled its stdin pipe with shas, and neither moved — the
        // first real CI gate run sat for 43 minutes until its job timeout (2026-11-19). macOS
        // pipes grow with the write, which is why every local run passed.
        let dir = scratch_repo("big-tree");
        std::fs::create_dir_all(dir.join("many")).unwrap();
        for index in 0..2000 {
            std::fs::write(
                dir.join("many").join(format!("f{index}.txt")),
                format!("{index}{}", "x".repeat(48)),
            )
            .unwrap();
        }
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
                "many",
            ],
        );

        // A deadlock must fail this test, not wedge the suite: the pack runs on its own thread
        // with a bounded wait, and a blocked worker dies with the test process.
        let probe = dir.clone();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let outcome = collect(&probe, Snapshot::Head).map(|tree| tree.files.len());
            let _ = done.send(outcome);
        });
        let files = finished
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("packing 2,000 extra files must finish, not deadlock")
            .expect("collect");
        assert!(files >= 2000, "every file is packed: {files}");
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
    fn files_that_share_a_blob_are_each_packed() {
        // Two identical files are one git blob. The collector moves each blob out of the map it
        // read, so a blob two paths share must be copied for the second one instead of vanishing.
        let dir = scratch_repo("shared-blob");
        std::fs::write(dir.join("src/one.rs"), "pub fn shared() -> u64 { 7 }\n").unwrap();
        std::fs::write(dir.join("src/two.rs"), "pub fn shared() -> u64 { 7 }\n").unwrap();
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
                "shared",
            ],
        );

        let tree = collect(&dir, Snapshot::Head).expect("collect");
        let paths: Vec<&str> = tree.files.iter().map(|file| file.path.as_str()).collect();
        assert!(paths.contains(&"src/one.rs"), "{paths:?}");
        assert!(paths.contains(&"src/two.rs"), "{paths:?}");
        let blob = pack(&tree).expect("pack");
        assert_eq!(
            source_archive::validate(&blob).expect("validate").files,
            tree.files.len()
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
