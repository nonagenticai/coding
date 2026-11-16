//! The source archive: a gzip-compressed tar of a working tree.
//!
//! One module for both directions on purpose. The client packs, the API validates what arrived,
//! and the Pod validates again before it unpacks — so the limits and the path rules live in one
//! place and cannot drift apart.
//!
//! What this module refuses, because the archive is the security boundary:
//!
//! * anything over [`MAX_COMPRESSED_BYTES`] on the wire or [`MAX_UNCOMPRESSED_BYTES`] emitted by
//!   the decompressor — the cap is on emitted bytes, never on a tar header's claim;
//! * symlinks, hardlinks, devices and every other non-regular entry;
//! * absolute paths, `..` components, empty components, duplicate paths;
//! * an archive with no files, more than [`MAX_ENTRIES`] files, or a file over [`MAX_FILE_BYTES`].

use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

/// The largest archive accepted on the wire.
///
/// Measured, not guessed: the estate's biggest Rust target (`conveyor`, 942 tracked files) packs to
/// **15.76 MiB**, so a 16 MiB cap would sit at 98 % and refuse the very repository the gate exists
/// for. 32 MiB leaves room for untracked files and keeps a worst-case upload at ~64 MiB of buffers
/// per replica (2 concurrent × [body + sqlx copy]), against the API pod's 512Mi limit. Beyond this
/// the transport moves to object storage — the cap does not keep growing.
pub const MAX_COMPRESSED_BYTES: usize = 32 * 1024 * 1024;
/// The largest total a decompressor may emit for one archive.
pub const MAX_UNCOMPRESSED_BYTES: u64 = 128 * 1024 * 1024;
/// The most files one archive may carry.
pub const MAX_ENTRIES: usize = 100_000;
/// The largest single file inside an archive.
pub const MAX_FILE_BYTES: u64 = 32 * 1024 * 1024;

/// Why an archive was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ArchiveError {
    /// The archive breaks a rule. The message is shown to the caller.
    #[error("{0}")]
    Invalid(String),
    /// The compressed body is over [`MAX_COMPRESSED_BYTES`].
    #[error("source archive is {size} bytes, over the {MAX_COMPRESSED_BYTES}-byte cap")]
    TooLarge { size: usize },
    /// Reading or writing the tree failed.
    #[error("{0}")]
    Io(String),
}

impl ArchiveError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// What an archive holds, after it has been validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Manifest {
    /// Number of regular files.
    pub files: usize,
    /// Total bytes of file data.
    pub bytes: u64,
}

/// One file the client puts into an archive.
#[derive(Debug, Clone)]
pub struct PackFile {
    /// Relative path, `/`-separated.
    pub path: String,
    pub data: Vec<u8>,
    /// Preserve the executable bit; everything else is normalised.
    pub executable: bool,
}

/// Pack files into a reproducible gzip tar.
///
/// Reproducible on purpose: paths are sorted, and mtime, uid, gid and every mode other than the
/// executable bit are normalised, so the sha256 of the bytes identifies the tree and nothing else.
pub fn pack(files: &[PackFile]) -> Result<Vec<u8>, ArchiveError> {
    let mut sorted: Vec<&PackFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));

    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    {
        let mut builder = tar::Builder::new(&mut encoder);
        for file in sorted {
            validate_inner_path(&file.path)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(file.data.len() as u64);
            header.set_mode(if file.executable { 0o755 } else { 0o644 });
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            builder
                .append_data(&mut header, &file.path, file.data.as_slice())
                .map_err(|err| ArchiveError::Io(format!("cannot pack {:?}: {err}", file.path)))?;
        }
        builder
            .into_inner()
            .map_err(|err| ArchiveError::Io(format!("cannot finish the tar stream: {err}")))?;
    }
    let blob = encoder
        .finish()
        .map_err(|err| ArchiveError::Io(format!("cannot finish the gzip stream: {err}")))?;
    if blob.len() > MAX_COMPRESSED_BYTES {
        return Err(ArchiveError::TooLarge { size: blob.len() });
    }
    Ok(blob)
}

/// Check an archive without unpacking it. Used by the API before it stores anything.
pub fn validate(blob: &[u8]) -> Result<Manifest, ArchiveError> {
    if blob.len() > MAX_COMPRESSED_BYTES {
        return Err(ArchiveError::TooLarge { size: blob.len() });
    }
    let mut totals = Totals::default();
    let mut seen = HashSet::new();
    walk(blob, |entry| {
        let path = entry_path(entry)?;
        let is_dir = entry.header().entry_type().is_dir();
        let size = entry_size(entry)?;
        totals.record(&path, size, is_dir, &mut seen)
    })?;
    totals.finish()
}

/// Validate an archive and unpack it under `dir`.
pub fn unpack(blob: &[u8], dir: &Path) -> Result<Manifest, ArchiveError> {
    if blob.len() > MAX_COMPRESSED_BYTES {
        return Err(ArchiveError::TooLarge { size: blob.len() });
    }
    std::fs::create_dir_all(dir)
        .map_err(|err| ArchiveError::Io(format!("cannot create {:?}: {err}", dir.display())))?;

    let mut totals = Totals::default();
    let mut seen = HashSet::new();
    walk(blob, |entry| {
        let path = entry_path(entry)?;
        let is_dir = entry.header().entry_type().is_dir();
        let size = entry_size(entry)?;
        totals.record(&path, size, is_dir, &mut seen)?;
        let target = dir.join(&path);
        if is_dir {
            std::fs::create_dir_all(&target)
                .map_err(|err| ArchiveError::Io(format!("cannot create {path:?}: {err}")))?;
            return Ok(());
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| ArchiveError::Io(format!("cannot create {path:?}: {err}")))?;
        }
        let executable = entry.header().mode().unwrap_or(0o644) & 0o111 != 0;
        let mut file = std::fs::File::create(&target)
            .map_err(|err| ArchiveError::Io(format!("cannot create {path:?}: {err}")))?;
        std::io::copy(entry, &mut file)
            .map_err(|err| ArchiveError::Io(format!("cannot write {path:?}: {err}")))?;
        drop(file);
        #[cfg(unix)]
        if executable {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755));
        }
        Ok(())
    })?;
    totals.finish()
}

/// The sha256 of the archive bytes, as lowercase hex. This is what a Pod verifies before it
/// unpacks, and what the API reports to the client.
pub fn sha256_hex(blob: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let digest = Sha256::digest(blob);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A path proven to stay under `dir`: relative, no `..`, no absolute component.
pub fn safe_target(dir: &Path, path: &str) -> Result<PathBuf, ArchiveError> {
    validate_inner_path(path)?;
    Ok(dir.join(path))
}

/// The concrete reader stack the tar archive is read from.
type ArchiveReader<'a> = CappedReader<GzDecoder<Cursor<&'a [u8]>>>;

/// Walk the entries of an archive, applying the shared rules and handing each entry to `visit`.
fn walk<'a>(
    blob: &'a [u8],
    mut visit: impl FnMut(&mut tar::Entry<'_, ArchiveReader<'a>>) -> Result<(), ArchiveError>,
) -> Result<(), ArchiveError> {
    let decoder = GzDecoder::new(Cursor::new(blob));
    let capped = CappedReader::new(decoder, MAX_UNCOMPRESSED_BYTES);
    let mut archive = tar::Archive::new(capped);
    let entries = archive
        .entries()
        .map_err(|err| ArchiveError::invalid(format!("not a tar archive: {err}")))?;
    for entry in entries {
        let mut entry =
            entry.map_err(|err| ArchiveError::invalid(format!("corrupt tar entry: {err}")))?;
        let kind = entry.header().entry_type();
        if !(kind.is_file() || kind.is_dir()) {
            return Err(ArchiveError::invalid(format!(
                "source archive carries a {kind:?} entry; only regular files and directories are allowed"
            )));
        }
        visit(&mut entry)?;
    }
    Ok(())
}

/// The validated, `/`-trimmed path of one entry.
fn entry_path(entry: &mut tar::Entry<'_, ArchiveReader<'_>>) -> Result<String, ArchiveError> {
    let path = entry
        .path()
        .map_err(|err| ArchiveError::invalid(format!("corrupt entry path: {err}")))?;
    let text = path
        .to_str()
        .ok_or_else(|| ArchiveError::invalid("source archive carries a non-UTF-8 path"))?
        .trim_end_matches('/')
        .to_owned();
    validate_inner_path(&text)?;
    Ok(text)
}

/// A claimed size, bounded before anything is written. A directory claims nothing.
fn entry_size(entry: &mut tar::Entry<'_, ArchiveReader<'_>>) -> Result<u64, ArchiveError> {
    if entry.header().entry_type().is_dir() {
        return Ok(0);
    }
    let size = entry
        .header()
        .size()
        .map_err(|err| ArchiveError::invalid(format!("corrupt entry size: {err}")))?;
    if size > MAX_FILE_BYTES {
        return Err(ArchiveError::invalid(format!(
            "a file in the source archive claims {size} bytes, over the {MAX_FILE_BYTES}-byte cap"
        )));
    }
    Ok(size)
}

/// Running totals, and the duplicate-path check.
#[derive(Default)]
struct Totals {
    files: usize,
    bytes: u64,
}

impl Totals {
    fn record(
        &mut self,
        path: &str,
        size: u64,
        is_dir: bool,
        seen: &mut HashSet<String>,
    ) -> Result<(), ArchiveError> {
        if !seen.insert(path.to_owned()) {
            return Err(ArchiveError::invalid(format!(
                "source archive repeats path {path:?}"
            )));
        }
        if is_dir {
            return Ok(());
        }
        self.files += 1;
        self.bytes = self.bytes.saturating_add(size);
        if self.files > MAX_ENTRIES {
            return Err(ArchiveError::invalid(format!(
                "source archive carries more than {MAX_ENTRIES} files"
            )));
        }
        Ok(())
    }

    fn finish(self) -> Result<Manifest, ArchiveError> {
        if self.files == 0 {
            return Err(ArchiveError::invalid("source archive contains no files"));
        }
        Ok(Manifest {
            files: self.files,
            bytes: self.bytes,
        })
    }
}

/// A relative file path with no way out of the tree.
fn validate_inner_path(path: &str) -> Result<(), ArchiveError> {
    if path.is_empty() || path.len() > 512 {
        return Err(ArchiveError::invalid(format!(
            "source archive path {path:?} must be 1..=512 characters"
        )));
    }
    if path.starts_with('/') || path.contains('\\') || path.contains('\0') {
        return Err(ArchiveError::invalid(format!(
            "source archive path {path:?} is not a relative path"
        )));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(ArchiveError::invalid(format!(
                "source archive path {path:?} escapes the source tree"
            )));
        }
    }
    Ok(())
}

/// A reader that refuses to emit more than `limit` bytes. This is how a decompression bomb is
/// stopped: the cap counts what the decompressor actually produced, not what a header claims.
struct CappedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R> CappedReader<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}

impl<R: Read> Read for CappedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let want = buf.len().min(self.remaining as usize + 1);
        let read = self.inner.read(&mut buf[..want])?;
        if read as u64 > self.remaining {
            return Err(std::io::Error::other(format!(
                "source archive expands past the {MAX_UNCOMPRESSED_BYTES}-byte cap"
            )));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn pack_file(path: &str, data: &str) -> PackFile {
        PackFile {
            path: path.to_owned(),
            data: data.as_bytes().to_vec(),
            executable: false,
        }
    }

    #[test]
    fn round_trips_a_tree() {
        let files = vec![
            pack_file("Cargo.toml", "[package]\nname = \"x\"\n"),
            pack_file("src/lib.rs", "pub fn f() {}\n"),
        ];
        let blob = pack(&files).expect("pack");
        let manifest = validate(&blob).expect("validate");
        assert_eq!(manifest.files, 2);
        assert_eq!(
            manifest.bytes,
            files.iter().map(|f| f.data.len() as u64).sum::<u64>()
        );

        let dir = std::env::temp_dir().join(format!("coding-unpack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let manifest = unpack(&blob, &dir).expect("unpack");
        assert_eq!(manifest.files, 2);
        assert_eq!(
            std::fs::read_to_string(dir.join("src/lib.rs")).unwrap(),
            "pub fn f() {}\n"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn packing_is_reproducible_regardless_of_input_order() {
        let a = pack(&[pack_file("a.txt", "a"), pack_file("b.txt", "b")]).expect("pack");
        let b = pack(&[pack_file("b.txt", "b"), pack_file("a.txt", "a")]).expect("pack");
        assert_eq!(a, b);
        assert_eq!(sha256_hex(&a), sha256_hex(&b));
    }

    #[test]
    fn an_archive_over_the_compressed_cap_is_refused_by_size() {
        let blob = vec![0u8; MAX_COMPRESSED_BYTES + 1];
        assert!(matches!(
            validate(&blob),
            Err(ArchiveError::TooLarge { .. })
        ));
    }

    #[test]
    fn an_empty_archive_is_refused() {
        let blob = pack(&[]).expect("pack");
        let error = validate(&blob).expect_err("no files");
        assert!(error.to_string().contains("no files"), "{error}");
    }

    /// A tar written by hand, because `pack` refuses to make a hostile one — which is the point.
    /// The name goes straight into the header bytes: `Header::set_path` correctly rejects the
    /// paths this test needs to express.
    fn hostile_tar(entries: &[(&str, tar::EntryType, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, kind, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(*kind);
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            {
                let gnu = header.as_gnu_mut().expect("gnu header");
                let name = path.as_bytes();
                gnu.name[..name.len()].copy_from_slice(name);
                if matches!(*kind, tar::EntryType::Symlink | tar::EntryType::Link) {
                    let link = b"/etc/passwd";
                    gnu.linkname[..link.len()].copy_from_slice(link);
                }
            }
            header.set_cksum();
            builder.append(&header, *data).expect("hostile entry");
        }
        builder.into_inner().expect("tar")
    }

    fn gzip(raw: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(raw).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn path_traversal_is_refused() {
        let blob = gzip(&hostile_tar(&[(
            "../escape.txt",
            tar::EntryType::Regular,
            b"x",
        )]));
        let error = validate(&blob).expect_err("traversal");
        assert!(error.to_string().contains("escapes"), "{error}");
    }

    #[test]
    fn absolute_paths_are_refused() {
        let blob = gzip(&hostile_tar(&[(
            "/etc/evil",
            tar::EntryType::Regular,
            b"x",
        )]));
        let error = validate(&blob).expect_err("absolute");
        assert!(error.to_string().contains("relative"), "{error}");
    }

    #[test]
    fn symlinks_and_hardlinks_are_refused() {
        for kind in [tar::EntryType::Symlink, tar::EntryType::Link] {
            let blob = gzip(&hostile_tar(&[("link", kind, b"")]));
            let error = validate(&blob).expect_err("link");
            assert!(
                error.to_string().contains("only regular files"),
                "{kind:?}: {error}"
            );
        }
    }

    #[test]
    fn duplicate_paths_are_refused() {
        let blob = gzip(&hostile_tar(&[
            ("a.txt", tar::EntryType::Regular, b"x"),
            ("a.txt", tar::EntryType::Regular, b"y"),
        ]));
        let error = validate(&blob).expect_err("duplicate");
        assert!(error.to_string().contains("repeats path"), "{error}");
    }

    #[test]
    fn a_decompression_bomb_is_refused_by_emitted_bytes() {
        // Five entries that each claim — and carry — a quarter of the total cap plus a little:
        // every single one is inside the per-file cap, and together they run past what the
        // decompressor may emit. The archive itself is tiny once compressed: that is the bomb.
        let chunk = vec![0u8; (MAX_UNCOMPRESSED_BYTES / 5 + 1024) as usize];
        let mut builder = tar::Builder::new(Vec::new());
        for index in 0..5 {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(chunk.len() as u64);
            header.set_mode(0o644);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            builder
                .append_data(&mut header, format!("big-{index}.bin"), chunk.as_slice())
                .expect("bomb entry");
        }
        let raw = builder.into_inner().expect("tar");
        let blob = gzip(&raw);
        assert!(
            blob.len() < MAX_COMPRESSED_BYTES,
            "the bomb must fit the compressed cap, or this tests the wrong limit"
        );
        let error = validate(&blob).expect_err("bomb");
        assert!(
            error.to_string().contains("cap") || error.to_string().contains("tar"),
            "{error}"
        );
    }

    #[test]
    fn a_truncated_archive_is_refused_not_ignored() {
        let blob = pack(&[pack_file("a.txt", &"hello".repeat(2000))]).expect("pack");
        let cut = &blob[..blob.len() / 2];
        assert!(validate(cut).is_err(), "a truncated archive must not pass");
    }

    #[test]
    fn safe_target_stays_inside_the_tree() {
        let dir = Path::new("/tmp/src");
        assert_eq!(
            safe_target(dir, "src/main.rs").unwrap(),
            Path::new("/tmp/src/src/main.rs")
        );
        for bad in ["../x", "/etc/passwd", "a/../../b", ""] {
            assert!(safe_target(dir, bad).is_err(), "{bad:?}");
        }
    }
}
