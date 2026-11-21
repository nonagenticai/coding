//! `github_op`: deliver a bounded output tree as a branch and a pull request.
//!
//! The tree is the job's uploaded `source` (the same archive a check job sends); the target is
//! `input.output`. This runs in the WORKER — no Pod is created — so the App's installation token
//! never reaches untrusted code, and the untrusted tree never reaches a shell. Every check runs
//! before the first write: the repo must be on `CODING_OUTPUT_REPOS`, the installation must see
//! it, the base ref must exist, and the archive's own caps bound the tree.

use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::domain::{JobError, JobInput, Source};
use crate::github_app::GitHubApp;
use crate::source_archive;
use crate::store::JobClaim;

/// The repositories a delivery may write to. Unset or empty is a configuration error, never a
/// silent "allow all": the worker is the one holding the write token.
const OUTPUT_REPOS_ENV: &str = "CODING_OUTPUT_REPOS";
/// A delivery is bounded well below the archive caps: every file is one GitHub blob request, so
/// the count is the API budget, not just the disk.
const MAX_DELIVER_FILES: usize = 2_000;
/// Branch-name suffixes tried when the derived branch already exists.
const BRANCH_ATTEMPTS: u32 = 9;

/// Deliver one `github_op` claim. Returns the job result (`status`, `pr_number`, `pr_url`,
/// `branch`, `head_sha`).
///
/// # Errors
///
/// `JobError` with `output_not_configured` (no allowlist / no App), `output_repo_not_allowed`,
/// `output_repo_not_visible`, `output_base_missing`, `source_unavailable`, `output_invalid` (the
/// archive), `github_app_error` or `github_api_error` (retryable on 5xx/429).
// One linear write sequence: every check precedes the first write, and splitting the steps into
// helpers would scatter that order across call sites — the order IS the safety argument.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    pool: &PgPool,
    github_app: Option<&GitHubApp>,
    claim: JobClaim,
) -> Result<Value, JobError> {
    let input: JobInput = serde_json::from_value(claim.input.clone())
        .map_err(|err| invalid(format!("the job input is not a valid JobInput: {err}")))?;
    let output = input
        .output
        .clone()
        .ok_or_else(|| invalid("github_op requires input.output".to_owned()))?;
    let source_id = match &input.source {
        Some(Source::Upload { id, .. }) => id.clone(),
        _ => return Err(invalid("github_op requires an upload source".to_owned())),
    };
    let allowlist = output_allowlist()?;
    if !allowlist
        .iter()
        .any(|repo| repo.eq_ignore_ascii_case(&output.repo))
    {
        return Err(JobError {
            code: "output_repo_not_allowed".to_owned(),
            message: format!(
                "{} is not on {OUTPUT_REPOS_ENV}; a delivery may only write to an allowlisted \
                 repository",
                output.repo
            ),
            details: None,
            retryable: false,
        });
    }
    let app = github_app.ok_or_else(|| JobError {
        code: "output_not_configured".to_owned(),
        message: "the GitHub App is not configured, so nothing can be delivered".to_owned(),
        details: None,
        retryable: false,
    })?;

    // The tree: the worker reads the upload straight from the store (the same row and token the
    // runner would fetch), unpacks it under the archive's caps, and walks it.
    let blob = fetch_upload(pool, &claim, &source_id).await?;
    let dir = std::env::temp_dir().join(format!("coding-deliver-{}", claim.id));
    let _ = std::fs::remove_dir_all(&dir);
    let _cleanup = Cleanup(dir.clone());
    let manifest = source_archive::unpack(&blob, &dir)
        .map_err(|err| invalid(format!("the output archive is invalid: {err}")))?;
    let files = collect_files(&dir)?;
    if files.is_empty() {
        return Err(invalid("the output archive has no files".to_owned()));
    }
    if files.len() > MAX_DELIVER_FILES {
        return Err(invalid(format!(
            "the output archive has {} files, over the {MAX_DELIVER_FILES}-file delivery cap",
            files.len()
        )));
    }

    let token = app.installation_token().await.map_err(|err| JobError {
        code: "github_app_error".to_owned(),
        message: err,
        details: None,
        retryable: false,
    })?;
    let api = app.api_base().trim_end_matches('/').to_owned();
    // A stalled connection must not pin a worker slot forever: the heartbeat keeps renewing the
    // lease while this future runs, so only these bounds (and the job's own timeout) end it.
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(u64::try_from(claim.timeout_seconds).unwrap_or(0));
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|err| JobError {
            code: "github_api_error".to_owned(),
            message: format!("could not build the GitHub client: {err}"),
            details: None,
            retryable: true,
        })?;
    let repo = output.repo.as_str();

    // Visible to the installation? A 404 here is about the repo, not the ref.
    ensure_time(deadline)?;
    let visible = github(
        &client,
        &api,
        &token,
        Method::GET,
        &format!("/repos/{repo}"),
        None,
    )
    .await?;
    if !visible.status.is_success() {
        return Err(match visible.status {
            StatusCode::NOT_FOUND => JobError {
                code: "output_repo_not_visible".to_owned(),
                message: format!(
                    "the App installation cannot see {repo}; check the installation's repository \
                     access"
                ),
                details: None,
                retryable: false,
            },
            status => github_failed(status, &visible.body, &format!("/repos/{repo}")),
        });
    }

    let base_path = format!("/repos/{repo}/git/ref/heads/{}", output.base);
    ensure_time(deadline)?;
    let base_ref = github(&client, &api, &token, Method::GET, &base_path, None).await?;
    let base_sha = if base_ref.status.is_success() {
        base_ref.body["object"]["sha"]
            .as_str()
            .ok_or_else(|| invalid(format!("the base ref {} has no commit", output.base)))?
            .to_owned()
    } else {
        return Err(match base_ref.status {
            StatusCode::NOT_FOUND => JobError {
                code: "output_base_missing".to_owned(),
                message: format!("{} has no branch named {}", repo, output.base),
                details: None,
                retryable: false,
            },
            status => github_failed(status, &base_ref.body, &base_path),
        });
    };

    // One blob per file, then one tree, one commit, one branch and one PR.
    let mut entries = Vec::with_capacity(files.len());
    for file in &files {
        let data = std::fs::read(&file.path)
            .map_err(|err| invalid(format!("cannot read {}: {err}", file.relative)))?;
        let body = json!({"content": base64_standard(&data), "encoding": "base64"});
        let blob = github(
            &client,
            &api,
            &token,
            Method::POST,
            &format!("/repos/{repo}/git/blobs"),
            Some(&body),
        )
        .await?;
        if !blob.status.is_success() {
            return Err(github_failed(
                blob.status,
                &blob.body,
                &format!("/repos/{repo}/git/blobs"),
            ));
        }
        let sha = blob.body["sha"]
            .as_str()
            .ok_or_else(|| invalid(format!("the blob for {} has no sha", file.relative)))?
            .to_owned();
        entries.push(json!({
            "path": file.relative,
            "mode": if file.executable { "100755" } else { "100644" },
            "type": "blob",
            "sha": sha,
        }));
    }
    let tree = json!({"tree": entries});
    ensure_time(deadline)?;
    let tree = github(
        &client,
        &api,
        &token,
        Method::POST,
        &format!("/repos/{repo}/git/trees"),
        Some(&tree),
    )
    .await?;
    if !tree.status.is_success() {
        return Err(github_failed(
            tree.status,
            &tree.body,
            &format!("/repos/{repo}/git/trees"),
        ));
    }
    let tree_sha = tree.body["sha"]
        .as_str()
        .ok_or_else(|| invalid("the tree has no sha".to_owned()))?
        .to_owned();

    let message = format!("{}\n\n{}", output.title, output.body)
        .trim()
        .to_owned();
    let commit = json!({"message": message, "tree": tree_sha, "parents": [base_sha]});
    ensure_time(deadline)?;
    let commit = github(
        &client,
        &api,
        &token,
        Method::POST,
        &format!("/repos/{repo}/git/commits"),
        Some(&commit),
    )
    .await?;
    if !commit.status.is_success() {
        return Err(github_failed(
            commit.status,
            &commit.body,
            &format!("/repos/{repo}/git/commits"),
        ));
    }
    let head_sha = commit.body["sha"]
        .as_str()
        .ok_or_else(|| invalid("the commit has no sha".to_owned()))?
        .to_owned();

    // The branch: `<prefix>/<job id>`, deduped with a suffix if a retried job already made it.
    let mut branch = format!("{}/{}", output.branch_prefix, claim.id);
    ensure_time(deadline)?;
    let mut attempt = 1;
    loop {
        let body = json!({"ref": format!("refs/heads/{branch}"), "sha": head_sha});
        let created = github(
            &client,
            &api,
            &token,
            Method::POST,
            &format!("/repos/{repo}/git/refs"),
            Some(&body),
        )
        .await?;
        if created.status.is_success() {
            break;
        }
        if created.status == StatusCode::UNPROCESSABLE_ENTITY && attempt < BRANCH_ATTEMPTS {
            attempt += 1;
            branch = format!("{}/{}-{attempt}", output.branch_prefix, claim.id);
            continue;
        }
        return Err(github_failed(
            created.status,
            &created.body,
            &format!("/repos/{repo}/git/refs"),
        ));
    }

    let pr_body = json!({
        "title": output.title,
        "body": output.body,
        "head": branch,
        "base": output.base,
    });
    ensure_time(deadline)?;
    let pr = github(
        &client,
        &api,
        &token,
        Method::POST,
        &format!("/repos/{repo}/pulls"),
        Some(&pr_body),
    )
    .await?;
    if !pr.status.is_success() {
        return Err(github_failed(
            pr.status,
            &pr.body,
            &format!("/repos/{repo}/pulls"),
        ));
    }
    let number = pr.body["number"]
        .as_u64()
        .ok_or_else(|| invalid("the PR has no number".to_owned()))?;
    let url = pr.body["html_url"].as_str().unwrap_or_default().to_owned();

    Ok(json!({
        "status": "succeeded",
        "summary": format!("opened PR #{number} in {repo}"),
        "pr_number": number,
        "pr_url": url,
        "branch": branch,
        "head_sha": head_sha,
        "files": manifest.files,
        "bytes": manifest.bytes,
    }))
}

/// Remove the unpacked tree on every exit path, including an early error.
struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The allowlisted repositories (`CODING_OUTPUT_REPOS`, comma- or whitespace-separated).
fn output_allowlist() -> Result<Vec<String>, JobError> {
    allowlist_from(&std::env::var(OUTPUT_REPOS_ENV).unwrap_or_default())
}

fn allowlist_from(raw: &str) -> Result<Vec<String>, JobError> {
    let repos: Vec<String> = raw
        .split([',', ' '])
        .map(str::trim)
        .filter(|repo| !repo.is_empty())
        .map(str::to_owned)
        .collect();
    if repos.is_empty() {
        return Err(JobError {
            code: "output_not_configured".to_owned(),
            message: format!(
                "{OUTPUT_REPOS_ENV} is empty; a delivery needs an explicit repository allowlist"
            ),
            details: None,
            retryable: false,
        });
    }
    Ok(repos)
}

/// The uploaded archive for this job: mint a capability token exactly as the runner path does,
/// then read the blob through the store.
async fn fetch_upload(
    pool: &PgPool,
    claim: &JobClaim,
    source_id: &str,
) -> Result<Vec<u8>, JobError> {
    let minted = crate::store::mint_source_token(pool, &claim.tenant_id, &claim.id, source_id)
        .await
        .map_err(|error| JobError {
            code: "source_unavailable".to_owned(),
            message: format!("could not mint a token for source {source_id}: {error}"),
            details: None,
            retryable: true,
        })?;
    let Some((token, sha256)) = minted else {
        return Err(invalid(format!(
            "source {source_id} does not exist for this tenant (it may have been reaped)"
        )));
    };
    let blob = crate::store::source_blob(pool, source_id, &token)
        .await
        .map_err(|error| JobError {
            code: "source_unavailable".to_owned(),
            message: format!("could not read source {source_id}: {error}"),
            details: None,
            retryable: true,
        })?
        .ok_or_else(|| invalid(format!("source {source_id} is gone")))?;
    if source_archive::sha256_hex(&blob) != sha256 {
        return Err(JobError {
            code: "source_unavailable".to_owned(),
            message: format!("source {source_id} does not match the hash it was stored with"),
            details: None,
            retryable: true,
        });
    }
    Ok(blob)
}

/// One regular file of the unpacked tree.
struct TreeFile {
    /// Path relative to the unpacked root, `/`-separated.
    relative: String,
    /// Absolute path on the worker's disk.
    path: PathBuf,
    executable: bool,
}

fn collect_files(root: &Path) -> Result<Vec<TreeFile>, JobError> {
    let mut files = Vec::new();
    collect_into(root, root, &mut files)?;
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(files)
}

fn collect_into(root: &Path, dir: &Path, files: &mut Vec<TreeFile>) -> Result<(), JobError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|err| invalid(format!("cannot read {:?}: {err}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|err| invalid(format!("cannot read an entry: {err}")))?;
        let path = entry.path();
        let meta = entry
            .metadata()
            .map_err(|err| invalid(format!("cannot stat {:?}: {err}", path.display())))?;
        if meta.is_dir() {
            collect_into(root, &path, files)?;
        } else if meta.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|err| invalid(format!("path escaped the archive root: {err}")))?
                .to_string_lossy()
                .replace('\\', "/");
            files.push(TreeFile {
                relative,
                path,
                executable: is_executable(&meta),
            });
        }
        // Anything else (a symlink, a device) is not a file a tree can carry. `unpack` only
        // ever writes regular files and directories, so this is belt-and-braces.
    }
    Ok(())
}

#[cfg(unix)]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_meta: &std::fs::Metadata) -> bool {
    false
}

/// One GitHub API answer. Transport and parse failures are `Err`; every HTTP status is `Ok` so
/// the caller decides which statuses are meaningful (404 for the repo, 422 for a taken branch).
struct Github {
    status: StatusCode,
    body: Value,
}

async fn github(
    client: &reqwest::Client,
    api: &str,
    token: &str,
    method: Method,
    path: &str,
    body: Option<&Value>,
) -> Result<Github, JobError> {
    let mut request = client
        .request(method, format!("{api}{path}"))
        .header("accept", "application/vnd.github+json")
        .header("x-github-api-version", "2022-11-28")
        .header("user-agent", "coding-worker")
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await.map_err(|err| JobError {
        code: "github_api_error".to_owned(),
        message: format!("the GitHub request failed: {err}"),
        details: None,
        retryable: true,
    })?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    let body = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or_else(|_| Value::String(text.clone()))
    };
    Ok(Github { status, body })
}

/// Refuse to start another step once the job's own timeout has passed.
fn ensure_time(deadline: tokio::time::Instant) -> Result<(), JobError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(JobError {
            code: "timeout".to_owned(),
            message: "the delivery did not finish within the job's timeout".to_owned(),
            details: None,
            retryable: true,
        });
    }
    Ok(())
}

fn github_failed(status: StatusCode, body: &Value, path: &str) -> JobError {
    JobError {
        code: "github_api_error".to_owned(),
        message: format!(
            "GitHub answered {status} for {path}: {}",
            body.to_string().chars().take(300).collect::<String>()
        ),
        details: None,
        retryable: status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
    }
}

fn invalid(message: String) -> JobError {
    JobError {
        code: "invalid_job_spec".to_owned(),
        message,
        details: None,
        retryable: false,
    }
}

/// Standard base64 with padding, as the blobs API expects for `encoding: "base64"`.
fn base64_standard(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).map_or(0, |byte| u32::from(*byte));
        let third = chunk.get(2).map_or(0, |byte| u32::from(*byte));
        let packed = (first << 16) | (second << 8) | third;
        let index = |shift: u32| -> char {
            let value = (packed >> shift) & 0x3F;
            char::from(ALPHABET[value as usize])
        };
        out.push(index(18));
        out.push(index(12));
        out.push(if chunk.len() > 1 { index(6) } else { '=' });
        out.push(if chunk.len() > 2 { index(0) } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_standard(b""), "");
        assert_eq!(base64_standard(b"f"), "Zg==");
        assert_eq!(base64_standard(b"fo"), "Zm8=");
        assert_eq!(base64_standard(b"foo"), "Zm9v");
        assert_eq!(base64_standard(b"foob"), "Zm9vYg==");
        assert_eq!(base64_standard(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_standard(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn the_allowlist_is_explicit_and_splits_on_commas_or_spaces() {
        let repos = allowlist_from(" nonagenticai/fixture, other/repo ").expect("allowlist");
        assert_eq!(repos, vec!["nonagenticai/fixture", "other/repo"]);
        let error = allowlist_from("  ,  ").expect_err("empty allowlist");
        assert_eq!(error.code, "output_not_configured");
    }

    #[test]
    fn tree_files_are_sorted_and_keep_their_executable_bit() {
        let dir = std::env::temp_dir().join(format!("coding-deliver-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).expect("mkdir");
        std::fs::write(dir.join("src/b.rs"), "b").expect("write");
        std::fs::write(dir.join("a.rs"), "a").expect("write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::write(dir.join("run.sh"), "#!/bin/sh\n").expect("write");
            std::fs::set_permissions(dir.join("run.sh"), std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
        }
        let files = collect_files(&dir).expect("collect");
        let names: Vec<&str> = files.iter().map(|file| file.relative.as_str()).collect();
        assert_eq!(names, vec!["a.rs", "run.sh", "src/b.rs"]);
        #[cfg(unix)]
        assert!(
            files
                .iter()
                .any(|file| file.relative == "run.sh" && file.executable)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
